"""Tests for offset (base_margin) support in training and production."""

import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression

from mvp.model.config import ExperimentConfig, OffsetConfig
from mvp.model.offset import fit_offset, offset_margin, resolve_offset_col
from mvp.model.predictor import _offset_margin_kwargs


def _yaml(offset: str = "", model: str = "xgboost", extra: str = "") -> str:
    return f"""
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - player_elo_surface_indoor_diff
    - win_rate(days=30)
{extra}model:
  type: {model}
{offset}"""


class TestOffsetNullRows:
    """A null offset value has no log-odds; both entry points must say so."""

    def test_fit_offset_names_the_feature_and_the_fix(self):
        X = np.array([[1.0], [np.nan], [3.0], [4.0]])
        y = np.array([0, 1, 1, 0])
        with pytest.raises(ValueError, match=r"player_x.*1 of 4 rows.*not_null"):
            fit_offset(X, 0, y, OffsetConfig(feature="player_x"))

    def test_offset_margin_refuses_null_rows_at_scoring(self):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(50, 1))
        y = (X[:, 0] + rng.normal(size=50) > 0).astype(int)
        model = fit_offset(X, 0, y, OffsetConfig(feature="player_x"))
        with pytest.raises(ValueError, match="rows being scored"):
            offset_margin(model, np.array([[0.5], [np.nan]]), 0)


class TestOffsetConfigValidation:
    """ExperimentConfig.validate_offset."""

    def test_valid_offset_parses(self):
        config = ExperimentConfig.from_yaml(
            _yaml("offset:\n  feature: player_elo_surface_indoor_diff\n")
        )

        assert config.offset is not None
        assert config.offset.feature == "player_elo_surface_indoor_diff"
        assert config.offset.type == "logistic"

    def test_absent_offset_is_none(self):
        assert ExperimentConfig.from_yaml(_yaml()).offset is None

    def test_feature_not_in_include_rejected(self):
        with pytest.raises(ValueError, match="must be listed in features.include"):
            ExperimentConfig.from_yaml("""
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - win_rate(days=30)
model:
  type: xgboost
offset:
  feature: player_elo_surface_indoor_diff
""")

    def test_compute_only_feature_rejected(self):
        """compute_only columns are loaded for filter evaluation and never
        reach feature_cols, so the offset column would be absent from X."""
        with pytest.raises(ValueError, match="must not be in"):
            ExperimentConfig.from_yaml("""
data:
  date_range:
    start: "2020-01-01"
    end: "2024-12-31"
features:
  include:
    - player_elo_surface_indoor_diff
  compute_only:
    - player_elo_surface_indoor_diff
model:
  type: xgboost
offset:
  feature: player_elo_surface_indoor_diff
""")

    def test_non_xgboost_rejected(self):
        with pytest.raises(ValueError, match="requires model.type='xgboost'"):
            ExperimentConfig.from_yaml(
                _yaml(
                    "offset:\n  feature: player_elo_surface_indoor_diff\n",
                    model="logistic",
                )
            )

    def test_ensemble_rejected(self):
        """EnsembleModel.fit takes no base_margin and no **kwargs."""
        with pytest.raises(ValueError, match="requires model.type='xgboost'"):
            ExperimentConfig.from_yaml(
                _yaml(
                    "offset:\n  feature: player_elo_surface_indoor_diff\n",
                    model="ensemble",
                )
            )

    def test_early_stopping_rejected(self):
        with pytest.raises(ValueError, match="not supported with early_stopping"):
            ExperimentConfig.from_yaml(
                _yaml(
                    "offset:\n  feature: player_elo_surface_indoor_diff\n"
                    "early_stopping:\n  enabled: true\n"
                )
            )

    def test_mtl_rejected(self):
        with pytest.raises(ValueError, match="not supported with MTL"):
            ExperimentConfig.from_yaml(
                _yaml(
                    "offset:\n  feature: player_elo_surface_indoor_diff\n"
                    "mtl:\n  auxiliary_targets: [game_margin]\n"
                )
            )


class TestOffsetHelpers:
    """fit_offset / offset_margin / resolve_offset_col."""

    @staticmethod
    def _data(n: int = 300):
        rng = np.random.RandomState(0)
        composite = rng.randn(n) * 150.0
        other = rng.randn(n)
        X = np.column_stack([composite, other])
        y = (composite / 150.0 + rng.randn(n) * 0.6 > 0).astype(int)
        return X, y

    @staticmethod
    def _cfg():
        return OffsetConfig(feature="composite")

    def test_resolve_offset_col(self):
        assert resolve_offset_col(["a", "composite", "b"], "composite") == 1

    def test_resolve_offset_col_missing_raises(self):
        with pytest.raises(ValueError, match="not in feature_cols"):
            resolve_offset_col(["a", "b"], "composite")

    def test_margin_is_raw_log_odds(self):
        """Not probabilities -- base_margin lives in log-odds space."""
        X, y = self._data()
        model = fit_offset(X, 0, y, self._cfg())

        margin = offset_margin(model, X, 0)

        assert margin.shape == (len(y),)
        assert margin.min() < 0 < margin.max()
        np.testing.assert_allclose(
            1.0 / (1.0 + np.exp(-margin)), model.predict_proba(X[:, [0]])[:, 1]
        )

    def test_scaling_is_invariant_when_consistent(self):
        """A 2-param logistic absorbs an affine rescale of its single input, so
        fitting and applying on the SAME representation gives identical margins
        whether that representation is raw or standardized."""
        X, y = self._data()
        Xs = X.copy()
        mean, std = Xs[:, 0].mean(), Xs[:, 0].std()
        Xs[:, 0] = (Xs[:, 0] - mean) / std

        raw = offset_margin(fit_offset(X, 0, y, self._cfg()), X, 0)
        scaled = offset_margin(fit_offset(Xs, 0, y, self._cfg()), Xs, 0)

        np.testing.assert_allclose(raw, scaled, rtol=1e-3, atol=1e-3)

    def test_mixing_representations_destroys_the_margin(self):
        """The failure this module exists to prevent: fit on raw, apply to
        standardized. No exception -- the offset silently collapses toward zero
        while the model expects a strong starting point."""
        X, y = self._data()
        Xs = X.copy()
        Xs[:, 0] = (Xs[:, 0] - Xs[:, 0].mean()) / Xs[:, 0].std()

        model = fit_offset(X, 0, y, self._cfg())
        correct = offset_margin(model, X, 0)
        mixed = offset_margin(model, Xs, 0)

        assert np.abs(correct).mean() > 20 * np.abs(mixed).mean()


class TestOffsetMarginKwargs:
    """The helper both predictor inference paths share."""

    def test_no_offset_in_artifact_returns_empty(self):
        X = np.zeros((5, 2))

        assert _offset_margin_kwargs({}, ["a", "b"], X) == {}

    def test_legacy_artifact_without_key_returns_empty(self):
        """Artifacts predating offsets carry no key at all."""
        X = np.zeros((5, 2))

        assert _offset_margin_kwargs({"model": None}, ["a", "b"], X) == {}

    def test_offset_artifact_returns_margins(self):
        rng = np.random.RandomState(1)
        X = np.column_stack([rng.randn(50) * 100, rng.randn(50)])
        y = (X[:, 0] > 0).astype(int)
        model = LogisticRegression(max_iter=1000).fit(X[:, [0]], y)
        artifact = {"offset_model": model, "offset_feature": "composite"}

        kwargs = _offset_margin_kwargs(artifact, ["composite", "other"], X)

        assert set(kwargs) == {"base_margin"}
        np.testing.assert_allclose(
            kwargs["base_margin"], model.decision_function(X[:, [0]])
        )

    def test_offset_feature_missing_from_cols_raises(self):
        """Would otherwise score with no margin against a model fit with one."""
        rng = np.random.RandomState(1)
        X = rng.randn(20, 2)
        y = (X[:, 0] > 0).astype(int)
        artifact = {
            "offset_model": LogisticRegression(max_iter=1000).fit(X[:, [0]], y),
            "offset_feature": "composite",
        }

        with pytest.raises(ValueError, match="not in feature_cols"):
            _offset_margin_kwargs(artifact, ["a", "b"], X)
