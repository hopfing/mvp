"""Smoke test for the MTL end-to-end path: runner branching, model
instantiation, and Optuna search-space extension."""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from mvp.model.config import ExperimentConfig
from mvp.model.models import XGBoostMTLModel
from mvp.model.runner import ExperimentRunner


class TestMTLSmoke:
    """End-to-end smoke test: MTL config + small dataset + actual fit."""

    @pytest.fixture(autouse=True)
    def ensure_features_registered(self, isolated_registry):
        """Reload feature modules so the registry is populated."""
        import mvp.model.features.h2h
        import mvp.model.features.ranking
        import mvp.model.features.serve
        import mvp.model.features.win_rate

        importlib.reload(mvp.model.features.h2h)
        importlib.reload(mvp.model.features.ranking)
        importlib.reload(mvp.model.features.serve)
        importlib.reload(mvp.model.features.win_rate)

    @pytest.fixture
    def mtl_matches(self, tmp_path: Path) -> Path:
        """Sample matches.parquet with all columns needed for MTL aux derivation.

        Even-indexed rows: player wins in straight sets (sets_played=2,
        scoreline 6-4 6-4). Odd-indexed rows: player loses in 3 sets
        (4-6 6-4 4-6). Reason is null (completed match). Built as
        dict-of-lists matching the existing `test_runner.py::sample_matches`
        pattern with explicit dtypes so the feature engine sees the
        column types it expects."""
        n = 200
        won = [i % 2 == 0 for i in range(n)]
        sets_played = [2 if w else 3 for w in won]
        # Set 1: winner takes 6-4 of their side (player wins for even, opp for odd)
        p_s1 = [6 if w else 4 for w in won]
        o_s1 = [4 if w else 6 for w in won]
        # Set 2: player takes 6-4 either way (player won set 2 in both scenarios)
        p_s2 = [6 for _ in won]
        o_s2 = [4 for _ in won]
        # Set 3: only odd (3-set matches), opp wins 6-4
        p_s3 = [None if w else 4 for w in won]
        o_s3 = [None if w else 6 for w in won]

        df = pl.DataFrame(
            {
                "match_uid": [f"M{i}" for i in range(n)],
                "player_id": [f"P{i % 10}" for i in range(n)],
                "opp_id": [f"P{(i + 5) % 10}" for i in range(n)],
                "effective_match_date": [
                    f"2024-01-{(i % 28) + 1:02d}" for i in range(n)
                ],
                "won": won,
                "player_rankings_points": [1000 - i for i in range(n)],
                "opp_rankings_points": [500 + i for i in range(n)],
                "circuit": ["tour" for _ in range(n)],
                # Explicit String dtype for `reason` — all None would otherwise
                # be inferred as Null dtype and break downstream string ops.
                "reason": pl.Series(
                    [None] * n, dtype=pl.String,
                ),
                "sets_played": sets_played,
                "best_of": [3 for _ in range(n)],
                "player_set1_games": p_s1,
                "opp_set1_games": o_s1,
                "player_set2_games": p_s2,
                "opp_set2_games": o_s2,
                "player_set3_games": pl.Series(p_s3, dtype=pl.Int64),
                "opp_set3_games": pl.Series(o_s3, dtype=pl.Int64),
                "player_set4_games": pl.Series([None] * n, dtype=pl.Int64),
                "opp_set4_games": pl.Series([None] * n, dtype=pl.Int64),
                "player_set5_games": pl.Series([None] * n, dtype=pl.Int64),
                "opp_set5_games": pl.Series([None] * n, dtype=pl.Int64),
            }
        ).with_columns(pl.col("effective_match_date").str.to_datetime())
        path = tmp_path / "matches.parquet"
        df.write_parquet(path)
        return path

    @pytest.fixture
    def mtl_config(self, tmp_path: Path) -> Path:
        """Sample experiment config with an MTL block."""
        config_str = """
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: xgboost
  params:
    n_estimators: 20
    max_depth: 3
    learning_rate: 0.1
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 50
  test_size: 25
mtl:
  auxiliary_targets:
    - game_margin
    - set_margin
    - set_count
"""
        path = tmp_path / "config.yaml"
        path.write_text(config_str)
        return path

    def test_runner_completes_end_to_end_with_mtl(
        self,
        mtl_config: Path,
        mtl_matches: Path,
        tmp_path: Path,
    ):
        """Full pipeline runs to completion with MTL config and returns
        sensible metrics on the primary head."""
        import mlflow

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=mtl_config,
            matches_path=mtl_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )
        results = runner.run()

        # Pipeline completed and primary-head metrics are sensible.
        assert "metrics" in results
        assert "accuracy" in results["metrics"]
        assert "log_loss" in results["metrics"]
        assert 0.0 <= results["metrics"]["accuracy"] <= 1.0
        assert results["metrics"]["log_loss"] >= 0.0
        assert results["n_folds"] == 2

        # MTL: aux head R² captured in results (H38 sanity-check gate). Friendly
        # aux names appear (without "_aux_" prefix). On this tiny synthetic data
        # the absolute R² values aren't meaningful; just verify the structure.
        assert results.get("aux_r2_test") is not None
        assert set(results["aux_r2_test"].keys()) == {
            "game_margin", "set_margin", "set_count",
        }
        # Per-fold aux R² also captured
        assert results.get("aux_r2_per_fold") is not None
        assert len(results["aux_r2_per_fold"]) == results["n_folds"]

    def test_runner_routes_through_mtl_model_when_mtl_active(
        self,
        mtl_config: Path,
        mtl_matches: Path,
        tmp_path: Path,
        monkeypatch,
    ):
        """Runner instantiates XGBoostMTLModel (not XGBoostModel) when MTL
        is active. Verified by recording fit calls on the MTL model class —
        if the runner branched wrong, the recorder wouldn't fire AND the
        non-MTL XGBoostModel would receive 2D y (which it doesn't expect)."""
        import mlflow

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        recorded_y_shapes: list[tuple] = []
        real_fit = XGBoostMTLModel.fit

        def recording_fit(self, X, y, *args, **kwargs):
            recorded_y_shapes.append(y.shape)
            return real_fit(self, X, y, *args, **kwargs)

        monkeypatch.setattr(XGBoostMTLModel, "fit", recording_fit)

        runner = ExperimentRunner(
            config_path=mtl_config,
            matches_path=mtl_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )
        runner.run()

        # MTL model fit was called at least once per fold (2 folds).
        assert len(recorded_y_shapes) >= 2
        # Each call received 2D y with 4 columns (primary + 3 aux).
        for shape in recorded_y_shapes:
            assert len(shape) == 2, f"Expected 2D y for MTL fit, got {shape}"
            assert shape[1] == 4, (
                f"Expected 4 target columns (won + 3 aux), got {shape[1]}"
            )

    def test_config_validator_rejects_mtl_with_ensemble(self, tmp_path: Path):
        """ExperimentConfig.validate_mtl_compatibility rejects mtl+ensemble
        at config load time so the user gets a clear error before any
        compute runs."""
        config_str = """
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
model:
  type: ensemble
  params:
    base_models: []
mtl:
  auxiliary_targets:
    - game_margin
"""
        with pytest.raises(ValueError, match="model.type='xgboost'"):
            ExperimentConfig.from_yaml(config_str)

    def test_config_validator_rejects_mtl_with_logistic(self, tmp_path: Path):
        """Same validator catches non-XGBoost model types."""
        config_str = """
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: logistic
mtl:
  auxiliary_targets:
    - game_margin
"""
        with pytest.raises(ValueError, match="model.type='xgboost'"):
            ExperimentConfig.from_yaml(config_str)

    def test_tuner_extends_search_space_with_mtl_weights(
        self,
        mtl_config: Path,
        tmp_path: Path,
    ):
        """HyperparamTuner adds one log-uniform `weight_{aux}` dimension per
        configured auxiliary target. Primary weight stays out of the search
        space (relative-weight invariance)."""
        from mvp.model.tuning import HyperparamTuner

        tuner = HyperparamTuner(
            config_path=mtl_config,
            state_dir=tmp_path / "tuning",
        )

        # Each aux target adds one weight_* dimension
        assert "weight_game_margin" in tuner.search_space
        assert "weight_set_margin" in tuner.search_space
        assert "weight_set_count" in tuner.search_space

        # Configured as loguniform 0.01-5.0 (widened post-H38 when set_margin
        # tuned to 0.96 at the prior 1.0 ceiling)
        for aux in ("game_margin", "set_margin", "set_count"):
            spec = tuner.search_space[f"weight_{aux}"]
            assert spec["type"] == "float"
            assert spec["low"] == 0.01
            assert spec["high"] == 5.0
            assert spec.get("log") is True

        # Primary weight is NOT in the search space — relative weights only
        assert "weight_won" not in tuner.search_space

        # Existing XGB HPs are still present
        assert "max_depth" in tuner.search_space
        assert "learning_rate" in tuner.search_space

    def test_tuner_runs_a_real_mtl_trial(
        self,
        mtl_config: Path,
        mtl_matches: Path,
        tmp_path: Path,
        monkeypatch,
    ):
        """End-to-end Optuna trial on an MTL config: the sampled weight_*
        params land in model.params of the temp config the tuner builds,
        and XGBoostMTLModel.fit receives them via the runner's MTL branch.
        This is the gate that catches any param-passing breakage between
        the tuner's search-space extension and the model's weight extraction."""
        import mlflow

        from mvp.model.tuning import HyperparamTuner

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")
        monkeypatch.setenv(
            "MVP_MATCHES_PATH", str(mtl_matches),
        )

        recorded_weights: list[np.ndarray] = []
        real_init = XGBoostMTLModel.__init__

        def recording_init(self, params, target_names, feature_names=None):
            real_init(self, params, target_names, feature_names=feature_names)
            recorded_weights.append(self.loss_weights.copy())

        monkeypatch.setattr(XGBoostMTLModel, "__init__", recording_init)

        tuner = HyperparamTuner(
            config_path=mtl_config,
            matches_path=mtl_matches,
            state_dir=tmp_path / "tuning",
        )
        tuner.run(n_trials=1)

        # At least one model was instantiated (per fold of the trial).
        assert len(recorded_weights) >= 1
        # Each instantiation had loss_weights with 4 entries (primary + 3 aux),
        # primary fixed at 1.0, aux values within the search range 0.01-5.0.
        for w in recorded_weights:
            assert w.shape == (4,)
            assert w[0] == 1.0, "primary weight should default to 1.0"
            for aux_w in w[1:]:
                assert 0.01 <= aux_w <= 5.0, (
                    f"aux weight {aux_w} outside search range [0.01, 5.0]"
                )
