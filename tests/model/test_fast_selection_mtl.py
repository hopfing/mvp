"""Tests for FastForwardSelector MTL extensions in fast_selection.py:
- Aux target derivation
- Stricter completeness gate (W/O + RET + DEF + UNP)
- DiscoveryConfig schema accepting `mtl:` block + validator
"""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector


def _synthetic_matches(tmp_path: Path) -> Path:
    """Match fixture with all columns the MTL path needs.

    Mix of completed and incomplete-reason rows:
      M0, M1: completed (None reason), straight-set / 3-set wins
      M2: completed loss
      M3: W/O — excluded under both single-task and MTL
      M4: RET — kept under single-task, excluded under MTL
      M5: DEF — kept under single-task, excluded under MTL
      M6: sets_played null with reason None — excluded under MTL
    """
    n = 7
    won = [True, True, False, True, True, False, True]
    reason = [None, None, None, "W/O", "RET", "DEF", None]
    sets_played = [2, 3, 2, None, 1, None, None]
    # Set scores: M0 6-4 6-4, M1 4-6 6-4 7-5, M2 6-7 4-6, M3-5 partials, M6 fully null
    p_s1 = [6, 4, 6, None, 6, None, None]
    o_s1 = [4, 6, 7, None, 4, None, None]
    p_s2 = [6, 6, 4, None, None, None, None]
    o_s2 = [4, 4, 6, None, None, None, None]
    p_s3 = [None, 7, None, None, None, None, None]
    o_s3 = [None, 5, None, None, None, None, None]

    df = pl.DataFrame(
        {
            "match_uid": [f"M{i}" for i in range(n)],
            "player_id": [f"P{i}" for i in range(n)],
            "opp_id": [f"P{(i + 1) % n}" for i in range(n)],
            "effective_match_date": [f"2024-01-{i + 1:02d}" for i in range(n)],
            "won": won,
            "player_rankings_points": [1000 - i * 50 for i in range(n)],
            "opp_rankings_points": [500 + i * 50 for i in range(n)],
            "circuit": ["tour"] * n,
            "reason": pl.Series(reason, dtype=pl.String),
            "sets_played": pl.Series(sets_played, dtype=pl.Int64),
            "best_of": [3] * n,
            "player_set1_games": pl.Series(p_s1, dtype=pl.Int64),
            "opp_set1_games": pl.Series(o_s1, dtype=pl.Int64),
            "player_set2_games": pl.Series(p_s2, dtype=pl.Int64),
            "opp_set2_games": pl.Series(o_s2, dtype=pl.Int64),
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


def _make_config(
    aux: list[str] | None = None,
    with_mtl: bool = True,
) -> DiscoveryConfig:
    """Build a minimal DiscoveryConfig with or without an MTL block."""
    cfg_dict: dict = {
        "data": {
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
        },
        "discovery": {
            "features": {"include": ["player_ranking_points_diff"]},
        },
        "model": {"type": "xgboost", "params": {"n_estimators": 5}},
        "validation": {
            "type": "walk_forward",
            "n_splits": 2,
            "min_train_size": 2,
            "test_size": 1,
        },
    }
    if with_mtl:
        aux_targets = aux if aux is not None else ["game_margin", "set_margin", "set_count"]
        cfg_dict["mtl"] = {"auxiliary_targets": aux_targets}
    return DiscoveryConfig.model_validate(cfg_dict)


class TestFastForwardSelectorMTLPrecompute:
    """Step 1 — precompute() MTL parity."""

    @pytest.fixture(autouse=True)
    def ensure_features_registered(self, isolated_registry):
        import mvp.model.features.ranking
        importlib.reload(mvp.model.features.ranking)

    def test_no_mtl_y_aux_is_none(self, tmp_path: Path):
        """Without an mtl block, y_aux and aux_target_names stay None."""
        matches = _synthetic_matches(tmp_path)
        cfg = _make_config(with_mtl=False)
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        assert selector.y_aux is None
        assert selector.aux_target_names is None

    def test_mtl_derives_aux_y_2d(self, tmp_path: Path):
        """With MTL active, y_aux is a 2D array shaped [n_rows, num_aux],
        column order mirrors auxiliary_targets, names captured."""
        matches = _synthetic_matches(tmp_path)
        cfg = _make_config(aux=["game_margin", "set_margin", "set_count"])
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        assert selector.y_aux is not None
        assert selector.y_aux.ndim == 2
        assert selector.y_aux.shape[1] == 3
        assert selector.aux_target_names == [
            "game_margin", "set_margin", "set_count",
        ]
        # y_aux rows must equal y rows (same surviving row set)
        assert selector.y_aux.shape[0] == selector.y.shape[0]

    def test_mtl_aux_values_correct(self, tmp_path: Path):
        """Aux derivations produce the expected per-row values on surviving rows."""
        matches = _synthetic_matches(tmp_path)
        cfg = _make_config(aux=["game_margin", "set_margin", "set_count"])
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        # M0, M1, M2 survive (M3 W/O, M4 RET, M5 DEF, M6 null sets_played all excluded)
        # Expected game_margin: M0 (6+6)-(4+4)=+4, M1 (4+6+7)-(6+4+5)=+2, M2 (6+4)-(7+6)=-3
        gm = selector.y_aux[:, 0]
        assert gm.tolist() == [4.0, 2.0, -3.0]
        # set_margin: M0 +2, M1 +1, M2 -2
        sm = selector.y_aux[:, 1]
        assert sm.tolist() == [2.0, 1.0, -2.0]
        # set_count: M0 2, M1 3, M2 2
        sc = selector.y_aux[:, 2]
        assert sc.tolist() == [2.0, 3.0, 2.0]

    def test_mtl_completeness_gate_excludes_invalid_reasons(self, tmp_path: Path):
        """Under MTL, rows with reason in {W/O, RET, DEF, UNP} are dropped
        (vs. single-task only excluding W/O)."""
        matches = _synthetic_matches(tmp_path)
        cfg = _make_config()
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        # Only M0, M1, M2 survive — 3 rows.
        assert selector.y.shape[0] == 3

    def test_mtl_completeness_gate_excludes_null_sets_played(self, tmp_path: Path):
        """Under MTL, rows with sets_played null are excluded even when
        reason is None (M6 in the fixture)."""
        matches = _synthetic_matches(tmp_path)
        cfg = _make_config()
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        # The 3 surviving rows are M0, M1, M2. M6 (reason=None, sets_played=None)
        # would survive the reason filter but must be killed by the sets_played
        # gate. Verify by counting (already covered) and checking the y values
        # don't include the M6 outcome.
        # M0 won=True, M1 won=True, M2 won=False  → y = [1, 1, 0]
        assert selector.y.tolist() == [1, 1, 0]

    def test_mtl_subset_aux_targets(self, tmp_path: Path):
        """Subset of aux targets in config → y_aux has matching columns only."""
        matches = _synthetic_matches(tmp_path)
        cfg = _make_config(aux=["set_margin"])
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        assert selector.aux_target_names == ["set_margin"]
        assert selector.y_aux.shape[1] == 1
        assert selector.y_aux[:, 0].tolist() == [2.0, 1.0, -2.0]


class TestFastForwardSelectorMTLScorer:
    """Step 2 — scorer model-instantiation MTL branch."""

    @pytest.fixture(autouse=True)
    def ensure_features_registered(self, isolated_registry):
        import mvp.model.features.ranking
        importlib.reload(mvp.model.features.ranking)

    def test_scorer_routes_through_mtl_model(self, tmp_path: Path, monkeypatch):
        """When MTL is active, the scorer instantiates XGBoostMTLModel
        (not via get_model) and passes 2D y of shape [n_train, num_target].

        Verified by monkeypatching XGBoostMTLModel.fit to record fit calls.
        If the runner branched wrong, recorder wouldn't fire AND the non-MTL
        get_model path would receive 2D y (which standard XGBoostModel
        doesn't expect)."""
        from mvp.model.discovery.fast_selection import XGBoostMTLModel as _XGBoostMTLModel

        matches = _synthetic_matches(tmp_path)
        cfg = _make_config(aux=["game_margin", "set_margin", "set_count"])
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        recorded_y_shapes: list[tuple] = []
        real_fit = _XGBoostMTLModel.fit

        def recording_fit(self, X, y, *args, **kwargs):
            recorded_y_shapes.append(y.shape)
            return real_fit(self, X, y, *args, **kwargs)

        monkeypatch.setattr(_XGBoostMTLModel, "fit", recording_fit)

        # brier_score (not log_loss) because the tiny synthetic fixture
        # ends up with single-class folds — log_loss raises on that, but
        # brier_score handles it. The test is about routing through MTL,
        # not which metric is computed.
        scorer = selector.create_scorer("brier_score")
        score = scorer(["player_ranking_points_diff"])

        # XGBoostMTLModel.fit invoked at least once per fold; selector has
        # 2 folds per the test config.
        assert len(recorded_y_shapes) >= 1
        # y shape must be 2D with 4 target columns (primary + 3 aux).
        for shape in recorded_y_shapes:
            assert len(shape) == 2, f"Expected 2D y for MTL fit; got {shape}"
            assert shape[1] == 4, (
                f"Expected 4 target columns (won + 3 aux); got {shape[1]}"
            )

        # Step 2 still scores on primary-head log_loss only (Step 3 swaps
        # in multi-task loss). Verify the scorer returned a finite number.
        assert np.isfinite(score)

    def test_scorer_passes_correct_target_names(self, tmp_path: Path, monkeypatch):
        """target_names passed to XGBoostMTLModel match
        [config.target, *config.mtl.auxiliary_targets] — friendly names
        (not the internal `_aux_*` derived-column names), so the model's
        weight_{name} extraction lines up with `model.params.weight_*`
        keys from the config."""
        from mvp.model.discovery.fast_selection import XGBoostMTLModel as _XGBoostMTLModel

        matches = _synthetic_matches(tmp_path)
        cfg = _make_config(aux=["game_margin", "set_margin"])
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        recorded_target_names: list[list[str]] = []
        real_init = _XGBoostMTLModel.__init__

        def recording_init(self, params, target_names, feature_names=None):
            recorded_target_names.append(list(target_names))
            real_init(self, params, target_names, feature_names=feature_names)

        monkeypatch.setattr(_XGBoostMTLModel, "__init__", recording_init)

        # brier_score (not log_loss) because the tiny synthetic fixture
        # ends up with single-class folds — log_loss raises on that, but
        # brier_score handles it. The test is about routing through MTL,
        # not which metric is computed.
        scorer = selector.create_scorer("brier_score")
        _ = scorer(["player_ranking_points_diff"])

        assert len(recorded_target_names) >= 1
        for names in recorded_target_names:
            assert names == ["won", "game_margin", "set_margin"]


class TestMultiTaskLossScoring:
    """Step 3 + 4 — multi-task loss as the FS scoring metric:
    `log_loss(primary) + sum_i w_i * MSE_std(aux_i)`. The standardization
    is the part the mle review (2026-06-01) flagged — aux MSE must be on
    standardized scale or large-range aux (game_margin) would dominate."""

    def _trained_mtl_model(self):
        """Train a tiny XGBoostMTLModel and return (model, X, y_primary, y_aux)
        for direct testing of the multi-task loss function."""
        from mvp.model.models import XGBoostMTLModel

        rng = np.random.default_rng(0)
        n, d = 80, 4
        X = rng.normal(size=(n, d))
        score = X[:, 0] + 0.3 * rng.normal(size=n)
        y_primary = (score > 0).astype(np.float64)
        # Game margin scaled like real tennis (±10s range)
        y_gm = (8.0 * score + rng.normal(size=n)).astype(np.float64)
        y_sm = np.clip(np.round(score), -3, 3).astype(np.float64)
        y = np.stack([y_primary, y_gm, y_sm], axis=1)

        model = XGBoostMTLModel(
            params={
                "n_estimators": 20,
                "max_depth": 3,
                "learning_rate": 0.1,
                "weight_game_margin": 0.5,
                "weight_set_margin": 0.3,
            },
            target_names=["won", "game_margin", "set_margin"],
        )
        model.fit(X, y)
        return model, X, y_primary, y[:, 1:]

    def test_multi_task_loss_matches_hand_computation(self):
        """`_compute_mtl_loss` matches a hand-rolled formula computation."""
        from mvp.model.discovery.fast_selection import _compute_mtl_loss
        from mvp.model.models import _sigmoid
        import xgboost as xgb

        model, X, y_primary, y_aux = self._trained_mtl_model()
        actual = _compute_mtl_loss(model, X, y_primary, y_aux)

        # Hand computation
        raw = model._booster.predict(xgb.DMatrix(X))
        p = _sigmoid(raw[:, 0])
        p_clip = np.clip(p, 1e-15, 1 - 1e-15)
        primary_ll = -float(np.mean(
            y_primary * np.log(p_clip) + (1 - y_primary) * np.log(1 - p_clip)
        ))
        y_aux_std = (y_aux - model._aux_mean) / model._aux_std
        p_aux_std = raw[:, 1:]
        aux_loss = 0.0
        for i in range(p_aux_std.shape[1]):
            mse = float(np.mean((p_aux_std[:, i] - y_aux_std[:, i]) ** 2))
            aux_loss += float(model.loss_weights[i + 1]) * mse
        expected = primary_ll + aux_loss

        assert abs(actual - expected) < 1e-10

    def test_scorer_uses_compute_mtl_loss_when_mtl(self, tmp_path, monkeypatch):
        """Scorer's per-fold score under MTL comes from `_compute_mtl_loss`,
        not from the single-target `metric_fn` path. Verified by replacing
        the function with a sentinel and confirming the scorer's returned
        fold-mean equals the sentinel."""
        from mvp.model.discovery import fast_selection

        matches = _synthetic_matches(tmp_path)
        cfg = _make_config(aux=["game_margin", "set_margin", "set_count"])
        selector = FastForwardSelector(
            config=cfg,
            all_feature_specs=["player_ranking_points_diff"],
            matches_path=matches,
            cache_dir=tmp_path / "cache",
        )
        selector.precompute()

        sentinel = 12.345
        monkeypatch.setattr(
            fast_selection, "_compute_mtl_loss",
            lambda *a, **k: sentinel,
        )

        scorer = selector.create_scorer("brier_score")
        score = scorer(["player_ranking_points_diff"])

        # Fold-mean of sentinel across all folds is the sentinel itself.
        assert score == pytest.approx(sentinel)

    def test_aux_mse_component_is_standardized_scale(self):
        """The aux MSE contribution must be O(1) (unit-variance scale), NOT
        O(original-variance). For `game_margin` with original std ~7, MSE
        on original scale would be ~50; on standardized scale it should be
        roughly 1. This is the mle-review gate against the scale bug."""
        from mvp.model.discovery.fast_selection import _compute_mtl_loss
        import xgboost as xgb

        model, X, y_primary, y_aux = self._trained_mtl_model()

        # Decompose: compute aux contribution alone by passing y_primary
        # values that yield zero log_loss (predict_proba is close to truth
        # because the model trained on this data; the residual is dominated
        # by aux). Cleaner: just compute aux MSE per target directly using
        # the same standardization the function uses.
        raw = model._booster.predict(xgb.DMatrix(X))
        y_aux_std = (y_aux - model._aux_mean) / model._aux_std
        p_aux_std = raw[:, 1:]

        # Standardized-scale MSE per target should be O(1).
        for i in range(p_aux_std.shape[1]):
            mse_std = float(np.mean((p_aux_std[:, i] - y_aux_std[:, i]) ** 2))
            # On standardized scale, a well-fit model has MSE < 1 (unit-var).
            # A pathologically bad fit could push above 1, but it should
            # definitely not be O(10s) like original-scale game_margin would.
            assert mse_std < 5.0, (
                f"Aux target {i} MSE on standardized scale is {mse_std:.2f} — "
                f"too large; suggests the standardization isn't applied. On "
                f"original game_margin scale (std~7) the MSE would be O(50)."
            )


class TestDiscoveryConfigMTLValidator:
    """Step 1 — DiscoveryConfig validator rejects MTL with non-xgboost."""

    def test_rejects_mtl_with_logistic(self):
        cfg_dict = {
            "data": {
                "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            },
            "model": {"type": "logistic"},
            "mtl": {"auxiliary_targets": ["game_margin"]},
        }
        with pytest.raises(ValueError, match="model.type='xgboost'"):
            DiscoveryConfig.model_validate(cfg_dict)

    def test_to_experiment_config_includes_mtl_block(self):
        """to_experiment_config_dict carries the mtl block through so the
        downstream ExperimentConfig the runner consumes also has MTL active."""
        cfg = _make_config(aux=["game_margin", "set_margin"])
        exp_dict = cfg.to_experiment_config_dict(["player_ranking_points_diff"])
        assert "mtl" in exp_dict
        assert exp_dict["mtl"]["auxiliary_targets"] == ["game_margin", "set_margin"]
