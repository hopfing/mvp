"""Tests for the IID forward-selection discovery module.

The orchestrator (`IIDProjectionDiscovery`) and `precompute()` are exercised
end-to-end by the CLI run; these tests cover:

1. `IIDDiscoveryConfig` YAML parsing.
2. The pure helpers (`_spec_to_column`, `_swap_perspective`).
3. `FastIIDDiscoverySelector.create_scorer()` on a hand-built precomputed
   state (no parquet I/O), verifying it returns a finite MAE for a candidate
   feature subset and `+inf` for invalid input.
"""

import textwrap

import numpy as np
import pytest

from mvp.projection.iid.config import IIDDiscoveryConfig
from mvp.projection.iid.discovery import (
    FastIIDDiscoverySelector,
    _spec_to_column,
    _swap_perspective,
)


class TestIIDDiscoveryConfig:
    def test_parse_minimal_yaml(self):
        yaml_str = textwrap.dedent(
            """
            description: "Test discovery"
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
              filters:
                draw_type: singles
                circuit: [tour, chal]
            serve_model:
              type: matchup
              regressor:
                type: ridge
                params:
                  alpha: 1.0
            features:
              window_sizes: [60, 90]
              max_features: 5
            metric: mae
            validation:
              type: expanding_window
              initial_train_size: 1000
              step_size: 1000
            """
        )
        cfg = IIDDiscoveryConfig.from_yaml(yaml_str)
        assert cfg.description == "Test discovery"
        assert cfg.serve_model.type == "matchup"
        assert cfg.serve_model.regressor.type == "ridge"
        assert cfg.features.window_sizes == [60, 90]
        assert cfg.features.max_features == 5
        assert cfg.metric == "mae"
        assert cfg.selection_method == "forward"

    def test_defaults(self):
        yaml_str = textwrap.dedent(
            """
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
            """
        )
        cfg = IIDDiscoveryConfig.from_yaml(yaml_str)
        assert cfg.metric == "mae"
        assert cfg.features.window_sizes == [60, 90]
        assert cfg.features.max_features is None
        assert cfg.features.base == []

    def test_invalid_metric(self):
        yaml_str = textwrap.dedent(
            """
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
            metric: nonsense
            """
        )
        with pytest.raises(Exception):  # pydantic ValidationError
            IIDDiscoveryConfig.from_yaml(yaml_str)

    def test_to_iid_config_dict_includes_both_perspectives(self):
        """Selected specs must produce features.include entries for BOTH
        player_* and opp_* perspectives (MatchupServeModel's swap needs both)
        and serve_model.feature_columns entries with resolved column names."""
        yaml_str = textwrap.dedent(
            """
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
            serve_model:
              type: matchup
              regressor:
                type: ridge
                params:
                  alpha: 1.0
            """
        )
        cfg = IIDDiscoveryConfig.from_yaml(yaml_str)
        out = cfg.to_iid_config_dict([
            "player_pts_service_won_pct(days=90)",
            "opp_serve_elo",
        ])

        # features.include has both perspectives of each selected spec
        include = out["features"]["include"]
        assert "player_pts_service_won_pct(days=90)" in include
        assert "opp_pts_service_won_pct(days=90)" in include
        assert "opp_serve_elo" in include
        assert "player_serve_elo" in include

        # serve_model.feature_columns has the resolved column names (row-player perspective)
        fcols = out["serve_model"]["feature_columns"]
        assert fcols == ["player_pts_service_won_pct_90d", "opp_serve_elo"]

        # Carries through serve_model type + regressor config
        assert out["serve_model"]["type"] == "matchup"
        assert out["serve_model"]["regressor"]["type"] == "ridge"
        assert out["serve_model"]["regressor"]["params"]["alpha"] == 1.0


class TestSpecHelpers:
    def test_spec_to_column_with_window(self):
        assert _spec_to_column("player_pts_service_won_pct(days=90)") == "player_pts_service_won_pct_90d"

    def test_spec_to_column_no_params(self):
        assert _spec_to_column("player_serve_elo") == "player_serve_elo"

    def test_swap_player_to_opp(self):
        assert _swap_perspective("player_serve_elo") == "opp_serve_elo"

    def test_swap_opp_to_player(self):
        assert _swap_perspective("opp_serve_elo") == "player_serve_elo"

    def test_swap_unprefixed_passthrough(self):
        assert _swap_perspective("best_of") == "best_of"


class TestFastIIDDiscoveryScorer:
    """Exercise the scorer with a hand-built precomputed state."""

    @staticmethod
    def _build_synthetic_state(seed: int = 0):
        """Construct a FastIIDDiscoverySelector with X_wide / targets / folds
        populated by hand. Skips precompute() entirely (no parquet I/O).

        Ground truth is generated FROM the chain itself: synthetic serve
        rates → chain → expected_games → integer rounding for y_games_*.
        This ensures the scorer's chain step can in principle recover the
        target if the model learns the serve rates well."""
        from mvp.projection.iid.chain import (
            match_distribution,
            p_service_game_win,
            p_tiebreak_game_win,
        )

        rng = np.random.default_rng(seed)
        n_matches = 200

        # Two synthetic features per perspective (4 columns total).
        # player_X is the SIGNAL feature: it determines the player's serve rate.
        # player_Y is NOISE: uncorrelated with serve rate.
        player_x = rng.normal(0.0, 1.0, n_matches)
        opp_x = rng.normal(0.0, 1.0, n_matches)
        player_y = rng.normal(0.0, 1.0, n_matches)
        opp_y = rng.normal(0.0, 1.0, n_matches)

        # Ground-truth per-match serve rates: each player's rate is a function
        # of THEIR OWN player_x value (in the row-player perspective), with
        # tiny noise. Range comfortably inside [0.30, 0.90].
        actual_a = np.clip(
            0.62 + 0.05 * player_x + rng.normal(0.0, 0.005, n_matches),
            0.40, 0.85,
        )
        actual_b = np.clip(
            0.62 + 0.05 * opp_x + rng.normal(0.0, 0.005, n_matches),
            0.40, 0.85,
        )

        # X_wide column order: ["opp_X_60d", "opp_Y_60d", "player_X_60d", "player_Y_60d"]
        # (sorted, so col_to_idx is alphabetical — matches what precompute() produces)
        col_names = ["opp_X_60d", "opp_Y_60d", "player_X_60d", "player_Y_60d"]
        X_wide = np.column_stack([opp_x, opp_y, player_x, player_y])
        col_to_idx = {c: i for i, c in enumerate(col_names)}

        # Generate y_games_* from the CHAIN itself, so the scorer can in
        # principle recover them by predicting the serve rates correctly.
        h_a = p_service_game_win(actual_a)
        h_b = p_service_game_win(actual_b)
        t_ab = p_tiebreak_game_win(actual_a, actual_b)
        best_of = np.full(n_matches, 3, dtype=np.int64)
        truth_dist = match_distribution(h_a, h_b, t_ab, best_of)

        # Add small per-match noise so MAE is not exactly zero even for the
        # perfect-feature case.
        y_games_a = truth_dist.expected_games_a + rng.normal(0.0, 0.3, n_matches)
        y_games_b = (
            (truth_dist.expected_total_games - truth_dist.expected_games_a)
            + rng.normal(0.0, 0.3, n_matches)
        )
        y_won = (actual_a > actual_b).astype(np.int64)

        # 2 folds: simple expanding-window split
        folds = [
            (np.arange(0, 100), np.arange(100, 150)),
            (np.arange(0, 150), np.arange(150, 200)),
        ]

        # Build a minimal config
        import textwrap

        cfg = IIDDiscoveryConfig.from_yaml(
            textwrap.dedent(
                """
                data:
                  date_range:
                    start: "2024-01-01"
                    end: "2025-12-31"
                serve_model:
                  type: matchup
                  regressor:
                    type: ridge
                    params:
                      alpha: 1.0
                """
            )
        )

        selector = FastIIDDiscoverySelector(
            config=cfg,
            all_feature_specs=["player_X(days=60)", "player_Y(days=60)"],
        )
        # Hand-populate the precomputed state
        selector.X_wide = X_wide
        selector.col_to_idx = col_to_idx
        selector.y_games_a = y_games_a
        selector.y_games_b = y_games_b
        selector.y_won = y_won
        selector.best_of = best_of
        selector.actual_serve_rate_a = actual_a
        selector.actual_serve_rate_b = actual_b
        selector.folds = folds
        return selector

    def test_scorer_returns_finite_mae(self):
        selector = self._build_synthetic_state()
        scorer = selector.create_scorer()
        mae = scorer(["player_X(days=60)"])
        assert np.isfinite(mae)
        assert mae > 0

    def test_scorer_two_features(self):
        selector = self._build_synthetic_state()
        scorer = selector.create_scorer()
        mae = scorer(["player_X(days=60)", "player_Y(days=60)"])
        assert np.isfinite(mae)
        assert mae > 0

    def test_empty_features_returns_inf(self):
        selector = self._build_synthetic_state()
        scorer = selector.create_scorer()
        assert scorer([]) == float("inf")

    def test_unknown_feature_returns_inf(self):
        selector = self._build_synthetic_state()
        scorer = selector.create_scorer()
        # Spec resolves to player_NOTREAL_60d which isn't in col_to_idx
        assert scorer(["player_NOTREAL(days=60)"]) == float("inf")

    def test_create_scorer_before_precompute_raises(self):
        import textwrap

        cfg = IIDDiscoveryConfig.from_yaml(
            textwrap.dedent(
                """
                data:
                  date_range:
                    start: "2024-01-01"
                    end: "2025-12-31"
                """
            )
        )
        selector = FastIIDDiscoverySelector(
            config=cfg,
            all_feature_specs=["player_X(days=60)"],
        )
        with pytest.raises(RuntimeError, match="precompute"):
            selector.create_scorer()

    def test_better_features_score_lower(self):
        """Sanity check: with synthetic ground-truth signal in player_X, the
        scorer should produce a lower (better) MAE for [player_X] than for
        [player_Y] (which is uncorrelated noise)."""
        selector = self._build_synthetic_state(seed=42)
        scorer = selector.create_scorer()
        mae_signal = scorer(["player_X(days=60)"])
        mae_noise = scorer(["player_Y(days=60)"])
        assert np.isfinite(mae_signal)
        assert np.isfinite(mae_noise)
        # Signal feature should beat noise feature
        assert mae_signal < mae_noise
