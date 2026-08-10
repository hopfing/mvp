"""Tests for ServeDiscoveryConfig schema and promotion helper."""

from datetime import date
from textwrap import dedent

import pytest

from mvp.projection.iid.config import (
    IIDProjectionConfig,
    ServeDiscoveryConfig,
    ServeDiscoveryFeaturesConfig,
)


class TestServeDiscoveryConfig:
    def test_minimal_from_yaml(self):
        yaml_str = dedent("""
            description: test
            data:
              date_range:
                start: 2022-01-01
                end: 2025-12-31
              filters:
                circuit: [tour]
            features:
              candidate_point_level_features:
                - is_break_point
                - is_server_set_point
        """)
        cfg = ServeDiscoveryConfig.from_yaml(yaml_str)
        assert cfg.description == "test"
        assert cfg.data.date_range.start == date(2022, 1, 1)
        assert "is_break_point" in cfg.features.candidate_point_level_features
        assert cfg.scoring_model.type == "logistic"  # default
        assert cfg.model_forms == ["logistic", "xgboost"]  # default
        assert cfg.metric == "log_loss"

    def test_full_from_yaml(self):
        yaml_str = dedent("""
            data:
              date_range:
                start: 2022-01-01
                end: 2025-12-31
              filters:
                circuit: [tour, chal]
            features:
              base_match_level_features:
                - player_pts_service_won_pct(days=90)
              base_point_level_features:
                - is_second_serve
              candidate_match_level_features:
                - player_serve_elo
                - opp_return_elo
              candidate_point_level_features:
                - is_server_set_point
                - is_returner_set_point
                - set_score_asymmetry
              max_features: 10
            scoring_model:
              type: logistic
              params:
                C: 0.5
            model_forms: [logistic, xgboost]
            model_params:
              xgboost:
                n_estimators: 300
                max_depth: 5
            metric: log_loss
            min_delta: 0.0005
        """)
        cfg = ServeDiscoveryConfig.from_yaml(yaml_str)
        assert cfg.features.max_features == 10
        assert cfg.scoring_model.type == "logistic"
        assert cfg.scoring_model.params["C"] == 0.5
        assert cfg.model_params["xgboost"]["n_estimators"] == 300
        assert cfg.min_delta == 0.0005

    def test_promoted_output_is_valid_iid_projection_config(self):
        cfg = ServeDiscoveryConfig.from_yaml(dedent("""
            data:
              date_range:
                start: 2022-01-01
                end: 2025-12-31
              filters: {}
            features:
              candidate_point_level_features: []
        """))
        emitted = cfg.to_iid_projection_config_dict(
            selected_match_level=["player_pts_service_won_pct(days=90)"],
            selected_point_level=["is_break_point", "is_server_set_point"],
            model_type="xgboost",
            model_params={"n_estimators": 100},
        )
        loaded = IIDProjectionConfig.model_validate(emitted)
        assert loaded.serve_model.type == "score_state"
        assert loaded.serve_model.model_type == "xgboost"
        assert loaded.serve_model.match_level_features == [
            "player_pts_service_won_pct(days=90)"
        ]
        assert loaded.serve_model.point_level_features == [
            "is_break_point", "is_server_set_point",
        ]
        assert loaded.serve_model.params["n_estimators"] == 100
        # features.include must contain both player_/opp_ versions for engine expansion
        assert "player_pts_service_won_pct(days=90)" in loaded.features.include
        assert "opp_pts_service_won_pct(days=90)" in loaded.features.include

    def test_promoted_config_carries_the_fs_objective(self):
        """The promoted config must state what its features were selected against,
        so `mvp tune` optimizes that by default instead of needing a flag."""
        cfg = ServeDiscoveryConfig.from_yaml(dedent("""
            data:
              date_range:
                start: 2022-01-01
                end: 2025-12-31
              filters: {}
            metric: iid_crps_total_games
            features:
              candidate_point_level_features: []
        """))
        emitted = cfg.to_iid_projection_config_dict(
            selected_match_level=["player_pts_service_won_pct(days=90)"],
            selected_point_level=["is_break_point"],
        )
        assert emitted["metrics"]["objective"] == ["iid_crps_total_games"]
        loaded = IIDProjectionConfig.model_validate(emitted)
        assert loaded.metrics.objective == ["iid_crps_total_games"]

    def test_promoted_objective_tracks_a_different_fs_metric(self):
        cfg = ServeDiscoveryConfig.from_yaml(dedent("""
            data:
              date_range:
                start: 2022-01-01
                end: 2025-12-31
              filters: {}
            metric: iid_total_cal
            features:
              candidate_point_level_features: []
        """))
        emitted = cfg.to_iid_projection_config_dict(
            selected_match_level=["player_pts_service_won_pct(days=90)"],
            selected_point_level=[],
        )
        assert emitted["metrics"]["objective"] == ["iid_total_cal"]

    def test_point_validation_does_not_leak_into_emitted_projection_config(self):
        """`point_validation` is FS-side (point-grain, millions of rows). Only
        the match-grain `validation` block is inherited into the emitted IID
        projection config."""
        cfg = ServeDiscoveryConfig.from_yaml(dedent("""
            data:
              date_range:
                start: 2022-01-01
                end: 2025-12-31
              filters: {}
            point_validation:
              type: walk_forward
              n_splits: 3
              min_train_size: 3000000
              test_size: 1000000
            validation:
              type: walk_forward
              n_splits: 3
              min_train_size: 20000
              test_size: 5000
            features:
              candidate_point_level_features: []
        """))
        emitted = cfg.to_iid_projection_config_dict(
            selected_match_level=["player_pts_service_won_pct(days=90)"],
            selected_point_level=[],
            model_type="logistic",
        )
        loaded = IIDProjectionConfig.model_validate(emitted)
        # Match-grain `validation` inherited as-is.
        assert loaded.validation.min_train_size == 20000
        assert loaded.validation.test_size == 5000
        # point_validation should never appear in the emitted dict.
        assert "point_validation" not in emitted


class TestChainIncompatiblePointFeatures:
    """Features the chain cannot represent must never reach a chain-metric pool.

    Two mechanisms, one list. `point_num` breaks the deuce closed form. `serve`
    and `is_second_serve` break something else: the chain has no serve tree and
    hardcodes serve_num=1 at every ScoreState it builds, so a model that
    conditions on serve number is only ever asked for the first-serve case and
    hands the chain ~0.69 where it needs the blended ~0.62.

    Measured rather than theorised: in FS round 1 both scored CRPS 3.910 against
    ~3.373 for every other candidate. They placed last, so nothing was harmed —
    but placing last is an outcome, not a guard, and the next pool or metric
    could rank them anywhere.
    """

    def test_serve_number_features_are_excluded(self):
        from mvp.projection.iid.serve_discovery import (
            _CHAIN_INCOMPATIBLE_POINT_FEATURES,
        )
        assert "serve" in _CHAIN_INCOMPATIBLE_POINT_FEATURES
        assert "is_second_serve" in _CHAIN_INCOMPATIBLE_POINT_FEATURES

    def test_point_num_still_excluded(self):
        from mvp.projection.iid.serve_discovery import (
            _CHAIN_INCOMPATIBLE_POINT_FEATURES,
        )
        assert "point_num" in _CHAIN_INCOMPATIBLE_POINT_FEATURES

    def test_excluded_names_exist_in_the_real_pool(self):
        # A typo here would silently exclude nothing, which is the failure mode
        # this list exists to prevent — so pin the names against the actual pool
        # rather than trusting the strings.
        from mvp.projection.iid.score_state_features import (
            default_point_level_candidate_pool,
        )
        from mvp.projection.iid.serve_discovery import (
            _CHAIN_INCOMPATIBLE_POINT_FEATURES,
        )
        pool = set(default_point_level_candidate_pool())
        missing = _CHAIN_INCOMPATIBLE_POINT_FEATURES - pool
        assert not missing, f"excluded names absent from the pool: {missing}"

    def test_state_flags_that_ARE_representable_stay_in(self):
        # The chain does carry game/set/match score state, so those must not be
        # swept up by a broader exclusion.
        from mvp.projection.iid.serve_discovery import (
            _CHAIN_INCOMPATIBLE_POINT_FEATURES,
        )
        for keep in ("is_break_point", "is_tiebreak", "sets_won_asymmetry",
                     "game_points_diff", "tiebreak_point_diff"):
            assert keep not in _CHAIN_INCOMPATIBLE_POINT_FEATURES
