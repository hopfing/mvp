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
