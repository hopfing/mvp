"""Tests for IIDProjectionRunner — config loading and helper methods.

Full end-to-end runner integration is exercised by running the CLI against a
real parquet (`poetry run py -m mvp iid-project iid_projection_identity`); the
tests here cover the runner's deterministic helpers (target resolution and
match-row collapse) plus IIDProjectionConfig parsing.
"""

import textwrap

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.config import IIDProjectionConfig
from mvp.projection.iid.runner import IIDProjectionRunner


class TestIIDProjectionConfig:
    def test_parse_minimal_yaml(self):
        yaml_str = textwrap.dedent(
            """
            description: "Test config"
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
              filters:
                draw_type: singles
                circuit: [tour, chal]
            features:
              include:
                - pts_service_won_pct(days=90)
            serve_model:
              type: identity
              window: 90
            validation:
              type: expanding_window
              initial_train_size: 1000
              step_size: 1000
            metrics:
              total_lines: [21.5, 22.5]
              spread_lines: [-2.5, 2.5]
            """
        )
        cfg = IIDProjectionConfig.from_yaml(yaml_str)
        assert cfg.description == "Test config"
        assert cfg.serve_model.type == "identity"
        assert cfg.serve_model.window == 90
        assert cfg.metrics.total_lines == [21.5, 22.5]
        assert cfg.metrics.spread_lines == [-2.5, 2.5]
        assert cfg.metrics.include_classification is True
        assert cfg.metrics.include_regression is True

    def test_default_serve_model(self):
        yaml_str = textwrap.dedent(
            """
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
            features:
              include:
                - pts_service_won_pct(days=90)
            """
        )
        cfg = IIDProjectionConfig.from_yaml(yaml_str)
        assert cfg.serve_model.type == "identity"
        assert cfg.serve_model.window == 90
        assert cfg.serve_model.clip_min == 0.30
        assert cfg.serve_model.clip_max == 0.90

    def test_invalid_serve_model_type(self):
        yaml_str = textwrap.dedent(
            """
            data:
              date_range:
                start: "2024-01-01"
                end: "2025-12-31"
            features:
              include:
                - pts_service_won_pct(days=90)
            serve_model:
              type: nonsense
            """
        )
        with pytest.raises(Exception):  # pydantic ValidationError
            IIDProjectionConfig.from_yaml(yaml_str)


class TestRunnerHelpers:
    """Test the runner's helper methods on hand-crafted DataFrames."""

    def _make_runner(self, tmp_path):
        # Build a minimal config file so the runner can be instantiated.
        config_path = tmp_path / "test.yaml"
        config_path.write_text(
            textwrap.dedent(
                """
                data:
                  date_range:
                    start: "2024-01-01"
                    end: "2025-12-31"
                features:
                  include:
                    - pts_service_won_pct(days=90)
                serve_model:
                  type: identity
                  window: 90
                validation:
                  type: expanding_window
                  initial_train_size: 100
                  step_size: 50
                """
            )
        )
        return IIDProjectionRunner(
            config_path=config_path,
            log_to_mlflow=False,
        )

    def _build_match_df(self):
        """Two players A and B in a 2-set 6-4 6-3 match (A wins).

        Mirrored: 2 rows per match, one with player=A, opp=B and the reverse.
        """
        return pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "won": [True, False],
                "reason": [None, None],
                "best_of": [3, 3],
                "circuit": ["tour", "tour"],
                "surface": ["Hard", "Hard"],
                "round": ["R32", "R32"],
                "player_set1_games": [6, 4],
                "player_set2_games": [6, 3],
                "player_set3_games": [None, None],
                "player_set4_games": [None, None],
                "player_set5_games": [None, None],
                "opp_set1_games": [4, 6],
                "opp_set2_games": [3, 6],
                "opp_set3_games": [None, None],
                "opp_set4_games": [None, None],
                "opp_set5_games": [None, None],
            }
        )

    def test_resolve_targets_adds_target_columns(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df()
        result = runner._resolve_targets(df)
        assert "_target_games_a" in result.columns
        assert "_target_games_b" in result.columns
        # Row 0: player=A, A won 6+6=12 games
        assert result["_target_games_a"][0] == 12.0
        assert result["_target_games_b"][0] == 7.0
        # Row 1: player=B, B won 4+3=7 games
        assert result["_target_games_a"][1] == 7.0
        assert result["_target_games_b"][1] == 12.0

    def test_resolve_targets_filters_walkovers(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df().with_columns(
            pl.lit("W/O").alias("reason"),
        )
        result = runner._resolve_targets(df)
        assert len(result) == 0

    def test_resolve_targets_filters_missing_set_scores(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df().with_columns(
            pl.lit(None).cast(pl.Int64).alias("player_set1_games"),
        )
        result = runner._resolve_targets(df)
        assert len(result) == 0

    def test_collapse_to_match_rows_one_per_match(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = self._build_match_df()
        collapsed = runner._collapse_to_match_rows(df)
        assert len(collapsed) == 1
        # Lower player_id "A" should be the kept row
        assert collapsed["player_id"][0] == "A"

    def test_collapse_picks_lower_id(self, tmp_path):
        runner = self._make_runner(tmp_path)
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1", "m2", "m2"],
                "player_id": ["zoe", "anna", "ben", "ada"],
                "best_of": [3, 3, 3, 3],
            }
        )
        collapsed = runner._collapse_to_match_rows(df)
        assert len(collapsed) == 2
        # m1 → "anna" (lex smaller than "zoe")
        # m2 → "ada" (lex smaller than "ben")
        kept = sorted(zip(collapsed["match_uid"].to_list(), collapsed["player_id"].to_list()))
        assert kept == [("m1", "anna"), ("m2", "ada")]
