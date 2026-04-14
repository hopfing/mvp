"""Tests for score-state derived point features."""

import polars as pl
import pytest

from mvp.projection.iid.score_state_features import (
    DERIVED_POINT_FEATURES,
    add_derived_point_features,
)


def _point(**overrides) -> dict:
    base = {
        "server": "1",
        "game_score_server": "0",
        "game_score_returner": "0",
        "set_score_server_games": 0,
        "set_score_returner_games": 0,
        "sets_won_server": 0,
        "sets_won_returner": 0,
        "is_tiebreak": False,
        "is_break_point": False,
        "is_set_point": False,
        "is_match_point": False,
        "serve": 1,
        "surface": "hard",
    }
    base.update(overrides)
    return base


class TestDerivedPointFeatures:
    def test_is_server_set_point_when_server_at_40_love(self):
        df = pl.DataFrame([
            _point(
                game_score_server="40", game_score_returner="0",
                set_score_server_games=5, set_score_returner_games=4,
                is_set_point=True,
            ),
        ])
        out = add_derived_point_features(df, ["is_server_set_point", "is_returner_set_point"])
        assert out["is_server_set_point"][0] is True
        assert out["is_returner_set_point"][0] is False

    def test_is_returner_set_point_when_returner_at_40_love(self):
        df = pl.DataFrame([
            _point(
                game_score_server="0", game_score_returner="40",
                set_score_server_games=4, set_score_returner_games=5,
                is_set_point=True,
            ),
        ])
        out = add_derived_point_features(df, ["is_server_set_point", "is_returner_set_point"])
        assert out["is_server_set_point"][0] is False
        assert out["is_returner_set_point"][0] is True

    def test_set_and_match_points_false_when_not_flagged(self):
        df = pl.DataFrame([
            _point(
                game_score_server="40", game_score_returner="0",
                # is_set_point=False, is_match_point=False by default
            ),
        ])
        out = add_derived_point_features(
            df, ["is_server_set_point", "is_returner_set_point", "is_server_match_point", "is_returner_match_point"]
        )
        assert out["is_server_set_point"][0] is False
        assert out["is_returner_set_point"][0] is False
        assert out["is_server_match_point"][0] is False
        assert out["is_returner_match_point"][0] is False

    def test_set_score_asymmetry(self):
        df = pl.DataFrame([
            _point(set_score_server_games=5, set_score_returner_games=3),
            _point(set_score_server_games=2, set_score_returner_games=4),
        ])
        out = add_derived_point_features(df, ["set_score_asymmetry"])
        assert out["set_score_asymmetry"][0] == 2
        assert out["set_score_asymmetry"][1] == -2

    def test_sets_won_asymmetry(self):
        df = pl.DataFrame([
            _point(sets_won_server=1, sets_won_returner=0),
        ])
        out = add_derived_point_features(df, ["sets_won_asymmetry"])
        assert out["sets_won_asymmetry"][0] == 1

    def test_game_score_diff_for_ad_scoring(self):
        df = pl.DataFrame([
            _point(game_score_server="AD", game_score_returner="40"),  # 50-40 = 10
            _point(game_score_server="40", game_score_returner="AD"),  # 40-50 = -10
            _point(game_score_server="D", game_score_returner="D"),    # 45-45 = 0
            _point(game_score_server="30", game_score_returner="15"),  # 30-15 = 15
        ])
        out = add_derived_point_features(df, ["game_score_diff"])
        assert out["game_score_diff"][0] == 10
        assert out["game_score_diff"][1] == -10
        assert out["game_score_diff"][2] == 0
        assert out["game_score_diff"][3] == 15

    def test_is_second_serve(self):
        df = pl.DataFrame([_point(serve=1), _point(serve=2)])
        out = add_derived_point_features(df, ["is_second_serve"])
        assert out["is_second_serve"][0] is False
        assert out["is_second_serve"][1] is True

    def test_surface_one_hots(self):
        df = pl.DataFrame([
            _point(surface="hard"),
            _point(surface="clay"),
            _point(surface="grass"),
        ])
        out = add_derived_point_features(df, ["is_surface_hard", "is_surface_clay", "is_surface_grass"])
        assert out["is_surface_hard"].to_list() == [True, False, False]
        assert out["is_surface_clay"].to_list() == [False, True, False]
        assert out["is_surface_grass"].to_list() == [False, False, True]

    def test_existing_column_not_overwritten(self):
        df = pl.DataFrame([_point()]).with_columns(pl.lit(True).alias("is_break_point"))
        # is_break_point already in df; request it anyway — should be preserved
        out = add_derived_point_features(df, ["is_break_point", "is_second_serve"])
        assert out["is_break_point"][0] is True
        assert "is_second_serve" in out.columns

    def test_unknown_feature_raises(self):
        df = pl.DataFrame([_point()])
        with pytest.raises(ValueError, match="unknown"):
            add_derived_point_features(df, ["not_a_real_feature"])

    def test_known_features_registry_covers_all_public(self):
        # All registered features should be computable on a baseline row.
        df = pl.DataFrame([_point(
            set_score_server_games=5, set_score_returner_games=4, sets_won_server=1,
            is_set_point=True, is_match_point=True,
            game_score_server="40",
        )])
        out = add_derived_point_features(df, list(DERIVED_POINT_FEATURES.keys()))
        for name in DERIVED_POINT_FEATURES:
            assert name in out.columns
