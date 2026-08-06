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

    def test_game_points_diff_for_ad_scoring(self):
        df = pl.DataFrame([
            _point(game_score_server="AD", game_score_returner="40"),  # 4-3 = 1
            _point(game_score_server="40", game_score_returner="AD"),  # 3-4 = -1
            _point(game_score_server="40", game_score_returner="40"),  # 3-3 = 0
            _point(game_score_server="D", game_score_returner="D"),    # 3-3 = 0
            _point(game_score_server="30", game_score_returner="15"),  # 2-1 = 1
        ])
        out = add_derived_point_features(df, ["game_points_diff"])
        assert out["game_points_diff"][0] == 1
        assert out["game_points_diff"][1] == -1
        # Deuce is written ("40","40") in the data and ("D","D") by the chain.
        # Both are deuce and both must land on the same value — under the old
        # display-score mapping they were 40 and 45.
        assert out["game_points_diff"][2] == 0
        assert out["game_points_diff"][3] == 0
        assert out["game_points_diff"][4] == 1

    def test_game_points_are_on_one_scale_across_formats(self):
        df = pl.DataFrame([
            _point(game_score_server="40", game_score_returner="30"),
            _point(game_score_server="3", game_score_returner="2", is_tiebreak=True),
            _point(game_score_server="11", game_score_returner="9", is_tiebreak=True),
        ])
        out = add_derived_point_features(
            df, ["game_points_server", "game_points_returner", "game_points_diff"]
        )
        # A regular 40-30 and a tiebreak at 3-2 are the same points-won state.
        # Deliberate: `is_tiebreak` carries the format, this carries the score.
        assert out["game_points_server"].to_list() == [3, 3, 11]
        assert out["game_points_returner"].to_list() == [2, 2, 9]
        assert out["game_points_diff"].to_list() == [1, 1, 2]

    def test_tiebreak_scores_are_not_null(self):
        # The defect this replaced: tiebreak labels fell through to null, and
        # only "0" and "15" parsed — onto the regular-game values 0 and 15.
        df = pl.DataFrame([
            _point(game_score_server=str(i), game_score_returner="0",
                   is_tiebreak=True)
            for i in range(0, 19)
        ])
        out = add_derived_point_features(df, ["game_points_server"])
        assert out["game_points_server"].null_count() == 0
        assert out["game_points_server"].to_list() == list(range(0, 19))

    def test_tiebreak_columns_are_zero_outside_a_tiebreak(self):
        df = pl.DataFrame([
            _point(game_score_server="40", game_score_returner="15"),
            _point(game_score_server="5", game_score_returner="3", is_tiebreak=True),
        ])
        out = add_derived_point_features(
            df, ["tiebreak_point_diff", "tiebreak_points_played"]
        )
        # Zero off-tiebreak makes these an interaction: the coefficient acts
        # only on tiebreak points.
        assert out["tiebreak_point_diff"].to_list() == [0, 2]
        assert out["tiebreak_points_played"].to_list() == [0, 8]

    def test_unreadable_label_is_null_not_zero(self):
        df = pl.DataFrame([_point(game_score_server="GAME", game_score_returner="40")])
        out = add_derived_point_features(df, ["game_points_server"])
        assert out["game_points_server"][0] is None

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


class TestTrainInferenceParity:
    """The training expressions and the inference state must read a score alike.

    This is the test that would have caught the original defect. Training sent
    tiebreak labels to null; inference sent them to 0 via a `.get(label, 0)`
    default. Both were wrong, differently, and nothing compared them — the model
    was fit on one encoding and queried on another.
    """

    STATES = [
        # (game_score_server, game_score_returner, is_tiebreak)
        ("0", "0", False), ("15", "0", False), ("30", "15", False),
        ("40", "0", False), ("40", "30", False), ("40", "40", False),
        ("AD", "40", False), ("40", "AD", False), ("D", "D", False),
        ("0", "0", True), ("3", "2", True), ("6", "5", True),
        ("7", "7", True), ("15", "13", True), ("18", "16", True),
    ]

    @pytest.mark.parametrize("gs_s,gs_r,is_tb", STATES)
    def test_same_values_from_both_paths(self, gs_s, gs_r, is_tb):
        from mvp.projection.iid.score_state import ScoreState

        df = pl.DataFrame([
            _point(game_score_server=gs_s, game_score_returner=gs_r,
                   is_tiebreak=is_tb)
        ])
        cols = ["game_points_server", "game_points_returner", "game_points_diff",
                "tiebreak_point_diff", "tiebreak_points_played"]
        out = add_derived_point_features(df, cols)

        state = ScoreState(
            serve_num=1, game_score_server=gs_s, game_score_returner=gs_r,
            is_tiebreak=is_tb, set_score_server_games=0,
            set_score_returner_games=0, sets_won_server=0,
            sets_won_returner=0, best_of=3,
        )
        assert out["game_points_server"][0] == state.game_points_server()
        assert out["game_points_returner"][0] == state.game_points_returner()
        assert out["game_points_diff"][0] == state.game_points_diff()
        assert out["tiebreak_point_diff"][0] == state.tiebreak_point_diff()
        assert out["tiebreak_points_played"][0] == state.tiebreak_points_played()
