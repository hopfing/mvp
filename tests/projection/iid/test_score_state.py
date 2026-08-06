"""Tests for ScoreState flag derivations."""

import pytest

from mvp.projection.iid.score_state import GAME_SCORE_STATES, ScoreState


def _state(
    *,
    serve_num: int = 1,
    gs_server: str = "0",
    gs_returner: str = "0",
    is_tiebreak: bool = False,
    set_server_games: int = 0,
    set_returner_games: int = 0,
    sets_server: int = 0,
    sets_returner: int = 0,
    best_of: int = 3,
) -> ScoreState:
    return ScoreState(
        serve_num=serve_num,
        game_score_server=gs_server,
        game_score_returner=gs_returner,
        is_tiebreak=is_tiebreak,
        set_score_server_games=set_server_games,
        set_score_returner_games=set_returner_games,
        sets_won_server=sets_server,
        sets_won_returner=sets_returner,
        best_of=best_of,
    )


class TestScoreStateFlags:
    def test_break_point_at_40_ret(self):
        assert _state(gs_server="30", gs_returner="40").is_break_point() is True
        assert _state(gs_server="0", gs_returner="40").is_break_point() is True
        assert _state(gs_server="40", gs_returner="AD").is_break_point() is True

    def test_not_break_point_when_tied(self):
        assert _state(gs_server="40", gs_returner="40").is_break_point() is False
        assert _state(gs_server="30", gs_returner="30").is_break_point() is False

    def test_break_point_suppressed_in_tiebreak(self):
        assert _state(gs_server="30", gs_returner="40", is_tiebreak=True).is_break_point() is False

    def test_server_game_point_at_40_lov(self):
        assert _state(gs_server="40", gs_returner="0").is_server_game_point() is True
        assert _state(gs_server="AD", gs_returner="40").is_server_game_point() is True

    def test_not_server_game_point_at_40_40(self):
        assert _state(gs_server="40", gs_returner="40").is_server_game_point() is False

    def test_server_set_point_at_5_4_40_love(self):
        s = _state(gs_server="40", gs_returner="0", set_server_games=5, set_returner_games=4)
        assert s.is_server_set_point() is True
        assert s.is_set_point() is True

    def test_not_set_point_at_5_5_40_love(self):
        # Winning game → 6-5, not a set win.
        s = _state(gs_server="40", gs_returner="0", set_server_games=5, set_returner_games=5)
        assert s.is_server_set_point() is False
        assert s.is_set_point() is False

    def test_set_point_at_7_via_6_6(self):
        # 6-6 → winning tiebreak makes 7-6 (caught via new_s == 7). Non-tiebreak
        # here, so we just check the rule: 6-6 + server wins game → 7-6 = set win.
        s = _state(gs_server="40", gs_returner="0", set_server_games=6, set_returner_games=6)
        assert s.is_server_set_point() is True

    def test_returner_set_point(self):
        # Returner at AD, server at 40, set score 4-5 → returner breaking wins set.
        s = _state(gs_server="40", gs_returner="AD", set_server_games=4, set_returner_games=5)
        assert s.is_returner_set_point() is True
        assert s.is_server_set_point() is False

    def test_match_point_bo3(self):
        # Server won set 1. Set 2: 5-4, 40-0. Winning game = 6-4 in set 2 = 2 sets = match.
        s = _state(
            gs_server="40", gs_returner="0",
            set_server_games=5, set_returner_games=4,
            sets_server=1, sets_returner=0, best_of=3,
        )
        assert s.is_server_match_point() is True
        assert s.is_match_point() is True

    def test_not_match_point_when_more_sets_needed(self):
        # Server at 40-0, 5-4 in set 1, 0 sets won. Set 1 not match-deciding.
        s = _state(
            gs_server="40", gs_returner="0",
            set_server_games=5, set_returner_games=4,
            sets_server=0, sets_returner=0, best_of=3,
        )
        assert s.is_server_match_point() is False
        assert s.is_server_set_point() is True  # still a set point though

    def test_match_point_bo5_needs_3_sets(self):
        # In BO5, 2 sets won doesn't win match; 3 does.
        s_not_yet = _state(
            gs_server="40", gs_returner="0",
            set_server_games=5, set_returner_games=4,
            sets_server=1, sets_returner=0, best_of=5,
        )
        assert s_not_yet.is_server_match_point() is False

        s_mp = _state(
            gs_server="40", gs_returner="0",
            set_server_games=5, set_returner_games=4,
            sets_server=2, sets_returner=0, best_of=5,
        )
        assert s_mp.is_server_match_point() is True

    def test_asymmetries(self):
        s = _state(set_server_games=5, set_returner_games=3, sets_server=1, sets_returner=0)
        assert s.set_score_asymmetry() == 2
        assert s.sets_won_asymmetry() == 1

    def test_tiebreak_suppresses_set_and_match_point(self):
        s = _state(
            gs_server="40", gs_returner="0",
            set_server_games=5, set_returner_games=4,
            sets_server=1, sets_returner=0, best_of=3,
            is_tiebreak=True,
        )
        assert s.is_set_point() is False
        assert s.is_match_point() is False

    def test_hashable(self):
        # Frozen dataclass should be usable as dict key / set member.
        s1 = _state(gs_server="40", gs_returner="30")
        s2 = _state(gs_server="40", gs_returner="30")
        assert s1 == s2
        {s1: 1}

    def test_game_score_states_cover_expected_shape(self):
        # 18 unique pre-point states (0-0 through AD-40 / 40-AD).
        assert len(GAME_SCORE_STATES) == 18
        assert ("0", "0") in GAME_SCORE_STATES
        assert ("AD", "40") in GAME_SCORE_STATES
        assert ("40", "AD") in GAME_SCORE_STATES
        assert ("D", "D") in GAME_SCORE_STATES


class TestGamePoints:
    """The points-won scale, shared with the training-time derivations."""

    @pytest.mark.parametrize(
        "gs_s,gs_r,expected",
        [
            ("0", "0", 0), ("15", "0", 1), ("30", "15", 1), ("40", "0", 3),
            ("40", "30", 1), ("40", "40", 0), ("AD", "40", 1), ("40", "AD", -1),
        ],
    )
    def test_regular_game_diff(self, gs_s, gs_r, expected):
        assert _state(gs_server=gs_s, gs_returner=gs_r).game_points_diff() == expected

    def test_deuce_written_both_ways_agrees(self):
        # The data writes deuce ("40","40"); the chain writes it ("D","D").
        # Same state, so the same feature value — the old display mapping gave
        # 40 and 45 respectively.
        as_data = _state(gs_server="40", gs_returner="40")
        as_chain = _state(gs_server="D", gs_returner="D")
        assert as_data.game_points_server() == as_chain.game_points_server() == 3
        assert as_data.game_points_diff() == as_chain.game_points_diff() == 0

    def test_tiebreak_score_parses_as_a_count(self):
        s = _state(gs_server="11", gs_returner="9", is_tiebreak=True)
        assert s.game_points_server() == 11
        assert s.game_points_returner() == 9
        assert s.game_points_diff() == 2

    def test_tiebreak_label_15_is_not_regular_15(self):
        # The specific silent miscode: "15" in a tiebreak is fifteen POINTS.
        tb = _state(gs_server="15", gs_returner="13", is_tiebreak=True)
        reg = _state(gs_server="15", gs_returner="0")
        assert tb.game_points_server() == 15
        assert reg.game_points_server() == 1

    def test_unreadable_label_is_none_not_zero(self):
        s = _state(gs_server="GAME", gs_returner="40")
        assert s.game_points_server() is None
        assert s.game_points_diff() is None

    def test_every_chain_game_state_is_readable(self):
        # Whatever the DP can construct, the feature layer must be able to read.
        for gs_s, gs_r in GAME_SCORE_STATES:
            s = _state(gs_server=gs_s, gs_returner=gs_r)
            assert s.game_points_diff() is not None, f"{gs_s}-{gs_r} unreadable"


class TestTiebreakInteractionColumns:
    def test_zero_outside_a_tiebreak(self):
        s = _state(gs_server="40", gs_returner="15")
        assert s.tiebreak_point_diff() == 0
        assert s.tiebreak_points_played() == 0

    def test_carries_the_score_inside_one(self):
        s = _state(gs_server="5", gs_returner="3", is_tiebreak=True)
        assert s.tiebreak_point_diff() == 2
        assert s.tiebreak_points_played() == 8

    def test_leverage_flags_stay_suppressed_in_a_tiebreak(self):
        # Matches training exactly: all 229,682 tiebreak points in
        # match_beats_points carry these flags at 0.0000, so inference must too.
        s = _state(gs_server="6", gs_returner="5", is_tiebreak=True,
                   set_server_games=6, set_returner_games=6)
        assert s.is_break_point() is False
        assert s.is_set_point() is False
        assert s.is_match_point() is False
