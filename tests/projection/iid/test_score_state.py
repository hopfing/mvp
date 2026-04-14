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
