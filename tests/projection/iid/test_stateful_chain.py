"""Tests for stateful chain math.

The critical test is scalar-equivalence: if the state-fn returns the same
probability regardless of state, the stateful chain must produce identical
output to the scalar chain at `chain.py`.
"""

import numpy as np
import pytest

from mvp.projection.iid.chain import (
    match_distribution,
    p_service_game_win,
    set_score_distribution,
)
from mvp.projection.iid.score_state import GAME_SCORE_STATES, ScoreState
from mvp.projection.iid.stateful_chain import (
    build_game_state_ps_per_side,
    hold_from_state_fn,
    match_distribution_from_state_fn,
    set_score_distribution_from_state_fn,
)


class TestHoldFromStateFn:
    def test_const_probabilities_match_scalar_chain(self):
        # Constant p across all game states → should equal scalar p_service_game_win(p).
        n = 5
        ps = np.array([0.55, 0.62, 0.70, 0.50, 0.80])
        p_at_state = {gs: ps.copy() for gs in GAME_SCORE_STATES}

        stateful_hold = hold_from_state_fn(p_at_state)
        scalar_hold = p_service_game_win(ps)

        np.testing.assert_allclose(stateful_hold, scalar_hold, rtol=1e-10)

    def test_extreme_p_values(self):
        n = 3
        # p = 1.0 → server always wins a point → always holds
        ps_hi = np.ones(n, dtype=np.float64)
        p_at_hi = {gs: ps_hi.copy() for gs in GAME_SCORE_STATES}
        np.testing.assert_allclose(hold_from_state_fn(p_at_hi), 1.0)

        # p = 0.0 → server never wins → never holds
        ps_lo = np.zeros(n, dtype=np.float64)
        p_at_lo = {gs: ps_lo.copy() for gs in GAME_SCORE_STATES}
        np.testing.assert_allclose(hold_from_state_fn(p_at_lo), 0.0)

    def test_state_variation_actually_matters(self):
        # Server wins easily except on break points (returner at 40+).
        n = 1
        base_p = 0.70
        bp_p = 0.30
        p_at_state = {}
        for gs_s, gs_r in GAME_SCORE_STATES:
            if gs_r in ("40", "AD"):
                p_at_state[(gs_s, gs_r)] = np.array([bp_p])
            else:
                p_at_state[(gs_s, gs_r)] = np.array([base_p])

        hold = hold_from_state_fn(p_at_state)
        # Compared to constant-base_p version:
        scalar_hold = p_service_game_win(np.array([base_p]))
        # Hold should be lower because server does worse at the crucial states.
        assert hold[0] < scalar_hold[0]

    def test_missing_state_raises(self):
        # Drop deuce, should raise.
        partial = {gs: np.array([0.5]) for gs in GAME_SCORE_STATES if gs != ("D", "D")}
        with pytest.raises(ValueError, match="missing"):
            hold_from_state_fn(partial)


class TestBuildGameStatePsPerSide:
    def test_calls_fn_for_all_game_states(self):
        calls = []

        def fn(state: ScoreState):
            calls.append((state.game_score_server, state.game_score_returner))
            return np.array([0.5])

        base = ScoreState(
            serve_num=1, game_score_server="0", game_score_returner="0",
            is_tiebreak=False,
            set_score_server_games=3, set_score_returner_games=2,
            sets_won_server=0, sets_won_returner=0, best_of=3,
        )
        out = build_game_state_ps_per_side(fn, base)

        # Each of the 18 game states queried exactly once
        assert set(out.keys()) == set(GAME_SCORE_STATES)
        assert sorted(calls) == sorted(GAME_SCORE_STATES)

    def test_overrides_tiebreak_to_false(self):
        seen_tiebreak_flags = []

        def fn(state: ScoreState):
            seen_tiebreak_flags.append(state.is_tiebreak)
            return np.array([0.6])

        base = ScoreState(
            serve_num=1, game_score_server="0", game_score_returner="0",
            is_tiebreak=True,  # Should be overridden to False in the calls
            set_score_server_games=3, set_score_returner_games=2,
            sets_won_server=0, sets_won_returner=0, best_of=3,
        )
        build_game_state_ps_per_side(fn, base)
        assert all(flag is False for flag in seen_tiebreak_flags)


class TestSetScoreDistributionFromStateFn:
    def test_const_state_fn_matches_scalar(self):
        n = 4
        p_a = np.array([0.62, 0.55, 0.70, 0.50])
        p_b = np.array([0.58, 0.60, 0.68, 0.52])

        def p_a_fn(state):
            return p_a

        def p_b_fn(state):
            return p_b

        stateful_pmf = set_score_distribution_from_state_fn(
            p_a_fn, p_b_fn, p_a, p_b,
            sets_won_a=0, sets_won_b=0, best_of=3,
        )

        # Scalar equivalent: h_a = hold(p_a), h_b = hold(p_b), t_ab = tiebreak(p_a, p_b).
        from mvp.projection.iid.chain import p_tiebreak_game_win
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        scalar_pmf = set_score_distribution(h_a, h_b, t_ab)

        np.testing.assert_allclose(stateful_pmf, scalar_pmf, rtol=1e-10)

    def test_pmf_sums_to_one(self):
        n = 3
        p_a = np.array([0.6, 0.5, 0.7])
        p_b = np.array([0.5, 0.6, 0.5])

        def p_a_fn(s):
            return p_a

        def p_b_fn(s):
            return p_b

        pmf = set_score_distribution_from_state_fn(
            p_a_fn, p_b_fn, p_a, p_b, sets_won_a=0, sets_won_b=0, best_of=3,
        )
        np.testing.assert_allclose(pmf.sum(axis=1), 1.0, atol=1e-10)


class TestMatchDistributionFromStateFn:
    def test_const_state_fn_matches_scalar(self):
        n = 3
        p_a = np.array([0.65, 0.55, 0.70])
        p_b = np.array([0.60, 0.62, 0.50])
        best_of = np.array([3, 3, 5], dtype=np.int64)

        def p_a_fn(s):
            return p_a

        def p_b_fn(s):
            return p_b

        stateful = match_distribution_from_state_fn(p_a_fn, p_b_fn, p_a, p_b, best_of)

        from mvp.projection.iid.chain import p_tiebreak_game_win
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        scalar = match_distribution(h_a, h_b, t_ab, best_of)

        np.testing.assert_allclose(stateful.p_match_win_a, scalar.p_match_win_a, rtol=1e-9)
        np.testing.assert_allclose(stateful.total_games_pmf, scalar.total_games_pmf, rtol=1e-9, atol=1e-12)
        np.testing.assert_allclose(stateful.spread_pmf, scalar.spread_pmf, rtol=1e-9, atol=1e-12)
        np.testing.assert_allclose(stateful.expected_total_games, scalar.expected_total_games, rtol=1e-9)
        np.testing.assert_allclose(stateful.expected_spread, scalar.expected_spread, rtol=1e-9)

    def test_empty(self):
        p_a = np.array([], dtype=np.float64)
        p_b = np.array([], dtype=np.float64)
        best_of = np.array([], dtype=np.int64)

        def p_a_fn(s):
            return p_a

        def p_b_fn(s):
            return p_b

        dist = match_distribution_from_state_fn(p_a_fn, p_b_fn, p_a, p_b, best_of)
        assert dist.p_match_win_a.shape == (0,)

    def test_invalid_best_of_raises(self):
        p = np.array([0.6])

        def fn(s):
            return p

        with pytest.raises(ValueError, match="best_of"):
            match_distribution_from_state_fn(fn, fn, p, p, np.array([4], dtype=np.int64))
