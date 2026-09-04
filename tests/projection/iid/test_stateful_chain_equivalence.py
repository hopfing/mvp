"""Stateful chain DP traffic changes are BYTE-identical to the previous path
(plan 2026-09-03-stateful-chain-dp-traffic).

Two self-relative equivalence checks (item 1 carries a verbatim copy of the
pre-change loop as its reference; item 3 pits the fast path against the
masked path on the same rows) plus one fact-pin for the change that was
considered and rejected. Comparisons are on bytes, not
`np.array_equal` (which treats -0.0 == 0.0 -- the whole item-1 argument is
about signed zero). No golden fixtures: reduction blocking is not stable
across numpy versions, so a stored .npz would buy false failures.
"""

import numpy as np
import pytest

from mvp.projection.iid import stateful_chain as sc
from mvp.projection.iid.chain import _SET_A_WINS, _SET_SPREAD, _SET_TOTAL_GAMES
from mvp.projection.iid.stateful_chain import (
    _match_marginals_stateful,
    _set_score_pmf_one_server_stateful,
    match_distribution_from_state_fn,
    set_score_distribution_from_state_fn,
)


def _same_bytes(a: np.ndarray, b: np.ndarray) -> bool:
    return a.dtype == b.dtype and a.shape == b.shape and a.tobytes() == b.tobytes()


def _state_fns(n: int, seed: int, *, extreme: bool = False, dtype=np.float64):
    """Point-win fns that genuinely vary with the ScoreState (set score, sets
    won, game score), so every game state the DP visits carries a distinct
    probability and a wrong window or row would move the bytes.
    `extreme` drives p to exactly 0.0 and 1.0 on some rows -- the case where
    the deuce closed form hits its clamp and `p_set * 0.0` is the only thing
    being elided."""
    rng = np.random.default_rng(seed)
    base_a = rng.uniform(0.55, 0.70, n)
    base_b = rng.uniform(0.55, 0.70, n)
    if extreme:
        third = max(n // 3, 1)
        base_a[:third] = 0.0
        base_a[third: 2 * third] = 1.0
        base_b[n - third:] = 1.0

    pts = {"0": 0, "15": 1, "30": 2, "40": 3, "AD": 4}

    def _vary(base):
        def fn(state):
            gs = 0.004 * (
                pts.get(state.game_score_server, 0)
                - pts.get(state.game_score_returner, 0)
            )
            p = (
                base
                + 0.01 * (state.set_score_server_games - state.set_score_returner_games)
                + 0.02 * (state.sets_won_server - state.sets_won_returner)
                + gs
                + (0.03 if state.is_tiebreak else 0.0)
            )
            return np.clip(p, 0.0, 1.0).astype(dtype)
        return fn

    return (
        _vary(base_a), _vary(base_b),
        base_a.astype(np.float64), base_b.astype(np.float64),
    )


# ---------------------------------------------------------------------------
# Item 1: slice-accumulate in _match_marginals_stateful
# ---------------------------------------------------------------------------

def _reference_marginals(p_a_fn, p_b_fn, p_a_avg, p_b_avg, best_of_const,
                         max_total, spread_offset, n):
    """Verbatim pre-change body of `_match_marginals_stateful` (the
    zeros_like / shift-copy / multiply / add form)."""
    target_sets = (best_of_const + 1) // 2
    spread_size = 2 * max_total + 1
    state_total = {(0, 0): np.zeros((n, max_total + 1), dtype=np.float64)}
    state_spread = {(0, 0): np.zeros((n, spread_size), dtype=np.float64)}
    state_total[(0, 0)][:, 0] = 1.0
    state_spread[(0, 0)][:, spread_offset] = 1.0
    total_games_terminal = np.zeros((n, max_total + 1), dtype=np.float64)
    spread_terminal = np.zeros((n, spread_size), dtype=np.float64)
    set_outcome_probs = {}
    for _ in range(best_of_const):
        new_state_total, new_state_spread = {}, {}
        for (sa, sb), pmf_t in state_total.items():
            pmf_s = state_spread[(sa, sb)]
            if sa >= target_sets or sb >= target_sets:
                total_games_terminal += pmf_t
                spread_terminal += pmf_s
                marginal = pmf_t.sum(axis=1)
                if (sa, sb) not in set_outcome_probs:
                    set_outcome_probs[(sa, sb)] = np.zeros(n, dtype=np.float64)
                set_outcome_probs[(sa, sb)] += marginal
                continue
            set_pmf = set_score_distribution_from_state_fn(
                p_a_fn, p_b_fn, p_a_avg, p_b_avg,
                sets_won_a=sa, sets_won_b=sb, best_of=best_of_const,
            )
            for i in range(14):
                shift_t = int(_SET_TOTAL_GAMES[i])
                shift_s = int(_SET_SPREAD[i])
                a_wins_set = bool(_SET_A_WINS[i])
                p_set = set_pmf[:, i]
                shifted_t = np.zeros_like(pmf_t)
                if shift_t <= max_total:
                    shifted_t[:, shift_t:] = pmf_t[:, : max_total + 1 - shift_t]
                shifted_s = np.zeros_like(pmf_s)
                if shift_s >= 0:
                    if shift_s <= 2 * max_total:
                        shifted_s[:, shift_s:] = pmf_s[:, : spread_size - shift_s]
                else:
                    abs_shift = -shift_s
                    if abs_shift <= 2 * max_total:
                        shifted_s[:, : spread_size - abs_shift] = pmf_s[:, abs_shift:]
                contribution_t = p_set[:, None] * shifted_t
                contribution_s = p_set[:, None] * shifted_s
                next_key = (sa + 1, sb) if a_wins_set else (sa, sb + 1)
                if next_key not in new_state_total:
                    new_state_total[next_key] = np.zeros_like(pmf_t)
                    new_state_spread[next_key] = np.zeros_like(pmf_s)
                new_state_total[next_key] += contribution_t
                new_state_spread[next_key] += contribution_s
        state_total, state_spread = new_state_total, new_state_spread
    for (sa, sb), pmf_t in state_total.items():
        pmf_s = state_spread[(sa, sb)]
        total_games_terminal += pmf_t
        spread_terminal += pmf_s
        marginal = pmf_t.sum(axis=1)
        if (sa, sb) not in set_outcome_probs:
            set_outcome_probs[(sa, sb)] = np.zeros(n, dtype=np.float64)
        set_outcome_probs[(sa, sb)] += marginal
    return total_games_terminal, spread_terminal, set_outcome_probs


class TestSliceAccumulate:
    @pytest.mark.parametrize("n", [1, 7, 500])
    @pytest.mark.parametrize("best_of", [3, 5])
    @pytest.mark.parametrize("extreme", [False, True])
    def test_marginals_are_byte_identical(self, n, best_of, extreme):
        p_a_fn, p_b_fn, p_a, p_b = _state_fns(n, seed=n + best_of, extreme=extreme)
        max_total = 5 * 13
        args = (p_a_fn, p_b_fn, p_a, p_b, best_of, max_total, max_total, n)
        ref_t, ref_s, ref_o = _reference_marginals(*args)
        new_t, new_s, new_o = _match_marginals_stateful(*args)
        assert _same_bytes(ref_t, new_t)
        assert _same_bytes(ref_s, new_s)
        assert ref_o.keys() == new_o.keys()
        for k in ref_o:
            assert _same_bytes(ref_o[k], new_o[k]), k


# ---------------------------------------------------------------------------
# The two first-server passes request DISJOINT hold keys (server at cell
# (a, b) = a_serves_first XOR parity(a + b)), so a shared hold cache was
# considered and found to save nothing -- pinned here so it is not re-tried.
# ---------------------------------------------------------------------------

class TestHoldPassesAreDisjoint:
    def test_first_server_passes_share_no_hold_keys(self, monkeypatch):
        seen: dict[bool, set] = {True: set(), False: set()}
        orig = sc.build_game_state_ps_per_side
        current = {"pass": None}

        def spy(p_fn, base_state):
            seen[current["pass"]].add((
                base_state.set_score_server_games, base_state.set_score_returner_games,
                p_fn,
            ))
            return orig(p_fn, base_state)

        monkeypatch.setattr(sc, "build_game_state_ps_per_side", spy)
        n = 3
        p_a_fn, p_b_fn, p_a, p_b = _state_fns(n, seed=5)
        for first in (True, False):
            current["pass"] = first
            _set_score_pmf_one_server_stateful(
                p_a_fn, p_b_fn, np.full(n, 0.5), 0, 0, 3, n, a_serves_first=first,
            )
        assert seen[True] and seen[False]
        assert seen[True].isdisjoint(seen[False])


# ---------------------------------------------------------------------------
# Item 3: identity fast path in the bo3/bo5 wrapper
# ---------------------------------------------------------------------------

def _dist_fields(dist):
    yield "total_games_pmf", dist.total_games_pmf
    yield "spread_pmf", dist.spread_pmf
    yield "p_match_win_a", dist.p_match_win_a
    yield "expected_total_games", dist.expected_total_games
    yield "expected_spread", dist.expected_spread
    for k in sorted(dist.set_outcome_probs):
        yield f"set_outcome_probs[{k}]", dist.set_outcome_probs[k]


class TestMaskFastPath:
    @pytest.mark.parametrize("n", [1, 7, 300])
    @pytest.mark.parametrize("dtype", [np.float64, np.float32])
    def test_all_bo3_batch_equals_the_bo3_rows_of_a_mixed_batch(self, n, dtype):
        """All-bo3 takes the fast path; the same rows plus one bo5 row take
        the masked path. The bo3 rows must come out byte-identical."""
        p_a_fn, p_b_fn, p_a, p_b = _state_fns(n + 1, seed=n, dtype=dtype)

        def head(fn):
            return lambda state: fn(state)[:n]

        pure = match_distribution_from_state_fn(
            head(p_a_fn), head(p_b_fn), p_a[:n], p_b[:n], np.full(n, 3, dtype=np.int64),
        )
        mixed = match_distribution_from_state_fn(
            p_a_fn, p_b_fn, p_a, p_b,
            np.array([3] * n + [5], dtype=np.int64),
        )
        for name, arr in _dist_fields(pure):
            other = getattr(mixed, name) if not name.startswith("set_outcome") else None
            if other is None:
                key = eval(name[len("set_outcome_probs["):-1])
                other = mixed.set_outcome_probs[key]
            assert _same_bytes(arr, other[:n]), name

    def test_shared_result_object_is_not_mutated(self):
        """Unwrapped, the DP holds the caller's array object (the copy used
        to insulate it). It must come out untouched."""
        n = 9
        shared = np.linspace(0.55, 0.70, n)
        snapshot = shared.copy()
        calls = []

        def fn(state):
            calls.append(1)
            return shared  # same object every call, like the fitted models do

        dist = match_distribution_from_state_fn(
            fn, fn, shared, shared, np.full(n, 3, dtype=np.int64),
        )
        assert calls
        assert _same_bytes(shared, snapshot)
        assert np.isfinite(dist.p_match_win_a).all()


class TestWrapperUntouchedOnMixedBatches:
    def test_mixed_batch_still_masks(self, monkeypatch):
        """With mixed best_of the wrapper path must still be taken."""
        seen = []
        orig = sc._match_marginals_stateful

        def spy(p_a_fn, p_b_fn, *args):
            seen.append(p_a_fn)
            return orig(p_a_fn, p_b_fn, *args)

        monkeypatch.setattr(sc, "_match_marginals_stateful", spy)
        n = 4
        p_a_fn, p_b_fn, p_a, p_b = _state_fns(n, seed=1)
        match_distribution_from_state_fn(
            p_a_fn, p_b_fn, p_a, p_b, np.array([3, 3, 5, 5], dtype=np.int64),
        )
        assert len(seen) == 2 and all(f is not p_a_fn for f in seen)
        seen.clear()
        match_distribution_from_state_fn(
            p_a_fn, p_b_fn, p_a, p_b, np.full(n, 3, dtype=np.int64),
        )
        assert len(seen) == 1 and seen[0] is p_a_fn  # fast path: unwrapped
