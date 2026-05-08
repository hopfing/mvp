"""Stateful-probability generalization of the chain math.

The scalar chain (`chain.py`) uses one per-point serve-win probability `p`
for every point in a match. The stateful chain accepts a per-state mapping
`{ScoreState: p_array}` and does a small DP over the 18 game-score states
(instead of the closed-form sum). Higher layers (set-score distribution,
match distribution) stay largely the same — they just take per-set-context
hold probabilities as input rather than a single scalar per match.

Vectorized over N matches, matching chain.py's array-per-match contract.

For Phase 3, tiebreak handling uses the existing scalar `p_tiebreak_game_win`
from chain.py — score-state variance within a tiebreak isn't captured by the
current ScoreState (no tiebreak-score field). An "average serve" probability
is passed in.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

import numpy as np

from mvp.projection.iid.chain import (
    _SET_A_WINS,
    _SET_SPREAD,
    _SET_TERMINAL_IDX,
    _SET_TOTAL_GAMES,
    _TIEBREAK_A_WIN_IDX,
    _TIEBREAK_B_WIN_IDX,
    MatchDistribution,
    p_tiebreak_game_win,
)
from mvp.projection.iid.score_state import GAME_SCORE_STATES, ScoreState


# Map game_score string to numeric rank (0=0, 1=15, 2=30, 3=40, 3=D, 4=AD).
# Tokens 0/15/30/40 are the 4 pre-deuce "pts"; "D" is deuce (3-3 equivalent),
# "AD" is advantage (one up from 40).
ServeStateFn = Callable[[ScoreState], np.ndarray]


def hold_from_state_fn(
    p_at_state: dict[tuple[str, str], np.ndarray],
) -> np.ndarray:
    """P(server wins service game) under per-game-state point probabilities.

    Args:
        p_at_state: dict mapping (game_score_server, game_score_returner) to
            a per-match (N,) array of point-win-for-server probability at that
            state. Must contain entries for all 18 states in GAME_SCORE_STATES.

    Returns:
        (N,) array of per-match hold probability.
    """
    for gs in GAME_SCORE_STATES:
        if gs not in p_at_state:
            raise ValueError(f"p_at_state missing required game state {gs}")

    n = len(p_at_state[("0", "0")])

    # Deuce closed-form. Let
    #   p_D   = p at ("D", "D")
    #   p_AD  = p at ("AD", "40")
    #   p_40A = p at ("40", "AD")
    #   x = P(server wins from D)
    #   y = P(server wins from AD-40)
    #   z = P(server wins from 40-AD)
    # Relations:
    #   y = p_AD + (1 - p_AD) * x
    #   z = p_40A * x + (1 - p_40A) * 0 = p_40A * x
    #   x = p_D * y + (1 - p_D) * z
    # Solving:
    #   x * (1 - p_D*(1-p_AD) - (1-p_D)*p_40A) = p_D * p_AD
    p_d = p_at_state[("D", "D")]
    p_ad = p_at_state[("AD", "40")]
    p_40a = p_at_state[("40", "AD")]
    denom = 1.0 - p_d * (1.0 - p_ad) - (1.0 - p_d) * p_40a
    # denom is in (0, 1] for any valid probabilities; guard numerics just in case.
    denom = np.where(denom <= 0.0, 1e-12, denom)
    p_win_from_deuce = (p_d * p_ad) / denom

    # Pre-deuce DP. States are (a_pts, b_pts) with a, b in {0, 1, 2, 3} encoded
    # as 0, 15, 30, 40. (3, 3) is deuce (handled above). (4, _) means server won,
    # (_, 4) means returner won. We walk backwards from terminal states.
    _num_to_str = {0: "0", 1: "15", 2: "30", 3: "40"}

    hold = {}  # (a_pts, b_pts) → (N,) prob server wins game from this state
    # Fill terminal columns / rows
    for a in range(5):
        for b in range(5):
            if a == 4 and a - b >= 1:
                hold[(a, b)] = np.ones(n, dtype=np.float64)
            elif b == 4 and b - a >= 1:
                hold[(a, b)] = np.zeros(n, dtype=np.float64)

    # Deuce (3, 3)
    hold[(3, 3)] = p_win_from_deuce

    # Fill non-terminal pre-deuce states in reverse topological order.
    # We iterate by decreasing (a+b). At each, lookup transitions.
    remaining = [(a, b) for a in range(4) for b in range(4) if (a, b) not in hold]
    remaining.sort(key=lambda ab: -(ab[0] + ab[1]))

    for a, b in remaining:
        gs_s = _num_to_str[a]
        gs_r = _num_to_str[b]
        p = p_at_state[(gs_s, gs_r)]
        # Transition: server wins point → (a+1, b); loses → (a, b+1).
        # (3,*) winning goes to (4,*)=terminal (except 3,3 handled above).
        # (*,3) losing goes to (*,4)=terminal.
        hold[(a, b)] = p * hold[(a + 1, b)] + (1.0 - p) * hold[(a, b + 1)]

    return hold[(0, 0)]


def build_game_state_ps_per_side(
    p_fn: ServeStateFn,
    base_state: ScoreState,
) -> dict[tuple[str, str], np.ndarray]:
    """Materialize {(gs_server, gs_returner) → (N,) probability} for every game-score state.

    `base_state` provides the match-level and set-level context (sets_won,
    set_score, best_of, serve_num, is_tiebreak). Within this helper we vary
    only (game_score_server, game_score_returner) — the set/match context is
    held fixed because the current game is played at a fixed set score.
    """
    out: dict[tuple[str, str], np.ndarray] = {}
    for gs_s, gs_r in GAME_SCORE_STATES:
        state = replace(
            base_state,
            game_score_server=gs_s,
            game_score_returner=gs_r,
            is_tiebreak=False,
        )
        out[(gs_s, gs_r)] = np.asarray(p_fn(state), dtype=np.float64)
    return out


def set_score_distribution_from_state_fn(
    p_a_fn: ServeStateFn,
    p_b_fn: ServeStateFn,
    p_a_avg: np.ndarray,
    p_b_avg: np.ndarray,
    sets_won_a: int,
    sets_won_b: int,
    best_of: int,
) -> np.ndarray:
    """(N, 14) set-score distribution under state-conditional point probs.

    `p_a_fn` / `p_b_fn` provide per-point probability from each player's serving
    perspective. `p_a_avg` / `p_b_avg` are the all-points-average serve win
    probs used for the tiebreak-game prob (tiebreak score-state isn't in the
    current ScoreState; we use the scalar approximation from chain.py).

    `sets_won_a` / `sets_won_b` / `best_of` set the MATCH context for deriving
    set-point / match-point flags inside each game.
    """
    n = len(p_a_avg)

    # Tiebreak game win prob — scalar approximation.
    t_ab = p_tiebreak_game_win(p_a_avg, p_b_avg)

    pmf_a_first = _set_score_pmf_one_server_stateful(
        p_a_fn, p_b_fn, t_ab, sets_won_a, sets_won_b, best_of, n, a_serves_first=True,
    )
    pmf_b_first = _set_score_pmf_one_server_stateful(
        p_a_fn, p_b_fn, t_ab, sets_won_a, sets_won_b, best_of, n, a_serves_first=False,
    )
    return 0.5 * (pmf_a_first + pmf_b_first)


def _set_score_pmf_one_server_stateful(
    p_a_fn: ServeStateFn,
    p_b_fn: ServeStateFn,
    t_ab: np.ndarray,
    sets_won_a: int,
    sets_won_b: int,
    best_of: int,
    n: int,
    *,
    a_serves_first: bool,
) -> np.ndarray:
    """Forward DP over (games_a, games_b), using per-set-state hold probabilities.

    Each set-DP cell (a, b) represents "player A has a games, player B has b
    games in the current set." The game played from this cell is either A
    serving or B serving (alternating from first-server). The per-game hold
    probability depends on the ScoreState, which varies by (a, b) AND by who
    serves AND by the match-level sets_won AND by best_of.
    """
    memo: dict[tuple[int, int], np.ndarray] = {}

    # Cache per-server hold probabilities per set-state.
    # Key: (serving_player, a_games, b_games) → hold probability (N,)
    hold_cache: dict[tuple[str, int, int], np.ndarray] = {}

    def _hold(serving: str, set_a: int, set_b: int) -> np.ndarray:
        key = (serving, set_a, set_b)
        if key in hold_cache:
            return hold_cache[key]
        # Build a base ScoreState for this serving player at this set context.
        # game_score fields will be varied by hold_from_state_fn internally.
        if serving == "A":
            # A serves: "server" = A, "returner" = B.
            base = ScoreState(
                serve_num=1,  # First-serve perspective; 2nd-serve handling deferred.
                game_score_server="0", game_score_returner="0",
                is_tiebreak=False,
                set_score_server_games=set_a, set_score_returner_games=set_b,
                sets_won_server=sets_won_a, sets_won_returner=sets_won_b,
                best_of=best_of,
            )
            ps = build_game_state_ps_per_side(p_a_fn, base)
        else:
            base = ScoreState(
                serve_num=1,
                game_score_server="0", game_score_returner="0",
                is_tiebreak=False,
                set_score_server_games=set_b, set_score_returner_games=set_a,
                sets_won_server=sets_won_b, sets_won_returner=sets_won_a,
                best_of=best_of,
            )
            ps = build_game_state_ps_per_side(p_b_fn, base)
        h = hold_from_state_fn(ps)
        hold_cache[key] = h
        return h

    def _from(a: int, b: int) -> np.ndarray:
        if (a, b) in memo:
            return memo[(a, b)]

        # Terminal: regular 6-x win (non-tiebreak).
        if (a, b) in _SET_TERMINAL_IDX:
            result = np.zeros((n, 14), dtype=np.float64)
            result[:, _SET_TERMINAL_IDX[(a, b)]] = 1.0
            memo[(a, b)] = result
            return result

        # 6-6 → tiebreak (scalar approximation for now).
        if a == 6 and b == 6:
            result = np.zeros((n, 14), dtype=np.float64)
            result[:, _TIEBREAK_A_WIN_IDX] = t_ab
            result[:, _TIEBREAK_B_WIN_IDX] = 1.0 - t_ab
            memo[(a, b)] = result
            return result

        # Figure out who serves the next game.
        total_games_played = a + b
        if a_serves_first:
            a_serving = (total_games_played % 2 == 0)
        else:
            a_serving = (total_games_played % 2 == 1)

        if a_serving:
            hold_a = _hold("A", a, b)
            p_a_wins_game = hold_a
        else:
            hold_b = _hold("B", a, b)
            p_a_wins_game = 1.0 - hold_b

        next_a = _from(a + 1, b)
        next_b = _from(a, b + 1)
        result = (
            p_a_wins_game[:, None] * next_a
            + (1.0 - p_a_wins_game)[:, None] * next_b
        )
        memo[(a, b)] = result
        return result

    return _from(0, 0)


def match_distribution_from_state_fn(
    p_a_fn: ServeStateFn,
    p_b_fn: ServeStateFn,
    p_a_avg: np.ndarray,
    p_b_avg: np.ndarray,
    best_of: np.ndarray,
) -> MatchDistribution:
    """Compute match distribution using stateful per-point probabilities.

    Iterates over sets, calling `set_score_distribution_from_state_fn` at each
    (sets_won_a, sets_won_b) state with the match context threaded through so
    set-point / match-point flags are derived correctly.

    `p_a_avg` / `p_b_avg` are used for the tiebreak approximation only.
    """
    n = len(p_a_avg)
    # Implementation reuses chain._match_marginals's DP structure, but with a
    # set-score-PMF that depends on (sets_won_a, sets_won_b). We inline a
    # context-aware version here.

    if n == 0:
        max_total = 5 * 13
        spread_size = 2 * max_total + 1
        return MatchDistribution(
            p_match_win_a=np.zeros(0, dtype=np.float64),
            set_outcome_probs={},
            total_games_pmf=np.zeros((0, max_total + 1), dtype=np.float64),
            spread_pmf=np.zeros((0, spread_size), dtype=np.float64),
            spread_offset=max_total,
            expected_total_games=np.zeros(0, dtype=np.float64),
            expected_spread=np.zeros(0, dtype=np.float64),
        )

    # We require a single `best_of` per match; the cache is keyed on sets_won
    # AND best_of. For vectorized clarity we group matches by best_of.
    best_of_arr = np.atleast_1d(np.asarray(best_of, dtype=np.int64))
    if best_of_arr.shape != (n,):
        raise ValueError(f"best_of shape {best_of_arr.shape} != ({n},)")
    invalid = (best_of_arr != 3) & (best_of_arr != 5)
    if invalid.any():
        raise ValueError(f"best_of must be 3 or 5; got {np.unique(best_of_arr[invalid]).tolist()}")

    max_total = 5 * 13
    spread_offset = max_total
    spread_size = 2 * max_total + 1

    total_games_pmf = np.zeros((n, max_total + 1), dtype=np.float64)
    spread_pmf = np.zeros((n, spread_size), dtype=np.float64)
    set_outcome_probs: dict[tuple[int, int], np.ndarray] = {}

    for bo_const in (3, 5):
        mask = best_of_arr == bo_const
        if not mask.any():
            continue
        sub_p_a = p_a_avg[mask]
        sub_p_b = p_b_avg[mask]
        sub_n = int(mask.sum())

        # Wrap p_fn to only return probabilities for the masked subset.
        def _sub_fn(fn, m):
            def wrapped(state):
                full = np.asarray(fn(state), dtype=np.float64)
                return full[m]
            return wrapped

        sub_p_a_fn = _sub_fn(p_a_fn, mask)
        sub_p_b_fn = _sub_fn(p_b_fn, mask)

        sub_total, sub_spread, sub_set_outcomes = _match_marginals_stateful(
            sub_p_a_fn, sub_p_b_fn, sub_p_a, sub_p_b,
            bo_const, max_total, spread_offset, sub_n,
        )

        total_games_pmf[mask] = sub_total
        spread_pmf[mask] = sub_spread
        for key, vec in sub_set_outcomes.items():
            if key not in set_outcome_probs:
                set_outcome_probs[key] = np.zeros(n, dtype=np.float64)
            set_outcome_probs[key][mask] = vec

    p_match_win_a = np.zeros(n, dtype=np.float64)
    for (sa, sb), vec in set_outcome_probs.items():
        if sa > sb:
            p_match_win_a += vec
    # Floating-point accumulation can overshoot 1.0 by ~1e-16 for matches
    # where one player's win prob is mathematically 1.0; clip to enforce
    # the invariant before downstream consumers (e.g. sklearn's strict
    # brier_score_loss) reject the array.
    np.clip(p_match_win_a, 0.0, 1.0, out=p_match_win_a)

    total_idx = np.arange(max_total + 1, dtype=np.float64)
    expected_total_games = (total_games_pmf * total_idx).sum(axis=1)
    spread_idx = np.arange(spread_size, dtype=np.float64) - spread_offset
    expected_spread = (spread_pmf * spread_idx).sum(axis=1)

    return MatchDistribution(
        p_match_win_a=p_match_win_a,
        set_outcome_probs=set_outcome_probs,
        total_games_pmf=total_games_pmf,
        spread_pmf=spread_pmf,
        spread_offset=spread_offset,
        expected_total_games=expected_total_games,
        expected_spread=expected_spread,
    )


def _match_marginals_stateful(
    p_a_fn: ServeStateFn,
    p_b_fn: ServeStateFn,
    p_a_avg: np.ndarray,
    p_b_avg: np.ndarray,
    best_of_const: int,
    max_total: int,
    spread_offset: int,
    n: int,
) -> tuple[np.ndarray, np.ndarray, dict[tuple[int, int], np.ndarray]]:
    """Forward DP over (sets_a, sets_b) with a context-dependent set PMF.

    Unlike chain._match_marginals which uses one set PMF across all states,
    this recomputes the set PMF at each (sets_a, sets_b) so the set-point /
    match-point derivations reflect the actual match context.
    """
    target_sets = (best_of_const + 1) // 2
    spread_size = 2 * max_total + 1

    state_total: dict[tuple[int, int], np.ndarray] = {
        (0, 0): np.zeros((n, max_total + 1), dtype=np.float64),
    }
    state_spread: dict[tuple[int, int], np.ndarray] = {
        (0, 0): np.zeros((n, spread_size), dtype=np.float64),
    }
    state_total[(0, 0)][:, 0] = 1.0
    state_spread[(0, 0)][:, spread_offset] = 1.0

    total_games_terminal = np.zeros((n, max_total + 1), dtype=np.float64)
    spread_terminal = np.zeros((n, spread_size), dtype=np.float64)
    set_outcome_probs: dict[tuple[int, int], np.ndarray] = {}

    for _ in range(best_of_const):
        new_state_total: dict[tuple[int, int], np.ndarray] = {}
        new_state_spread: dict[tuple[int, int], np.ndarray] = {}
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

            # Set PMF conditioned on THIS match state (sets_won_a=sa, sets_won_b=sb).
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

                if a_wins_set:
                    next_key = (sa + 1, sb)
                else:
                    next_key = (sa, sb + 1)

                if next_key not in new_state_total:
                    new_state_total[next_key] = np.zeros_like(pmf_t)
                    new_state_spread[next_key] = np.zeros_like(pmf_s)
                new_state_total[next_key] += contribution_t
                new_state_spread[next_key] += contribution_s

        state_total = new_state_total
        state_spread = new_state_spread

    for (sa, sb), pmf_t in state_total.items():
        pmf_s = state_spread[(sa, sb)]
        total_games_terminal += pmf_t
        spread_terminal += pmf_s
        marginal = pmf_t.sum(axis=1)
        if (sa, sb) not in set_outcome_probs:
            set_outcome_probs[(sa, sb)] = np.zeros(n, dtype=np.float64)
        set_outcome_probs[(sa, sb)] += marginal

    return total_games_terminal, spread_terminal, set_outcome_probs
