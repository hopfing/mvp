"""Pure tennis math: IID/Markov chain from serve point win prob to match distribution.

Given each player's per-point serve win probability, this module computes the joint
distribution over match outcomes via the standard tennis chain:

    point → game → set → match

All functions are pure and vectorized over numpy arrays. No I/O, no polars, no
features-engine dependencies. The set-level Markov DP and the lookup-table pattern
mirror src/mvp/model/features/iid.py:22-125.

Modeling assumptions (v1):
    - Points are independent within a service game (the "I" in IID).
    - Tiebreak win prob is averaged over both first-server assignments — see
      `p_tiebreak_game_win`. The actual ATP rule alternates first-server based on
      the previous set's last-game parity; for v1 we accept the small bias.
    - All tiebreaks are 7-point. Some Grand Slam Bo5 deciders use 10-point
      super-tiebreaks since ~2022; that bias is not corrected here.
"""

from dataclasses import dataclass
from typing import Final

import numpy as np


# =============================================================================
# Set score labels and per-score game/spread mappings
# =============================================================================

SET_SCORE_LABELS: Final[tuple[str, ...]] = (
    "6-0", "6-1", "6-2", "6-3", "6-4", "7-5", "7-6",
    "0-6", "1-6", "2-6", "3-6", "4-6", "5-7", "6-7",
)

_SET_SCORE_GAMES: Final[np.ndarray] = np.array(
    [
        (6, 0, 1), (6, 1, 1), (6, 2, 1), (6, 3, 1), (6, 4, 1),
        (7, 5, 1), (7, 6, 1),
        (0, 6, 0), (1, 6, 0), (2, 6, 0), (3, 6, 0), (4, 6, 0),
        (5, 7, 0), (6, 7, 0),
    ],
    dtype=np.int64,
)
_SET_TOTAL_GAMES: Final[np.ndarray] = (
    _SET_SCORE_GAMES[:, 0] + _SET_SCORE_GAMES[:, 1]
)
_SET_SPREAD: Final[np.ndarray] = (
    _SET_SCORE_GAMES[:, 0] - _SET_SCORE_GAMES[:, 1]
)
_SET_A_WINS: Final[np.ndarray] = _SET_SCORE_GAMES[:, 2].astype(bool)


# =============================================================================
# Game-level: hold probability and tiebreak game win probability
# =============================================================================


def p_service_game_win(p: np.ndarray | float) -> np.ndarray:
    """P(hold serve) given P(win point on serve) = p.

    Closed-form sum over the tennis game scoring tree (4-0, 4-1, 4-2, deuce).
    Vectorized analogue of `_iid_hold_probability` at
    src/mvp/model/features/iid.py:22-38.
    """
    p_arr = np.asarray(p, dtype=np.float64)
    q = 1.0 - p_arr
    pre_deuce = p_arr ** 4 * (1.0 + 4.0 * q + 10.0 * q ** 2)
    # p^2 + q^2 is in [0.5, 1] for p in [0, 1] — never zero
    deuce_contrib = 20.0 * p_arr ** 5 * q ** 3 / (p_arr ** 2 + q ** 2)
    return pre_deuce + deuce_contrib


def _scalar_tiebreak_win_prob_a_first(
    p_a: float, p_b: float, max_points: int = 50,
) -> float:
    """P(player A wins a 7-point tiebreak), assuming A serves point 1.

    Standard 7-point tiebreak: first to 7 points, win by 2. Server alternates
    A, BB, AA, BB, AA, ... (A serves point 1, then pairs after that).

    The recursion is exact except for a 0.5 fallback at extreme depth, which
    has effectively zero probability mass.
    """
    if p_a >= 1.0 and p_b <= 0.0:
        return 1.0
    if p_a <= 0.0 and p_b >= 1.0:
        return 0.0

    memo: dict[tuple[int, int], float] = {}

    def _dp(a: int, b: int) -> float:
        if a >= 7 and a - b >= 2:
            return 1.0
        if b >= 7 and b - a >= 2:
            return 0.0
        if a + b >= max_points:
            return 0.5
        if (a, b) in memo:
            return memo[(a, b)]

        pt_number = a + b + 1
        if pt_number == 1:
            a_serves = True
        else:
            # Pairs after pt 1: (2,3) B, (4,5) A, (6,7) B, (8,9) A, ...
            pair_index = (pt_number - 2) // 2
            a_serves = (pair_index % 2 == 1)

        p_a_wins_pt = p_a if a_serves else (1.0 - p_b)
        result = (
            p_a_wins_pt * _dp(a + 1, b)
            + (1.0 - p_a_wins_pt) * _dp(a, b + 1)
        )
        memo[(a, b)] = result
        return result

    return _dp(0, 0)


# Precomputed lookup table: cell [i, j] holds P(A wins tiebreak | A serves first)
# for p_a = i/100, p_b = j/100. Mirrors the lookup-table pattern at
# src/mvp/model/features/iid.py:104-115.
_TIEBREAK_GRID_SIZE: Final[int] = 101
_TIEBREAK_WIN_PROB_TABLE_A_FIRST: Final[np.ndarray] = np.zeros(
    (_TIEBREAK_GRID_SIZE, _TIEBREAK_GRID_SIZE), dtype=np.float64,
)
for _i in range(_TIEBREAK_GRID_SIZE):
    for _j in range(_TIEBREAK_GRID_SIZE):
        _p_a = _i / (_TIEBREAK_GRID_SIZE - 1)
        _p_b = _j / (_TIEBREAK_GRID_SIZE - 1)
        _TIEBREAK_WIN_PROB_TABLE_A_FIRST[_i, _j] = (
            _scalar_tiebreak_win_prob_a_first(_p_a, _p_b)
        )


def _lookup_tiebreak_a_first(
    p_a: np.ndarray, p_b: np.ndarray,
) -> np.ndarray:
    """Vectorized lookup of P(A wins tiebreak | A serves first)."""
    i = np.clip(
        np.round(p_a * (_TIEBREAK_GRID_SIZE - 1)).astype(np.int64),
        0, _TIEBREAK_GRID_SIZE - 1,
    )
    j = np.clip(
        np.round(p_b * (_TIEBREAK_GRID_SIZE - 1)).astype(np.int64),
        0, _TIEBREAK_GRID_SIZE - 1,
    )
    return _TIEBREAK_WIN_PROB_TABLE_A_FIRST[i, j]


def p_tiebreak_game_win(
    p_a: np.ndarray | float, p_b: np.ndarray | float,
) -> np.ndarray:
    """P(player A wins a 7-point tiebreak), averaged over both first-server assignments.

    Vectorized via a precomputed 101x101 lookup table on a 0.01 grid. Inputs
    outside [0, 1] are clipped to the grid edges. Averaging over both first-
    server cases makes the function symmetric in the sense
    `p_tiebreak_game_win(p_a, p_b) + p_tiebreak_game_win(p_b, p_a) == 1`.
    """
    p_a_arr = np.atleast_1d(np.asarray(p_a, dtype=np.float64))
    p_b_arr = np.atleast_1d(np.asarray(p_b, dtype=np.float64))
    pwin_a_first = _lookup_tiebreak_a_first(p_a_arr, p_b_arr)
    # P(A wins | B serves first) = 1 - P(B wins | B serves first), and
    # P(B wins | B serves first) is just the lookup with the inputs swapped.
    pwin_b_first = 1.0 - _lookup_tiebreak_a_first(p_b_arr, p_a_arr)
    return 0.5 * (pwin_a_first + pwin_b_first)


# =============================================================================
# Set-level: full set-score distribution and set win probability
# =============================================================================


# Map (a_games, b_games) terminal state to its column index in SET_SCORE_LABELS,
# excluding (6, 6) which is handled by the tiebreak branch.
_SET_TERMINAL_IDX: Final[dict[tuple[int, int], int]] = {
    (6, 0): 0, (6, 1): 1, (6, 2): 2, (6, 3): 3, (6, 4): 4, (7, 5): 5,
    (0, 6): 7, (1, 6): 8, (2, 6): 9, (3, 6): 10, (4, 6): 11, (5, 7): 12,
}
_TIEBREAK_A_WIN_IDX: Final[int] = 6   # 7-6
_TIEBREAK_B_WIN_IDX: Final[int] = 13  # 6-7


def set_score_distribution(
    h_a: np.ndarray | float,
    h_b: np.ndarray | float,
    t_ab: np.ndarray | float,
) -> np.ndarray:
    """P(set ends in each of the 14 outcomes) per match, shape (N, 14).

    Averages over both first-server assignments. Each row sums to 1. Column
    order matches `SET_SCORE_LABELS`.

    Args:
        h_a: P(player A holds a service game), per match.
        h_b: P(player B holds a service game), per match.
        t_ab: P(player A wins a tiebreak game), per match.
    """
    h_a_arr = np.atleast_1d(np.asarray(h_a, dtype=np.float64))
    h_b_arr = np.atleast_1d(np.asarray(h_b, dtype=np.float64))
    t_ab_arr = np.atleast_1d(np.asarray(t_ab, dtype=np.float64))

    if not (h_a_arr.shape == h_b_arr.shape == t_ab_arr.shape):
        raise ValueError(
            f"Shape mismatch: h_a={h_a_arr.shape}, h_b={h_b_arr.shape}, t_ab={t_ab_arr.shape}"
        )

    if len(h_a_arr) == 0:
        return np.zeros((0, 14), dtype=np.float64)

    pmf_a_first = _set_score_pmf_one_server(
        h_a_arr, h_b_arr, t_ab_arr, a_serves_first=True,
    )
    pmf_b_first = _set_score_pmf_one_server(
        h_a_arr, h_b_arr, t_ab_arr, a_serves_first=False,
    )
    return 0.5 * (pmf_a_first + pmf_b_first)


def p_set_win(
    h_a: np.ndarray | float,
    h_b: np.ndarray | float,
    t_ab: np.ndarray | float,
) -> np.ndarray:
    """P(player A wins the set), per match. Vectorized."""
    pmf = set_score_distribution(h_a, h_b, t_ab)
    return pmf[:, :7].sum(axis=1)


def _set_score_pmf_one_server(
    h_a: np.ndarray,
    h_b: np.ndarray,
    t_ab: np.ndarray,
    *,
    a_serves_first: bool,
) -> np.ndarray:
    """Vectorized DP from set state (0, 0) to terminal set scores.

    Returns the (N, 14) distribution conditioned on a fixed first-server.
    """
    n_matches = len(h_a)
    memo: dict[tuple[int, int], np.ndarray] = {}

    def _from(a: int, b: int) -> np.ndarray:
        if (a, b) in memo:
            return memo[(a, b)]

        # Terminal A or B win without tiebreak
        if (a, b) in _SET_TERMINAL_IDX:
            result = np.zeros((n_matches, 14), dtype=np.float64)
            result[:, _SET_TERMINAL_IDX[(a, b)]] = 1.0
            memo[(a, b)] = result
            return result

        # 6-6 → tiebreak resolves to 7-6 (A) with prob t_ab or 6-7 (B) with prob 1 - t_ab
        if a == 6 and b == 6:
            result = np.zeros((n_matches, 14), dtype=np.float64)
            result[:, _TIEBREAK_A_WIN_IDX] = t_ab
            result[:, _TIEBREAK_B_WIN_IDX] = 1.0 - t_ab
            memo[(a, b)] = result
            return result

        # Determine who serves the next game in the set. The first game's server
        # is fixed by `a_serves_first`; subsequent games alternate.
        total_games_played = a + b
        if a_serves_first:
            a_serving = (total_games_played % 2 == 0)
        else:
            a_serving = (total_games_played % 2 == 1)

        if a_serving:
            p_a_wins_game = h_a  # A holds
        else:
            p_a_wins_game = 1.0 - h_b  # B fails to hold (A breaks)

        next_a = _from(a + 1, b)
        next_b = _from(a, b + 1)
        result = (
            p_a_wins_game[:, None] * next_a
            + (1.0 - p_a_wins_game)[:, None] * next_b
        )
        memo[(a, b)] = result
        return result

    return _from(0, 0)


# =============================================================================
# Match-level: Bo3 / Bo5 chain over set outcomes
# =============================================================================


@dataclass
class MatchDistribution:
    """Per-match summary distributions over tennis outcomes.

    All arrays are aligned by row (one row per match). Marginal pmfs are sized
    to the maximum Bo5 support so Bo3 and Bo5 matches can share storage; Bo3
    matches have zero mass past their natural maximum.
    """

    p_match_win_a: np.ndarray
    set_outcome_probs: dict[tuple[int, int], np.ndarray]
    total_games_pmf: np.ndarray
    spread_pmf: np.ndarray
    spread_offset: int
    expected_total_games: np.ndarray
    expected_spread: np.ndarray

    @property
    def expected_games_a(self) -> np.ndarray:
        return 0.5 * (self.expected_total_games + self.expected_spread)

    @property
    def expected_games_b(self) -> np.ndarray:
        return 0.5 * (self.expected_total_games - self.expected_spread)

    def p_over_total(self, line: float) -> np.ndarray:
        """P(total games strictly greater than `line`) per match."""
        threshold = int(np.floor(line)) + 1
        if threshold < 0:
            threshold = 0
        if threshold >= self.total_games_pmf.shape[1]:
            return np.zeros(self.total_games_pmf.shape[0], dtype=np.float64)
        return self.total_games_pmf[:, threshold:].sum(axis=1)

    def p_a_spread_cover(self, line: float) -> np.ndarray:
        """P((games_a - games_b) strictly greater than `line`) per match."""
        threshold = int(np.floor(line)) + 1 + self.spread_offset
        if threshold < 0:
            threshold = 0
        if threshold >= self.spread_pmf.shape[1]:
            return np.zeros(self.spread_pmf.shape[0], dtype=np.float64)
        return self.spread_pmf[:, threshold:].sum(axis=1)


def match_distribution(
    h_a: np.ndarray | float,
    h_b: np.ndarray | float,
    t_ab: np.ndarray | float,
    best_of: np.ndarray | int,
) -> MatchDistribution:
    """Compute per-match summary distributions from per-game/set IID inputs.

    Args:
        h_a: shape (N,) P(A holds a service game)
        h_b: shape (N,) P(B holds a service game)
        t_ab: shape (N,) P(A wins a 7-point tiebreak)
        best_of: shape (N,) array of 3 or 5 per match
    """
    h_a_arr = np.atleast_1d(np.asarray(h_a, dtype=np.float64))
    h_b_arr = np.atleast_1d(np.asarray(h_b, dtype=np.float64))
    t_ab_arr = np.atleast_1d(np.asarray(t_ab, dtype=np.float64))
    best_of_arr = np.atleast_1d(np.asarray(best_of, dtype=np.int64))

    if not (
        h_a_arr.shape == h_b_arr.shape == t_ab_arr.shape == best_of_arr.shape
    ):
        raise ValueError(
            f"Shape mismatch: h_a={h_a_arr.shape}, h_b={h_b_arr.shape}, "
            f"t_ab={t_ab_arr.shape}, best_of={best_of_arr.shape}"
        )

    n_matches = len(h_a_arr)
    # Max total games is best_of * 13 (every set going 7-6 tiebreak). Bo5 caps at 65.
    # Spread max |games_a - games_b| at any state is bounded by 6 * sets_played; we
    # share the bound for storage simplicity.
    max_total = 5 * 13
    spread_offset = max_total

    if n_matches == 0:
        return MatchDistribution(
            p_match_win_a=np.zeros(0, dtype=np.float64),
            set_outcome_probs={},
            total_games_pmf=np.zeros((0, max_total + 1), dtype=np.float64),
            spread_pmf=np.zeros((0, 2 * max_total + 1), dtype=np.float64),
            spread_offset=spread_offset,
            expected_total_games=np.zeros(0, dtype=np.float64),
            expected_spread=np.zeros(0, dtype=np.float64),
        )

    invalid_mask = (best_of_arr != 3) & (best_of_arr != 5)
    if invalid_mask.any():
        invalid_values = np.unique(best_of_arr[invalid_mask])
        raise ValueError(
            f"best_of must be 3 or 5; got {invalid_values.tolist()}"
        )

    set_pmf = set_score_distribution(h_a_arr, h_b_arr, t_ab_arr)

    total_games_pmf = np.zeros((n_matches, max_total + 1), dtype=np.float64)
    spread_pmf = np.zeros((n_matches, 2 * max_total + 1), dtype=np.float64)
    set_outcome_probs: dict[tuple[int, int], np.ndarray] = {}

    for best_of_const in (3, 5):
        mask = best_of_arr == best_of_const
        if not mask.any():
            continue
        sub_pmf = set_pmf[mask]
        sub_total, sub_spread, sub_set_outcomes = _match_marginals(
            sub_pmf, best_of_const, max_total, spread_offset,
        )
        total_games_pmf[mask] = sub_total
        spread_pmf[mask] = sub_spread
        for key, vec in sub_set_outcomes.items():
            if key not in set_outcome_probs:
                set_outcome_probs[key] = np.zeros(n_matches, dtype=np.float64)
            set_outcome_probs[key][mask] = vec

    p_match_win_a = np.zeros(n_matches, dtype=np.float64)
    for (sa, sb), vec in set_outcome_probs.items():
        if sa > sb:
            p_match_win_a += vec

    total_idx = np.arange(max_total + 1, dtype=np.float64)
    expected_total_games = (total_games_pmf * total_idx).sum(axis=1)
    spread_idx = np.arange(2 * max_total + 1, dtype=np.float64) - spread_offset
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


def _match_marginals(
    set_pmf: np.ndarray,
    best_of_const: int,
    max_total: int,
    spread_offset: int,
) -> tuple[np.ndarray, np.ndarray, dict[tuple[int, int], np.ndarray]]:
    """Forward DP over (sets_a, sets_b) states, emitting total_games and spread marginals.

    One iteration per set. Each iteration distributes the current state mass
    across the 14 set-score outcomes, accumulating shifted mass into next-state
    buckets and into terminal accumulators when a player reaches the set
    majority.
    """
    n_matches = set_pmf.shape[0]
    target_sets = (best_of_const + 1) // 2
    spread_size = 2 * max_total + 1

    state_total: dict[tuple[int, int], np.ndarray] = {
        (0, 0): np.zeros((n_matches, max_total + 1), dtype=np.float64),
    }
    state_spread: dict[tuple[int, int], np.ndarray] = {
        (0, 0): np.zeros((n_matches, spread_size), dtype=np.float64),
    }
    state_total[(0, 0)][:, 0] = 1.0
    state_spread[(0, 0)][:, spread_offset] = 1.0

    total_games_terminal = np.zeros((n_matches, max_total + 1), dtype=np.float64)
    spread_terminal = np.zeros((n_matches, spread_size), dtype=np.float64)
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
                    set_outcome_probs[(sa, sb)] = np.zeros(n_matches, dtype=np.float64)
                set_outcome_probs[(sa, sb)] += marginal
                continue

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

    # Any remaining state after the final iteration is terminal by construction.
    for (sa, sb), pmf_t in state_total.items():
        pmf_s = state_spread[(sa, sb)]
        total_games_terminal += pmf_t
        spread_terminal += pmf_s
        marginal = pmf_t.sum(axis=1)
        if (sa, sb) not in set_outcome_probs:
            set_outcome_probs[(sa, sb)] = np.zeros(n_matches, dtype=np.float64)
        set_outcome_probs[(sa, sb)] += marginal

    return total_games_terminal, spread_terminal, set_outcome_probs
