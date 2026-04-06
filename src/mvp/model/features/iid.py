"""Features derived from the iid (independent, identically distributed) tennis model.

The iid model assumes each point on serve is won independently with
probability p. From this, we derive:

1. P(hold serve) — non-linear transformation of service point win rate
2. Expected games per set — via Markov chain over set scores
3. P(tiebreak) — probability a set reaches 6-6
"""

import numpy as np
import polars as pl

from mvp.model.registry import feature, register_diff, register_sum


# =============================================================================
# iid hold probability formula
# =============================================================================


def _iid_hold_probability(p: float) -> float:
    """Compute P(hold serve) given P(win point on serve) = p.

    Derived from the tennis game scoring tree:
    - Win in 4 pts (40-0): p^4
    - Win at 40-15: 4 * p^4 * (1-p)
    - Win at 40-30: 10 * p^4 * (1-p)^2
    - Reach deuce (3-3): 20 * p^3 * (1-p)^3, then win from deuce: p^2/(p^2 + (1-p)^2)
    """
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0
    q = 1 - p
    pre_deuce = p ** 4 * (1 + 4 * q + 10 * q ** 2)
    deuce_contrib = 20 * p ** 5 * q ** 3 / (p ** 2 + q ** 2)
    return pre_deuce + deuce_contrib


# =============================================================================
# Set Markov chain — precomputed lookup tables
# =============================================================================


def _compute_set_stats(h1: float, h2: float, a_serves_first: bool = True) -> tuple[float, float]:
    """Compute (expected_games, p_tiebreak) for a set via Markov chain.

    Args:
        h1: P(player A holds serve)
        h2: P(player B holds serve)
        a_serves_first: Whether player A serves the first game.

    States are (a, b) = games won by each player.
    Who serves is determined by game number (a+b) and first-server.
    """
    memo: dict[tuple[int, int], tuple[float, float]] = {}

    def _dp(a: int, b: int) -> tuple[float, float]:
        """Returns (expected_total_games, p_tiebreak) from state (a, b)."""
        if (a, b) in memo:
            return memo[(a, b)]

        total = a + b

        # Terminal states
        if a >= 6 and a - b >= 2 and a <= 7:
            result = (float(total), 0.0)
        elif b >= 6 and b - a >= 2 and b <= 7:
            result = (float(total), 0.0)
        elif a == 6 and b == 6:
            result = (13.0, 1.0)
        else:
            # Determine who serves based on game number and first-server
            a_serving = (total % 2 == 0) if a_serves_first else (total % 2 == 1)

            if a_serving:
                # A serves: hold → (a+1, b), break → (a, b+1)
                eg_hold, pt_hold = _dp(a + 1, b)
                eg_break, pt_break = _dp(a, b + 1)
                eg = h1 * eg_hold + (1 - h1) * eg_break
                pt = h1 * pt_hold + (1 - h1) * pt_break
            else:
                # B serves: hold → (a, b+1), break → (a+1, b)
                eg_hold, pt_hold = _dp(a, b + 1)
                eg_break, pt_break = _dp(a + 1, b)
                eg = h2 * eg_hold + (1 - h2) * eg_break
                pt = h2 * pt_hold + (1 - h2) * pt_break
            result = (eg, pt)

        memo[(a, b)] = result
        return result

    return _dp(0, 0)


def _compute_set_stats_avg(h1: float, h2: float) -> tuple[float, float]:
    """Average set stats over both possible first-server assignments."""
    eg_a, pt_a = _compute_set_stats(h1, h2, a_serves_first=True)
    eg_b, pt_b = _compute_set_stats(h1, h2, a_serves_first=False)
    return ((eg_a + eg_b) / 2, (pt_a + pt_b) / 2)


# Precompute lookup tables at import time
_GRID_SIZE = 101  # 0.00, 0.01, ..., 1.00
_EXPECTED_GAMES = np.zeros((_GRID_SIZE, _GRID_SIZE), dtype=np.float64)
_TIEBREAK_PROB = np.zeros((_GRID_SIZE, _GRID_SIZE), dtype=np.float64)

for _i in range(_GRID_SIZE):
    for _j in range(_GRID_SIZE):
        _h1 = _i / (_GRID_SIZE - 1)
        _h2 = _j / (_GRID_SIZE - 1)
        _eg, _pt = _compute_set_stats_avg(_h1, _h2)
        _EXPECTED_GAMES[_i, _j] = _eg
        _TIEBREAK_PROB[_i, _j] = _pt


def _lookup_from_table(table: np.ndarray, s: pl.Series) -> pl.Series:
    """Vectorized lookup in a precomputed 2D table from a struct series."""
    h1 = s.struct.field("h1").to_numpy()
    h2 = s.struct.field("h2").to_numpy()
    # Clamp and convert to grid indices
    i = np.clip(np.round(h1 * (_GRID_SIZE - 1)).astype(int), 0, _GRID_SIZE - 1)
    j = np.clip(np.round(h2 * (_GRID_SIZE - 1)).astype(int), 0, _GRID_SIZE - 1)
    return pl.Series(table[i, j])


# =============================================================================
# Features
# =============================================================================


@feature(
    name="iid_hold_prob",
    params=["days"],
    description="Theoretical P(hold serve) from iid model (non-linear transform of service point win rate)",
    depends_on=["pts_service_won_pct"],
    mirror=True,
)
def iid_hold_prob(days: int | None = None) -> pl.Expr:
    if days is None:
        p = pl.col("player_pts_service_won_pct")
    else:
        p = pl.col(f"player_pts_service_won_pct_{days}d")
    q = 1 - p
    pre_deuce = p ** 4 * (1 + 4 * q + 10 * q ** 2)
    deuce_contrib = 20 * p ** 5 * q ** 3 / (p ** 2 + q ** 2)
    return pre_deuce + deuce_contrib


register_diff("iid_hold_prob")
register_sum("iid_hold_prob")


@feature(
    name="iid_expected_games_per_set",
    params=["days"],
    description="Expected games per set from iid model (Markov chain over set scores)",
    depends_on=["iid_hold_prob"],
    mirror=False,
    match_level=True,
    impute="median",
)
def iid_expected_games_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        h1 = pl.col("player_iid_hold_prob")
        h2 = pl.col("opp_iid_hold_prob")
    else:
        h1 = pl.col(f"player_iid_hold_prob_{days}d")
        h2 = pl.col(f"opp_iid_hold_prob_{days}d")
    return pl.struct([h1.alias("h1"), h2.alias("h2")]).map_batches(
        lambda s: _lookup_from_table(_EXPECTED_GAMES, s),
        return_dtype=pl.Float64,
    )


@feature(
    name="iid_tiebreak_prob",
    params=["days"],
    description="Theoretical P(tiebreak) from iid model (Markov chain over set scores)",
    depends_on=["iid_hold_prob"],
    mirror=False,
    match_level=True,
    impute="median",
)
def iid_tiebreak_prob(days: int | None = None) -> pl.Expr:
    if days is None:
        h1 = pl.col("player_iid_hold_prob")
        h2 = pl.col("opp_iid_hold_prob")
    else:
        h1 = pl.col(f"player_iid_hold_prob_{days}d")
        h2 = pl.col(f"opp_iid_hold_prob_{days}d")
    return pl.struct([h1.alias("h1"), h2.alias("h2")]).map_batches(
        lambda s: _lookup_from_table(_TIEBREAK_PROB, s),
        return_dtype=pl.Float64,
    )
