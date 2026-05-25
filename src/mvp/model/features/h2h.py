"""Head-to-head features."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_sum,
    rolling_count,
    rolling_sum,
)
from mvp.model.registry import feature, register_diff


@feature(
    name="h2h_wins",
    params=["days"],
    description="Wins against specific opponent (windowed or all-time)",
    mirror=True,
    impute=None,
)
def h2h_wins(days: int | None = None) -> pl.Expr:
    """Wins against specific opponent.

    Groups by [player_id, opp_id] to track head-to-head record. Result
    count, so first-occurrence (no prior matches vs this opp) returns NaN
    rather than 0 — otherwise "never played them" conflates with "played
    and lost every prior H2H match".

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing h2h wins.
    """
    if days is None:
        return cumulative_sum("won", group_by=["player_id", "opp_id"], fill_with=None)
    return rolling_sum("won", days=days, group_by=["player_id", "opp_id"], fill_with=None)


@feature(
    name="h2h_win_pct",
    params=["days"],
    description="Win percentage against specific opponent (windowed or all-time)",
    mirror=True,
    impute=None,
)
def h2h_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage against specific opponent.

    Returns NaN when no prior matches vs this opp (the engine's
    ``otherwise(None)`` already produces NaN; ``impute=None`` preserves it
    rather than filling with the 0.5 prior — XGB's missing-direction split
    distinguishes "no info" from "low-sample 50% win rate" natively).
    """
    group_by = ["player_id", "opp_id"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)


register_diff("h2h_wins")


@feature(
    name="h2h_surface_wins",
    params=["days"],
    description="Wins against opponent on current surface (windowed or all-time)",
    mirror=True,
    impute=None,
)
def h2h_surface_wins(days: int | None = None) -> pl.Expr:
    """Wins against specific opponent on current surface. NaN on first
    same-surface encounter — same conflation rationale as h2h_wins."""
    group_by = ["player_id", "opp_id", "surface"]
    if days is None:
        return cumulative_sum("won", group_by=group_by, fill_with=None)
    return rolling_sum("won", days=days, group_by=group_by, fill_with=None)


@feature(
    name="h2h_surface_win_pct",
    params=["days"],
    description="Win pct against opponent on current surface (windowed or all-time)",
    mirror=True,
    impute=None,
)
def h2h_surface_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage against opponent on current surface. NaN when no
    prior same-surface matches vs this opp."""
    group_by = ["player_id", "opp_id", "surface"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)
