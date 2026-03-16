"""Head-to-head features."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_sum,
    rolling_count,
    rolling_sum,
)
from mvp.model.registry import feature


@feature(
    name="h2h_wins",
    params=["days"],
    description="Wins against specific opponent (windowed or all-time)",
    mirror=True,
    impute=0,
)
def h2h_wins(days: int | None = None) -> pl.Expr:
    """Wins against specific opponent.

    Groups by [player_id, opp_id] to track head-to-head record.

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing h2h wins.
    """
    if days is None:
        return cumulative_sum("won", group_by=["player_id", "opp_id"])
    return rolling_sum("won", days=days, group_by=["player_id", "opp_id"])


@feature(
    name="h2h_win_pct",
    params=["days"],
    description="Win percentage against specific opponent (windowed or all-time)",
    mirror=True,
    impute=0.5,
)
def h2h_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage against specific opponent."""
    group_by = ["player_id", "opp_id"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)


@feature(
    name="h2h_wins_diff",
    params=["days"],
    description="H2H wins difference (player - opponent)",
    depends_on=["h2h_wins"],
    mirror=False,
)
def h2h_wins_diff(days: int | None = None) -> pl.Expr:
    """H2H wins difference between player and opponent."""
    if days is None:
        return pl.col("player_h2h_wins") - pl.col("opp_h2h_wins")
    return pl.col(f"player_h2h_wins_{days}d") - pl.col(f"opp_h2h_wins_{days}d")


@feature(
    name="h2h_surface_wins",
    params=["days"],
    description="Wins against opponent on current surface (windowed or all-time)",
    mirror=True,
    impute=0,
)
def h2h_surface_wins(days: int | None = None) -> pl.Expr:
    """Wins against specific opponent on current surface."""
    group_by = ["player_id", "opp_id", "surface"]
    if days is None:
        return cumulative_sum("won", group_by=group_by)
    return rolling_sum("won", days=days, group_by=group_by)


@feature(
    name="h2h_surface_win_pct",
    params=["days"],
    description="Win pct against opponent on current surface (windowed or all-time)",
    mirror=True,
    impute=0.5,
)
def h2h_surface_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage against opponent on current surface."""
    group_by = ["player_id", "opp_id", "surface"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)
