"""Win rate related features."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_mean,
    rolling_count,
    rolling_mean,
)
from mvp.model.registry import feature


@feature(
    name="win_pct",
    params=["days"],
    description="Win percentage (windowed or all-time)",
    mirror=True,
)
def win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage over past N days, or all-time if days is None.

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing the win percentage.
    """
    if days is None:
        return cumulative_mean("won", group_by="player_id")
    return rolling_mean("won", days=days, group_by="player_id")


@feature(
    name="matches_played",
    params=["days"],
    description="Number of matches played (windowed or all-time)",
    mirror=True,
)
def matches_played(days: int | None = None) -> pl.Expr:
    """Number of matches played in past N days, or all-time if days is None.

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing the match count.
    """
    if days is None:
        return cumulative_count(group_by="player_id")
    return rolling_count(days=days, group_by="player_id")


@feature(
    name="win_pct_diff",
    params=["days"],
    description="Difference between player and opponent win percentage",
    depends_on=["win_pct"],
    mirror=False,
)
def win_pct_diff(days: int | None = None) -> pl.Expr:
    """Difference between player and opponent win percentage.

    Requires win_pct to be computed first for both player and opponent.

    Args:
        days: Window size in days. If None, uses all-time.

    Returns:
        Polars expression computing player_win_pct - opp_win_pct.
    """
    if days is None:
        return pl.col("player_win_pct") - pl.col("opp_win_pct")
    return pl.col(f"player_win_pct_{days}d") - pl.col(f"opp_win_pct_{days}d")
