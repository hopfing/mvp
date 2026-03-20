"""Win rate related features."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_mean,
    rolling_count,
    rolling_mean,
)
from mvp.model.registry import feature, register_diff


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
    impute=0,
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


register_diff("win_pct")
