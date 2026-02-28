"""Win rate related features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_mean,
    rolling_count,
    rolling_mean,
)
from mvp.model.registry import feature


@feature(
    name="win_rate",
    params=["days"],
    description="Win rate (windowed or all-time)",
    mirror=True,
)
def win_rate(days: int | None = None) -> pl.Expr:
    """Win rate over past N days, or all-time if days is None.

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing the win rate.
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
    name="win_rate_diff",
    params=["days"],
    description="Difference between player and opponent win rate",
    depends_on=["win_rate"],
    mirror=False,
)
def win_rate_diff(days: int | None = None) -> pl.Expr:
    """Difference between player and opponent win rate.

    Requires win_rate to be computed first for both player and opponent.

    Args:
        days: Window size in days. If None, uses all-time.

    Returns:
        Polars expression computing player_win_rate - opp_win_rate.
    """
    if days is None:
        return pl.col("player_win_rate") - pl.col("opp_win_rate")
    return pl.col(f"player_win_rate_{days}d") - pl.col(f"opp_win_rate_{days}d")
