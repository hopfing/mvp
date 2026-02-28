"""Win rate related features."""

from __future__ import annotations

import polars as pl

from mvp.experimentation.primitives import rolling_count, rolling_mean
from mvp.experimentation.registry import feature


@feature(
    name="win_rate",
    params=["days"],
    description="Rolling win rate over past N days",
    mirror=True,
)
def win_rate(days: int) -> pl.Expr:
    """Rolling win rate over past N days.

    Args:
        days: Window size in days.

    Returns:
        Polars expression computing the rolling win rate.
    """
    return rolling_mean("won", days=days, group_by="player_id")


@feature(
    name="matches_played",
    params=["days"],
    description="Number of matches played in past N days",
    mirror=True,
)
def matches_played(days: int) -> pl.Expr:
    """Number of matches played in past N days.

    Args:
        days: Window size in days.

    Returns:
        Polars expression computing the rolling match count.
    """
    return rolling_count(days=days, group_by="player_id")


@feature(
    name="win_rate_diff",
    params=["days"],
    description="Difference between player and opponent win rate",
    depends_on=["win_rate"],
    mirror=False,
)
def win_rate_diff(days: int) -> pl.Expr:
    """Difference between player and opponent win rate.

    Requires win_rate to be computed first for both player and opponent.

    Args:
        days: Window size in days.

    Returns:
        Polars expression computing player_win_rate - opp_win_rate.
    """
    return pl.col(f"player_win_rate_{days}d") - pl.col(f"opp_win_rate_{days}d")
