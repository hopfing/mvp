"""Form and momentum features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import rolling_count
from mvp.model.registry import feature

DATE_COL = "effective_match_date"


@feature(
    name="win_streak",
    params=[],
    description="Current win streak (consecutive wins before this match)",
    mirror=True,
)
def win_streak() -> pl.Expr:
    """Current win streak before this match.

    Counts consecutive wins ending at the previous match.
    Resets to 0 after a loss.
    """
    # Use cumulative approach: count consecutive 1s before current row
    # When won=0, streak resets. We track streak ending at each row.
    won = pl.col("won")

    # Create groups that reset on each loss (won=0)
    # Group ID increments each time we see a loss
    loss_marker = (won == 0).cast(pl.Int64)
    group_id = loss_marker.cum_sum().over("player_id", order_by=DATE_COL)

    # Within each group, count wins (rows where won=1)
    # Shift by 1 to exclude current match
    streak = won.cum_sum().over(["player_id", group_id], order_by=DATE_COL).shift(1).over(
        "player_id", order_by=DATE_COL
    )

    return streak.fill_null(0)


@feature(
    name="loss_streak",
    params=[],
    description="Current loss streak (consecutive losses before this match)",
    mirror=True,
)
def loss_streak() -> pl.Expr:
    """Current loss streak before this match.

    Counts consecutive losses ending at the previous match.
    Resets to 0 after a win.
    """
    lost = (pl.col("won") == 0).cast(pl.Int64)

    # Group ID increments each time we see a win
    win_marker = pl.col("won").cast(pl.Int64)
    group_id = win_marker.cum_sum().over("player_id", order_by=DATE_COL)

    # Within each group, count losses
    streak = lost.cum_sum().over(["player_id", group_id], order_by=DATE_COL).shift(1).over(
        "player_id", order_by=DATE_COL
    )

    return streak.fill_null(0)


@feature(
    name="matches_last_30d",
    params=[],
    description="Matches played in last 30 days (activity/fatigue indicator)",
    mirror=True,
)
def matches_last_30d() -> pl.Expr:
    """Number of matches in the last 30 days."""
    return rolling_count(days=30, group_by="player_id")


@feature(
    name="matches_last_15d",
    params=[],
    description="Matches played in last 15 days (short-term activity)",
    mirror=True,
)
def matches_last_15d() -> pl.Expr:
    """Number of matches in the last 15 days."""
    return rolling_count(days=15, group_by="player_id")


@feature(
    name="matches_last_30d_diff",
    params=[],
    description="Matches last 30d difference (player - opponent)",
    depends_on=["matches_last_30d"],
    mirror=False,
)
def matches_last_30d_diff() -> pl.Expr:
    """Recent activity difference between player and opponent."""
    return pl.col("player_matches_last_30d") - pl.col("opp_matches_last_30d")


@feature(
    name="matches_last_15d_diff",
    params=[],
    description="Matches last 15d difference (player - opponent)",
    depends_on=["matches_last_15d"],
    mirror=False,
)
def matches_last_15d_diff() -> pl.Expr:
    """Short-term activity difference between player and opponent."""
    return pl.col("player_matches_last_15d") - pl.col("opp_matches_last_15d")
