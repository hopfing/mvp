"""Form and momentum features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import rolling_count
from mvp.model.registry import feature

DATE_COL = "effective_match_date"


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
