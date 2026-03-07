"""Form and momentum features."""


import polars as pl

from mvp.model.primitives import rolling_count
from mvp.model.registry import feature


@feature(
    name="match_count",
    params=["days"],
    description="Matches played in rolling window (activity/fatigue indicator)",
    mirror=True,
)
def match_count(days: int | None = None) -> pl.Expr:
    """Number of matches in a rolling window.

    Only meaningful with a days parameter (e.g. days=30).
    The alltime variant (days=None) returns cumulative count,
    which is rarely useful as a predictor.
    """
    if days is None:
        return pl.col("player_id").cum_count().over("player_id") - 1
    return rolling_count(days=days, group_by="player_id")


@feature(
    name="match_count_diff",
    params=["days"],
    description="Match count difference (player - opponent)",
    depends_on=["match_count"],
    mirror=False,
)
def match_count_diff(days: int | None = None) -> pl.Expr:
    """Activity difference between player and opponent."""
    if days is None:
        return pl.col("player_match_count") - pl.col("opp_match_count")
    return pl.col(f"player_match_count_{days}d") - pl.col(f"opp_match_count_{days}d")
