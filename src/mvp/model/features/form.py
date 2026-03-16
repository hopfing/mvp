"""Form and momentum features."""


import polars as pl

from mvp.model.primitives import rolling_count
from mvp.model.registry import feature

DATE_COL = "effective_match_date"


@feature(
    name="match_count",
    params=["days"],
    description="Matches played in rolling window (activity/fatigue indicator)",
    mirror=True,
    impute=0,
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
    impute=0,
)
def match_count_diff(days: int | None = None) -> pl.Expr:
    """Activity difference between player and opponent."""
    if days is None:
        return pl.col("player_match_count") - pl.col("opp_match_count")
    return pl.col(f"player_match_count_{days}d") - pl.col(f"opp_match_count_{days}d")


@feature(
    name="days_since_last_match",
    params=[],
    description="Days since player's most recent match (any surface/tournament)",
    mirror=True,
    impute="median",
)
def days_since_last_match() -> pl.Expr:
    """Days since this player last played any match."""
    prev_date = pl.col(DATE_COL).shift(1).over("player_id", order_by=DATE_COL)
    return (pl.col(DATE_COL) - prev_date).dt.total_days().cast(pl.Float64)


@feature(
    name="days_since_last_match_diff",
    params=[],
    description="Player days_since_last_match minus opponent",
    depends_on=["days_since_last_match"],
    mirror=False,
    impute=0,
)
def days_since_last_match_diff() -> pl.Expr:
    """Days since last match difference (player - opponent)."""
    return pl.col("player_days_since_last_match") - pl.col("opp_days_since_last_match")


@feature(
    name="prev_tourn_round_reached",
    params=[],
    description="Round ordinal of player's last match in their previous same-draw-type tournament",
    mirror=True,
    impute="median",
)
def prev_tourn_round_reached() -> pl.Expr:
    """How deep the player went in their previous same-draw-type tournament."""
    # Group by draw_type so doubles tournaments don't affect singles signal
    return (
        pl.when(pl.col("tournament_id") != pl.col("tournament_id").shift(1))
        .then(pl.col("round_order").shift(1).cast(pl.Float64))
        .otherwise(None)
        .forward_fill()
    ).over(["player_id", "draw_type"], order_by=DATE_COL)


@feature(
    name="prev_tourn_round_reached_diff",
    params=[],
    description="Player prev_tourn_round_reached minus opponent",
    depends_on=["prev_tourn_round_reached"],
    mirror=False,
    impute=0,
)
def prev_tourn_round_reached_diff() -> pl.Expr:
    """Previous tournament depth difference (player - opponent)."""
    return pl.col("player_prev_tourn_round_reached") - pl.col("opp_prev_tourn_round_reached")
