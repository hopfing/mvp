"""Form and momentum features."""


import polars as pl

from mvp.model.primitives import rolling_count
from mvp.model.registry import feature, register_diff

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


register_diff("match_count")


@feature(
    name="match_count_max",
    params=["days"],
    description="Max of player and opp match count (ceiling activity)",
    depends_on=["match_count"],
    mirror=False,
    match_level=True,
    impute=0,
)
def match_count_max(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.max_horizontal("player_match_count", "opp_match_count")
    return pl.max_horizontal(f"player_match_count_{days}d", f"opp_match_count_{days}d")


@feature(
    name="days_since_last_match",
    params=[],
    description="Days since player's most recent match (any surface/tournament)",
    mirror=True,
    impute=None,
)
def days_since_last_match() -> pl.Expr:
    """Days since this player last played any match."""
    prev_date = pl.col(DATE_COL).shift(1).over("player_id", order_by=DATE_COL)
    return (pl.col(DATE_COL) - prev_date).dt.total_days().cast(pl.Float64)


register_diff("days_since_last_match")


@feature(
    name="days_since_singles",
    params=[],
    description="Days since player's most recent singles match (doubles excluded)",
    mirror=True,
    impute=None,
)
def days_since_singles() -> pl.Expr:
    """Days since this player last played a SINGLES match.

    Sibling to days_since_last_match, which counts any match as total court
    workload. This gates the look-back to singles, so a same-day doubles match
    no longer reads as zero rest — a doubles-free recency/rust signal. The most
    recent prior singles date is carried forward across intervening doubles rows.
    """
    singles_date = pl.when(pl.col("draw_type") == "singles").then(pl.col(DATE_COL))
    prev_singles = (
        singles_date.shift(1).forward_fill().over("player_id", order_by=DATE_COL)
    )
    return (pl.col(DATE_COL) - prev_singles).dt.total_days().cast(pl.Float64)


register_diff("days_since_singles")


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


register_diff("prev_tourn_round_reached")
