"""Opponent-quality-adjusted form features."""


import polars as pl

from mvp.model.primitives import cumulative_mean, ratio_feature, rolling_mean
from mvp.model.registry import feature, register_diff

# --- Base features ---


@feature(
    name="quality_win_rate",
    params=["days"],
    description="Win rate weighted by opponent Elo",
    mirror=True,
    impute="median",
)
def quality_win_rate(days: int | None = None) -> pl.Expr:
    """sum(won * opp_elo) / sum(opp_elo)."""
    won_weighted = pl.col("won").cast(pl.Float64) * pl.col("opp_elo")
    opp_elo = pl.col("opp_elo")
    return ratio_feature(won_weighted, opp_elo, days)


@feature(
    name="opp_elo_beaten_avg",
    params=["days"],
    description="Avg Elo of opponents beaten in window",
    mirror=True,
    impute="median",
)
def opp_elo_beaten_avg(days: int | None = None) -> pl.Expr:
    """Average Elo of opponents beaten."""
    beaten_elo = pl.when(pl.col("won").cast(pl.Boolean)).then(pl.col("opp_elo")).otherwise(None)
    beaten_elo_filled = beaten_elo.fill_null(0)
    beaten_count = pl.col("won").cast(pl.Int64)
    return ratio_feature(beaten_elo_filled, beaten_count, days)


@feature(
    name="opp_elo_faced_avg",
    params=["days"],
    description="Avg Elo of all opponents faced (strength of schedule)",
    mirror=True,
    impute=None,
)
def opp_elo_faced_avg(days: int | None = None) -> pl.Expr:
    """Average Elo of all opponents faced."""
    if days is None:
        return cumulative_mean(pl.col("opp_elo"), group_by="player_id")
    return rolling_mean(pl.col("opp_elo"), days=days, group_by="player_id")


# --- Derived diff features ---

register_diff("quality_win_rate")
register_diff("opp_elo_beaten_avg")
register_diff("opp_elo_faced_avg")
