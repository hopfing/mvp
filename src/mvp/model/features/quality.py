"""Opponent-quality-adjusted form features."""


import polars as pl

from mvp.model.primitives import cumulative_mean, ratio_feature, rolling_mean
from mvp.model.registry import feature


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
    impute="median",
)
def opp_elo_faced_avg(days: int | None = None) -> pl.Expr:
    """Average Elo of all opponents faced."""
    if days is None:
        return cumulative_mean(pl.col("opp_elo"), group_by="player_id")
    return rolling_mean(pl.col("opp_elo"), days=days, group_by="player_id")


# --- Derived diff features ---


@feature(
    name="quality_win_rate_diff",
    params=["days"],
    description="Player - opponent quality win rate",
    depends_on=["quality_win_rate"],
    mirror=False,
    impute=0,
)
def quality_win_rate_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_quality_win_rate") - pl.col("opp_quality_win_rate")
    return pl.col(f"player_quality_win_rate_{days}d") - pl.col(f"opp_quality_win_rate_{days}d")


@feature(
    name="opp_elo_beaten_avg_diff",
    params=["days"],
    description="Player - opponent avg beaten Elo",
    depends_on=["opp_elo_beaten_avg"],
    mirror=False,
    impute=0,
)
def opp_elo_beaten_avg_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_opp_elo_beaten_avg") - pl.col("opp_opp_elo_beaten_avg")
    return pl.col(f"player_opp_elo_beaten_avg_{days}d") - pl.col(f"opp_opp_elo_beaten_avg_{days}d")


@feature(
    name="opp_elo_faced_avg_diff",
    params=["days"],
    description="Player - opponent avg faced Elo",
    depends_on=["opp_elo_faced_avg"],
    mirror=False,
    impute=0,
)
def opp_elo_faced_avg_diff(days: int | None = None) -> pl.Expr:
    if days is None:
        return pl.col("player_opp_elo_faced_avg") - pl.col("opp_opp_elo_faced_avg")
    return pl.col(f"player_opp_elo_faced_avg_{days}d") - pl.col(f"opp_opp_elo_faced_avg_{days}d")
