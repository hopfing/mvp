"""Opponent-quality-adjusted form features."""


import polars as pl

from mvp.model.registry import feature

DATE_COL = "effective_match_date"


def _rolling_ratio(numerator: pl.Expr, denominator: pl.Expr, days: int) -> pl.Expr:
    """Rolling sum(numerator) / rolling sum(denominator) over past N days."""
    num = numerator.rolling_sum_by(by=DATE_COL, window_size=f"{days}d", closed="left").over("player_id").fill_null(0)
    den = denominator.rolling_sum_by(by=DATE_COL, window_size=f"{days}d", closed="left").over("player_id").fill_null(0)
    return pl.when(den > 0).then(num / den).otherwise(None)


def _cumulative_ratio(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    """Cumulative sum(numerator) / cumulative sum(denominator), excluding current row."""
    num = numerator.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    den = denominator.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return pl.when(den > 0).then(num / den).otherwise(None)


def _rolling_mean(expr: pl.Expr, days: int) -> pl.Expr:
    """Rolling mean of an expression over past N days."""
    return expr.rolling_mean_by(by=DATE_COL, window_size=f"{days}d", closed="left").over("player_id")


def _cumulative_mean(expr: pl.Expr) -> pl.Expr:
    """Cumulative mean of an expression, excluding current row."""
    cum_s = expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL)
    cum_c = (
        pl.col(DATE_COL).is_not_null().cast(pl.Int64)
        .cum_sum().shift(1).over("player_id", order_by=DATE_COL)
    )
    return cum_s / cum_c


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
    if days is None:
        return _cumulative_ratio(won_weighted, opp_elo)
    return _rolling_ratio(won_weighted, opp_elo, days)


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
    if days is None:
        # Cumulative: sum(beaten_elo) / count(beaten)
        beaten_elo_filled = beaten_elo.fill_null(0)
        beaten_count = pl.col("won").cast(pl.Int64)
        return _cumulative_ratio(beaten_elo_filled, beaten_count)
    # Rolling: sum(beaten_elo) / count(beaten)
    beaten_elo_filled = beaten_elo.fill_null(0)
    beaten_count = pl.col("won").cast(pl.Int64)
    return _rolling_ratio(beaten_elo_filled, beaten_count, days)


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
        return _cumulative_mean(pl.col("opp_elo"))
    return _rolling_mean(pl.col("opp_elo"), days)


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
