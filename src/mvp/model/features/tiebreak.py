"""Tiebreak-related features."""

from __future__ import annotations

import polars as pl

from mvp.model.registry import feature

DATE_COL = "effective_match_date"


def _tiebreaks_played() -> pl.Expr:
    """Count tiebreaks played in this match."""
    return (
        pl.col("player_set1_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set2_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set3_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set4_tiebreak").is_not_null().cast(pl.Int64)
        + pl.col("player_set5_tiebreak").is_not_null().cast(pl.Int64)
    )


def _tiebreaks_won() -> pl.Expr:
    """Count tiebreaks won in this match."""
    tb1_won = (pl.col("player_set1_tiebreak") > pl.col("opp_set1_tiebreak")).fill_null(False).cast(pl.Int64)
    tb2_won = (pl.col("player_set2_tiebreak") > pl.col("opp_set2_tiebreak")).fill_null(False).cast(pl.Int64)
    tb3_won = (pl.col("player_set3_tiebreak") > pl.col("opp_set3_tiebreak")).fill_null(False).cast(pl.Int64)
    tb4_won = (pl.col("player_set4_tiebreak") > pl.col("opp_set4_tiebreak")).fill_null(False).cast(pl.Int64)
    tb5_won = (pl.col("player_set5_tiebreak") > pl.col("opp_set5_tiebreak")).fill_null(False).cast(pl.Int64)
    return tb1_won + tb2_won + tb3_won + tb4_won + tb5_won


@feature(
    name="tiebreak_win_rate",
    params=["days"],
    description="Tiebreak win percentage",
    mirror=True,
)
def tiebreak_win_rate(days: int | None = None) -> pl.Expr:
    """Percentage of tiebreaks won."""
    won_expr = _tiebreaks_won()
    played_expr = _tiebreaks_played()

    if days is None:
        won = won_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
        played = played_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    else:
        won = (
            won_expr
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
        played = (
            played_expr
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
    return pl.when(played > 0).then(won / played).otherwise(None)


@feature(
    name="tiebreak_win_rate_diff",
    params=["days"],
    description="Player tiebreak win rate minus opponent tiebreak win rate",
    depends_on=["tiebreak_win_rate"],
    mirror=False,
)
def tiebreak_win_rate_diff(days: int | None = None) -> pl.Expr:
    """Tiebreak win rate difference."""
    if days is None:
        return pl.col("player_tiebreak_win_rate") - pl.col("opp_tiebreak_win_rate")
    return pl.col(f"player_tiebreak_win_rate_{days}d") - pl.col(f"opp_tiebreak_win_rate_{days}d")


@feature(
    name="tiebreak_pct",
    params=["days"],
    description="Percentage of sets that go to tiebreak",
    mirror=True,
)
def tiebreak_pct(days: int | None = None) -> pl.Expr:
    """How often player's sets go to tiebreak."""
    played_expr = _tiebreaks_played()

    if days is None:
        tiebreaks = played_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
        sets = pl.col("sets_played").cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    else:
        tiebreaks = (
            played_expr
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
        sets = (
            pl.col("sets_played")
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
    return pl.when(sets > 0).then(tiebreaks / sets).otherwise(None)


@feature(
    name="tiebreaks_played",
    params=["days"],
    description="Total tiebreaks played (clutch situation volume)",
    mirror=True,
)
def tiebreaks_played(days: int | None = None) -> pl.Expr:
    """Volume of tiebreaks played - measures clutch experience."""
    played_expr = _tiebreaks_played()

    if days is None:
        return played_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return (
        played_expr
        .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
        .over("player_id")
        .fill_null(0)
    )


@feature(
    name="deciding_set_rate",
    params=["days"],
    description="How often matches go to deciding set",
    mirror=True,
)
def deciding_set_rate(days: int | None = None) -> pl.Expr:
    """Rate of matches going to deciding set (3rd in best-of-3, 5th in best-of-5)."""
    deciding = (pl.col("sets_played") == pl.col("number_of_sets")).cast(pl.Int64)

    if days is None:
        deciding_sum = deciding.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
        total = pl.lit(1).cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    else:
        deciding_sum = (
            deciding
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
        total = (
            pl.lit(1)
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
    return pl.when(total > 0).then(deciding_sum / total).otherwise(None)


@feature(
    name="deciding_set_win_rate",
    params=["days"],
    description="Win rate in deciding sets",
    mirror=True,
)
def deciding_set_win_rate(days: int | None = None) -> pl.Expr:
    """Win rate when match goes to deciding set."""
    deciding = (pl.col("sets_played") == pl.col("number_of_sets")).cast(pl.Int64)
    deciding_won = deciding * pl.col("won").cast(pl.Int64)

    if days is None:
        wins = deciding_won.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
        played = deciding.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    else:
        wins = (
            deciding_won
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
        played = (
            deciding
            .rolling_sum_by(DATE_COL, window_size=f"{days}d", closed="left")
            .over("player_id")
            .fill_null(0)
        )
    return pl.when(played > 0).then(wins / played).otherwise(None)
