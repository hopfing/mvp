"""Tiebreak-related features."""


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
    name="tiebreak_win_pct",
    params=[],
    description="Tiebreak win percentage (all-time)",
    mirror=True,
)
def tiebreak_win_pct() -> pl.Expr:
    """Percentage of tiebreaks won."""
    won_expr = _tiebreaks_won()
    played_expr = _tiebreaks_played()
    won = won_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    played = played_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return pl.when(played > 0).then(won / played).otherwise(None)


@feature(
    name="tiebreak_win_pct_diff",
    params=[],
    description="Player tiebreak win pct minus opponent tiebreak win pct",
    depends_on=["tiebreak_win_pct"],
    mirror=False,
)
def tiebreak_win_pct_diff() -> pl.Expr:
    """Tiebreak win percentage difference."""
    return pl.col("player_tiebreak_win_pct") - pl.col("opp_tiebreak_win_pct")


@feature(
    name="tiebreak_pct",
    params=[],
    description="Percentage of sets that go to tiebreak (all-time)",
    mirror=True,
)
def tiebreak_pct() -> pl.Expr:
    """How often player's sets go to tiebreak."""
    played_expr = _tiebreaks_played()
    tiebreaks = played_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    sets = pl.col("sets_played").cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return pl.when(sets > 0).then(tiebreaks / sets).otherwise(None)


@feature(
    name="tiebreaks_played",
    params=[],
    description="Total tiebreaks played (all-time, clutch situation volume)",
    mirror=True,
)
def tiebreaks_played() -> pl.Expr:
    """Volume of tiebreaks played - measures clutch experience."""
    played_expr = _tiebreaks_played()
    return played_expr.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)


@feature(
    name="deciding_set_pct",
    params=[],
    description="How often matches go to deciding set (all-time)",
    mirror=True,
)
def deciding_set_pct() -> pl.Expr:
    """Percentage of matches going to deciding set (3rd in best-of-3, 5th in best-of-5)."""
    deciding = (pl.col("sets_played") == pl.col("number_of_sets")).cast(pl.Int64)
    deciding_sum = deciding.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    total = pl.lit(1).cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return pl.when(total > 0).then(deciding_sum / total).otherwise(None)


@feature(
    name="deciding_set_win_pct",
    params=[],
    description="Win percentage in deciding sets (all-time)",
    mirror=True,
)
def deciding_set_win_pct() -> pl.Expr:
    """Win percentage when match goes to deciding set."""
    deciding = (pl.col("sets_played") == pl.col("number_of_sets")).cast(pl.Int64)
    deciding_won = deciding * pl.col("won").cast(pl.Int64)
    wins = deciding_won.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    played = deciding.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return pl.when(played > 0).then(wins / played).otherwise(None)


@feature(
    name="deciding_set_win_pct_diff",
    params=[],
    description="Player deciding set win pct minus opponent deciding set win pct",
    depends_on=["deciding_set_win_pct"],
    mirror=False,
)
def deciding_set_win_pct_diff() -> pl.Expr:
    """Deciding set win percentage difference."""
    return pl.col("player_deciding_set_win_pct") - pl.col("opp_deciding_set_win_pct")
