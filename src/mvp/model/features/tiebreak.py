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


def _deciding_set_flag() -> pl.Expr:
    """1 if this match went to a deciding set, 0 otherwise."""
    return (pl.col("sets_played") == pl.col("number_of_sets")).cast(pl.Int64)


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


@feature(
    name="tiebreak_win_pct",
    params=["days"],
    description="Tiebreak win percentage (windowed or all-time)",
    mirror=True,
)
def tiebreak_win_pct(days: int | None = None) -> pl.Expr:
    """Percentage of tiebreaks won."""
    if days is None:
        return _cumulative_ratio(_tiebreaks_won(), _tiebreaks_played())
    return _rolling_ratio(_tiebreaks_won(), _tiebreaks_played(), days)


@feature(
    name="tiebreak_win_pct_diff",
    params=["days"],
    description="Player tiebreak win pct minus opponent tiebreak win pct",
    depends_on=["tiebreak_win_pct"],
    mirror=False,
)
def tiebreak_win_pct_diff(days: int | None = None) -> pl.Expr:
    """Tiebreak win percentage difference."""
    if days is None:
        return pl.col("player_tiebreak_win_pct") - pl.col("opp_tiebreak_win_pct")
    return pl.col(f"player_tiebreak_win_pct_{days}d") - pl.col(f"opp_tiebreak_win_pct_{days}d")


@feature(
    name="tiebreak_pct",
    params=["days"],
    description="Percentage of sets that go to tiebreak (windowed or all-time)",
    mirror=True,
)
def tiebreak_pct(days: int | None = None) -> pl.Expr:
    """How often player's sets go to tiebreak."""
    if days is None:
        return _cumulative_ratio(
            _tiebreaks_played(),
            pl.col("sets_played").cast(pl.Int64),
        )
    return _rolling_ratio(
        _tiebreaks_played(),
        pl.col("sets_played").cast(pl.Int64),
        days,
    )


@feature(
    name="tiebreaks_played",
    params=["days"],
    description="Total tiebreaks played (windowed or all-time)",
    mirror=True,
)
def tiebreaks_played(days: int | None = None) -> pl.Expr:
    """Volume of tiebreaks played."""
    played = _tiebreaks_played()
    if days is None:
        return played.cum_sum().shift(1).over("player_id", order_by=DATE_COL).fill_null(0)
    return (
        played
        .rolling_sum_by(by=DATE_COL, window_size=f"{days}d", closed="left")
        .over("player_id")
        .fill_null(0)
    )


@feature(
    name="deciding_set_pct",
    params=["days"],
    description="How often matches go to deciding set (windowed or all-time)",
    mirror=True,
)
def deciding_set_pct(days: int | None = None) -> pl.Expr:
    """Percentage of matches going to deciding set."""
    deciding = _deciding_set_flag()
    total = pl.col(DATE_COL).is_not_null().cast(pl.Int64)
    if days is None:
        return _cumulative_ratio(deciding, total)
    return _rolling_ratio(deciding, total, days)


@feature(
    name="deciding_set_win_pct",
    params=["days"],
    description="Win percentage in deciding sets (windowed or all-time)",
    mirror=True,
)
def deciding_set_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage when match goes to deciding set."""
    deciding = _deciding_set_flag()
    deciding_won = deciding * pl.col("won").cast(pl.Int64)
    if days is None:
        return _cumulative_ratio(deciding_won, deciding)
    return _rolling_ratio(deciding_won, deciding, days)


@feature(
    name="deciding_set_win_pct_diff",
    params=["days"],
    description="Player deciding set win pct minus opponent deciding set win pct",
    depends_on=["deciding_set_win_pct"],
    mirror=False,
)
def deciding_set_win_pct_diff(days: int | None = None) -> pl.Expr:
    """Deciding set win percentage difference."""
    if days is None:
        return pl.col("player_deciding_set_win_pct") - pl.col("opp_deciding_set_win_pct")
    return pl.col(f"player_deciding_set_win_pct_{days}d") - pl.col(f"opp_deciding_set_win_pct_{days}d")
