"""Tiebreak-related features."""


import polars as pl

from mvp.model.features._score_helpers import (
    deciding_set_flag as _deciding_set_flag,
    tiebreaks_played as _tiebreaks_played,
    tiebreaks_won as _tiebreaks_won,
)
from mvp.model.primitives import cumulative_sum, ratio_feature, rolling_sum
from mvp.model.registry import feature

DATE_COL = "effective_match_date"


@feature(
    name="tiebreak_win_pct",
    params=["days"],
    description="Tiebreak win percentage (windowed or all-time)",
    mirror=True,
    impute=0.5,
)
def tiebreak_win_pct(days: int | None = None) -> pl.Expr:
    """Percentage of tiebreaks won."""
    return ratio_feature(_tiebreaks_won(), _tiebreaks_played(), days)


@feature(
    name="tiebreak_win_pct_diff",
    params=["days"],
    description="Player tiebreak win pct minus opponent tiebreak win pct",
    depends_on=["tiebreak_win_pct"],
    mirror=False,
    impute=0,
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
    return ratio_feature(
        _tiebreaks_played(),
        pl.col("sets_played").cast(pl.Int64),
        days,
    )


@feature(
    name="tiebreaks_played",
    params=["days"],
    description="Total tiebreaks played (windowed or all-time)",
    mirror=True,
    impute=0,
)
def tiebreaks_played_feat(days: int | None = None) -> pl.Expr:
    """Volume of tiebreaks played."""
    played = _tiebreaks_played()
    if days is None:
        return cumulative_sum(played, group_by="player_id")
    return rolling_sum(played, days=days, group_by="player_id")


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
    return ratio_feature(deciding, total, days)


@feature(
    name="deciding_set_win_pct",
    params=["days"],
    description="Win percentage in deciding sets (windowed or all-time)",
    mirror=True,
    impute=0.5,
)
def deciding_set_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage when match goes to deciding set."""
    deciding = _deciding_set_flag()
    deciding_won = deciding * pl.col("won").cast(pl.Int64)
    return ratio_feature(deciding_won, deciding, days)


@feature(
    name="deciding_set_win_pct_diff",
    params=["days"],
    description="Player deciding set win pct minus opponent deciding set win pct",
    depends_on=["deciding_set_win_pct"],
    mirror=False,
    impute=0,
)
def deciding_set_win_pct_diff(days: int | None = None) -> pl.Expr:
    """Deciding set win percentage difference."""
    if days is None:
        return pl.col("player_deciding_set_win_pct") - pl.col("opp_deciding_set_win_pct")
    return pl.col(f"player_deciding_set_win_pct_{days}d") - pl.col(f"opp_deciding_set_win_pct_{days}d")
