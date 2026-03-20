"""Tournament-context features: within-tournament cumulative stats."""


import polars as pl

from mvp.model.features._score_helpers import (
    sets_lost as _sets_lost,
    sets_won as _sets_won,
    total_games_lost as _total_games_lost,
    total_games_won as _total_games_won,
)
from mvp.model.registry import feature, register_diff

DATE_COL = "effective_match_date"
TOURN_GROUP = ["player_id", "tournament_id", "year", "draw_type"]


def _tourn_cumulative(expr: pl.Expr) -> pl.Expr:
    """Cumulative sum shifted by 1, grouped by tournament context."""
    return (
        expr.cum_sum()
        .shift(1)
        .over(TOURN_GROUP, order_by=DATE_COL)
        .fill_null(0)
    )


# --- Base features ---


@feature(
    name="tourn_sets_won",
    params=[],
    description="Cumulative sets won in tournament",
    mirror=True,
    impute=0,
)
def tourn_sets_won() -> pl.Expr:
    return _tourn_cumulative(_sets_won())


@feature(
    name="tourn_sets_lost",
    params=[],
    description="Cumulative sets lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_sets_lost() -> pl.Expr:
    return _tourn_cumulative(_sets_lost())


@feature(
    name="tourn_sets_margin",
    params=[],
    description="Cumulative sets won - lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_sets_margin() -> pl.Expr:
    return _tourn_cumulative(_sets_won() - _sets_lost())


@feature(
    name="tourn_games_won",
    params=[],
    description="Cumulative games won in tournament",
    mirror=True,
    impute=0,
)
def tourn_games_won() -> pl.Expr:
    return _tourn_cumulative(_total_games_won())


@feature(
    name="tourn_games_lost",
    params=[],
    description="Cumulative games lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_games_lost() -> pl.Expr:
    return _tourn_cumulative(_total_games_lost())


@feature(
    name="tourn_games_margin",
    params=[],
    description="Cumulative games won - lost in tournament",
    mirror=True,
    impute=0,
)
def tourn_games_margin() -> pl.Expr:
    return _tourn_cumulative(_total_games_won() - _total_games_lost())


@feature(
    name="tourn_matches_won",
    params=[],
    description="Matches won in this tournament so far",
    mirror=True,
    impute=0,
)
def tourn_matches_won() -> pl.Expr:
    return _tourn_cumulative(pl.col("won").cast(pl.Int64))


# Cross-draw-type workload: counts ALL matches (singles + doubles + qualifying)
TOURN_WORKLOAD_GROUP = ["player_id", "tournament_id", "year"]


@feature(
    name="tourn_matches_played",
    params=[],
    description="Total matches played at this tournament (all draw types incl. doubles)",
    mirror=True,
    impute=0,
)
def tourn_matches_played() -> pl.Expr:
    """Workload signal: counts singles, doubles, and qualifying matches."""
    return (
        pl.col(DATE_COL)
        .is_not_null()
        .cast(pl.Int64)
        .cum_sum()
        .shift(1)
        .over(TOURN_WORKLOAD_GROUP, order_by=DATE_COL)
        .fill_null(0)
    )


# --- Derived diff features ---

for _base in [
    "tourn_sets_won", "tourn_sets_lost", "tourn_sets_margin",
    "tourn_games_won", "tourn_games_lost", "tourn_games_margin",
    "tourn_matches_won", "tourn_matches_played",
]:
    register_diff(_base)
