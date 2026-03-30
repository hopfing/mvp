"""Score-depth features: how a player is winning (rolling window)."""


import polars as pl

from mvp.model.features._score_helpers import (
    is_straight_set_win as _is_straight_set_win,
)
from mvp.model.features._score_helpers import (
    total_games_lost as _total_games_lost,
)
from mvp.model.features._score_helpers import (
    total_games_won as _total_games_won,
)
from mvp.model.primitives import cumulative_mean, ratio_feature, rolling_mean
from mvp.model.registry import feature, register_diff


def _games_won_per_set() -> pl.Expr:
    """Games won per set in this match."""
    return _total_games_won().cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


def _games_lost_per_set() -> pl.Expr:
    """Games lost per set in this match."""
    return _total_games_lost().cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


def _games_margin_per_set() -> pl.Expr:
    """(Games won - games lost) per set in this match."""
    return (_total_games_won() - _total_games_lost()).cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


def _games_per_set() -> pl.Expr:
    """Total games per set in this match."""
    return (_total_games_won() + _total_games_lost()).cast(pl.Float64) / pl.col("sets_played").cast(pl.Float64)


# --- Base features ---


@feature(
    name="sets_per_match",
    params=["days"],
    description="Avg sets played per match in window",
    mirror=True,
    impute="median",
)
def sets_per_match(days: int | None = None) -> pl.Expr:
    expr = pl.col("sets_played").cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="straight_sets_win_pct",
    params=["days"],
    description="Fraction of wins in straight sets",
    mirror=True,
    impute=0.5,
)
def straight_sets_win_pct(days: int | None = None) -> pl.Expr:
    wins = pl.col("won").cast(pl.Int64)
    ss_wins = _is_straight_set_win()
    return ratio_feature(ss_wins, wins, days)


@feature(
    name="games_won_per_set",
    params=["days"],
    description="Avg games won per set in window",
    mirror=True,
    impute="median",
)
def games_won_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_won_per_set(), group_by="player_id")
    return rolling_mean(_games_won_per_set(), days=days, group_by="player_id")


@feature(
    name="games_lost_per_set",
    params=["days"],
    description="Avg games lost per set in window",
    mirror=True,
    impute="median",
)
def games_lost_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_lost_per_set(), group_by="player_id")
    return rolling_mean(_games_lost_per_set(), days=days, group_by="player_id")


@feature(
    name="games_margin_per_set",
    params=["days"],
    description="Avg (games won - lost) per set in window",
    mirror=True,
    impute="median",
)
def games_margin_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_margin_per_set(), group_by="player_id")
    return rolling_mean(_games_margin_per_set(), days=days, group_by="player_id")


@feature(
    name="games_per_set",
    params=["days"],
    description="Avg total games per set in window (tightness)",
    mirror=True,
    impute="median",
)
def games_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_per_set(), group_by="player_id")
    return rolling_mean(_games_per_set(), days=days, group_by="player_id")


# --- Derived diff features ---

for _base in [
    "sets_per_match", "straight_sets_win_pct", "games_won_per_set",
    "games_lost_per_set", "games_margin_per_set", "games_per_set",
]:
    register_diff(_base)
