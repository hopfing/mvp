"""Score-depth features: how a player is winning (rolling window)."""


import polars as pl

from mvp.model.features._score_helpers import (
    blowout_sets as _blowout_sets,
)
from mvp.model.features._score_helpers import (
    is_straight_set_win as _is_straight_set_win,
)
from mvp.model.features._score_helpers import (
    tight_sets as _tight_sets,
)
from mvp.model.features._score_helpers import (
    total_games_lost as _total_games_lost,
)
from mvp.model.features._score_helpers import (
    total_games_won as _total_games_won,
)
from mvp.model.primitives import (
    cumulative_mean,
    cumulative_sum,
    ratio_feature,
    rolling_mean,
    rolling_sum,
)
from mvp.model.registry import feature, register_diff, register_sum


def _per_set(numerator: pl.Expr) -> pl.Expr:
    """Per-completed-set rate of ``numerator``.

    Null when ``sets_played`` is 0 (e.g. a first-set retirement: games were
    played but no set completed). Without the guard the bare divide yields
    ``inf`` — which, unlike a null, survives imputation and poisons every
    rolling/cumulative-window row that includes the match. Null matches these
    features' ``impute=None`` policy (it reaches the model as missing).
    """
    sets = pl.col("sets_played").cast(pl.Float64)
    return pl.when(sets > 0).then(numerator.cast(pl.Float64) / sets).otherwise(None)


def _games_won_per_set() -> pl.Expr:
    """Games won per set in this match."""
    return _per_set(_total_games_won())


def _games_lost_per_set() -> pl.Expr:
    """Games lost per set in this match."""
    return _per_set(_total_games_lost())


def _games_margin_per_set() -> pl.Expr:
    """(Games won - games lost) per set in this match."""
    return _per_set(_total_games_won() - _total_games_lost())


def _games_margin_per_set_won() -> pl.Expr:
    """Per-set games margin, but only on matches the player won (else null)."""
    return pl.when(pl.col("won").cast(pl.Int64) == 1).then(_games_margin_per_set()).otherwise(None)


def _games_margin_per_set_lost() -> pl.Expr:
    """Per-set games margin, but only on matches the player lost (else null)."""
    return pl.when(pl.col("won").cast(pl.Int64) == 0).then(_games_margin_per_set()).otherwise(None)


def _games_per_set() -> pl.Expr:
    """Total games per set in this match."""
    return _per_set(_total_games_won() + _total_games_lost())


# --- Base features ---


@feature(
    name="sets_per_match",
    params=["days"],
    description="Avg sets played per match in window",
    mirror=True,
    impute=None,
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
    impute=None,
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
    impute=None,
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
    impute=None,
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
    impute=None,
)
def games_margin_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_margin_per_set(), group_by="player_id")
    return rolling_mean(_games_margin_per_set(), days=days, group_by="player_id")


@feature(
    name="games_margin_per_set_won",
    params=["days"],
    description="Avg (games won - lost) per set over the player's WON matches in window",
    mirror=True,
    impute=None,
)
def games_margin_per_set_won(days: int | None = None) -> pl.Expr:
    """Dominance in wins: how decisively the player wins, conditioned on winning.

    Conditioning on the outcome keeps this from washing out for ~50% players the
    way the pooled (win+loss) ``games_margin_per_set`` does — a balanced record
    no longer cancels positive-margin wins against negative-margin losses.
    """
    if days is None:
        return cumulative_mean(_games_margin_per_set_won(), group_by="player_id")
    return rolling_mean(_games_margin_per_set_won(), days=days, group_by="player_id")


@feature(
    name="games_margin_per_set_lost",
    params=["days"],
    description="Avg (games won - lost) per set over the player's LOST matches in window",
    mirror=True,
    impute=None,
)
def games_margin_per_set_lost(days: int | None = None) -> pl.Expr:
    """Competitiveness in losses: how close vs. blown-out the player's losses are
    (negative; nearer 0 = more competitive). The half of the dominance picture no
    existing feature captures, and the most discriminating for middle-pack players.
    """
    if days is None:
        return cumulative_mean(_games_margin_per_set_lost(), group_by="player_id")
    return rolling_mean(_games_margin_per_set_lost(), days=days, group_by="player_id")


@feature(
    name="games_per_set",
    params=["days"],
    description="Avg total games per set in window (tightness)",
    mirror=True,
    impute=None,
)
def games_per_set(days: int | None = None) -> pl.Expr:
    if days is None:
        return cumulative_mean(_games_per_set(), group_by="player_id")
    return rolling_mean(_games_per_set(), days=days, group_by="player_id")


@feature(
    name="total_games_won",
    params=["days"],
    description="Avg total games won per match in window",
    mirror=True,
    impute=None,
)
def total_games_won(days: int | None = None) -> pl.Expr:
    expr = _total_games_won().cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="total_games_lost",
    params=["days"],
    description="Avg total games conceded per match in window",
    mirror=True,
    impute=None,
)
def total_games_lost(days: int | None = None) -> pl.Expr:
    expr = _total_games_lost().cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="total_games",
    params=["days"],
    description="Avg total games per match in window (match length tendency)",
    mirror=True,
    impute=None,
)
def total_games(days: int | None = None) -> pl.Expr:
    expr = (_total_games_won() + _total_games_lost()).cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by="player_id")
    return rolling_mean(expr, days=days, group_by="player_id")


@feature(
    name="recent_games_load",
    params=["days"],
    description="Total games played (won + lost) in window (physical load proxy)",
    mirror=True,
    impute=0,
)
def recent_games_load(days: int | None = None) -> pl.Expr:
    """Total games played in the window — better fatigue proxy than match count.

    A player who played three 5-set matches has more load than one
    who played three straight-set wins.
    """
    expr = (_total_games_won() + _total_games_lost()).cast(pl.Float64)
    if days is None:
        return cumulative_sum(expr, group_by="player_id")
    return rolling_sum(expr, days=days, group_by="player_id")


@feature(
    name="tight_set_pct",
    params=["days"],
    description="Fraction of sets at 7-5 or 7-6 (tight set tendency)",
    mirror=True,
    impute=None,
)
def tight_set_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(_tight_sets(), pl.col("sets_played").cast(pl.Int64), days)


@feature(
    name="blowout_set_pct",
    params=["days"],
    description="Fraction of sets at 6-0 or 6-1 (blowout tendency)",
    mirror=True,
    impute=None,
)
def blowout_set_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(_blowout_sets(), pl.col("sets_played").cast(pl.Int64), days)


# --- Derived diff features ---

for _base in [
    "sets_per_match", "straight_sets_win_pct", "games_won_per_set",
    "games_lost_per_set", "games_margin_per_set", "games_margin_per_set_won",
    "games_margin_per_set_lost", "games_per_set",
    "total_games_won", "total_games_lost", "total_games",
    "recent_games_load", "tight_set_pct", "blowout_set_pct",
]:
    register_diff(_base)

for _base in ["games_per_set", "sets_per_match", "total_games",
              "tight_set_pct", "blowout_set_pct"]:
    register_sum(_base)
