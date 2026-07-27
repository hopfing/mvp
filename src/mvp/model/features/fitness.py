"""Fitness and durability features: retirement/walkover history."""


import polars as pl

from mvp.model.features._duration_helpers import (
    MATCH_POINTS,
    match_minutes,
    match_minutes_for_rates,
)
from mvp.model.primitives import (
    cumulative_count,
    cumulative_std,
    rolling_count,
    rolling_std_same_day,
    rolling_sum_same_day,
)
from mvp.model.registry import feature, register_diff

DATE_COL = "effective_match_date"


def _player_retired() -> pl.Expr:
    """1 if the player themselves retired mid-match (lost via RET).

    Retirement-only: walkovers (W/O) are a noisier durability signal (they
    happen for scheduling/illness/visa, not just injury) and are filtered out
    of the feature stream upstream anyway.
    """
    return (
        (pl.col("reason").fill_null("") == "RET")
        & ~pl.col("won").cast(pl.Boolean)
    ).cast(pl.Int64)


@feature(
    name="retirement_rate",
    params=["days"],
    description="Fraction of recent same-draw-type matches ending in player's own retirement",
    mirror=True,
    impute=None,
)
def retirement_rate(days: int | None = None) -> pl.Expr:
    """Rolling rate of player's own retirements/walkovers (singles-only when filtered)."""
    # Group by draw_type so doubles retirements don't pollute singles rate
    group_by = ["player_id", "draw_type"]
    retired = _player_retired()
    if days is None:
        ret_count = (
            retired.cum_sum().shift(1).over(group_by, order_by=[DATE_COL, "tournament_start_date", "round_order", "match_uid"]).fill_null(0)
        )
        total = cumulative_count(group_by=group_by)
    else:
        ret_count = (
            retired
            .rolling_sum_by(by=DATE_COL, window_size=f"{days}d", closed="left")
            .over(group_by)
            .fill_null(0)
        )
        total = rolling_count(days=days, group_by=group_by)
    return pl.when(total > 0).then(ret_count / total).otherwise(None)


@feature(
    name="last_match_retirement",
    params=[],
    description="1 if player's previous same-draw-type match ended in their own retirement",
    mirror=True,
    impute=None,
)
def last_match_retirement() -> pl.Expr:
    """Whether the player retired/walked over in their most recent same-draw-type match."""
    # Group by draw_type so a doubles retirement doesn't flag a singles match
    group_by = ["player_id", "draw_type"]
    return _player_retired().cast(pl.Float64).shift(1).over(group_by, order_by=[DATE_COL, "tournament_start_date", "round_order", "match_uid"])


# --- On-court workload (rolling, cross-tournament) ----------------------------
#
# The in-tournament `tourn_minutes_*` block resets at every event; these carry
# load across a player's whole recent schedule, which is what actually survives
# a player losing qualifying at one tournament and starting another mid-week.
#
# Two grouping keys, following the rule stated in `tournament_points.py`: pure
# minutes accumulate across draw types, anything divided by a stats-feed
# denominator does not.
#
#   WORKLOAD_GROUP — player_id alone. A doubles match is real time on court, and
#     duration coverage on tour+chal doubles is 98.3%, so it belongs in a load
#     signal. Used by the volume accumulators.
#   DRAW_GROUP — adds draw_type, so a singles row sees only singles history.
#     Used by every RATE, because `pts_total_pts_played` / `svc_games_played` /
#     `sets_played` describe a doubles TEAM's match rather than the individual's
#     — dividing pooled minutes by them mixes two incomparable denominators.
#     Also used by the `singles_*` counterparts of the volume features, and by
#     `minutes_std` for a third reason spelled out at its definition.
#
# Each pooled volume feature ships with its DRAW_GROUP twin, so the model can
# learn what a doubles minute costs instead of the pooled key hardcoding that it
# costs exactly what a singles minute does. Same pattern as
# `tourn_matches_played` / `tourn_singles_played`.
#
# `pooled - singles` is a real doubles quantity for `minutes_played`, whose
# accumulator is additive. It is NOT one for `last_match_minutes_per_rest_day`:
# the two halves key off different matches, so their difference means nothing
# and the twin stands on its own.
#
# These use the `*_same_day` primitives so an earlier match on the SAME date is
# inside the window. The plain `rolling_*` helpers exclude it: `closed="left"`
# drops ties, and pre-2026 `effective_match_date` carries no clock time, so
# every match a player plays on one date shares a timestamp. See
# `same_day_ordering_key` for why round_order ordering cannot leak forward.

ORDER = [DATE_COL, "tournament_start_date", "round_order", "match_uid"]
WORKLOAD_GROUP = ["player_id"]
DRAW_GROUP = ["player_id", "draw_type"]

_MINUTES = match_minutes()
_MINUTES_RATE = match_minutes_for_rates()
_GAMES = (
    pl.col("svc_games_played").cast(pl.Float64)
    + pl.col("ret_games_played").cast(pl.Float64)
)
_SETS = pl.col("sets_played").cast(pl.Float64)
_ONE = pl.lit(1.0)


def _window_sum(expr: pl.Expr, days: int | None, group: list[str]) -> pl.Expr:
    """Sum over the trailing window, or over all prior matches when days is None."""
    if days is None:
        return expr.cum_sum().shift(1).over(group, order_by=ORDER)
    return rolling_sum_same_day(expr, days=days, group_by=group, fill_with=None)


def _masked_total(expr: pl.Expr, days: int | None, group: list[str]) -> pl.Expr:
    """Windowed sum of known values; null until one exists in the window."""
    valid = expr.is_not_null()
    total = _window_sum(
        pl.when(valid).then(expr.cast(pl.Float64)).otherwise(0.0), days, group,
    )
    seen = _window_sum(valid.cast(pl.Int64), days, group)
    return pl.when(seen > 0).then(total).otherwise(None)


def _workload_ratio(
    num: pl.Expr, den: pl.Expr, days: int | None, group: list[str],
) -> pl.Expr:
    """Windowed ratio, accumulating only rows where BOTH sides are known."""
    valid = num.is_not_null() & den.is_not_null()
    cnum = _window_sum(
        pl.when(valid).then(num.cast(pl.Float64)).otherwise(0.0), days, group,
    )
    cden = _window_sum(
        pl.when(valid).then(den.cast(pl.Float64)).otherwise(0.0), days, group,
    )
    return pl.when(cden > 0).then(cnum / cden).otherwise(None)


def _load_over_recovery(group: list[str]) -> pl.Expr:
    """Last match's minutes over days of rest since it, within `group`.

    Both the minutes and the rest interval key off the SAME prior match — the
    most recent one carrying a usable duration — so the ratio never mixes one
    match's length with another's date.
    """
    known = _MINUTES.is_not_null()
    prev_minutes = (
        pl.when(known).then(_MINUTES)
        .shift(1).forward_fill().over(group, order_by=ORDER)
    )
    prev_date = (
        pl.when(known).then(pl.col(DATE_COL))
        .shift(1).forward_fill().over(group, order_by=ORDER)
    )
    rest = (pl.col(DATE_COL) - prev_date).dt.total_days().cast(pl.Float64)
    return prev_minutes / (rest + 1.0)


# --- volume: pooled across draw types, each with its singles-only twin --------


@feature(
    name="minutes_played", params=["days"], mirror=True, impute=None,
    description="On-court minutes over the trailing window (all draw types)",
)
def minutes_played(days: int | None = None) -> pl.Expr:
    return _masked_total(_MINUTES, days, WORKLOAD_GROUP)


@feature(
    name="singles_minutes_played", params=["days"], mirror=True, impute=None,
    description="On-court SINGLES minutes over the trailing window (doubles excluded)",
)
def singles_minutes_played(days: int | None = None) -> pl.Expr:
    return _masked_total(_MINUTES, days, DRAW_GROUP)


@feature(
    name="last_match_minutes_per_rest_day", params=[], mirror=True, impute=None,
    description="Minutes of the last match divided by days of rest since it (+1)",
)
def last_match_minutes_per_rest_day() -> pl.Expr:
    """Load-over-recovery: a long match yesterday weighs more than one last week."""
    return _load_over_recovery(WORKLOAD_GROUP)


@feature(
    name="singles_last_match_minutes_per_rest_day", params=[], mirror=True,
    impute=None,
    description="Minutes of the last SINGLES match over days of rest since it (+1)",
)
def singles_last_match_minutes_per_rest_day() -> pl.Expr:
    """Singles-only twin: an intervening doubles match neither supplies the
    minutes nor resets the rest clock. Same reason `days_since_singles` exists
    alongside `days_since_last_match` in `form.py`.
    """
    return _load_over_recovery(DRAW_GROUP)


# --- rates: DRAW_GROUP, since the denominators are draw-type-specific ---------
#
# `minutes_std` sits here too but scopes for its own reason — see its docstring.


@feature(
    name="minutes_per_point", params=["days"], mirror=True, impute=None,
    description="Minutes per point played over the trailing window (match tempo)",
)
def minutes_per_point(days: int | None = None) -> pl.Expr:
    return _workload_ratio(_MINUTES_RATE, MATCH_POINTS, days, DRAW_GROUP)


@feature(
    name="minutes_per_game", params=["days"], mirror=True, impute=None,
    description="Minutes per game played over the trailing window",
)
def minutes_per_game(days: int | None = None) -> pl.Expr:
    return _workload_ratio(_MINUTES_RATE, _GAMES, days, DRAW_GROUP)


@feature(
    name="minutes_per_set", params=["days"], mirror=True, impute=None,
    description="Minutes per set played over the trailing window",
)
def minutes_per_set(days: int | None = None) -> pl.Expr:
    return _workload_ratio(_MINUTES_RATE, _SETS, days, DRAW_GROUP)


@feature(
    name="minutes_per_match", params=["days"], mirror=True, impute=None,
    description="Minutes per match over the trailing window",
)
def minutes_per_match(days: int | None = None) -> pl.Expr:
    return _workload_ratio(_MINUTES_RATE, _ONE, days, DRAW_GROUP)


@feature(
    name="minutes_std", params=["days"], mirror=True, impute=None,
    description="Std of match length over the trailing window (tempo consistency)",
)
def minutes_std(days: int | None = None) -> pl.Expr:
    """Null with fewer than two prior matches carrying a usable duration.

    DRAW_GROUP for a reason of its own — this divides by nothing, so the rate
    argument above does not apply. Pooling two distributions with different
    means inflates the variance by the between-group term, so a player who
    simply plays both formats reads as an inconsistent one.

    Deliberately has no pooled twin: variance is not additive, so a
    `pooled - singles` difference would not be a doubles quantity the way it is
    for the volume sums.
    """
    if days is not None:
        return rolling_std_same_day(_MINUTES, days=days, group_by=DRAW_GROUP)
    return cumulative_std(_MINUTES, group_by=DRAW_GROUP)


# --- Derived diff features ---

register_diff("retirement_rate")
register_diff("last_match_retirement")

for _b in [
    "minutes_played", "singles_minutes_played",
    "minutes_per_point", "minutes_per_game", "minutes_per_set",
    "minutes_per_match", "minutes_std",
    "last_match_minutes_per_rest_day", "singles_last_match_minutes_per_rest_day",
]:
    register_diff(_b)
