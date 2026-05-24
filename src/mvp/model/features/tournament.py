"""Tournament-context features: within-tournament cumulative stats."""


import polars as pl

from mvp.model.features._score_helpers import (
    sets_lost as _sets_lost,
)
from mvp.model.features._score_helpers import (
    sets_won as _sets_won,
)
from mvp.model.features._score_helpers import (
    total_games_lost as _total_games_lost,
)
from mvp.model.features._score_helpers import (
    total_games_won as _total_games_won,
)
from mvp.model.primitives import (
    cumulative_count,
    cumulative_mean,
    cumulative_sum,
)
from mvp.model.registry import feature, register_diff

DATE_COL = "effective_match_date"
TOURN_GROUP = ["player_id", "tournament_id", "year", "draw_type"]


def _tourn_cumulative(expr: pl.Expr, fill_with: int | None = 0) -> pl.Expr:
    """Cumulative sum shifted by 1, grouped by tournament context.

    Args:
        expr: Per-row value to accumulate.
        fill_with: Value to fill the shift(1) null with for a player's first
            match at the tournament. ``0`` is correct for true counts (sum of
            an empty prior history is 0). ``None`` is correct for margin /
            average features where "no prior data" is not the same as "net
            zero" and should propagate as NaN to NaN-tolerant models.
    """
    cum = expr.cum_sum().shift(1).over(TOURN_GROUP, order_by=DATE_COL)
    if fill_with is None:
        return cum
    return cum.fill_null(fill_with)


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
    impute=None,
)
def tourn_sets_margin() -> pl.Expr:
    return _tourn_cumulative(_sets_won() - _sets_lost(), fill_with=None)


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
    impute=None,
)
def tourn_games_margin() -> pl.Expr:
    return _tourn_cumulative(
        _total_games_won() - _total_games_lost(), fill_with=None,
    )


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


# =============================================================================
# Cross-year tournament history (group by player_id + tournament_id + draw_type,
# NOT year — accumulates across all prior appearances at the same tournament).
# =============================================================================

TOURN_HISTORY_GROUP = ["player_id", "tournament_id", "draw_type"]
TOURN_HISTORY_INSTANCE_GROUP = [*TOURN_HISTORY_GROUP, "year"]


def _is_last_in_year_instance() -> pl.Expr:
    """Shift-based marker: row is the last match of a year-instance if the next row
    in (player, tournament, draw_type) has a different year (or doesn't exist).
    """
    next_year = pl.col("year").shift(-1).over(TOURN_HISTORY_GROUP, order_by=DATE_COL)
    return (pl.col("year") != next_year) | next_year.is_null()


@feature(
    name="tourn_history_year_instances_completed",
    params=[], mirror=True, impute=0,
    description="Count of prior year-instances at this tournament (cross-year, all-time)",
)
def tourn_history_year_instances_completed() -> pl.Expr:
    """Number of completed prior appearances (counted by year, not by match)."""
    is_last_int = _is_last_in_year_instance().cast(pl.Int64)
    return (
        is_last_int.cum_sum().shift(1)
        .over(TOURN_HISTORY_GROUP, order_by=DATE_COL)
        .fill_null(0)
    )


def _per_year_instance_avg(value_per_match: pl.Expr) -> pl.Expr:
    """Average over PRIOR year-instances at this tournament.

    Each year contributes ONCE regardless of how many matches were played.
    Uses the pre-computed (cached) `tourn_history_year_instances_completed`
    as the denominator so all 9 `_avg_per_year` features share that work.
    """
    year_total = value_per_match.sum().over(TOURN_HISTORY_INSTANCE_GROUP)
    is_last = _is_last_in_year_instance()
    contribution = pl.when(is_last).then(year_total).otherwise(pl.lit(0, dtype=pl.Float64))
    cum_sum_expr = (
        contribution.cum_sum().shift(1)
        .over(TOURN_HISTORY_GROUP, order_by=DATE_COL)
        .fill_null(0)
    )
    cum_count_expr = pl.col("player_tourn_history_year_instances_completed")
    return pl.when(cum_count_expr > 0).then(cum_sum_expr / cum_count_expr).otherwise(None)


# --- Counts (cumulative across all prior matches at this tournament, cross-year) ---


@feature(
    name="tourn_history_matches_played",
    params=[], mirror=True, impute=0,
    description="Prior appearances at this tournament (any year)",
)
def tourn_history_matches_played() -> pl.Expr:
    return cumulative_count(group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_matches_won",
    params=[], mirror=True, impute=0,
    description="Prior matches won at this tournament (any year)",
)
def tourn_history_matches_won() -> pl.Expr:
    return cumulative_sum(pl.col("won").cast(pl.Int64), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_matches_lost",
    params=[], mirror=True, impute=0,
    description="Prior matches lost at this tournament (any year)",
)
def tourn_history_matches_lost() -> pl.Expr:
    return cumulative_sum(1 - pl.col("won").cast(pl.Int64), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_sets_won",
    params=[], mirror=True, impute=0,
    description="Prior sets won at this tournament (any year)",
)
def tourn_history_sets_won() -> pl.Expr:
    return cumulative_sum(_sets_won(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_sets_lost",
    params=[], mirror=True, impute=0,
    description="Prior sets lost at this tournament (any year)",
)
def tourn_history_sets_lost() -> pl.Expr:
    return cumulative_sum(_sets_lost(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_games_won",
    params=[], mirror=True, impute=0,
    description="Prior games won at this tournament (any year)",
)
def tourn_history_games_won() -> pl.Expr:
    return cumulative_sum(_total_games_won(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_games_lost",
    params=[], mirror=True, impute=0,
    description="Prior games lost at this tournament (any year)",
)
def tourn_history_games_lost() -> pl.Expr:
    return cumulative_sum(_total_games_lost(), group_by=TOURN_HISTORY_GROUP)


# --- Cumulative margin sums (won − lost) ---


@feature(
    name="tourn_history_matches_margin_sum",
    params=[], mirror=True, impute=None,
    description="Prior (matches won − matches lost) at this tournament",
)
def tourn_history_matches_margin_sum() -> pl.Expr:
    return cumulative_sum(2 * pl.col("won").cast(pl.Int64) - 1, group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_sets_margin_sum",
    params=[], mirror=True, impute=None,
    description="Prior sets margin (won − lost) at this tournament",
)
def tourn_history_sets_margin_sum() -> pl.Expr:
    return cumulative_sum(_sets_won() - _sets_lost(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_games_margin_sum",
    params=[], mirror=True, impute=None,
    description="Prior games margin (won − lost) at this tournament",
)
def tourn_history_games_margin_sum() -> pl.Expr:
    return cumulative_sum(
        _total_games_won() - _total_games_lost(),
        group_by=TOURN_HISTORY_GROUP,
    )


# --- Per-match rate ---


@feature(
    name="tourn_history_win_pct",
    params=[], mirror=True, impute=0.5,
    description="Career win pct at this tournament (matches_won / matches_played)",
)
def tourn_history_win_pct() -> pl.Expr:
    return cumulative_mean(pl.col("won").cast(pl.Int64), group_by=TOURN_HISTORY_GROUP)


# --- Per-prior-match averages (matches_won_per_match would be win_pct; skipped) ---


@feature(
    name="tourn_history_sets_won_avg_per_match",
    params=[], mirror=True, impute=None,
    description="Per-prior-match average of sets won at this tournament",
)
def tourn_history_sets_won_avg_per_match() -> pl.Expr:
    return cumulative_mean(_sets_won(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_sets_lost_avg_per_match",
    params=[], mirror=True, impute=None,
    description="Per-prior-match average of sets lost at this tournament",
)
def tourn_history_sets_lost_avg_per_match() -> pl.Expr:
    return cumulative_mean(_sets_lost(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_games_won_avg_per_match",
    params=[], mirror=True, impute=None,
    description="Per-prior-match average of games won at this tournament",
)
def tourn_history_games_won_avg_per_match() -> pl.Expr:
    return cumulative_mean(_total_games_won(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_games_lost_avg_per_match",
    params=[], mirror=True, impute=None,
    description="Per-prior-match average of games lost at this tournament",
)
def tourn_history_games_lost_avg_per_match() -> pl.Expr:
    return cumulative_mean(_total_games_lost(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_sets_margin_avg_per_match",
    params=[], mirror=True, impute=None,
    description="Per-prior-match average of sets margin at this tournament",
)
def tourn_history_sets_margin_avg_per_match() -> pl.Expr:
    return cumulative_mean(_sets_won() - _sets_lost(), group_by=TOURN_HISTORY_GROUP)


@feature(
    name="tourn_history_games_margin_avg_per_match",
    params=[], mirror=True, impute=None,
    description="Per-prior-match average of games margin at this tournament",
)
def tourn_history_games_margin_avg_per_match() -> pl.Expr:
    return cumulative_mean(
        _total_games_won() - _total_games_lost(),
        group_by=TOURN_HISTORY_GROUP,
    )


# --- Per-prior-year-instance averages (each year contributes once) ---


@feature(
    name="tourn_history_matches_won_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly matches won at this tournament",
)
def tourn_history_matches_won_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(pl.col("won").cast(pl.Int64))


@feature(
    name="tourn_history_matches_lost_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly matches lost at this tournament",
)
def tourn_history_matches_lost_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(1 - pl.col("won").cast(pl.Int64))


@feature(
    name="tourn_history_matches_margin_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly (W − L) at this tournament",
)
def tourn_history_matches_margin_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(2 * pl.col("won").cast(pl.Int64) - 1)


@feature(
    name="tourn_history_sets_won_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly sets won at this tournament",
)
def tourn_history_sets_won_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(_sets_won())


@feature(
    name="tourn_history_sets_lost_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly sets lost at this tournament",
)
def tourn_history_sets_lost_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(_sets_lost())


@feature(
    name="tourn_history_sets_margin_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly sets margin at this tournament",
)
def tourn_history_sets_margin_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(_sets_won() - _sets_lost())


@feature(
    name="tourn_history_games_won_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly games won at this tournament",
)
def tourn_history_games_won_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(_total_games_won())


@feature(
    name="tourn_history_games_lost_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly games lost at this tournament",
)
def tourn_history_games_lost_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(_total_games_lost())


@feature(
    name="tourn_history_games_margin_avg_per_year",
    params=[], mirror=True, impute=None,
    depends_on=["tourn_history_year_instances_completed"],
    description="Per-prior-year-instance average of yearly games margin at this tournament",
)
def tourn_history_games_margin_avg_per_year() -> pl.Expr:
    return _per_year_instance_avg(_total_games_won() - _total_games_lost())


_HISTORY_BASES = [
    # Counts
    "tourn_history_matches_played",
    "tourn_history_year_instances_completed",
    "tourn_history_matches_won",
    "tourn_history_matches_lost",
    "tourn_history_sets_won",
    "tourn_history_sets_lost",
    "tourn_history_games_won",
    "tourn_history_games_lost",
    # Margin sums
    "tourn_history_matches_margin_sum",
    "tourn_history_sets_margin_sum",
    "tourn_history_games_margin_sum",
    # Per-match rate
    "tourn_history_win_pct",
    # Per-prior-match avgs
    "tourn_history_sets_won_avg_per_match",
    "tourn_history_sets_lost_avg_per_match",
    "tourn_history_games_won_avg_per_match",
    "tourn_history_games_lost_avg_per_match",
    "tourn_history_sets_margin_avg_per_match",
    "tourn_history_games_margin_avg_per_match",
    # Per-prior-year-instance avgs
    "tourn_history_matches_won_avg_per_year",
    "tourn_history_matches_lost_avg_per_year",
    "tourn_history_matches_margin_avg_per_year",
    "tourn_history_sets_won_avg_per_year",
    "tourn_history_sets_lost_avg_per_year",
    "tourn_history_sets_margin_avg_per_year",
    "tourn_history_games_won_avg_per_year",
    "tourn_history_games_lost_avg_per_year",
    "tourn_history_games_margin_avg_per_year",
]
for _base in _HISTORY_BASES:
    register_diff(_base)
