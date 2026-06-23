"""Style radar — within-player style profile (spec 2026-06-22-style-radar-lookup).

Built in stages:
  Stage 1 (this file, so far): the five per-axis rolling TENDENCY signals — the
    raw inputs. Each is a point-weighted rolling rate (sum/sum over the window,
    null at zero history, no fabrication), one signal per broad axis (spec §3a,
    one-signal-per-axis — no mean-of-z across mixed-coverage constituents).
  Stage 2 (next): cross-player z-score (LOO) -> shrink-to-prior -> within-player
    centering -> the 5 radar axes.
  Stage 3: the style-matchup lookup (rating-residual retrieval over the radar).

Sources (all broad; per spec §2): ace rate from match_stats; net / winner / UE
from the stats_plus columns (`player_sp_*`); rally-lean from match_beats. The
stats_plus signals share one denominator, `pts_total_pts_played` (match_stats),
resolving the per-signal denominator split (SDE review).

These are NEW radar inputs; the existing `style.py` features are left untouched
(the standalone `style_net_approach_frequency` re-source is a separable change).
"""

import polars as pl

from mvp.model.primitives import ratio_feature
from mvp.model.registry import feature

# Radar window: wider than the 365d form window — style is a slow trait and
# accumulation is what makes coverage comprehensive (spec D-WIN; exp-decay is the
# proposed refinement, flat window is what the primitive supports today).
_RADAR_DAYS = 1095
_GRP = "player_id"
_DATE = "effective_match_date"

# Cross-player standardization window (spec §4 Step 3; mirrors style.py's
# leakage-safe `_THRESHOLD_WINDOW_DAYS`).
_STD_DAYS = 730


def _w(days: int | None) -> int:
    return days or _RADAR_DAYS


def _sp_rate(num_col: str, days: int | None) -> pl.Expr:
    """Rolling per-point rate for a stats_plus signal.

    The numerator (stats_plus, sparse) and the denominator (`pts_total_pts_played`,
    broad) come from different feeds, so the denominator is masked to matches
    where the stats_plus value is present — otherwise a player with total-points
    history but no stats_plus history reads as a real 0 (e.g. "0% net") instead
    of null/unknown. Both then sum over the same stats_plus-present matches.
    """
    num = pl.col(num_col)
    den = pl.when(num.is_not_null()).then(pl.col("pts_total_pts_played")).otherwise(None)
    return ratio_feature(num, den, days=_w(days), group_by=_GRP)


@feature(
    name="style_ace_rate",
    params=["days"],
    description="A1 serve: rolling aces per service point (point-weighted)",
    mirror=True,
    impute=None,
)
def style_ace_rate(days: int | None = None) -> pl.Expr:
    return ratio_feature("svc_aces", "pts_service_pts_played", days=_w(days), group_by=_GRP)


@feature(
    name="style_net_rate",
    params=["days"],
    description="A2 net: rolling net approaches per point (stats_plus net)",
    mirror=True,
    impute=None,
)
def style_net_rate(days: int | None = None) -> pl.Expr:
    return _sp_rate("player_sp_net_points_played", days)


@feature(
    name="style_sp_winner_rate",
    params=["days"],
    description="A3 aggression: rolling winners per point (stats_plus, broad)",
    mirror=True,
    impute=None,
)
def style_sp_winner_rate(days: int | None = None) -> pl.Expr:
    return _sp_rate("player_sp_winners", days)


@feature(
    name="style_sp_ue_rate",
    params=["days"],
    description="A4 error: rolling unforced errors per point (stats_plus, broad)",
    mirror=True,
    impute=None,
)
def style_sp_ue_rate(days: int | None = None) -> pl.Expr:
    return _sp_rate("player_sp_unforced_errors", days)


@feature(
    name="style_rally_lean",
    params=["days"],
    description="A5 rally: rolling (long-share - short-share) rally lean (match_beats)",
    mirror=True,
    impute=None,
)
def style_rally_lean(days: int | None = None) -> pl.Expr:
    # Cast to signed first: rally_*_count are UInt32, so a bare subtraction
    # underflows to ~4e9 whenever short > long.
    net_long = pl.col("rally_long_count").cast(pl.Int64) - pl.col("rally_short_count").cast(pl.Int64)
    return ratio_feature(net_long, "rally_points_with_data", days=_w(days), group_by=_GRP)


# =============================================================================
# Stage 2a: cross-player standardized axes (leave-one-player-out z-score)
#
# Each axis = the player's rolling signal z-scored against the FIELD's
# distribution of that signal over the past 730d (units-only step, spec §4
# Step 3). Two corrections over a naive population z-score:
#   - leave-one-player-out: the population mean/SD exclude the target player's
#     own rows (population stat minus the player's own running contribution),
#     so a heavy-match-volume player's z isn't compressed toward their own
#     history (review MLE-1).
#   - joint population: one field distribution across both circuits, so radar
#     coordinates stay comparable for cross-circuit lookups (review MLE-6).
# Leakage-safe: closed="left" excludes the current row; the window is past-only.
# =============================================================================


def _roll730(e: pl.Expr, *, over: bool) -> pl.Expr:
    """730d trailing rolling sum (past-only). `over=True` partitions by player."""
    r = e.rolling_sum_by(by=_DATE, window_size=f"{_STD_DAYS}d", closed="left")
    if over:
        r = r.over(_GRP)
    return r.fill_null(0)


def _loo_z(col: str) -> pl.Expr:
    """Leave-one-player-out, joint-population z-score of a rolling signal column."""
    s = pl.col(col)
    sq = s ** 2
    one = s.is_not_null().cast(pl.Int64)
    # LOO moments: field total minus this player's own contribution.
    n = _roll730(one, over=False) - _roll730(one, over=True)
    sm = _roll730(s, over=False) - _roll730(s, over=True)
    smsq = _roll730(sq, over=False) - _roll730(sq, over=True)
    mean = sm / n
    var = ((smsq - sm ** 2 / n) / (n - 1)).clip(lower_bound=0.0)
    std = var.sqrt()
    return pl.when((n >= 2) & (std > 0)).then((s - mean) / std).otherwise(None)


@feature(name="style_z_serve", params=[], mirror=True, impute=None,
         depends_on=["style_ace_rate"], description="A1 serve axis: LOO z-score of ace rate")
def style_z_serve() -> pl.Expr:
    return _loo_z("player_style_ace_rate")


@feature(name="style_z_net", params=[], mirror=True, impute=None,
         depends_on=["style_net_rate"], description="A2 net axis: LOO z-score of net rate")
def style_z_net() -> pl.Expr:
    return _loo_z("player_style_net_rate")


@feature(name="style_z_aggression", params=[], mirror=True, impute=None,
         depends_on=["style_sp_winner_rate"], description="A3 aggression axis: LOO z-score of winner rate")
def style_z_aggression() -> pl.Expr:
    return _loo_z("player_style_sp_winner_rate")


@feature(name="style_z_error", params=[], mirror=True, impute=None,
         depends_on=["style_sp_ue_rate"], description="A4 error axis: LOO z-score of UE rate")
def style_z_error() -> pl.Expr:
    return _loo_z("player_style_sp_ue_rate")


@feature(name="style_z_rally", params=[], mirror=True, impute=None,
         depends_on=["style_rally_lean"], description="A5 rally axis: LOO z-score of rally lean")
def style_z_rally() -> pl.Expr:
    return _loo_z("player_style_rally_lean")
