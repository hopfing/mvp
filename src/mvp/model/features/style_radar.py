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
