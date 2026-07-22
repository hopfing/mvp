"""Net-play features from the stats-plus net-point feed.

Stats-plus is the only source that carries net points (won/played); the
style-radar's ``style_net_rate`` uses the *played* count as a net-approach
*frequency* signal. This module surfaces the raw net-point volumes (won / lost)
and the effectiveness rate (won %) — none of which any existing feature
captures, and none derivable from the serve/return/total point rates.

``net_points_won`` and ``net_points_lost`` are per-match rolling means (volume),
mirroring score_depth's ``total_games_won`` / ``total_games_lost``;
``net_points_won_pct`` is the point-weighted effectiveness ratio.
"""


import polars as pl

from mvp.model.primitives import (
    cumulative_mean,
    ratio_feature,
    rolling_mean,
    surface_ratio_feature,
)
from mvp.model.registry import feature, register_diff

_GRP = "player_id"


@feature(
    name="net_points_won",
    params=["days"],
    description="Avg net points won per match in window (successful net-play volume)",
    mirror=True,
    impute=None,
)
def net_points_won(days: int | None = None) -> pl.Expr:
    """Per-match net points won, averaged over matches with net data in the window.

    Null on matches without net data (stats-plus sparse), so the mean is over
    net-present matches only — matching ``impute=None`` rather than reading a
    no-data match as 0 net points won.
    """
    expr = pl.col("player_sp_net_points_won").cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by=_GRP)
    return rolling_mean(expr, days=days, group_by=_GRP)


@feature(
    name="net_points_lost",
    params=["days"],
    description="Avg net points lost per match in window (failed net-play volume)",
    mirror=True,
    impute=None,
)
def net_points_lost(days: int | None = None) -> pl.Expr:
    """Per-match net points lost (played - won), averaged over net-present matches.

    Numerator and denominator share the stats-plus feed, so both are null
    together on a no-net-data match and that match drops out of the mean.
    """
    expr = (
        pl.col("player_sp_net_points_played") - pl.col("player_sp_net_points_won")
    ).cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by=_GRP)
    return rolling_mean(expr, days=days, group_by=_GRP)


@feature(
    name="net_points_won_pct",
    params=["days"],
    description="Net points won / net points played (effectiveness at net), windowed or all-time",
    mirror=True,
    impute=None,
)
def net_points_won_pct(days: int | None = None) -> pl.Expr:
    """Share of net points won over the window.

    Numerator and denominator come from the same stats-plus feed, so a match
    without net data is null in both and contributes nothing to either rolling
    sum; the ratio is null (not 0/0) at zero net-point history, matching
    ``impute=None``. No EB shrinkage — no per-family ``k`` has been estimated for
    net points, so this mirrors the raw-ratio treatment net data already gets in
    ``style_radar``.
    """
    return ratio_feature(
        "player_sp_net_points_won", "player_sp_net_points_played", days
    )


for _base in ["net_points_won", "net_points_lost", "net_points_won_pct"]:
    register_diff(_base)


# =============================================================================
# Surface-conditioned variants (Tier B) — the volume means (no shrinkage k).
# net_points_won_pct is an aggregate ratio and is handled with the k-shrunk
# surface ratios (needs a per-family k first), not here.
# =============================================================================

_SURFACE_GROUP = ["player_id", "surface"]


@feature(
    name="surface_net_points_won",
    params=["days"],
    description="Avg net points won per match on current surface (net-play volume)",
    mirror=True,
    impute=None,
)
def surface_net_points_won(days: int | None = None) -> pl.Expr:
    expr = pl.col("player_sp_net_points_won").cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by=_SURFACE_GROUP)
    return rolling_mean(expr, days=days, group_by=_SURFACE_GROUP)


@feature(
    name="surface_net_points_lost",
    params=["days"],
    description="Avg net points lost per match on current surface (failed net-play volume)",
    mirror=True,
    impute=None,
)
def surface_net_points_lost(days: int | None = None) -> pl.Expr:
    expr = (
        pl.col("player_sp_net_points_played") - pl.col("player_sp_net_points_won")
    ).cast(pl.Float64)
    if days is None:
        return cumulative_mean(expr, group_by=_SURFACE_GROUP)
    return rolling_mean(expr, days=days, group_by=_SURFACE_GROUP)


register_diff("surface_net_points_won")
register_diff("surface_net_points_lost")


# Net effectiveness proportion, per-surface-shrunk (k from scripts/_eb_shrinkage_k.py).
@feature(
    name="surface_net_points_won_pct",
    params=["days"],
    description="Net points won / played on current surface (net effectiveness)",
    mirror=True,
    impute=None,
)
def surface_net_points_won_pct(days: int | None = None) -> pl.Expr:
    return surface_ratio_feature(
        "player_sp_net_points_won", "player_sp_net_points_played", days, k=81.0,
    )


register_diff("surface_net_points_won_pct")
