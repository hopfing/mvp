"""Surface-specific features."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    ratio_feature,
    rolling_count,
)
from mvp.model.registry import feature, register_diff, register_matchup, register_sum

_SURFACE_GROUP = ["player_id", "surface"]


@feature(
    name="surface_win_pct",
    params=["days"],
    description="Win percentage on current match surface (windowed or all-time)",
    mirror=True,
    impute=None,
)
def surface_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage on the current match's surface.

    Groups by (player_id, surface) so each player has separate
    win percentages for clay, hard, grass, etc.
    """
    won = pl.col("won").cast(pl.Int64)
    valid = pl.col("won").is_not_null().cast(pl.Int64)
    return ratio_feature(won, valid, days, group_by=_SURFACE_GROUP, k=13.0)


@feature(
    name="surface_matches",
    params=["days"],
    description="Matches played on current surface (windowed or all-time)",
    mirror=True,
    impute=0,
)
def surface_matches(days: int | None = None) -> pl.Expr:
    """Number of matches played on the current match's surface."""
    if days is None:
        return cumulative_count(group_by=_SURFACE_GROUP)
    return rolling_count(days=days, group_by=_SURFACE_GROUP)


register_diff("surface_win_pct")


@feature(
    name="surface_quality_win_rate",
    params=["days"],
    description="Elo-weighted win rate on current surface (quality_win_rate by surface)",
    mirror=True,
    impute=None,
)
def surface_quality_win_rate(days: int | None = None) -> pl.Expr:
    won_weighted = pl.col("won").cast(pl.Float64) * pl.col("opp_elo")
    return ratio_feature(won_weighted, pl.col("opp_elo"), days, group_by=_SURFACE_GROUP)


register_diff("surface_quality_win_rate")


# =============================================================================
# Surface-stratified serve stats
# =============================================================================


@feature(
    name="surface_first_serve_win_pct",
    params=["days"],
    description="First serve points won % on current surface",
    mirror=True,
    impute=None,
)
def surface_first_serve_win_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "svc_first_serve_pts_won", "svc_first_serve_pts_played",
        days, group_by=_SURFACE_GROUP, k=56.0,
    )


@feature(
    name="surface_second_serve_win_pct",
    params=["days"],
    description="Second serve points won % on current surface",
    mirror=True,
    impute=None,
)
def surface_second_serve_win_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "svc_second_serve_pts_won", "svc_second_serve_pts_played",
        days, group_by=_SURFACE_GROUP, k=114.0,
    )


@feature(
    name="surface_ace_pct",
    params=["days"],
    description="Ace % on current surface",
    mirror=True,
    impute=None,
)
def surface_ace_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "svc_aces", "svc_first_serve_att",
        days, group_by=_SURFACE_GROUP, k=77.0,
    )


@feature(
    name="surface_df_pct",
    params=["days"],
    description="Double fault % on current surface",
    mirror=True,
    impute=None,
)
def surface_df_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "svc_double_faults", "svc_first_serve_att",
        days, group_by=_SURFACE_GROUP, k=80.0,
    )


@feature(
    name="surface_bp_save_pct",
    params=["days"],
    description="Break points saved % on current surface",
    mirror=True,
    impute=None,
)
def surface_bp_save_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "svc_bp_saved", "svc_bp_faced",
        days, group_by=_SURFACE_GROUP, k=64.0,
    )


@feature(
    name="surface_first_serve_in_pct",
    params=["days"],
    description="First serve in % on current surface",
    mirror=True,
    impute=None,
)
def surface_first_serve_in_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "svc_first_serve_in", "svc_first_serve_att",
        days, group_by=_SURFACE_GROUP, k=95.0,
    )


@feature(
    name="surface_hold_pct",
    params=["days"],
    description="Service hold % on current surface",
    mirror=True,
    impute=None,
)
def surface_hold_pct(days: int | None = None) -> pl.Expr:
    holds = pl.col("svc_games_played") - (pl.col("svc_bp_faced") - pl.col("svc_bp_saved"))
    return ratio_feature(holds, "svc_games_played", days, group_by=_SURFACE_GROUP, k=12.0)


# =============================================================================
# Surface-stratified return stats
# =============================================================================


@feature(
    name="surface_ret_first_serve_win_pct",
    params=["days"],
    description="First serve return points won % on current surface",
    mirror=True,
    impute=None,
)
def surface_ret_first_serve_win_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "ret_first_serve_pts_won", "ret_first_serve_pts_played",
        days, group_by=_SURFACE_GROUP, k=126.0,
    )


@feature(
    name="surface_ret_second_serve_win_pct",
    params=["days"],
    description="Second serve return points won % on current surface",
    mirror=True,
    impute=None,
)
def surface_ret_second_serve_win_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "ret_second_serve_pts_won", "ret_second_serve_pts_played",
        days, group_by=_SURFACE_GROUP, k=137.0,
    )


@feature(
    name="surface_ret_bp_convert_pct",
    params=["days"],
    description="Break points converted % on current surface",
    mirror=True,
    impute=None,
)
def surface_ret_bp_convert_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "ret_bp_converted", "ret_bp_opportunities",
        days, group_by=_SURFACE_GROUP, k=180.0,
    )


# =============================================================================
# Surface-stratified points stats
# =============================================================================


@feature(
    name="surface_pts_service_won_pct",
    params=["days"],
    description="Service points won % on current surface",
    mirror=True,
    impute=None,
)
def surface_pts_service_won_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "pts_service_pts_won", "pts_service_pts_played",
        days, group_by=_SURFACE_GROUP, k=82.0,
    )


@feature(
    name="surface_pts_return_won_pct",
    params=["days"],
    description="Return points won % on current surface",
    mirror=True,
    impute=None,
)
def surface_pts_return_won_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "pts_return_pts_won", "pts_return_pts_played",
        days, group_by=_SURFACE_GROUP, k=144.0,
    )


# =============================================================================
# Diff and sum features
# =============================================================================

for _base in [
    "surface_first_serve_win_pct", "surface_second_serve_win_pct",
    "surface_ace_pct", "surface_df_pct", "surface_bp_save_pct",
    "surface_first_serve_in_pct", "surface_hold_pct",
    "surface_ret_first_serve_win_pct", "surface_ret_second_serve_win_pct",
    "surface_ret_bp_convert_pct",
    "surface_pts_service_won_pct", "surface_pts_return_won_pct",
]:
    register_diff(_base)
    register_sum(_base)


# =============================================================================
# Matchup features (serve vs return on surface)
# =============================================================================

register_matchup(
    "surface_first_serve_win_pct_matchup",
    "player_surface_first_serve_win_pct", "opp_surface_ret_first_serve_win_pct",
    "surface_first_serve_win_pct", "surface_ret_first_serve_win_pct",
    "Player 1st serve win % on surface minus opp 1st return win % on surface",
)
register_matchup(
    "surface_second_serve_win_pct_matchup",
    "player_surface_second_serve_win_pct", "opp_surface_ret_second_serve_win_pct",
    "surface_second_serve_win_pct", "surface_ret_second_serve_win_pct",
    "Player 2nd serve win % on surface minus opp 2nd return win % on surface",
)
register_matchup(
    "surface_bp_pct_matchup",
    "player_surface_bp_save_pct", "opp_surface_ret_bp_convert_pct",
    "surface_bp_save_pct", "surface_ret_bp_convert_pct",
    "Player BP save % on surface minus opp BP convert % on surface",
)

register_matchup(
    "surface_ret_first_serve_win_pct_matchup",
    "player_surface_ret_first_serve_win_pct", "opp_surface_first_serve_win_pct",
    "surface_ret_first_serve_win_pct", "surface_first_serve_win_pct",
    "Player 1st return win % on surface minus opp 1st serve win % on surface",
)
register_matchup(
    "surface_ret_second_serve_win_pct_matchup",
    "player_surface_ret_second_serve_win_pct", "opp_surface_second_serve_win_pct",
    "surface_ret_second_serve_win_pct", "surface_second_serve_win_pct",
    "Player 2nd return win % on surface minus opp 2nd serve win % on surface",
)
register_matchup(
    "surface_ret_bp_pct_matchup",
    "player_surface_ret_bp_convert_pct", "opp_surface_bp_save_pct",
    "surface_ret_bp_convert_pct", "surface_bp_save_pct",
    "Player BP convert % on surface minus opp BP save % on surface",
)

register_matchup(
    "surface_svc_pts_won_pct_matchup",
    "player_surface_pts_service_won_pct", "opp_surface_pts_return_won_pct",
    "surface_pts_service_won_pct", "surface_pts_return_won_pct",
    "Player service pts % on surface minus opp return pts % on surface",
)
register_matchup(
    "surface_ret_pts_won_pct_matchup",
    "player_surface_pts_return_won_pct", "opp_surface_pts_service_won_pct",
    "surface_pts_return_won_pct", "surface_pts_service_won_pct",
    "Player return pts % on surface minus opp service pts % on surface",
)
