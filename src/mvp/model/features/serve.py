"""Serve-related features."""


import polars as pl

from mvp.model.primitives import cumulative_mean, ratio_feature, rolling_mean
from mvp.model.registry import feature, register_diff, register_matchup, register_sum

# =============================================================================
# Single Stats
# =============================================================================


@feature(
    name="svc_first_serve_win_pct",
    params=["days"],
    description="First serve points won percentage (windowed or all-time)",
    mirror=True,
)
def svc_first_serve_win_pct(days: int | None = None) -> pl.Expr:
    """First serve points won percentage."""
    return ratio_feature("svc_first_serve_pts_won", "svc_first_serve_pts_played", days)


@feature(
    name="svc_second_serve_win_pct",
    params=["days"],
    description="Second serve points won percentage (windowed or all-time)",
    mirror=True,
)
def svc_second_serve_win_pct(days: int | None = None) -> pl.Expr:
    """Second serve points won percentage."""
    return ratio_feature("svc_second_serve_pts_won", "svc_second_serve_pts_played", days)


@feature(
    name="svc_ace_pct",
    params=["days"],
    description="Ace percentage (aces / first serve attempts)",
    mirror=True,
)
def svc_ace_pct(days: int | None = None) -> pl.Expr:
    """Ace percentage - aces per first serve attempt."""
    return ratio_feature("svc_aces", "svc_first_serve_att", days)


@feature(
    name="svc_df_pct",
    params=["days"],
    description="Double fault percentage (double faults / first serve attempts)",
    mirror=True,
)
def svc_df_pct(days: int | None = None) -> pl.Expr:
    """Double fault percentage - double faults per first serve attempt."""
    return ratio_feature("svc_double_faults", "svc_first_serve_att", days)


@feature(
    name="svc_bp_save_pct",
    params=["days"],
    description="Break points saved percentage (windowed or all-time)",
    mirror=True,
)
def svc_bp_save_pct(days: int | None = None) -> pl.Expr:
    """Break points saved percentage."""
    return ratio_feature("svc_bp_saved", "svc_bp_faced", days)


@feature(
    name="svc_first_serve_in_pct",
    params=["days"],
    description="First serve in percentage (first serves in / attempts)",
    mirror=True,
)
def svc_first_serve_in_pct(days: int | None = None) -> pl.Expr:
    """First serve in percentage."""
    return ratio_feature("svc_first_serve_in", "svc_first_serve_att", days)


@feature(
    name="svc_rating",
    params=["days"],
    description="ATP serve rating average (windowed or all-time)",
    mirror=True,
)
def svc_rating(days: int | None = None) -> pl.Expr:
    """ATP composite serve rating (average over time)."""
    if days is None:
        return cumulative_mean("svc_serve_rating", group_by="player_id")
    return rolling_mean("svc_serve_rating", days=days, group_by="player_id")


@feature(
    name="hold_pct",
    params=["days"],
    description="Service games held percentage (windowed or all-time)",
    mirror=True,
)
def hold_pct(days: int | None = None) -> pl.Expr:
    """Percentage of service games held.

    Holds = service games played minus games broken (bp_faced - bp_saved).
    """
    holds = pl.col("svc_games_played") - (pl.col("svc_bp_faced") - pl.col("svc_bp_saved"))
    return ratio_feature(holds, "svc_games_played", days)


# =============================================================================
# Diff Features (player - opponent, same stat)
# =============================================================================

for _base in [
    "svc_first_serve_win_pct", "svc_second_serve_win_pct", "svc_ace_pct",
    "svc_df_pct", "svc_bp_save_pct", "svc_first_serve_in_pct", "svc_rating",
    "hold_pct",
]:
    register_diff(_base)

for _base in [
    "svc_first_serve_win_pct", "svc_second_serve_win_pct",
    "svc_ace_pct", "svc_bp_save_pct", "svc_first_serve_in_pct",
    "hold_pct",
]:
    register_sum(_base)


# =============================================================================
# Matchup Features (player serve vs opponent return)
# =============================================================================

register_matchup(
    "svc_first_serve_win_pct_matchup",
    "player_svc_first_serve_win_pct", "opp_ret_first_serve_win_pct",
    "svc_first_serve_win_pct", "ret_first_serve_win_pct",
    "Player first serve win % minus opponent first return win %",
)
register_matchup(
    "svc_second_serve_win_pct_matchup",
    "player_svc_second_serve_win_pct", "opp_ret_second_serve_win_pct",
    "svc_second_serve_win_pct", "ret_second_serve_win_pct",
    "Player second serve win % minus opponent second return win %",
)
register_matchup(
    "svc_bp_pct_matchup",
    "player_svc_bp_save_pct", "opp_ret_bp_convert_pct",
    "svc_bp_save_pct", "ret_bp_convert_pct",
    "Player BP save % minus opponent BP convert %",
)
