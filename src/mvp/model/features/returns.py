"""Return-related features."""


import polars as pl

from mvp.model.primitives import cumulative_mean, ratio_feature, rolling_mean
from mvp.model.registry import feature, register_diff, register_matchup, register_sum

# =============================================================================
# Single Stats
# =============================================================================


@feature(
    name="ret_first_serve_win_pct",
    params=["days"],
    description="First serve return points won percentage (windowed or all-time)",
    mirror=True,
)
def ret_first_serve_win_pct(days: int | None = None) -> pl.Expr:
    """First serve return points won percentage."""
    return ratio_feature("ret_first_serve_pts_won", "ret_first_serve_pts_played", days)


@feature(
    name="ret_second_serve_win_pct",
    params=["days"],
    description="Second serve return points won percentage (windowed or all-time)",
    mirror=True,
)
def ret_second_serve_win_pct(days: int | None = None) -> pl.Expr:
    """Second serve return points won percentage."""
    return ratio_feature("ret_second_serve_pts_won", "ret_second_serve_pts_played", days)


@feature(
    name="ret_bp_convert_pct",
    params=["days"],
    description="Break points converted percentage (windowed or all-time)",
    mirror=True,
)
def ret_bp_convert_pct(days: int | None = None) -> pl.Expr:
    """Break points converted percentage."""
    return ratio_feature("ret_bp_converted", "ret_bp_opportunities", days)


@feature(
    name="ret_rating",
    params=["days"],
    description="ATP return rating average (windowed or all-time)",
    mirror=True,
)
def ret_rating(days: int | None = None) -> pl.Expr:
    """ATP composite return rating (average over time)."""
    if days is None:
        return cumulative_mean("ret_return_rating", group_by="player_id")
    return rolling_mean("ret_return_rating", days=days, group_by="player_id")


# =============================================================================
# Diff Features (player - opponent, same stat)
# =============================================================================

for _base in [
    "ret_first_serve_win_pct", "ret_second_serve_win_pct",
    "ret_bp_convert_pct", "ret_rating",
]:
    register_diff(_base)

for _base in ["ret_bp_convert_pct"]:
    register_sum(_base)


# =============================================================================
# Matchup Features (player return vs opponent serve)
# =============================================================================

register_matchup(
    "ret_first_serve_win_pct_matchup",
    "player_ret_first_serve_win_pct", "opp_svc_first_serve_win_pct",
    "ret_first_serve_win_pct", "svc_first_serve_win_pct",
    "Player first return win % minus opponent first serve win %",
)
register_matchup(
    "ret_second_serve_win_pct_matchup",
    "player_ret_second_serve_win_pct", "opp_svc_second_serve_win_pct",
    "ret_second_serve_win_pct", "svc_second_serve_win_pct",
    "Player second return win % minus opponent second serve win %",
)
register_matchup(
    "ret_bp_pct_matchup",
    "player_ret_bp_convert_pct", "opp_svc_bp_save_pct",
    "ret_bp_convert_pct", "svc_bp_save_pct",
    "Player BP convert % minus opponent BP save %",
)
