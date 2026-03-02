"""Return-related features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import cumulative_mean, ratio_feature, rolling_mean
from mvp.model.registry import feature


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


@feature(
    name="ret_first_serve_win_pct_diff",
    params=["days"],
    description="First serve return win pct difference (player - opponent)",
    depends_on=["ret_first_serve_win_pct"],
    mirror=False,
)
def ret_first_serve_win_pct_diff(days: int | None = None) -> pl.Expr:
    """First serve return win percentage difference."""
    if days is None:
        return pl.col("player_ret_first_serve_win_pct") - pl.col("opp_ret_first_serve_win_pct")
    return (
        pl.col(f"player_ret_first_serve_win_pct_{days}d")
        - pl.col(f"opp_ret_first_serve_win_pct_{days}d")
    )


@feature(
    name="ret_second_serve_win_pct_diff",
    params=["days"],
    description="Second serve return win pct difference (player - opponent)",
    depends_on=["ret_second_serve_win_pct"],
    mirror=False,
)
def ret_second_serve_win_pct_diff(days: int | None = None) -> pl.Expr:
    """Second serve return win percentage difference."""
    if days is None:
        return pl.col("player_ret_second_serve_win_pct") - pl.col("opp_ret_second_serve_win_pct")
    return (
        pl.col(f"player_ret_second_serve_win_pct_{days}d")
        - pl.col(f"opp_ret_second_serve_win_pct_{days}d")
    )


@feature(
    name="ret_bp_convert_pct_diff",
    params=["days"],
    description="Break point convert percentage difference (player - opponent)",
    depends_on=["ret_bp_convert_pct"],
    mirror=False,
)
def ret_bp_convert_pct_diff(days: int | None = None) -> pl.Expr:
    """Break point convert percentage difference."""
    if days is None:
        return pl.col("player_ret_bp_convert_pct") - pl.col("opp_ret_bp_convert_pct")
    return pl.col(f"player_ret_bp_convert_pct_{days}d") - pl.col(f"opp_ret_bp_convert_pct_{days}d")


@feature(
    name="ret_rating_diff",
    params=["days"],
    description="ATP return rating difference (player - opponent)",
    depends_on=["ret_rating"],
    mirror=False,
)
def ret_rating_diff(days: int | None = None) -> pl.Expr:
    """ATP return rating difference."""
    if days is None:
        return pl.col("player_ret_rating") - pl.col("opp_ret_rating")
    return pl.col(f"player_ret_rating_{days}d") - pl.col(f"opp_ret_rating_{days}d")


# =============================================================================
# Matchup Features (player return vs opponent serve)
# =============================================================================


@feature(
    name="ret_first_serve_win_pct_matchup",
    params=["days"],
    description="Player first return win % minus opponent first serve win %",
    depends_on=["ret_first_serve_win_pct", "svc_first_serve_win_pct"],
    mirror=False,
)
def ret_first_serve_win_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's first serve return vs opponent's first serve."""
    if days is None:
        return pl.col("player_ret_first_serve_win_pct") - pl.col("opp_svc_first_serve_win_pct")
    return (
        pl.col(f"player_ret_first_serve_win_pct_{days}d")
        - pl.col(f"opp_svc_first_serve_win_pct_{days}d")
    )


@feature(
    name="ret_second_serve_win_pct_matchup",
    params=["days"],
    description="Player second return win % minus opponent second serve win %",
    depends_on=["ret_second_serve_win_pct", "svc_second_serve_win_pct"],
    mirror=False,
)
def ret_second_serve_win_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's second serve return vs opponent's second serve."""
    if days is None:
        return pl.col("player_ret_second_serve_win_pct") - pl.col("opp_svc_second_serve_win_pct")
    return (
        pl.col(f"player_ret_second_serve_win_pct_{days}d")
        - pl.col(f"opp_svc_second_serve_win_pct_{days}d")
    )


@feature(
    name="ret_bp_pct_matchup",
    params=["days"],
    description="Player BP convert % minus opponent BP save %",
    depends_on=["ret_bp_convert_pct", "svc_bp_save_pct"],
    mirror=False,
)
def ret_bp_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's clutch returning vs opponent's clutch serving."""
    if days is None:
        return pl.col("player_ret_bp_convert_pct") - pl.col("opp_svc_bp_save_pct")
    return pl.col(f"player_ret_bp_convert_pct_{days}d") - pl.col(f"opp_svc_bp_save_pct_{days}d")
