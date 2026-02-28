"""Return-related features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import ratio_feature
from mvp.model.registry import feature


@feature(
    name="ret_first_win_pct",
    params=["days"],
    description="First serve return points won percentage (windowed or all-time)",
    mirror=True,
)
def ret_first_win_pct(days: int | None = None) -> pl.Expr:
    """First serve return points won percentage."""
    return ratio_feature("ret_first_serve_pts_won", "ret_first_serve_pts_played", days)


@feature(
    name="ret_second_win_pct",
    params=["days"],
    description="Second serve return points won percentage (windowed or all-time)",
    mirror=True,
)
def ret_second_win_pct(days: int | None = None) -> pl.Expr:
    """Second serve return points won percentage."""
    return ratio_feature("ret_second_serve_pts_won", "ret_second_serve_pts_played", days)


@feature(
    name="bp_convert_pct",
    params=["days"],
    description="Break points converted percentage (windowed or all-time)",
    mirror=True,
)
def bp_convert_pct(days: int | None = None) -> pl.Expr:
    """Break points converted percentage."""
    return ratio_feature("ret_bp_converted", "ret_bp_opportunities", days)
