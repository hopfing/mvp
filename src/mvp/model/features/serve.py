"""Serve-related features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import ratio_feature
from mvp.model.registry import feature


@feature(
    name="svc_first_win_pct",
    params=["days"],
    description="First serve points won percentage (windowed or all-time)",
    mirror=True,
)
def svc_first_win_pct(days: int | None = None) -> pl.Expr:
    """First serve points won percentage."""
    return ratio_feature("svc_first_serve_pts_won", "svc_first_serve_pts_played", days)


@feature(
    name="svc_second_win_pct",
    params=["days"],
    description="Second serve points won percentage (windowed or all-time)",
    mirror=True,
)
def svc_second_win_pct(days: int | None = None) -> pl.Expr:
    """Second serve points won percentage."""
    return ratio_feature("svc_second_serve_pts_won", "svc_second_serve_pts_played", days)


@feature(
    name="ace_rate",
    params=["days"],
    description="Aces per serve point (windowed or all-time)",
    mirror=True,
)
def ace_rate(days: int | None = None) -> pl.Expr:
    """Aces per serve point."""
    return ratio_feature("svc_aces", "svc_first_serve_pts_played", days)


@feature(
    name="df_rate",
    params=["days"],
    description="Double faults per serve point (windowed or all-time)",
    mirror=True,
)
def df_rate(days: int | None = None) -> pl.Expr:
    """Double faults per serve point."""
    return ratio_feature("svc_double_faults", "svc_first_serve_pts_played", days)


@feature(
    name="bp_save_pct",
    params=["days"],
    description="Break points saved percentage (windowed or all-time)",
    mirror=True,
)
def bp_save_pct(days: int | None = None) -> pl.Expr:
    """Break points saved percentage."""
    return ratio_feature("svc_bp_saved", "svc_bp_faced", days)
