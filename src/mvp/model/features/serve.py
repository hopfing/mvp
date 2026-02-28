"""Serve-related features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import rolling_sum
from mvp.model.registry import feature


@feature(
    name="svc_first_win_pct",
    params=["days"],
    description="Rolling first serve points won percentage over past N days",
    mirror=True,
)
def svc_first_win_pct(days: int) -> pl.Expr:
    """Rolling first serve points won percentage over past N days.

    Computes sum(svc_first_serve_pts_won) / sum(svc_first_serve_pts_played)
    over the rolling window.

    Args:
        days: Window size in days.

    Returns:
        Polars expression computing the rolling first serve win percentage.
    """
    won = rolling_sum("svc_first_serve_pts_won", days=days, group_by="player_id")
    played = rolling_sum("svc_first_serve_pts_played", days=days, group_by="player_id")
    # Use when to avoid division by zero, returning null when no data
    return pl.when(played > 0).then(won / played).otherwise(None)
