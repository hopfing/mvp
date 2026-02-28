"""Serve-related features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import cumulative_sum, rolling_sum
from mvp.model.registry import feature


@feature(
    name="svc_first_win_pct",
    params=["days"],
    description="First serve points won percentage (windowed or all-time)",
    mirror=True,
)
def svc_first_win_pct(days: int | None = None) -> pl.Expr:
    """First serve points won percentage.

    Computes sum(svc_first_serve_pts_won) / sum(svc_first_serve_pts_played)
    over the window (or all-time if days is None).

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing the first serve win percentage.
    """
    if days is None:
        won = cumulative_sum("svc_first_serve_pts_won", group_by="player_id")
        played = cumulative_sum("svc_first_serve_pts_played", group_by="player_id")
    else:
        won = rolling_sum("svc_first_serve_pts_won", days=days, group_by="player_id")
        played = rolling_sum("svc_first_serve_pts_played", days=days, group_by="player_id")
    # Use when to avoid division by zero, returning null when no data
    return pl.when(played > 0).then(won / played).otherwise(None)
