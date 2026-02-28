"""Surface-specific features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_mean,
    rolling_count,
    rolling_mean,
)
from mvp.model.registry import feature


@feature(
    name="surface_win_rate",
    params=["days"],
    description="Win rate on current match surface (windowed or all-time)",
    mirror=True,
)
def surface_win_rate(days: int | None = None) -> pl.Expr:
    """Win rate on the current match's surface.

    Groups by (player_id, surface) so each player has separate
    win rates for clay, hard, grass, etc.
    """
    group_by = ["player_id", "surface"]
    if days is None:
        return cumulative_mean("won", group_by=group_by)
    return rolling_mean("won", days=days, group_by=group_by)


@feature(
    name="surface_matches",
    params=["days"],
    description="Matches played on current surface (windowed or all-time)",
    mirror=True,
)
def surface_matches(days: int | None = None) -> pl.Expr:
    """Number of matches played on the current match's surface."""
    group_by = ["player_id", "surface"]
    if days is None:
        return cumulative_count(group_by=group_by)
    return rolling_count(days=days, group_by=group_by)


@feature(
    name="surface_win_rate_diff",
    params=["days"],
    description="Difference in surface win rate (player - opponent)",
    depends_on=["surface_win_rate"],
    mirror=False,
)
def surface_win_rate_diff(days: int | None = None) -> pl.Expr:
    """Difference between player and opponent surface win rate."""
    if days is None:
        return pl.col("player_surface_win_rate") - pl.col("opp_surface_win_rate")
    return pl.col(f"player_surface_win_rate_{days}d") - pl.col(f"opp_surface_win_rate_{days}d")
