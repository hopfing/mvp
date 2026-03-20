"""Surface-specific features."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_mean,
    rolling_count,
    rolling_mean,
)
from mvp.model.registry import feature, register_diff


@feature(
    name="surface_win_pct",
    params=["days"],
    description="Win percentage on current match surface (windowed or all-time)",
    mirror=True,
)
def surface_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage on the current match's surface.

    Groups by (player_id, surface) so each player has separate
    win percentages for clay, hard, grass, etc.
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
    impute=0,
)
def surface_matches(days: int | None = None) -> pl.Expr:
    """Number of matches played on the current match's surface."""
    group_by = ["player_id", "surface"]
    if days is None:
        return cumulative_count(group_by=group_by)
    return rolling_count(days=days, group_by=group_by)


register_diff("surface_win_pct")
