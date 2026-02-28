"""Head-to-head features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import cumulative_sum, rolling_sum
from mvp.model.registry import feature


@feature(
    name="h2h_wins",
    params=["days"],
    description="Wins against specific opponent (windowed or all-time)",
    mirror=True,
)
def h2h_wins(days: int | None = None) -> pl.Expr:
    """Wins against specific opponent.

    Groups by [player_id, opp_id] to track head-to-head record.

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing h2h wins.
    """
    if days is None:
        return cumulative_sum("won", group_by=["player_id", "opp_id"])
    return rolling_sum("won", days=days, group_by=["player_id", "opp_id"])
