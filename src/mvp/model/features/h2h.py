"""Head-to-head features."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import cumulative_sum
from mvp.model.registry import feature


@feature(
    name="h2h_wins",
    params=[],
    description="Cumulative wins against specific opponent",
    mirror=True,
)
def h2h_wins() -> pl.Expr:
    """Cumulative wins against specific opponent.

    Groups by [player_id, opp_id] to track head-to-head record.

    Returns:
        Polars expression computing cumulative h2h wins.
    """
    return cumulative_sum("won", group_by=["player_id", "opp_id"])
