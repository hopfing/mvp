"""Ranking-related features."""

from __future__ import annotations

import polars as pl

from mvp.model.registry import feature


@feature(
    name="ranking_points_diff",
    params=[],
    description="Difference between player and opponent ranking points",
    mirror=False,
)
def ranking_points_diff() -> pl.Expr:
    """Difference between player and opponent ranking points.

    Uses player_rankings_points and opp_rankings_points from matches.parquet.

    Returns:
        Polars expression computing player_rankings_points - opp_rankings_points.
    """
    return pl.col("player_rankings_points") - pl.col("opp_rankings_points")
