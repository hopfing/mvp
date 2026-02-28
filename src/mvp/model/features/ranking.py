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

    Uses pre-existing player_ranking_points and opp_ranking_points columns
    from the matches.parquet data.

    Returns:
        Polars expression computing player_ranking_points - opp_ranking_points.
    """
    return pl.col("player_ranking_points") - pl.col("opp_ranking_points")
