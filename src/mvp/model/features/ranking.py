"""Ranking-related features."""


import polars as pl

from mvp.model.primitives import cumulative_mean, rolling_mean
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


@feature(
    name="ranking_rank_diff",
    params=[],
    description="Difference between player and opponent ranking (lower is better)",
    mirror=False,
)
def ranking_rank_diff() -> pl.Expr:
    """Difference in rankings (player_rank - opp_rank).

    Negative means player is ranked higher (better).
    """
    return pl.col("player_rank") - pl.col("opp_rank")


@feature(
    name="ranking_ratio",
    params=[],
    description="Ratio of player rank to opponent rank",
    mirror=False,
)
def ranking_ratio() -> pl.Expr:
    """Ratio of rankings (player_rank / opp_rank).

    < 1 means player is ranked higher (better).
    """
    return pl.col("player_rank") / pl.col("opp_rank")


@feature(
    name="ranking_ratio_capped",
    params=["cap"],
    description="Ranking ratio capped at specified value (reduces outlier influence)",
    mirror=False,
)
def ranking_ratio_capped(cap: float = 3.0) -> pl.Expr:
    """Ranking ratio capped symmetrically at [1/cap, cap]."""
    ratio = pl.col("player_rank") / pl.col("opp_rank")
    return ratio.clip(lower_bound=1/cap, upper_bound=cap)


@feature(
    name="avg_opp_ranking",
    params=["days"],
    description="Average ranking of opponents faced (strength of schedule)",
    mirror=True,
)
def avg_opp_ranking(days: int | None = None) -> pl.Expr:
    """Average ranking of opponents faced recently.

    Higher means player has faced weaker opponents (worse rankings).
    Lower means player has faced stronger opponents (better rankings).
    """
    if days is None:
        return cumulative_mean("opp_rank", group_by="player_id")
    return rolling_mean("opp_rank", days=days, group_by="player_id")
