"""Win rate related features."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    ratio_feature,
    rolling_count,
)
from mvp.model.registry import feature, register_diff


@feature(
    name="win_pct",
    params=["days"],
    description="Win percentage (windowed or all-time)",
    mirror=True,
    impute=None,
)
def win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage over past N days, or all-time if days is None.

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing the (shrunk) win percentage.
    """
    # Shrink toward the pooled win rate (k=13 matches, EB). den is a per-valid-row
    # 1 that cumulative-sums to the match count; the is_not_null guard excludes any
    # null-won row from both numerator and denominator.
    won = pl.col("won").cast(pl.Int64)
    valid = pl.col("won").is_not_null().cast(pl.Int64)
    return ratio_feature(won, valid, days, k=13.0)


@feature(
    name="matches_played",
    params=["days"],
    description="Number of matches played (windowed or all-time)",
    mirror=True,
    impute=0,
)
def matches_played(days: int | None = None) -> pl.Expr:
    """Number of matches played in past N days, or all-time if days is None.

    Args:
        days: Window size in days. If None, uses all-time cumulative.

    Returns:
        Polars expression computing the match count.
    """
    if days is None:
        return cumulative_count(group_by="player_id")
    return rolling_count(days=days, group_by="player_id")


register_diff("win_pct")
