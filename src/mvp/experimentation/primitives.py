"""Temporal-safe primitives for feature computation.

All primitives enforce temporal safety: for any row, only data from rows
with strictly earlier effective_match_date is included.
"""

from __future__ import annotations

import polars as pl


def rolling_sum(
    col: str,
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Sum of column over past N days, excluding current row.

    Args:
        col: Column to sum.
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the rolling sum.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    # Window is "Nd" meaning N days, closed="left" excludes current row's date
    return (
        pl.col(col)
        .rolling_sum_by(
            by=date_col,
            window_size=f"{days}d",
            closed="left",
        )
        .over(group_by)
        .fill_null(0)
    )


def rolling_mean(
    col: str,
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Mean of column over past N days, excluding current row.

    Args:
        col: Column to average.
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the rolling mean.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    return (
        pl.col(col)
        .rolling_mean_by(by=date_col, window_size=f"{days}d", closed="left")
        .over(group_by)
    )
