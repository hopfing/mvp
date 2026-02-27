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


def rolling_count(
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Count of rows over past N days, excluding current row.

    Args:
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the rolling count.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    # Use is_not_null().cast(Int64) to create a column expression tied to actual data
    # pl.lit(1) doesn't work with rolling_*_by since it has no column context
    return (
        pl.col(date_col)
        .is_not_null()
        .cast(pl.Int64)
        .rolling_sum_by(by=date_col, window_size=f"{days}d", closed="left")
        .over(group_by)
        .fill_null(0)
    )


def cumulative_sum(
    col: str,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Cumulative sum over all prior rows, excluding current row.

    Args:
        col: Column to sum.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the cumulative sum.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    return (
        pl.col(col)
        .cum_sum()
        .shift(1)
        .over(group_by, order_by=date_col)
        .fill_null(0)
    )


def cumulative_mean(
    col: str,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Cumulative mean over all prior rows, excluding current row.

    Args:
        col: Column to average.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the cumulative mean.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    cum_sum = pl.col(col).cum_sum().shift(1).over(group_by, order_by=date_col)
    # Use is_not_null().cast(Int64) because pl.lit(1) doesn't work with .over()
    cum_count = (
        pl.col(date_col)
        .is_not_null()
        .cast(pl.Int64)
        .cum_sum()
        .shift(1)
        .over(group_by, order_by=date_col)
    )
    return cum_sum / cum_count
