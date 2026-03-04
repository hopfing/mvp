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


def rolling_max(
    col: str,
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Max of column over past N days, excluding current row.

    Args:
        col: Column to find max of.
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the rolling max.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    return (
        pl.col(col)
        .rolling_max_by(by=date_col, window_size=f"{days}d", closed="left")
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

    return pl.col(col).cum_sum().shift(1).over(group_by, order_by=date_col).fill_null(0)


def cumulative_count(
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Cumulative count over all prior rows, excluding current row.

    Args:
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the cumulative count.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    return (
        pl.col(date_col)
        .is_not_null()
        .cast(pl.Int64)
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


def ratio_feature(
    numerator_col: str,
    denominator_col: str,
    days: int | None = None,
    group_by: str | list[str] = "player_id",
) -> pl.Expr:
    """Ratio of two columns (windowed or all-time).

    Computes sum(numerator) / sum(denominator) with null when denominator is 0.

    Args:
        numerator_col: Column for the numerator.
        denominator_col: Column for the denominator.
        days: Window size in days. If None, uses all-time cumulative.
        group_by: Column(s) to group by.

    Returns:
        Polars expression computing the ratio.
    """
    if days is None:
        num = cumulative_sum(numerator_col, group_by=group_by)
        denom = cumulative_sum(denominator_col, group_by=group_by)
    else:
        num = rolling_sum(numerator_col, days=days, group_by=group_by)
        denom = rolling_sum(denominator_col, days=days, group_by=group_by)
    return pl.when(denom > 0).then(num / denom).otherwise(None)
