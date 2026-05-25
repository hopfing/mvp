"""Temporal-safe primitives for feature computation.

All primitives enforce temporal safety: for any row, only data from rows
with strictly earlier effective_match_date is included.
"""


import polars as pl


def _to_expr(col: str | pl.Expr) -> pl.Expr:
    """Convert a column name or expression to a Polars expression."""
    return pl.col(col) if isinstance(col, str) else col


def rolling_sum(
    col: str | pl.Expr,
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
    fill_with: int | None = 0,
) -> pl.Expr:
    """Sum of column over past N days, excluding current row.

    Args:
        col: Column name or expression to sum.
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.
        fill_with: Value to fill the null on rows with no prior activity in
            the window. ``0`` (default) is correct for opportunity counts
            where 0 = "no qualifying events in window". Pass ``None`` for
            result counts where the no-activity row must be distinguished
            from a real 0 (e.g. "won 0 out of 5 prior matches vs power
            servers" vs "had no prior such opponents"); the feature should
            declare ``impute=None`` so the NaN survives to a NaN-tolerant
            model.

    Returns:
        Polars expression computing the rolling sum.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    # Window is "Nd" meaning N days, closed="left" excludes current row's date
    rolling = (
        _to_expr(col)
        .rolling_sum_by(
            by=date_col,
            window_size=f"{days}d",
            closed="left",
        )
        .over(group_by)
    )
    if fill_with is None:
        return rolling
    return rolling.fill_null(fill_with)


def rolling_mean(
    col: str | pl.Expr,
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Mean of column over past N days, excluding current row.

    Args:
        col: Column name or expression to average.
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the rolling mean.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    return (
        _to_expr(col)
        .rolling_mean_by(by=date_col, window_size=f"{days}d", closed="left")
        .over(group_by)
    )


def rolling_max(
    col: str | pl.Expr,
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Max of column over past N days, excluding current row.

    Args:
        col: Column name or expression to find max of.
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the rolling max.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    return (
        _to_expr(col)
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
    col: str | pl.Expr,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
    fill_with: int | None = 0,
) -> pl.Expr:
    """Cumulative sum over all prior rows, excluding current row.

    Args:
        col: Column name or expression to sum.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.
        fill_with: Value to fill the shift(1) null on the first row of each
            group. ``0`` (default) is correct for true counts (sum of an empty
            prior history is 0). Pass ``None`` for result counts where "no
            prior data" must be distinguished from "had data, result was 0"
            — the column carries NaN on first-occurrence rows and the feature
            should declare ``impute=None``.

    Returns:
        Polars expression computing the cumulative sum.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    cum = _to_expr(col).cum_sum().shift(1).over(group_by, order_by=date_col)
    if fill_with is None:
        return cum
    return cum.fill_null(fill_with)


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
    col: str | pl.Expr,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Cumulative mean over all prior rows, excluding current row.

    Args:
        col: Column name or expression to average.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the cumulative mean.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    cum_sum = _to_expr(col).cum_sum().shift(1).over(group_by, order_by=date_col)
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
    numerator_col: str | pl.Expr,
    denominator_col: str | pl.Expr,
    days: int | None = None,
    group_by: str | list[str] = "player_id",
) -> pl.Expr:
    """Ratio of two columns or expressions (windowed or all-time).

    Computes sum(numerator) / sum(denominator) with null when denominator is 0.

    Args:
        numerator_col: Column name or expression for the numerator.
        denominator_col: Column name or expression for the denominator.
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


