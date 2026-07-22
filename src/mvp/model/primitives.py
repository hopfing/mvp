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


def rolling_std(
    col: str | pl.Expr,
    days: int,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Sample std of column over past N days, excluding current row.

    Null on rows with fewer than 2 prior observations in the window (a std
    needs at least two points). Pair the feature with ``impute=None`` so the
    thin-history null survives to a NaN-tolerant model rather than being
    fabricated into a spurious "low volatility".

    Args:
        col: Column name or expression to take the std of.
        days: Window size in days.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the rolling sample std.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    return (
        _to_expr(col)
        .rolling_std_by(
            by=date_col, window_size=f"{days}d", closed="left", min_samples=2,
        )
        .over(group_by)
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

    cum = _to_expr(col).cum_sum().shift(1).over(group_by, order_by=[date_col, "tournament_start_date", "round_order", "match_uid"])
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
        .over(group_by, order_by=[date_col, "tournament_start_date", "round_order", "match_uid"])
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

    # Count only non-null values of `col`, not every row: a null source (e.g. a
    # per-set rate on a match with no completed sets) must not sit in the
    # denominator without contributing to the sum, which would dilute the mean
    # toward 0. Mirror cumulative_std's bookkeeping — fill nulls to 0 for the
    # sum, count via the value's own non-null mask — and return null (not a
    # 0/0 NaN) when there is no prior non-null history.
    x = _to_expr(col)
    valid = x.is_not_null().cast(pl.Float64)
    cum_sum = x.fill_null(0.0).cum_sum().shift(1).over(group_by, order_by=[date_col, "tournament_start_date", "round_order", "match_uid"])
    cum_count = valid.cum_sum().shift(1).over(group_by, order_by=[date_col, "tournament_start_date", "round_order", "match_uid"])
    return pl.when(cum_count > 0).then(cum_sum / cum_count).otherwise(None)


def cumulative_std(
    col: str | pl.Expr,
    group_by: str | list[str],
    date_col: str = "effective_match_date",
) -> pl.Expr:
    """Sample std over all prior rows, excluding current row.

    Computed from running first/second moments (sum, sum-of-squares, count),
    each shifted by 1 so the current row is excluded. Null-valued source rows
    are dropped from the moments (they contribute to neither count nor sums),
    so a missing-outcome match doesn't distort the std. Null on rows with fewer
    than 2 prior non-null observations.

    Args:
        col: Column name or expression to take the std of.
        group_by: Column(s) to group by (e.g., "player_id").
        date_col: Date column for temporal ordering.

    Returns:
        Polars expression computing the cumulative sample std.
    """
    if isinstance(group_by, str):
        group_by = [group_by]

    x = _to_expr(col)
    valid = x.is_not_null().cast(pl.Float64)
    xf = x.fill_null(0.0)
    n = valid.cum_sum().shift(1).over(group_by, order_by=[date_col, "tournament_start_date", "round_order", "match_uid"])
    s1 = xf.cum_sum().shift(1).over(group_by, order_by=[date_col, "tournament_start_date", "round_order", "match_uid"])
    s2 = (xf ** 2).cum_sum().shift(1).over(group_by, order_by=[date_col, "tournament_start_date", "round_order", "match_uid"])
    # Sample variance via the moment identity; clip tiny negatives from float error.
    var = ((s2 - s1 ** 2 / n) / (n - 1)).clip(lower_bound=0.0)
    return pl.when(n >= 2).then(var.sqrt()).otherwise(None)


def ratio_feature(
    numerator_col: str | pl.Expr,
    denominator_col: str | pl.Expr,
    days: int | None = None,
    group_by: str | list[str] = "player_id",
    k: float | None = None,
    prior: float | pl.Expr | None = None,
) -> pl.Expr:
    """Ratio of two columns or expressions (windowed or all-time).

    With ``k=None`` (default): raw sum(numerator)/sum(denominator), null when the
    denominator is 0 — the original behavior.

    With ``k`` set: empirical-Bayes shrinkage toward the population mean,
    ``(sum(num) + k*m) / (sum(den) + k)``. This dampens low-sample rates (a 1-0
    record no longer reads as a confident 100%) and converges to the raw ratio
    as the denominator grows. ``m`` defaults to the global pooled mean
    sum(num)/sum(den) over the whole frame (a population-level constant; pass
    ``prior`` to supply a train-fold value instead). Shrinkage only regularizes
    rows that have at least one observation: at zero history (denominator 0) the
    row is **null**, never the prior — so shrinkage never fabricates a value for
    debut / new-surface / thin-window rows, and composes with ``impute=None``.

    See ``EB_SHRINK_K`` for the per-family k values and the script that derived
    them.

    Args:
        numerator_col: Column name or expression for the numerator.
        denominator_col: Column name or expression for the denominator.
        days: Window size in days. If None, uses all-time cumulative.
        group_by: Column(s) to group by.
        k: Shrinkage pseudo-count in the denominator's native units. None = raw.
        prior: Override for the shrinkage target mean m (default: global pooled).

    Returns:
        Polars expression computing the (optionally shrunk) ratio.
    """
    num_src, den_src = numerator_col, denominator_col
    if k is not None:
        # Null-safe: fill source nulls BEFORE the windowed sum so a missing-stat
        # match contributes (0, 0) and never leaves a null position that shift(1)
        # would carry into the next row's ratio. (Raw path keeps null semantics.)
        num_src = _to_expr(numerator_col).fill_null(0)
        den_src = _to_expr(denominator_col).fill_null(0)
    if days is None:
        num = cumulative_sum(num_src, group_by=group_by)
        denom = cumulative_sum(den_src, group_by=group_by)
    else:
        num = rolling_sum(num_src, days=days, group_by=group_by)
        denom = rolling_sum(den_src, days=days, group_by=group_by)
    if k is None:
        return pl.when(denom > 0).then(num / denom).otherwise(None)
    # m (pooled prior) uses the original columns; .sum() skips nulls already.
    m = prior if prior is not None else (
        _to_expr(numerator_col).sum() / _to_expr(denominator_col).sum()
    )
    # No fabrication at zero history: shrink only rows with >=1 observation;
    # debut / no-data rows stay null (XGBoost missing-direction split).
    denom_filled = denom.fill_null(0)
    shrunk = (num.fill_null(0) + k * m) / (denom_filled + k)
    return pl.when(denom_filled > 0).then(shrunk).otherwise(None)


def surface_ratio_feature(
    numerator_col: str | pl.Expr,
    denominator_col: str | pl.Expr,
    days: int | None = None,
    k: float | None = None,
    by: str = "surface",
) -> pl.Expr:
    """EB ratio grouped by ``(player_id, by)`` that shrinks toward the per-``by``
    pooled rate rather than the whole-frame global mean.

    ``ratio_feature``'s default prior ``m`` is a single whole-frame constant, so
    a surface-split ratio pulls thin strata toward an all-surface average that is
    dominated by the majority surfaces (clay/hard). This passes ``prior`` as the
    per-``by`` pooled rate instead — same whole-frame, date-blind posture as the
    default ``m`` (just stratified), so it introduces no new leakage, and thin
    rows shrink toward their own surface's mean.

    Falls back to the global pooled mean for any ``by`` stratum whose denominator
    sums to 0 over the compute frame; otherwise that stratum's prior would be null
    and ``k*m`` would null every row in it — including rows with real data.
    """
    num_e, den_e = _to_expr(numerator_col), _to_expr(denominator_col)
    den_sum = den_e.sum().over(by)
    stratum_prior = num_e.sum().over(by) / den_sum
    pooled_prior = num_e.sum() / den_e.sum()
    prior = pl.when(den_sum > 0).then(stratum_prior).otherwise(pooled_prior)
    return ratio_feature(
        numerator_col, denominator_col, days=days,
        group_by=["player_id", by], k=k, prior=prior,
    )


# ---------------------------------------------------------------------------
# Empirical-Bayes shrinkage strengths (k = alpha+beta, Beta-Binomial MoM).
#
# DOCUMENTATION ONLY. The per-family k is passed as a literal at each
# ratio_feature CALL SITE (colocated with the feature, so an edit is captured by
# the feature-source cache hash and correctly invalidates). Source of truth for
# the numbers: scripts/_eb_shrinkage_k.py (singles, walkovers excluded, players
# with >=20 matches, ~5,700). k is in each ratio's native denominator units;
# estimation biases are conservative (toward under-shrinkage).
#
#   win_pct                   13   (matches)        ret_first_serve_win_pct  126 (points)
#   hold_pct                  12   (games)          ret_second_serve_win_pct 137 (points)
#   first_serve_win_pct       56   (points)         pts_return_won_pct       144 (points)
#   bp_save_pct               64   (break points)   ret_bp_convert_pct       180 (break points)
#   ace_pct                   77   (serves)
#   df_pct                    80   (serves)
#   pts_service_won_pct       82   (points)
#   first_serve_in_pct        95   (serves)
#   second_serve_win_pct     114   (points)
#   pts_total_won_pct        194   (points)
#
# Narrow win-rate subsets (vs surface specialists, vs opp-type) inherit win_pct
# k (~13) as a placeholder, floor 5, until subset-specific EB k is estimated.
# Sustainable regeneration tooling + train-fold m persistence: see issue #94.
# ---------------------------------------------------------------------------


