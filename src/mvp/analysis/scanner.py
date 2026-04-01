# src/mvp/analysis/scanner.py
"""Insight scanner — automated cross-cut anomaly detection."""

from __future__ import annotations

import math
from itertools import combinations

import polars as pl

# --- Odds buckets (same breaks as odds page) ---
ODDS_BREAKS = [1.00, 1.25, 1.50, 1.75, 2.00, 2.25, 2.50]
ODDS_LABELS = [
    "1.00-1.25", "1.25-1.50", "1.50-1.75", "1.75-2.00",
    "2.00-2.25", "2.25-2.50", "2.50+",
]

# --- Edge buckets (2.5pp bands) ---
EDGE_BREAKS = [-0.10, -0.075, -0.05, -0.025, 0.0, 0.025, 0.05, 0.075, 0.10]
EDGE_LABELS = [
    "below -10%", "-10% to -7.5%", "-7.5% to -5%", "-5% to -2.5%",
    "-2.5% to 0%", "0% to 2.5%", "2.5% to 5%", "5% to 7.5%",
    "7.5% to 10%", "10%+",
]

# --- Dimension definitions ---
DIMENSIONS = [
    ("consensus", "Consensus"),
    ("edge_bucket", "Edge"),
    ("odds_bucket", "Odds"),
    ("circuit", "Circuit"),
    ("surface", "Surface"),
]

MIN_N = 10


def bucket_dimensions(ds: pl.DataFrame) -> pl.DataFrame:
    """Add bucketed columns for continuous dimensions.

    Filters to resolved rows with non-null odds/edge, then adds
    odds_bucket and edge_bucket columns.
    """
    result = ds.filter(
        (pl.col("status") == "resolved")
        & pl.col("pred_odds_best_close").is_not_null()
        & pl.col("model_edge_best_close").is_not_null()
        & pl.col("model_correct").is_not_null()
    )

    # Odds bucketing
    o = pl.col("pred_odds_best_close")
    odds_expr = pl.when(o < ODDS_BREAKS[1]).then(pl.lit(ODDS_LABELS[0]))
    for i in range(1, len(ODDS_BREAKS) - 1):
        odds_expr = odds_expr.when(o < ODDS_BREAKS[i + 1]).then(
            pl.lit(ODDS_LABELS[i])
        )
    odds_expr = odds_expr.otherwise(pl.lit(ODDS_LABELS[-1]))
    result = result.with_columns(odds_expr.alias("odds_bucket"))

    # Edge bucketing
    e = pl.col("model_edge_best_close")
    edge_expr = pl.when(e < EDGE_BREAKS[0]).then(pl.lit(EDGE_LABELS[0]))
    for i in range(len(EDGE_BREAKS) - 1):
        edge_expr = edge_expr.when(e < EDGE_BREAKS[i + 1]).then(
            pl.lit(EDGE_LABELS[i + 1])
        )
    edge_expr = edge_expr.otherwise(pl.lit(EDGE_LABELS[-1]))
    result = result.with_columns(edge_expr.alias("edge_bucket"))

    # Cast consensus to string for uniform grouping
    if "consensus" in result.columns:
        result = result.with_columns(
            pl.col("consensus").cast(pl.Utf8).alias("consensus")
        )

    return result


def _compute_group_metrics(
    df: pl.DataFrame,
    odds_col: str = "pred_odds_best_close",
) -> dict:
    """Compute accuracy, ROI, P&L for a group of rows."""
    n = len(df)
    if n == 0:
        return {"n": 0, "accuracy": None, "roi": None, "pnl": None}

    accuracy = df["model_correct"].mean()

    pnl_series = (
        pl.when(pl.col("model_correct"))
        .then(pl.col(odds_col) - 1.0)
        .otherwise(pl.lit(-1.0))
    )
    pnl_vals = df.select(pnl_series.alias("_pnl"))["_pnl"]
    pnl = pnl_vals.sum()
    roi = pnl / n

    return {"n": n, "accuracy": accuracy, "roi": roi, "pnl": pnl}


def compute_slices(
    bucketed: pl.DataFrame,
    max_depth: int = 2,
    min_n: int = MIN_N,
) -> pl.DataFrame:
    """Enumerate all dimension slices up to max_depth and compute metrics."""
    available_dims = [
        (col, label) for col, label in DIMENSIONS if col in bucketed.columns
    ]

    rows: list[dict] = []

    # Depth 0: overall
    metrics = _compute_group_metrics(bucketed)
    rows.append({"depth": 0, "dimensions": "", "filters": "overall", **metrics})

    # Depth 1..max_depth
    for depth in range(1, max_depth + 1):
        for dim_combo in combinations(available_dims, depth):
            cols = [c for c, _ in dim_combo]
            dim_names = "|".join(c for c, _ in dim_combo)

            groups = bucketed.group_by(cols)
            for group_vals, group_df in groups:
                if len(group_df) < min_n:
                    continue

                if not isinstance(group_vals, tuple):
                    group_vals = (group_vals,)
                filter_parts = [str(v) for v in group_vals]
                filter_str = " | ".join(filter_parts)

                metrics = _compute_group_metrics(group_df)
                rows.append({
                    "depth": depth,
                    "dimensions": dim_names,
                    "filters": filter_str,
                    **metrics,
                })

    return pl.DataFrame(rows)


def score_surprises(slices: pl.DataFrame) -> pl.DataFrame:
    """Compare each slice to parent slices and compute surprise scores.

    For depth-N slices, the parent is the depth-(N-1) slice with the largest
    absolute ROI delta. Depth-0 has no parent.
    """
    lookup: dict[tuple[str, str], float] = {}
    for row in slices.iter_rows(named=True):
        lookup[(row["dimensions"], row["filters"])] = row["roi"]

    results = []
    for row in slices.iter_rows(named=True):
        row = dict(row)

        if row["depth"] == 0:
            row["parent_dimensions"] = None
            row["parent_filters"] = None
            row["parent_roi"] = None
            row["roi_delta"] = None
            row["direction"] = None
            row["surprise"] = None
            results.append(row)
            continue

        child_dims = row["dimensions"].split("|")
        child_filters = row["filters"].split(" | ")

        best_parent_dims = None
        best_parent_filters = None
        best_parent_roi = None
        best_delta = None

        for i in range(len(child_dims)):
            parent_dim_list = child_dims[:i] + child_dims[i + 1:]
            parent_filter_list = child_filters[:i] + child_filters[i + 1:]

            parent_dims_str = "|".join(parent_dim_list)
            parent_filters_str = (
                " | ".join(parent_filter_list) if parent_filter_list else "overall"
            )

            parent_roi = lookup.get((parent_dims_str, parent_filters_str))
            if parent_roi is None:
                continue

            delta = row["roi"] - parent_roi
            if best_delta is None or abs(delta) > abs(best_delta):
                best_delta = delta
                best_parent_dims = parent_dims_str
                best_parent_filters = parent_filters_str
                best_parent_roi = parent_roi

        row["parent_dimensions"] = best_parent_dims
        row["parent_filters"] = best_parent_filters
        row["parent_roi"] = best_parent_roi
        row["roi_delta"] = best_delta

        if best_delta is not None:
            row["direction"] = "outperformer" if best_delta >= 0 else "danger_zone"
            row["surprise"] = abs(best_delta) * math.sqrt(row["n"])
        else:
            row["direction"] = None
            row["surprise"] = None

        results.append(row)

    return pl.DataFrame(results)
