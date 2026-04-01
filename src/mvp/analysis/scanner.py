# src/mvp/analysis/scanner.py
"""Insight scanner — automated cross-cut anomaly detection."""

from __future__ import annotations

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
