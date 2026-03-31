"""Odds page — edge in context of price level."""

from __future__ import annotations

import polars as pl

ODDS_BREAKS = [1.00, 1.25, 1.50, 1.75, 2.00, 2.25, 2.50]
ODDS_LABELS = ["1.00-1.25", "1.25-1.50", "1.50-1.75", "1.75-2.00", "2.00-2.25", "2.25-2.50", "2.50+"]

# Map odds column -> edge column
_ODDS_TO_EDGE: dict[str, str] = {
    "pred_odds_best_close": "model_edge_best_close",
    "pred_odds_open": "model_edge_open",
    "pred_odds_market_formed": "model_edge_market_formed",
}

# Odds basis options shown in the radio
_BASIS_OPTIONS = [
    ("close", "pred_odds_best_close", "Close"),
    ("open", "pred_odds_open", "Open"),
    ("formed", "pred_odds_market_formed", "Mkt Formed"),
]


def bucket_by_odds(ds: pl.DataFrame, odds_col: str) -> pl.DataFrame:
    """Add an *odds_bucket* column to resolved rows with non-null odds.

    Buckets are defined by ODDS_BREAKS / ODDS_LABELS.  Rows with odds >= 2.50
    fall into the "2.50+" bucket.  Rows with null odds or non-resolved status
    are dropped.
    """
    df = ds.filter(
        (pl.col("status") == "resolved") & pl.col(odds_col).is_not_null()
    )

    # Build pl.when().then() chain for bucketing
    o = pl.col(odds_col)
    expr = (
        pl.when(o < ODDS_BREAKS[1]).then(pl.lit(ODDS_LABELS[0]))
        .when(o < ODDS_BREAKS[2]).then(pl.lit(ODDS_LABELS[1]))
        .when(o < ODDS_BREAKS[3]).then(pl.lit(ODDS_LABELS[2]))
        .when(o < ODDS_BREAKS[4]).then(pl.lit(ODDS_LABELS[3]))
        .when(o < ODDS_BREAKS[5]).then(pl.lit(ODDS_LABELS[4]))
        .when(o < ODDS_BREAKS[6]).then(pl.lit(ODDS_LABELS[5]))
        .otherwise(pl.lit(ODDS_LABELS[6]))
        .alias("odds_bucket")
    )

    return df.with_columns(expr)


def odds_range_summary(ds: pl.DataFrame, odds_col: str) -> pl.DataFrame:
    """Group by odds_bucket and compute n, accuracy, roi, pnl.

    roi is flat $1 stake ROI: (odds - 1) if correct, -1 if incorrect.
    """
    bucketed = bucket_by_odds(ds, odds_col=odds_col)

    # Compute per-row P&L
    pnl_expr = (
        pl.when(pl.col("model_correct"))
        .then(pl.col(odds_col) - 1.0)
        .otherwise(pl.lit(-1.0))
        .alias("_pnl")
    )
    bucketed = bucketed.with_columns(pnl_expr)

    summary = (
        bucketed.group_by("odds_bucket")
        .agg([
            pl.len().alias("n"),
            pl.col("model_correct").mean().alias("accuracy"),
            pl.col("_pnl").mean().alias("roi"),
            pl.col("_pnl").sum().alias("pnl"),
        ])
    )

    # Sort by canonical bucket order
    bucket_order_df = pl.DataFrame({
        "odds_bucket": ODDS_LABELS,
        "_sort_key": list(range(len(ODDS_LABELS))),
    })
    summary = (
        summary.join(bucket_order_df, on="odds_bucket", how="left")
        .sort("_sort_key")
        .drop("_sort_key")
    )

    return summary


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the odds page."""
    import streamlit as st

    st.header("Odds")

    # Determine which odds columns are present in ds
    available_bases = [
        (key, col, label)
        for key, col, label in _BASIS_OPTIONS
        if col in ds.columns
    ]

    if not available_bases:
        st.info("No odds columns found in dataset.")
        return

    basis_key = st.radio(
        "Odds basis",
        options=[b[0] for b in available_bases],
        format_func=lambda x: next(label for k, _, label in available_bases if k == x),
        horizontal=True,
    )

    odds_col = next(col for k, col, _ in available_bases if k == basis_key)
    edge_col = _ODDS_TO_EDGE.get(odds_col)

    # --- Performance by Odds Range table ---
    st.subheader("Performance by Odds Range")

    summary = odds_range_summary(ds, odds_col=odds_col)

    if summary.is_empty():
        st.info("No resolved data for selected odds basis.")
    else:
        display = summary.select([
            pl.col("odds_bucket").alias("Odds Range"),
            pl.col("n").alias("N"),
            (pl.col("accuracy") * 100).round(1).alias("Acc %"),
            (pl.col("roi") * 100).round(2).alias("ROI %"),
            pl.col("pnl").round(2).alias("P&L"),
        ])
        st.dataframe(display, use_container_width=True, hide_index=True)

    # --- Edge vs Odds Level scatter ---
    if edge_col and edge_col in ds.columns:
        st.subheader("Edge vs Odds Level")

        scatter_df = ds.filter(
            (pl.col("status") == "resolved")
            & pl.col(odds_col).is_not_null()
            & pl.col(edge_col).is_not_null()
        ).select([
            pl.col(odds_col).alias("Odds"),
            (pl.col(edge_col) * 100).alias("Edge %"),
            pl.col("model_correct").cast(pl.Int8).alias("Result"),
        ])

        st.scatter_chart(
            scatter_df.to_pandas(),
            x="Odds",
            y="Edge %",
            color="Result",
        )
