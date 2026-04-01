# src/mvp/analysis/dashboard/insights.py
"""Insights page — auto-surfaced notable cross-cuts."""

from __future__ import annotations

import polars as pl


def filter_insights(
    insights: pl.DataFrame,
    depth: int,
    direction: str | None = None,
    min_surprise: float = 0.0,
) -> pl.DataFrame:
    """Filter and sort insights for display."""
    result = insights.filter(
        (pl.col("depth") == depth)
        & pl.col("surprise").is_not_null()
    )

    if direction is not None:
        result = result.filter(pl.col("direction") == direction)

    if min_surprise > 0:
        result = result.filter(pl.col("surprise") >= min_surprise)

    return result.sort("surprise", descending=True)


def render(
    ds: pl.DataFrame,
    sims: pl.DataFrame,
    insights: pl.DataFrame | None = None,
) -> None:
    """Render the insights page."""
    import streamlit as st

    from mvp.analysis.dashboard.components import model_selector

    if insights is None or len(insights) == 0:
        st.info("No insights available. Run the pipeline to generate them.")
        return

    # Model selector — defaults to production model
    model_version = model_selector(ds, key="insights", default_to_active=True)

    # Filter insights to selected model
    if "model_version" in insights.columns:
        if model_version is None:
            # "All Models" selected — default to production
            from mvp.analysis.dashboard.components import get_active_model

            effective = get_active_model()
        else:
            effective = model_version

        if effective is not None:
            filtered = insights.filter(
                pl.col("model_version") == effective
            )
            if len(filtered) > 0:
                insights = filtered
            else:
                st.warning(f"No insights for model '{effective}'.")
                return

    max_depth = insights["depth"].max()

    # --- Controls ---
    col1, col2 = st.columns(2)
    with col1:
        direction_opt = st.radio(
            "Direction",
            options=["all", "danger_zone", "outperformer"],
            format_func=lambda x: {
                "all": "All",
                "danger_zone": "Danger Zones",
                "outperformer": "Outperformers",
            }[x],
            horizontal=True,
        )
    direction = None if direction_opt == "all" else direction_opt

    # --- Depth 1: Single-dimension findings ---
    st.subheader("Single-Dimension Findings")
    d1 = filter_insights(insights, depth=1, direction=direction)
    if len(d1) > 0:
        display = d1.select([
            pl.col("dimensions").alias("Dimension"),
            pl.col("filters").alias("Value"),
            pl.col("n").alias("N"),
            (pl.col("accuracy") * 100).round(1).alias("Acc %"),
            (pl.col("roi") * 100).round(1).alias("ROI %"),
            pl.col("pnl").round(2).alias("P&L"),
            (pl.col("parent_roi") * 100).round(1).alias("Parent ROI %"),
            (pl.col("roi_delta") * 100).round(1).alias("Delta %"),
            pl.col("direction").alias("Direction"),
            pl.col("surprise").round(3).alias("Surprise"),
        ])
        st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)
    else:
        st.info("No single-dimension findings.")

    # --- Depth 2: Cross-cut findings ---
    if max_depth >= 2:
        st.subheader("Cross-Cut Findings")
        d2 = filter_insights(insights, depth=2, direction=direction)
        if len(d2) > 0:
            display = d2.select([
                pl.col("dimensions").alias("Dimensions"),
                pl.col("filters").alias("Values"),
                pl.col("n").alias("N"),
                (pl.col("accuracy") * 100).round(1).alias("Acc %"),
                (pl.col("roi") * 100).round(1).alias("ROI %"),
                pl.col("pnl").round(2).alias("P&L"),
                pl.col("parent_filters").alias("vs Parent"),
                (pl.col("parent_roi") * 100).round(1).alias("Parent ROI %"),
                (pl.col("roi_delta") * 100).round(1).alias("Delta %"),
                pl.col("direction").alias("Direction"),
                pl.col("surprise").round(3).alias("Surprise"),
            ])
            st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)
        else:
            st.info("No cross-cut findings.")
