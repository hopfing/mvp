"""Shared UI components for the analysis dashboard."""

from __future__ import annotations

import polars as pl


def metric_card_data(
    label: str,
    value: float | int | None,
    fmt: str = ".1f",
    delta: float | None = None,
    delta_fmt: str | None = None,
) -> dict:
    """Prepare metric card data. Returns dict with label, value, delta strings.

    Does not call Streamlit — pure data prep so it's testable.
    """
    if value is None:
        formatted = "\u2014"
    elif "%" in fmt:
        formatted = f"{value:{fmt}}"
    elif "$" in fmt:
        sign = "+" if value >= 0 else ""
        formatted = f"{sign}${value:{fmt.replace('$', '')}}"
    else:
        formatted = f"{value:{fmt}}"

    result: dict = {"label": label, "value": formatted}

    if delta is not None and delta_fmt is not None:
        if "%" in delta_fmt:
            result["delta"] = f"{delta:{delta_fmt}}"
        else:
            result["delta"] = f"{delta:{delta_fmt}}"

    return result


def render_metric_cards(cards: list[dict]) -> None:
    """Render a row of metric cards using st.metric."""
    import streamlit as st

    cols = st.columns(len(cards))
    for col, card in zip(cols, cards):
        with col:
            st.metric(
                label=card["label"],
                value=card["value"],
                delta=card.get("delta"),
            )


def style_roi(roi: float) -> str:
    """Return CSS color string for ROI value."""
    if roi > 0.05:
        return "color: #4CAF50; font-weight: bold"
    elif roi > 0:
        return "color: #81C784"
    elif roi > -0.05:
        return "color: #EF9A9A"
    else:
        return "color: #EF5350; font-weight: bold"


def format_sim_table(
    sims: pl.DataFrame,
    scenarios: list[str],
    segment: str = "overall",
    segment_value: str = "overall",
    model_version: str = "all",
) -> pl.DataFrame:
    """Filter and format simulation results into a display table.

    Returns a Polars DataFrame with columns: Scenario, N, Acc, ROI, P&L.
    """
    filtered = sims.filter(
        pl.col("scenario").is_in(scenarios)
        & (pl.col("segment") == segment)
        & (pl.col("segment_value") == segment_value)
        & (pl.col("model_version") == model_version)
    )

    if len(filtered) == 0:
        return pl.DataFrame(
            schema={
                "Scenario": pl.Utf8,
                "N": pl.Int64,
                "Acc": pl.Utf8,
                "ROI": pl.Utf8,
                "P&L": pl.Utf8,
            }
        )

    # Preserve scenario order from input list
    order = {name: i for i, name in enumerate(scenarios)}
    result = (
        filtered.with_columns(
            pl.col("scenario")
            .replace_strict(order, default=999)
            .alias("_order")
        )
        .sort("_order")
        .select(
            pl.col("scenario").alias("Scenario"),
            pl.col("n_bets").alias("N"),
            pl.format("{}%", (pl.col("accuracy") * 100).round(1)).alias("Acc"),
            pl.format("{}%", (pl.col("roi") * 100).round(1)).alias("ROI"),
            pl.col("net_pnl").round(2).alias("P&L"),
        )
    )

    return result
