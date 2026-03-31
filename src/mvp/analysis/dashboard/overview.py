"""Overview page — headline metrics at a glance."""

from __future__ import annotations

import polars as pl
import streamlit as st

from mvp.analysis.dashboard.components import metric_card_data, render_metric_cards


def compute_headlines(ds: pl.DataFrame) -> dict:
    """Extract headline metrics from analysis dataset. Pure data, no Streamlit."""
    n_predictions = len(ds)

    resolved = (
        ds.filter(pl.col("status") == "resolved") if "status" in ds.columns else ds
    )
    n_resolved = len(resolved)

    accuracy = None
    if n_resolved > 0 and "model_correct" in resolved.columns:
        correct = resolved["model_correct"].sum()
        accuracy = correct / n_resolved if n_resolved > 0 else None

    odds_coverage = None
    if "pred_odds_best_close" in ds.columns:
        odds_coverage = ds.filter(pl.col("pred_odds_best_close").is_not_null()).shape[0]

    n_bets = 0
    pnl = None
    mean_clv = None
    median_clv = None
    if "stake" in ds.columns:
        bets = ds.filter(pl.col("stake").is_not_null() & (pl.col("stake") != ""))
        n_bets = len(bets)
        if n_bets > 0 and "net" in ds.columns:
            net_vals = bets["net"].cast(pl.Float64, strict=False).drop_nulls()
            if len(net_vals) > 0:
                pnl = net_vals.sum()
        clv_col = next(
            (c for c in ["clv_vs_avg", "clv_vs_best"] if c in bets.columns),
            None,
        )
        if n_bets > 0 and clv_col is not None:
            clv = bets[clv_col].drop_nulls()
            if len(clv) > 0:
                mean_clv = clv.mean()
                median_clv = clv.median()

    return {
        "n_predictions": n_predictions,
        "n_resolved": n_resolved,
        "accuracy": accuracy,
        "odds_coverage": odds_coverage,
        "n_bets": n_bets,
        "pnl": pnl,
        "mean_clv": mean_clv,
        "median_clv": median_clv,
    }


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the overview page."""
    h = compute_headlines(ds)

    cards = [
        metric_card_data("Predictions", h["n_predictions"], fmt="d"),
        metric_card_data("Resolved", h["n_resolved"], fmt="d"),
        metric_card_data("Accuracy", h["accuracy"], fmt=".1%"),
        metric_card_data("Bets", h["n_bets"], fmt="d"),
    ]
    render_metric_cards(cards)

    cards2 = [
        metric_card_data("Mean CLV", h["mean_clv"], fmt=".2%"),
        metric_card_data("Median CLV", h["median_clv"], fmt=".2%"),
        metric_card_data("P&L", h["pnl"], fmt="$.2f"),
        metric_card_data("Odds Coverage", h["odds_coverage"], fmt="d"),
    ]
    render_metric_cards(cards2)
