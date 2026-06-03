"""Tier 2 overlays — loss attribution and ROI per feature decile.

Operates on the joined DataFrame from `feature_join.join_predictions_with_features`
when sourced from backtest.csv (which carries pnl_open / pnl_close per pick).

Analyses:
  3F. loss_attribution: per feature decile, sum log_loss_contrib + pnl_open/close
  3G. roi_overlay: per Tier 1 flagged feature, ROI by decile
"""
from __future__ import annotations

import logging
from typing import Sequence

import numpy as np
import polars as pl

from mvp.model.error_analysis.analyses import (
    N_DECILES,
    _decile_buckets,
    _decile_edges,
    _identify_feature_columns,
)

logger = logging.getLogger(__name__)


def loss_attribution(
    df: pl.DataFrame,
    *,
    prob_col: str = "model_prob",
    target_col: str = "won",
    feature_cols: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Per feature decile, sum log loss contribution + pnl_open + pnl_close.

    Distinguishes "where the model is wrong" (loss sum) from "where it costs
    money" (pnl_open / pnl_close sum). A high-LL bucket might be low-PnL
    impact if it's a small or low-stake slice.

    Requires `pnl_open` and `pnl_close` columns (Tier 2; backtest.csv path).
    """
    if "pnl_open" not in df.columns or "pnl_close" not in df.columns:
        raise ValueError(
            "loss_attribution requires pnl_open and pnl_close columns "
            "(Tier 2 backtest.csv source). For Tier 1 use the other analyses."
        )

    if feature_cols is None:
        feature_cols = _identify_feature_columns(df)
    else:
        feature_cols = [c for c in feature_cols if c in df.columns]

    y_prob = df[prob_col].to_numpy().astype(np.float64)
    y_true = df[target_col].cast(pl.Float64).to_numpy()
    eps = 1e-15
    yp = np.clip(y_prob, eps, 1 - eps)
    log_loss_contrib = -(y_true * np.log(yp) + (1 - y_true) * np.log(1 - yp))

    pnl_open = df["pnl_open"].to_numpy().astype(np.float64)
    pnl_close = df["pnl_close"].to_numpy().astype(np.float64)
    pnl_open = np.nan_to_num(pnl_open, nan=0.0)
    pnl_close = np.nan_to_num(pnl_close, nan=0.0)

    rows = []
    for feat in feature_cols:
        vals = df[feat].to_numpy().astype(np.float64)
        buckets, edges, is_cat = _decile_buckets(vals)
        n_buckets = len(edges) if is_cat else N_DECILES
        for b in range(1, n_buckets + 1):
            mask = buckets == b
            n = int(mask.sum())
            if n == 0:
                continue
            v_low, v_high = _decile_edges(edges, b, is_cat)
            rows.append({
                "feature": feat,
                "bucket": b,
                "value_low": v_low,
                "value_high": v_high,
                "is_categorical": is_cat,
                "n": n,
                "log_loss_sum": float(log_loss_contrib[mask].sum()),
                "log_loss_avg": float(log_loss_contrib[mask].mean()),
                "pnl_open_sum": float(pnl_open[mask].sum()),
                "pnl_close_sum": float(pnl_close[mask].sum()),
                "roi_open": float(pnl_open[mask].mean()),
                "roi_close": float(pnl_close[mask].mean()),
            })

    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={
            "feature": pl.Utf8, "bucket": pl.Int64,
            "value_low": pl.Float64, "value_high": pl.Float64,
            "is_categorical": pl.Boolean, "n": pl.Int64,
            "log_loss_sum": pl.Float64, "log_loss_avg": pl.Float64,
            "pnl_open_sum": pl.Float64, "pnl_close_sum": pl.Float64,
            "roi_open": pl.Float64, "roi_close": pl.Float64,
        }
    )


def loss_attribution_by_segment(
    df: pl.DataFrame,
    segment_col: str,
    *,
    prob_col: str = "model_prob",
    target_col: str = "won",
    feature_cols: list[str] | None = None,
) -> pl.DataFrame:
    """loss_attribution per segment value (e.g., per-circuit, per-round)."""
    if segment_col not in df.columns:
        raise ValueError(f"segment_col '{segment_col}' not in df.columns")
    out_frames = []
    for seg in df[segment_col].drop_nulls().unique().to_list():
        sub = df.filter(pl.col(segment_col) == seg)
        if sub.height == 0:
            continue
        seg_loss = loss_attribution(
            sub, prob_col=prob_col, target_col=target_col, feature_cols=feature_cols,
        )
        seg_loss = seg_loss.with_columns(pl.lit(str(seg)).alias(segment_col))
        out_frames.append(seg_loss)
    if not out_frames:
        return pl.DataFrame()
    return pl.concat(out_frames, how="diagonal_relaxed")
