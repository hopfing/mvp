"""Tier 1 analyses for the feature-error pipeline.

Operates on the joined DataFrame from `feature_join.join_predictions_with_features`.
Each function returns a CSV-ready DataFrame; the runner orchestrates writes.

Analyses (per the plan):
  3A. per_feature_calibration: decile bucket each feature, mean(pred) vs mean(actual)
  3B. feature_distribution_coverage: train vs inference distribution overlap
      (NOTE: requires a separate train-time distribution; deferred until the
      training-time distribution snapshot is wired — see TODO in this file)
  3D. naive_baseline_comparison: model vs tournament-average baseline per bucket
  3E. temporal_drift: half-period split, per-feature calibration gap stability

All analyses use player-level bootstrap CIs to honour the non-i.i.d. structure
of picks (same player appears in many picks).
"""
from __future__ import annotations

import logging
from typing import Sequence

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# Sample-size discipline per plan: min n=50 per bucket (suggestive below).
MIN_N_PER_BUCKET = 50
N_DECILES = 10
BOOTSTRAP_ITERATIONS = 500
# Features with at most this many distinct numeric values get treated
# categorically (one bucket per distinct value) instead of being deciled.
# Stops categorical-like features (e.g., round_ordinal with 12 levels) from
# lumping adjacent values into the same decile.
LOW_CARDINALITY_THRESHOLD = 15


# ---- helpers ----------------------------------------------------------------


def _is_numeric(dtype: pl.DataType) -> bool:
    """Return True if a Polars dtype is numeric (Float* or Int*)."""
    return dtype.is_numeric()


def _identify_feature_columns(
    df: pl.DataFrame,
    *,
    drop_cols: Sequence[str] | None = None,
) -> list[str]:
    """Identify numeric feature columns by dropping known prediction-side cols.

    Brittleness trade-off: hardcoded skip-set silently excludes any column
    matching these names, including future features that might collide. If a
    feature with one of these names is introduced, it'll be silently dropped.
    Pass `feature_cols` explicitly to the analysis functions to bypass.
    """
    defaults = {
        "match_uid", "player_id", "opp_id", "effective_match_date",
        "circuit", "surface", "round", "fold_idx",
        "y_test", "y_prob", "won", "model_prob", "is_pick",
        "best_opening_odds", "best_closing_odds",
        "opening_implied", "closing_implied", "opening_edge", "closing_edge",
        "clv", "pnl_open", "pnl_close", "cell_cal", "cal_tier",
        "tournament_id", "tournament_name", "side",
        "consensus", "voter_count", "predicted_at", "match_date",
        "p1_id", "p2_id", "p1_name", "p2_name", "model_version",
        "schedule_day", "scheduled_datetime", "result", "status",
        "model_correct",
    }
    skip = set(drop_cols or [])
    skip |= defaults
    return [
        c for c in df.columns
        if c not in skip and _is_numeric(df.schema[c])
    ]


def _decile_buckets(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, bool]:
    """Return (bucket_indices, edges, is_categorical).

    For low-cardinality features (≤ LOW_CARDINALITY_THRESHOLD distinct values),
    treats each distinct value as its own bucket — avoids lumping adjacent
    categorical-like values (e.g., round_ordinal = 7 and 8) into the same
    decile. Returns is_categorical=True in that case.

    bucket_indices: array of bucket ids (1..K) per row. NaN → bucket 0.
    edges: For continuous: length N_DECILES+1, quantile boundaries.
        For categorical: length K, the distinct values (one per bucket).
    is_categorical: True when categorical mode was used.
    """
    out = np.zeros(len(values), dtype=np.int16)
    finite_mask = np.isfinite(values)
    if not finite_mask.any():
        return out, np.array([]), False
    finite_vals = values[finite_mask]
    distinct = np.unique(finite_vals)
    if len(distinct) <= LOW_CARDINALITY_THRESHOLD:
        value_to_bucket = {float(v): i + 1 for i, v in enumerate(distinct)}
        buckets = np.array(
            [value_to_bucket[float(v)] for v in finite_vals], dtype=np.int16,
        )
        out[finite_mask] = buckets
        return out, distinct, True
    quantiles = np.quantile(
        finite_vals, np.linspace(0, 1, N_DECILES + 1)
    )
    quantiles = np.unique(quantiles)
    if len(quantiles) < 2:
        out[finite_mask] = 1
        return out, quantiles, False
    buckets = np.digitize(finite_vals, quantiles[1:-1], right=False) + 1
    out[finite_mask] = buckets.astype(np.int16)
    return out, quantiles, False


def _decile_edges(edges: np.ndarray, bucket: int, is_categorical: bool = False) -> tuple[float, float]:
    """Return (low, high) value range for a given bucket.

    Continuous: bucket b spans [edges[b-1], edges[b]] from quantile boundaries.
    Categorical: bucket b corresponds to a single distinct value edges[b-1].
    Returns (nan, nan) if the bucket is out of range.
    """
    if is_categorical:
        if bucket < 1 or bucket > len(edges):
            return (float("nan"), float("nan"))
        v = float(edges[bucket - 1])
        return (v, v)
    if len(edges) < 2 or bucket < 1 or bucket > len(edges) - 1:
        return (float("nan"), float("nan"))
    return (float(edges[bucket - 1]), float(edges[bucket]))


def _player_level_bootstrap_ci(
    player_ids: np.ndarray,
    values: np.ndarray,
    *,
    n_iterations: int = BOOTSTRAP_ITERATIONS,
    confidence: float = 0.95,
    rng: np.random.Generator | None = None,
) -> tuple[float, float]:
    """Bootstrap CI for mean(values), resampling at the player level.

    For each iteration: sample players with replacement; for each sampled player,
    include all their picks. Compute mean across the resampled set. Final CI
    is the percentile range across iterations.

    This preserves within-player correlation (same player in many picks).
    Pick-level bootstrap would treat each pick as independent → too narrow.
    """
    if len(values) == 0:
        return (float("nan"), float("nan"))
    rng = rng or np.random.default_rng(seed=42)
    unique_players, player_indices = np.unique(player_ids, return_inverse=True)
    n_players = len(unique_players)
    if n_players == 0:
        return (float("nan"), float("nan"))
    # Index picks by player
    player_to_picks: dict[int, np.ndarray] = {
        i: np.where(player_indices == i)[0] for i in range(n_players)
    }
    means = np.empty(n_iterations, dtype=np.float64)
    for it in range(n_iterations):
        sampled_players = rng.integers(0, n_players, size=n_players)
        sampled_idx = np.concatenate(
            [player_to_picks[p] for p in sampled_players]
        )
        means[it] = float(np.mean(values[sampled_idx]))
    alpha = (1 - confidence) / 2
    lo = float(np.quantile(means, alpha))
    hi = float(np.quantile(means, 1 - alpha))
    return (lo, hi)


# ---- 3A: per-feature calibration -------------------------------------------


def per_feature_calibration(
    df: pl.DataFrame,
    *,
    prob_col: str = "y_prob",
    target_col: str = "y_test",
    feature_cols: Sequence[str] | None = None,
    min_n: int = MIN_N_PER_BUCKET,
) -> pl.DataFrame:
    """For each feature, decile-bucket picks and compare mean(pred) vs mean(actual).

    Returns:
        Long-format DataFrame: one row per (feature, decile) cell with
        columns: feature, decile, n, mean_pred, mean_actual, gap,
        ci_low, ci_high, suggestive (True when n < min_n).
    """
    if feature_cols is None:
        feature_cols = _identify_feature_columns(df)
    else:
        feature_cols = [c for c in feature_cols if c in df.columns]

    y_prob = df[prob_col].to_numpy().astype(np.float64)
    y_true = df[target_col].to_numpy().astype(np.float64)
    player_ids = df["player_id"].to_numpy() if "player_id" in df.columns else None

    rows = []
    for feat in feature_cols:
        feat_vals = df[feat].to_numpy().astype(np.float64)
        buckets, edges, is_cat = _decile_buckets(feat_vals)
        n_buckets = len(edges) if is_cat else N_DECILES
        for b in range(1, n_buckets + 1):
            mask = buckets == b
            n = int(mask.sum())
            if n == 0:
                continue
            mean_pred = float(y_prob[mask].mean())
            mean_actual = float(y_true[mask].mean())
            gap = mean_actual - mean_pred  # positive = model underconfident
            if player_ids is not None and n >= 2:
                ci_lo, ci_hi = _player_level_bootstrap_ci(
                    player_ids[mask], y_true[mask] - y_prob[mask],
                )
            else:
                ci_lo, ci_hi = (float("nan"), float("nan"))
            v_low, v_high = _decile_edges(edges, b, is_cat)
            rows.append({
                "feature": feat,
                "bucket": b,
                "value_low": v_low,
                "value_high": v_high,
                "is_categorical": is_cat,
                "n": n,
                "mean_pred": mean_pred,
                "mean_actual": mean_actual,
                "gap": gap,
                "ci_low": ci_lo,
                "ci_high": ci_hi,
                "suggestive": n < min_n,
            })

    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={
            "feature": pl.Utf8, "bucket": pl.Int64,
            "value_low": pl.Float64, "value_high": pl.Float64,
            "is_categorical": pl.Boolean, "n": pl.Int64,
            "mean_pred": pl.Float64, "mean_actual": pl.Float64, "gap": pl.Float64,
            "ci_low": pl.Float64, "ci_high": pl.Float64, "suggestive": pl.Boolean,
        }
    )


def per_feature_calibration_by_segment(
    df: pl.DataFrame,
    segment_col: str,
    *,
    prob_col: str = "y_prob",
    target_col: str = "y_test",
    feature_cols: Sequence[str] | None = None,
    min_n: int = MIN_N_PER_BUCKET,
) -> pl.DataFrame:
    """Same as per_feature_calibration but bucketing happens within each
    segment value (e.g., per-circuit), avoiding pooling across segments where
    feature distributions or calibration profiles differ.
    """
    if segment_col not in df.columns:
        raise ValueError(f"segment_col '{segment_col}' not in df.columns")
    out_frames = []
    for seg in df[segment_col].drop_nulls().unique().to_list():
        sub = df.filter(pl.col(segment_col) == seg)
        if sub.height == 0:
            continue
        seg_cal = per_feature_calibration(
            sub, prob_col=prob_col, target_col=target_col,
            feature_cols=feature_cols, min_n=min_n,
        )
        seg_cal = seg_cal.with_columns(
            pl.lit(str(seg)).alias(segment_col)
        )
        out_frames.append(seg_cal)
    if not out_frames:
        return pl.DataFrame()
    return pl.concat(out_frames, how="diagonal_relaxed")


# ---- 3D: naive-baseline comparison -----------------------------------------


def naive_baseline_comparison(
    df: pl.DataFrame,
    *,
    prob_col: str = "y_prob",
    target_col: str = "y_test",
    feature_cols: Sequence[str] | None = None,
    baseline_col: str | None = None,
) -> pl.DataFrame:
    """For each feature decile, compare model log loss against a naive baseline.

    Naive baseline = mean win rate within the `baseline_col` group, applied
    uniformly to all picks in that group. Plan calls for tournament-average
    (`tournament_id`); falls back to `circuit` when tournament_id isn't on the
    df (e.g., a join lost it). Pass `baseline_col` explicitly to override.

    A bucket where the naive baseline performs equally well as the model means
    the model has no signal in that feature regime.
    """
    if baseline_col is None:
        if "tournament_id" in df.columns:
            baseline_col = "tournament_id"
        elif "circuit" in df.columns:
            baseline_col = "circuit"
            logger.warning(
                "tournament_id not on df; using circuit as naive-baseline grouping"
            )
        else:
            raise ValueError(
                "neither tournament_id nor circuit available for naive baseline; "
                "pass baseline_col explicitly"
            )
    if feature_cols is None:
        feature_cols = _identify_feature_columns(df)
    else:
        feature_cols = [c for c in feature_cols if c in df.columns]

    y_prob = df[prob_col].to_numpy().astype(np.float64)
    y_true = df[target_col].to_numpy().astype(np.float64)

    # Pre-compute naive baseline: per-group mean win rate
    if baseline_col not in df.columns:
        raise ValueError(
            f"baseline_col '{baseline_col}' not in df; "
            f"cannot compute naive baseline."
        )
    group_means = (
        df.group_by(baseline_col).agg(pl.col(target_col).mean().alias("_grp_mean"))
    )
    baseline_lookup = dict(
        zip(group_means[baseline_col].to_list(), group_means["_grp_mean"].to_list())
    )
    naive = np.array(
        [baseline_lookup[v] for v in df[baseline_col].to_list()],
        dtype=np.float64,
    )

    # log loss (clipped) per pick — model and naive
    eps = 1e-15
    yp = np.clip(y_prob, eps, 1 - eps)
    yn = np.clip(naive, eps, 1 - eps)
    model_ll = -(y_true * np.log(yp) + (1 - y_true) * np.log(1 - yp))
    naive_ll = -(y_true * np.log(yn) + (1 - y_true) * np.log(1 - yn))

    rows = []
    for feat in feature_cols:
        feat_vals = df[feat].to_numpy().astype(np.float64)
        buckets, edges, is_cat = _decile_buckets(feat_vals)
        n_buckets = len(edges) if is_cat else N_DECILES
        for b in range(1, n_buckets + 1):
            mask = buckets == b
            n = int(mask.sum())
            if n == 0:
                continue
            model_loss = float(model_ll[mask].mean())
            naive_loss = float(naive_ll[mask].mean())
            v_low, v_high = _decile_edges(edges, b, is_cat)
            rows.append({
                "feature": feat,
                "bucket": b,
                "value_low": v_low,
                "value_high": v_high,
                "is_categorical": is_cat,
                "n": n,
                "model_loss": model_loss,
                "naive_loss": naive_loss,
                "improvement": naive_loss - model_loss,
            })

    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={
            "feature": pl.Utf8, "bucket": pl.Int64,
            "value_low": pl.Float64, "value_high": pl.Float64,
            "is_categorical": pl.Boolean, "n": pl.Int64,
            "model_loss": pl.Float64, "naive_loss": pl.Float64,
            "improvement": pl.Float64,
        }
    )


# ---- 3E: temporal drift -----------------------------------------------------


def temporal_drift(
    df: pl.DataFrame,
    *,
    prob_col: str = "y_prob",
    target_col: str = "y_test",
    feature_cols: Sequence[str] | None = None,
    date_col: str = "effective_match_date",
) -> pl.DataFrame:
    """Half-period split: per-feature calibration gap stability across time.

    Splits df into first-half and second-half by effective_match_date median,
    computes per-feature-decile gap in each half, reports the difference.

    Features with stable gap across halves → likely a stable feature-level
    miscalibration. Features with gap that differs across halves → regime
    shift or feature drift, not a stable feature-level issue.
    """
    if feature_cols is None:
        feature_cols = _identify_feature_columns(df)
    else:
        feature_cols = [c for c in feature_cols if c in df.columns]

    median_date = df[date_col].median()
    first_half = df.filter(pl.col(date_col) < median_date)
    second_half = df.filter(pl.col(date_col) >= median_date)

    def _compute_gaps(sub: pl.DataFrame, period: str) -> list[dict]:
        if sub.height == 0:
            return []
        yp = sub[prob_col].to_numpy().astype(np.float64)
        yt = sub[target_col].to_numpy().astype(np.float64)
        out = []
        for feat in feature_cols:
            vals = sub[feat].to_numpy().astype(np.float64)
            buckets, edges, is_cat = _decile_buckets(vals)
            n_buckets = len(edges) if is_cat else N_DECILES
            for b in range(1, n_buckets + 1):
                mask = buckets == b
                n = int(mask.sum())
                if n == 0:
                    continue
                v_low, v_high = _decile_edges(edges, b, is_cat)
                out.append({
                    "feature": feat,
                    "bucket": b,
                    "value_low": v_low,
                    "value_high": v_high,
                    "is_categorical": is_cat,
                    "period": period,
                    "n": n,
                    "gap": float(yt[mask].mean() - yp[mask].mean()),
                })
        return out

    rows = _compute_gaps(first_half, "first_half") + _compute_gaps(
        second_half, "second_half"
    )
    if not rows:
        return pl.DataFrame(
            schema={
                "feature": pl.Utf8, "decile": pl.Int64,
                "gap_first_half": pl.Float64, "gap_second_half": pl.Float64,
                "n_first_half": pl.Int64, "n_second_half": pl.Int64,
                "gap_diff": pl.Float64,
            }
        )

    long_df = pl.DataFrame(rows)
    wide = long_df.pivot(
        on="period", index=["feature", "bucket", "is_categorical"],
        values=["gap", "n", "value_low", "value_high"],
    )
    return wide.with_columns(
        (pl.col("gap_second_half").fill_null(0.0) - pl.col("gap_first_half").fill_null(0.0)).alias("gap_diff"),
    )
