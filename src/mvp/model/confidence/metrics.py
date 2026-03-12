"""Confidence validation metrics — rolling window signed calibration."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl


@dataclass(frozen=True)
class WindowDistribution:
    """Distribution statistics from rolling window calibration errors."""

    median: float
    p25: float
    p75: float
    min: float
    max: float
    n_windows: int
    median_n_per_window: int

    @classmethod
    def from_values(
        cls, errors: list[float], window_ns: list[int] | None = None
    ) -> WindowDistribution | None:
        if not errors:
            return None
        arr = np.array(errors)
        return cls(
            median=float(np.median(arr)),
            p25=float(np.percentile(arr, 25)),
            p75=float(np.percentile(arr, 75)),
            min=float(np.min(arr)),
            max=float(np.max(arr)),
            n_windows=len(errors),
            median_n_per_window=int(np.median(window_ns)) if window_ns else 0,
        )


@dataclass(frozen=True)
class ReliabilityProfile:
    """Full reliability profile for a slice of predictions."""

    n_matches: int
    accuracy: float
    err80: float
    signed_cal: float
    log_loss: float
    brier_score: float
    roc_auc: float | None
    cal_3mo: WindowDistribution | None
    cal_6mo: WindowDistribution | None
    cal_12mo: WindowDistribution | None


def compute_rolling_signed_calibration(
    df: pl.DataFrame,
    window_months: int,
    step_months: int = 1,
    min_matches_per_window: int = 10,
) -> WindowDistribution | None:
    """Compute signed calibration error in rolling time windows.

    signed_error = mean(favored_won) - mean(favored_prob)
    Positive = underconfident. Negative = overconfident.
    """
    dates = df["effective_match_date"].cast(pl.Date).sort()
    min_date = dates.min()
    max_date = dates.max()
    if min_date is None or max_date is None:
        return None

    errors: list[float] = []
    window_ns: list[int] = []

    current_start = min_date
    while True:
        window_end = _add_months(current_start, window_months)
        if window_end > max_date:
            break

        window_df = df.filter(
            (pl.col("effective_match_date") >= current_start)
            & (pl.col("effective_match_date") < window_end)
        )

        if len(window_df) >= min_matches_per_window:
            actual = window_df["favored_won"].mean()
            predicted = window_df["favored_prob"].mean()
            signed_error = actual - predicted
            errors.append(signed_error)
            window_ns.append(len(window_df))

        current_start = _add_months(current_start, step_months)

    return WindowDistribution.from_values(errors, window_ns)


def compute_reliability_profile(df: pl.DataFrame) -> ReliabilityProfile:
    """Compute full reliability profile for a slice of OOF predictions."""
    n = len(df)
    if n == 0:
        return ReliabilityProfile(
            n_matches=0, accuracy=0.0, err80=0.0, signed_cal=0.0,
            log_loss=0.0, brier_score=0.0, roc_auc=None,
            cal_3mo=None, cal_6mo=None, cal_12mo=None,
        )

    accuracy = float(df["favored_won"].mean())

    high_conf = df.filter(pl.col("favored_prob") >= 0.80)
    if len(high_conf) > 0:
        err80 = 1.0 - float(high_conf["favored_won"].mean())
    else:
        err80 = 0.0

    signed_cal = float(df["favored_won"].mean() - df["favored_prob"].mean())

    # Scoring metrics (use raw y_prob/y_true for proper computation)
    y_true = df["y_true"].to_numpy().astype(float)
    y_prob = np.clip(df["y_prob"].to_numpy(), 1e-15, 1 - 1e-15)
    log_loss = -float(np.mean(y_true * np.log(y_prob) + (1 - y_true) * np.log(1 - y_prob)))
    brier_score = float(np.mean((y_prob - y_true) ** 2))

    roc_auc = _roc_auc(y_true, y_prob)

    cal_3mo = compute_rolling_signed_calibration(df, window_months=3)
    cal_6mo = compute_rolling_signed_calibration(df, window_months=6)
    cal_12mo = compute_rolling_signed_calibration(df, window_months=12)

    return ReliabilityProfile(
        n_matches=n,
        accuracy=accuracy,
        err80=err80,
        signed_cal=signed_cal,
        log_loss=log_loss,
        brier_score=brier_score,
        roc_auc=roc_auc,
        cal_3mo=cal_3mo,
        cal_6mo=cal_6mo,
        cal_12mo=cal_12mo,
    )


def _roc_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float | None:
    """Compute ROC AUC using rank-based method. Returns None if single-class."""
    n_pos = int(y_true.sum())
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    order = np.argsort(y_prob)
    ranks = np.empty(len(y_prob), dtype=float)
    ranks[order] = np.arange(1, len(y_prob) + 1, dtype=float)
    # Handle ties: replace ranks with average rank for tied values
    sorted_probs = y_prob[order]
    i = 0
    while i < len(sorted_probs):
        j = i + 1
        while j < len(sorted_probs) and sorted_probs[j] == sorted_probs[i]:
            j += 1
        if j > i + 1:
            avg_rank = (i + 1 + j) / 2.0
            for k in range(i, j):
                ranks[order[k]] = avg_rank
        i = j
    auc = (ranks[y_true == 1].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
    return float(auc)


def _add_months(d: date, months: int) -> date:
    """Add months to a date, clamping day to valid range."""
    month = d.month + months
    year = d.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    max_day = calendar.monthrange(year, month)[1]
    day = min(d.day, max_day)
    return date(year, month, day)
