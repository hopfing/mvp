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
            cal_3mo=None, cal_6mo=None, cal_12mo=None,
        )

    accuracy = float(df["favored_won"].mean())

    high_conf = df.filter(pl.col("favored_prob") >= 0.80)
    if len(high_conf) > 0:
        err80 = 1.0 - float(high_conf["favored_won"].mean())
    else:
        err80 = 0.0

    signed_cal = float(df["favored_won"].mean() - df["favored_prob"].mean())

    cal_3mo = compute_rolling_signed_calibration(df, window_months=3)
    cal_6mo = compute_rolling_signed_calibration(df, window_months=6)
    cal_12mo = compute_rolling_signed_calibration(df, window_months=12)

    return ReliabilityProfile(
        n_matches=n,
        accuracy=accuracy,
        err80=err80,
        signed_cal=signed_cal,
        cal_3mo=cal_3mo,
        cal_6mo=cal_6mo,
        cal_12mo=cal_12mo,
    )


def _add_months(d: date, months: int) -> date:
    """Add months to a date, clamping day to valid range."""
    month = d.month + months
    year = d.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    max_day = calendar.monthrange(year, month)[1]
    day = min(d.day, max_day)
    return date(year, month, day)
