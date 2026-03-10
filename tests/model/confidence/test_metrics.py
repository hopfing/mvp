"""Tests for confidence metrics."""

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from mvp.model.confidence.metrics import (
    WindowDistribution,
    compute_rolling_signed_calibration,
    compute_reliability_profile,
    ReliabilityProfile,
)


class TestWindowDistribution:
    def test_from_values(self):
        dist = WindowDistribution.from_values([1.0, 2.0, 3.0, 4.0, 5.0])
        assert dist.median == pytest.approx(3.0)
        assert dist.p25 == pytest.approx(2.0)
        assert dist.p75 == pytest.approx(4.0)
        assert dist.min == pytest.approx(1.0)
        assert dist.max == pytest.approx(5.0)
        assert dist.n_windows == 5

    def test_from_values_empty(self):
        dist = WindowDistribution.from_values([])
        assert dist is None


class TestRollingSignedCalibration:
    def _make_df(self, n, dates, probs, outcomes):
        return pl.DataFrame({
            "effective_match_date": dates,
            "favored_prob": probs,
            "favored_won": outcomes,
        }).with_columns(pl.col("effective_match_date").cast(pl.Date))

    def test_perfectly_calibrated(self):
        rng = np.random.default_rng(42)
        n = 2000
        dates = [date(2022, 1, 1) + timedelta(days=i) for i in range(n)]
        probs = np.full(n, 0.65)
        outcomes = np.zeros(n, dtype=int)
        for start in range(0, n, 90):
            chunk = min(90, n - start)
            n_wins = int(round(chunk * 0.65))
            outcomes[start:start + n_wins] = 1
            rng.shuffle(outcomes[start:start + chunk])

        df = self._make_df(n, dates, probs, outcomes)
        dist = compute_rolling_signed_calibration(df, window_months=3)
        assert dist is not None
        assert abs(dist.median) < 0.03

    def test_underconfident_positive_error(self):
        n = 1000
        dates = [date(2022, 1, 1) + timedelta(days=i) for i in range(n)]
        probs = np.full(n, 0.60)
        outcomes = np.ones(n, dtype=int)

        df = self._make_df(n, dates, probs, outcomes)
        dist = compute_rolling_signed_calibration(df, window_months=3)
        assert dist is not None
        assert dist.median > 0.3

    def test_overconfident_negative_error(self):
        n = 1000
        dates = [date(2022, 1, 1) + timedelta(days=i) for i in range(n)]
        probs = np.full(n, 0.80)
        outcomes = np.zeros(n, dtype=int)

        df = self._make_df(n, dates, probs, outcomes)
        dist = compute_rolling_signed_calibration(df, window_months=3)
        assert dist is not None
        assert dist.median < -0.5

    def test_window_count_reasonable(self):
        n = 1000
        dates = [date(2022, 1, 1) + timedelta(days=int(i * 1095 / n)) for i in range(n)]
        probs = np.full(n, 0.60)
        outcomes = np.ones(n, dtype=int)

        df = self._make_df(n, dates, probs, outcomes)
        dist = compute_rolling_signed_calibration(df, window_months=3, step_months=1)
        assert dist is not None
        assert dist.n_windows >= 30

    def test_short_data_returns_none_or_sparse(self):
        dates = [date(2022, 1, 1) + timedelta(days=i) for i in range(30)]
        probs = np.full(30, 0.60)
        outcomes = np.ones(30, dtype=int)

        df = self._make_df(30, dates, probs, outcomes)
        dist = compute_rolling_signed_calibration(df, window_months=12)
        assert dist is None or dist.n_windows <= 1


class TestReliabilityProfile:
    def test_basic_profile(self, make_oof_df):
        from mvp.model.confidence.validator import prepare_oof
        df = make_oof_df(n=2000, cal_bias=0.02)
        oof = prepare_oof([{
            "df": df.drop("y_true", "y_prob"),
            "y_true": df["y_true"].to_numpy(),
            "y_prob": df["y_prob"].to_numpy(),
        }])
        profile = compute_reliability_profile(oof)
        assert profile.n_matches == 2000
        assert 0.0 <= profile.accuracy <= 1.0
        assert profile.err80 >= 0.0
        assert profile.cal_3mo is not None or profile.cal_6mo is not None or profile.cal_12mo is not None
