"""Tests for Platt scaling calibration."""

import numpy as np
import pytest

from mvp.model.calibration import PlattCalibrator


class TestPlattCalibrator:
    def test_near_identity_on_well_calibrated(self):
        """Platt on well-calibrated data should be near-identity (slope ~1, intercept ~0)."""
        rng = np.random.RandomState(42)
        n = 5000
        probs = rng.uniform(0.1, 0.9, n)
        labels = (rng.uniform(0, 1, n) < probs).astype(int)

        cal = PlattCalibrator()
        cal.fit(probs, labels)

        assert abs(cal.slope - 1.0) < 0.15
        assert abs(cal.intercept) < 0.15

    def test_improves_miscalibrated(self):
        """Platt should reduce calibration error on systematically biased probs."""
        rng = np.random.RandomState(99)
        n = 3000
        true_probs = rng.uniform(0.2, 0.8, n)
        labels = (rng.uniform(0, 1, n) < true_probs).astype(int)
        # Systematically shift predictions high
        biased_probs = np.clip(true_probs + 0.15, 0.01, 0.99)

        cal = PlattCalibrator()
        cal.fit(biased_probs, labels)
        calibrated = cal.transform(biased_probs)

        # Measure calibration error (mean absolute deviation in 5% buckets)
        def bucket_cal_error(probs, y):
            errors = []
            for lo in np.arange(0, 1, 0.1):
                mask = (probs >= lo) & (probs < lo + 0.1)
                if mask.sum() > 10:
                    errors.append(abs(probs[mask].mean() - y[mask].mean()))
            return np.mean(errors) if errors else 0.0

        raw_err = bucket_cal_error(biased_probs, labels)
        cal_err = bucket_cal_error(calibrated, labels)
        assert cal_err < raw_err

    def test_preserves_ordering(self):
        """Platt is monotonic — relative ordering should be preserved."""
        rng = np.random.RandomState(7)
        n = 2000
        probs = rng.uniform(0.1, 0.9, n)
        labels = (rng.uniform(0, 1, n) < probs).astype(int)

        cal = PlattCalibrator()
        cal.fit(probs, labels)
        calibrated = cal.transform(probs)

        sorted_idx = np.argsort(probs)
        calibrated_sorted = calibrated[sorted_idx]
        assert np.all(np.diff(calibrated_sorted) >= -1e-10)

    def test_output_in_unit_interval(self):
        """All outputs must be in [0, 1]."""
        rng = np.random.RandomState(1)
        n = 2000
        probs = rng.uniform(0.05, 0.95, n)
        labels = (rng.uniform(0, 1, n) < probs).astype(int)

        cal = PlattCalibrator()
        cal.fit(probs, labels)
        calibrated = cal.transform(probs)

        assert np.all(calibrated >= 0.0)
        assert np.all(calibrated <= 1.0)

    def test_unfitted_transform_is_noop(self):
        """Transform on unfitted calibrator returns input unchanged."""
        probs = np.array([0.1, 0.5, 0.9])
        cal = PlattCalibrator()
        result = cal.transform(probs)
        np.testing.assert_array_equal(result, probs)

    def test_unfitted_properties_raise(self):
        cal = PlattCalibrator()
        assert not cal.is_fitted
        with pytest.raises(ValueError):
            _ = cal.slope
        with pytest.raises(ValueError):
            _ = cal.intercept

    def test_fitted_properties(self):
        rng = np.random.RandomState(42)
        probs = rng.uniform(0.1, 0.9, 1000)
        labels = (rng.uniform(0, 1, 1000) < probs).astype(int)

        cal = PlattCalibrator()
        cal.fit(probs, labels)
        assert cal.is_fitted
        assert isinstance(cal.slope, float)
        assert isinstance(cal.intercept, float)

    def test_serialization_roundtrip(self, tmp_path):
        """Calibrator should survive joblib save/load."""
        import joblib

        rng = np.random.RandomState(42)
        probs = rng.uniform(0.1, 0.9, 1000)
        labels = (rng.uniform(0, 1, 1000) < probs).astype(int)

        cal = PlattCalibrator()
        cal.fit(probs, labels)
        original_output = cal.transform(probs)

        path = tmp_path / "calibrator.joblib"
        joblib.dump(cal, path)
        loaded = joblib.load(path)

        np.testing.assert_array_almost_equal(loaded.transform(probs), original_output)
        assert abs(loaded.slope - cal.slope) < 1e-10
        assert abs(loaded.intercept - cal.intercept) < 1e-10

    def test_edge_probabilities(self):
        """Should handle 0.0 and 1.0 input probabilities."""
        rng = np.random.RandomState(42)
        probs = rng.uniform(0.1, 0.9, 1000)
        labels = (rng.uniform(0, 1, 1000) < probs).astype(int)

        cal = PlattCalibrator()
        cal.fit(probs, labels)

        edge_probs = np.array([0.0, 1.0, 0.5])
        result = cal.transform(edge_probs)
        assert np.all(np.isfinite(result))
        assert np.all(result >= 0.0)
        assert np.all(result <= 1.0)
