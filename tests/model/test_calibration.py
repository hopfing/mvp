"""Tests for Platt scaling calibration."""

import numpy as np
import polars as pl
import pytest

from mvp.model.calibration import PlattCalibrator, SegmentedPlattCalibrator


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


class TestSegmentedPlattCalibrator:
    @staticmethod
    def _make_data(n_per_segment: dict[str, int], seed: int = 42):
        """Build (probs, labels, df) where each segment has its own bias."""
        rng = np.random.RandomState(seed)
        all_probs, all_labels, all_circuits = [], [], []
        # Each segment gets a different multiplicative shift to its labels
        for circuit, n in n_per_segment.items():
            true_probs = rng.uniform(0.2, 0.8, n)
            # Skew per-segment so each Platt fit lands at different slope/intercept
            shift = {"tour": 0.10, "chal": -0.05, "itf": 0.0}.get(circuit, 0.0)
            biased = np.clip(true_probs + shift, 0.05, 0.95)
            labels = (rng.uniform(0, 1, n) < true_probs).astype(int)
            all_probs.append(biased)
            all_labels.append(labels)
            all_circuits.extend([circuit] * n)
        probs = np.concatenate(all_probs)
        labels = np.concatenate(all_labels)
        df = pl.DataFrame({"circuit": all_circuits})
        return probs, labels, df

    def test_per_segment_fit_with_sufficient_n(self):
        """Each segment with n >= min_n gets its own Platt with distinct slope."""
        probs, labels, df = self._make_data({"tour": 1000, "chal": 1000}, seed=1)
        cal = SegmentedPlattCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        assert cal.is_fitted
        assert cal.n_segments == 2
        assert "tour" in cal._per_segment
        assert "chal" in cal._per_segment
        # Different biases → different Platts
        assert (
            cal._per_segment["tour"].slope
            != pytest.approx(cal._per_segment["chal"].slope, abs=1e-4)
        )

    def test_thin_segment_falls_back_to_global(self):
        """Segment with n < min_n is excluded from per-segment dict."""
        # tour: 500 rows, chal: 50 rows (below min_n=200)
        probs, labels, df = self._make_data({"tour": 500, "chal": 50}, seed=2)
        cal = SegmentedPlattCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        assert "tour" in cal._per_segment
        assert "chal" not in cal._per_segment
        # chal rows at transform-time should route through the global calibrator
        # which is fitted on all data
        chal_probs = np.array([0.5, 0.7, 0.3])
        chal_df = pl.DataFrame({"circuit": ["chal", "chal", "chal"]})
        out = cal.transform(chal_probs, chal_df)
        # Should equal what the global produces
        np.testing.assert_array_almost_equal(out, cal._global.transform(chal_probs))

    def test_unknown_segment_falls_back_to_global(self):
        """Segment not seen during fit routes through global at transform time."""
        probs, labels, df = self._make_data({"tour": 500, "chal": 500}, seed=3)
        cal = SegmentedPlattCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        # ITF was never seen
        itf_probs = np.array([0.6, 0.4])
        itf_df = pl.DataFrame({"circuit": ["itf", "itf"]})
        out = cal.transform(itf_probs, itf_df)
        np.testing.assert_array_almost_equal(out, cal._global.transform(itf_probs))

    def test_tournament_stage_derived_inside_calibrator(self):
        """tournament_stage is auto-computed from round when used as a segment."""
        rng = np.random.RandomState(5)
        n = 600
        # Mix of rounds; tournament_stage will derive these into Qualifying/Early/Late
        rounds = rng.choice(["Q1", "R32", "F"], size=n)
        probs = rng.uniform(0.3, 0.7, n)
        labels = (rng.uniform(0, 1, n) < probs).astype(int)
        df = pl.DataFrame({"round": rounds})

        cal = SegmentedPlattCalibrator(segments=["tournament_stage"], min_n=50)
        cal.fit(probs, labels, df)
        # Three stages got fit (Qualifying, Early, Late)
        assert set(cal._per_segment.keys()).issubset({"Qualifying", "Early", "Late"})
        assert cal.n_segments >= 2  # at least 2 of the 3 should have n >= 50

    def test_save_load_roundtrip(self, tmp_path):
        """Pickled SegmentedPlattCalibrator round-trips with identical outputs."""
        import joblib

        probs, labels, df = self._make_data({"tour": 800, "chal": 800}, seed=7)
        cal = SegmentedPlattCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)
        original_out = cal.transform(probs, df)

        path = tmp_path / "segmented.joblib"
        joblib.dump(cal, path)
        loaded = joblib.load(path)

        assert loaded.segments == cal.segments
        assert loaded.n_segments == cal.n_segments
        assert set(loaded._per_segment.keys()) == set(cal._per_segment.keys())
        np.testing.assert_array_almost_equal(loaded.transform(probs, df), original_out)

    def test_transform_output_shape_and_range(self):
        """Output length == input, values in [0, 1]."""
        probs, labels, df = self._make_data({"tour": 500, "chal": 500}, seed=11)
        cal = SegmentedPlattCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        out = cal.transform(probs, df)
        assert out.shape == probs.shape
        assert np.all(out >= 0.0)
        assert np.all(out <= 1.0)

    def test_single_class_segment_skipped(self):
        """Segment with only one class label is skipped (Platt needs both classes)."""
        # tour: all labels = 1 (no negative class), chal: balanced
        rng = np.random.RandomState(13)
        tour_probs = rng.uniform(0.5, 0.9, 400)
        tour_labels = np.ones(400, dtype=int)
        chal_probs = rng.uniform(0.2, 0.8, 400)
        chal_labels = (rng.uniform(0, 1, 400) < chal_probs).astype(int)
        probs = np.concatenate([tour_probs, chal_probs])
        labels = np.concatenate([tour_labels, chal_labels])
        df = pl.DataFrame({"circuit": ["tour"] * 400 + ["chal"] * 400})

        cal = SegmentedPlattCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        assert "tour" not in cal._per_segment  # skipped — single class
        assert "chal" in cal._per_segment
