"""Tests for Platt scaling + isotonic regression calibration."""

import numpy as np
import polars as pl
import pytest

from mvp.model.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    SegmentedIsotonicCalibrator,
    SegmentedPlattCalibrator,
    fit_calibrator_with_nested_cv,
    make_calibrator,
)


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


class TestIsotonicCalibrator:
    def test_improves_miscalibrated(self):
        rng = np.random.RandomState(99)
        n = 3000
        true_probs = rng.uniform(0.2, 0.8, n)
        labels = (rng.uniform(0, 1, n) < true_probs).astype(int)
        biased_probs = np.clip(true_probs + 0.15, 0.01, 0.99)

        cal = IsotonicCalibrator()
        cal.fit(biased_probs, labels)
        calibrated = cal.transform(biased_probs)

        def bucket_cal_error(probs, y):
            errors = []
            for lo in np.arange(0, 1, 0.1):
                mask = (probs >= lo) & (probs < lo + 0.1)
                if mask.sum() > 10:
                    errors.append(abs(probs[mask].mean() - y[mask].mean()))
            return np.mean(errors) if errors else 0.0

        assert bucket_cal_error(calibrated, labels) < bucket_cal_error(
            biased_probs, labels
        )

    def test_preserves_ordering(self):
        rng = np.random.RandomState(7)
        n = 2000
        probs = rng.uniform(0.1, 0.9, n)
        labels = (rng.uniform(0, 1, n) < probs).astype(int)

        cal = IsotonicCalibrator()
        cal.fit(probs, labels)
        calibrated = cal.transform(probs)

        sorted_idx = np.argsort(probs)
        calibrated_sorted = calibrated[sorted_idx]
        assert np.all(np.diff(calibrated_sorted) >= -1e-10)

    def test_output_in_unit_interval(self):
        rng = np.random.RandomState(1)
        n = 2000
        probs = rng.uniform(0.05, 0.95, n)
        labels = (rng.uniform(0, 1, n) < probs).astype(int)

        cal = IsotonicCalibrator()
        cal.fit(probs, labels)
        calibrated = cal.transform(probs)
        assert np.all(calibrated >= 0.0)
        assert np.all(calibrated <= 1.0)

    def test_unfitted_transform_is_noop(self):
        probs = np.array([0.1, 0.5, 0.9])
        cal = IsotonicCalibrator()
        np.testing.assert_array_equal(cal.transform(probs), probs)

    def test_unfitted_properties_raise(self):
        cal = IsotonicCalibrator()
        assert not cal.is_fitted
        with pytest.raises(ValueError):
            _ = cal.n_thresholds
        with pytest.raises(ValueError):
            _ = cal.y_min
        with pytest.raises(ValueError):
            _ = cal.y_max
        with pytest.raises(ValueError):
            _ = cal.grid_sample()

    def test_fitted_properties(self):
        rng = np.random.RandomState(42)
        probs = rng.uniform(0.1, 0.9, 2000)
        labels = (rng.uniform(0, 1, 2000) < probs).astype(int)

        cal = IsotonicCalibrator()
        cal.fit(probs, labels)
        assert cal.is_fitted
        assert cal.n_thresholds >= 2
        assert 0.0 <= cal.y_min <= cal.y_max <= 1.0
        grid = cal.grid_sample()
        assert len(grid) == 5
        # Grid must be monotonic non-decreasing
        assert all(grid[i] <= grid[i + 1] + 1e-10 for i in range(len(grid) - 1))

    def test_serialization_roundtrip(self, tmp_path):
        import joblib

        rng = np.random.RandomState(42)
        probs = rng.uniform(0.1, 0.9, 1000)
        labels = (rng.uniform(0, 1, 1000) < probs).astype(int)

        cal = IsotonicCalibrator()
        cal.fit(probs, labels)
        original_output = cal.transform(probs)

        path = tmp_path / "iso.joblib"
        joblib.dump(cal, path)
        loaded = joblib.load(path)
        np.testing.assert_array_almost_equal(loaded.transform(probs), original_output)
        assert loaded.n_thresholds == cal.n_thresholds

    def test_edge_probabilities(self):
        rng = np.random.RandomState(42)
        probs = rng.uniform(0.1, 0.9, 1000)
        labels = (rng.uniform(0, 1, 1000) < probs).astype(int)

        cal = IsotonicCalibrator()
        cal.fit(probs, labels)

        edge_probs = np.array([0.0, 1.0, 0.5])
        result = cal.transform(edge_probs)
        assert np.all(np.isfinite(result))
        assert np.all(result >= 0.0)
        assert np.all(result <= 1.0)

    def test_can_fit_non_sigmoid_shape(self):
        """Isotonic can fit calibration shapes Platt cannot — e.g., a flat-then-rising curve."""
        rng = np.random.RandomState(123)
        n = 4000
        probs = rng.uniform(0.0, 1.0, n)
        # Construct a "step" miscalibration: below 0.5 → always 0, above 0.5 → matches prob
        labels = np.where(probs < 0.5, 0, (rng.uniform(0, 1, n) < probs).astype(int))

        iso = IsotonicCalibrator().fit(probs, labels)
        # Predictions for prob < 0.5 should be ~0 (within tolerance)
        low = iso.transform(np.array([0.2, 0.3, 0.4]))
        # Predictions for prob > 0.5 should be > 0
        high = iso.transform(np.array([0.7, 0.8]))
        assert low.mean() < 0.1
        assert high.mean() > low.mean() + 0.2


class TestSegmentedIsotonicCalibrator:
    @staticmethod
    def _make_data(n_per_segment: dict[str, int], seed: int = 42):
        rng = np.random.RandomState(seed)
        all_probs, all_labels, all_circuits = [], [], []
        for circuit, n in n_per_segment.items():
            true_probs = rng.uniform(0.2, 0.8, n)
            shift = {"tour": 0.10, "chal": -0.05, "itf": 0.0}.get(circuit, 0.0)
            biased = np.clip(true_probs + shift, 0.05, 0.95)
            labels = (rng.uniform(0, 1, n) < true_probs).astype(int)
            all_probs.append(biased)
            all_labels.append(labels)
            all_circuits.extend([circuit] * n)
        return (
            np.concatenate(all_probs),
            np.concatenate(all_labels),
            pl.DataFrame({"circuit": all_circuits}),
        )

    def test_per_segment_fit(self):
        probs, labels, df = self._make_data({"tour": 1000, "chal": 1000}, seed=1)
        cal = SegmentedIsotonicCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        assert cal.is_fitted
        assert cal.n_segments == 2
        assert "tour" in cal._per_segment
        assert "chal" in cal._per_segment
        assert cal.mean_n_thresholds() > 0
        assert cal.max_n_thresholds() > 0

    def test_thin_segment_falls_back_to_global(self):
        probs, labels, df = self._make_data({"tour": 500, "chal": 50}, seed=2)
        cal = SegmentedIsotonicCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        assert "tour" in cal._per_segment
        assert "chal" not in cal._per_segment
        chal_probs = np.array([0.5, 0.7, 0.3])
        chal_df = pl.DataFrame({"circuit": ["chal", "chal", "chal"]})
        out = cal.transform(chal_probs, chal_df)
        np.testing.assert_array_almost_equal(out, cal._global.transform(chal_probs))

    def test_unknown_segment_falls_back_to_global(self):
        probs, labels, df = self._make_data({"tour": 500, "chal": 500}, seed=3)
        cal = SegmentedIsotonicCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)

        itf_probs = np.array([0.6, 0.4])
        itf_df = pl.DataFrame({"circuit": ["itf", "itf"]})
        out = cal.transform(itf_probs, itf_df)
        np.testing.assert_array_almost_equal(out, cal._global.transform(itf_probs))

    def test_save_load_roundtrip(self, tmp_path):
        import joblib

        probs, labels, df = self._make_data({"tour": 800, "chal": 800}, seed=7)
        cal = SegmentedIsotonicCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)
        original_out = cal.transform(probs, df)

        path = tmp_path / "seg_iso.joblib"
        joblib.dump(cal, path)
        loaded = joblib.load(path)

        assert loaded.segments == cal.segments
        assert loaded.n_segments == cal.n_segments
        assert set(loaded._per_segment.keys()) == set(cal._per_segment.keys())
        np.testing.assert_array_almost_equal(loaded.transform(probs, df), original_out)

    def test_output_shape_and_range(self):
        probs, labels, df = self._make_data({"tour": 500, "chal": 500}, seed=11)
        cal = SegmentedIsotonicCalibrator(segments=["circuit"], min_n=200)
        cal.fit(probs, labels, df)
        out = cal.transform(probs, df)
        assert out.shape == probs.shape
        assert np.all(out >= 0.0)
        assert np.all(out <= 1.0)


class TestMakeCalibrator:
    def test_default_method_is_platt(self):
        class Cfg:
            method = "platt"
            segments = None
            min_n = 200
        assert isinstance(make_calibrator(Cfg()), PlattCalibrator)

    def test_segmented_platt(self):
        class Cfg:
            method = "platt"
            segments = ["circuit"]
            min_n = 200
        assert isinstance(make_calibrator(Cfg()), SegmentedPlattCalibrator)

    def test_isotonic(self):
        class Cfg:
            method = "isotonic"
            segments = None
            min_n = 200
        assert isinstance(make_calibrator(Cfg()), IsotonicCalibrator)

    def test_segmented_isotonic(self):
        class Cfg:
            method = "isotonic"
            segments = ["circuit"]
            min_n = 200
        assert isinstance(make_calibrator(Cfg()), SegmentedIsotonicCalibrator)

    def test_none_config_returns_none(self):
        assert make_calibrator(None) is None

    def test_missing_method_attribute_defaults_to_platt(self):
        """Older configs without `method` attribute should still produce Platt."""

        class Cfg:
            segments = None
            min_n = 200

        assert isinstance(make_calibrator(Cfg()), PlattCalibrator)


class TestNestedCVCalibration:
    """Verify fit_calibrator_with_nested_cv produces honest diagnostic preds."""

    @staticmethod
    def _make_folds(n_folds=4, n_per_fold=500, seed=42):
        rng = np.random.RandomState(seed)
        folds = []
        for k in range(n_folds):
            probs = rng.uniform(0.1, 0.9, n_per_fold)
            labels = (rng.uniform(0, 1, n_per_fold) < probs).astype(int)
            df = pl.DataFrame({"circuit": ["tour"] * n_per_fold})
            folds.append({"y_prob": probs.copy(), "y_true": labels, "df": df})
        return folds

    def test_deployed_calibrator_fit_on_all_folds(self):
        """Deployed calibrator's slope should equal fitting Platt on the union."""

        class Cfg:
            method = "platt"
            segments = None
            min_n = 200

        folds = self._make_folds(n_folds=4)
        union_probs = np.concatenate([f["y_prob"] for f in folds])
        union_labels = np.concatenate([f["y_true"] for f in folds])
        # Pre-compute reference: Platt fit on union
        reference = PlattCalibrator().fit(union_probs, union_labels)

        deployed = fit_calibrator_with_nested_cv(folds, Cfg())
        assert isinstance(deployed, PlattCalibrator)
        assert abs(deployed.slope - reference.slope) < 1e-10
        assert abs(deployed.intercept - reference.intercept) < 1e-10

    def test_tuning_preds_are_not_calibrated_by_deployed(self):
        """The fold preds should differ from what the deployed calibrator would produce —
        proof that nested-out (not deployed) was applied."""

        class Cfg:
            method = "isotonic"
            segments = None
            min_n = 200

        folds = self._make_folds(n_folds=4, seed=7)
        # Snapshot what deployed-cal-on-all would output for each fold
        raw_per_fold = [f["y_prob"].copy() for f in folds]
        deployed_for_check = IsotonicCalibrator().fit(
            np.concatenate(raw_per_fold),
            np.concatenate([f["y_true"] for f in folds]),
        )
        deployed_outputs = [deployed_for_check.transform(p) for p in raw_per_fold]

        # Run nested CV
        fit_calibrator_with_nested_cv(folds, Cfg())

        # For at least one fold, nested output should differ from deployed output
        any_diff = any(
            not np.allclose(folds[i]["y_prob"], deployed_outputs[i])
            for i in range(len(folds))
        )
        assert any_diff, (
            "Nested-CV preds should differ from deployed-cal-on-all preds; "
            "if they match, nested wasn't actually run."
        )

    def test_isotonic_in_sample_perfection_removed(self):
        """Isotonic fit on union+evaluated on union → perfect cal.
        Under nested CV, fold preds should NOT achieve perfect cal."""

        class Cfg:
            method = "isotonic"
            segments = None
            min_n = 200

        # Build folds where labels are noisy versions of probs
        rng = np.random.RandomState(99)
        folds = []
        for k in range(4):
            probs = rng.uniform(0.1, 0.9, 800)
            labels = (rng.uniform(0, 1, 800) < probs).astype(int)
            folds.append({
                "y_prob": probs.copy(),
                "y_true": labels,
                "df": pl.DataFrame({"circuit": ["tour"] * 800}),
            })

        # Calculate in-sample perfect-fit calibration (sanity)
        union_probs = np.concatenate([f["y_prob"] for f in folds])
        union_labels = np.concatenate([f["y_true"] for f in folds])
        in_sample_iso = IsotonicCalibrator().fit(union_probs, union_labels)
        in_sample_preds = in_sample_iso.transform(union_probs)

        def bucket_cal_error(probs, y):
            errors = []
            for lo in np.arange(0, 1, 0.1):
                mask = (probs >= lo) & (probs < lo + 0.1)
                if mask.sum() > 10:
                    errors.append(abs(probs[mask].mean() - y[mask].mean()))
            return np.mean(errors) if errors else 0.0

        in_sample_err = bucket_cal_error(in_sample_preds, union_labels)

        # Now nested
        fit_calibrator_with_nested_cv(folds, Cfg())
        nested_preds = np.concatenate([f["y_prob"] for f in folds])
        nested_err = bucket_cal_error(nested_preds, union_labels)

        assert in_sample_err < 0.005, "in-sample isotonic should be near-perfect"
        assert nested_err > in_sample_err, (
            f"Nested-CV should reveal residual miscalibration "
            f"(in_sample={in_sample_err:.4f}, nested={nested_err:.4f})"
        )

    def test_single_fold_falls_back_to_in_sample(self):
        """Single-fold case can't nest; should apply deployed in-sample."""

        class Cfg:
            method = "platt"
            segments = None
            min_n = 200

        folds = self._make_folds(n_folds=1, n_per_fold=500)
        raw = folds[0]["y_prob"].copy()
        labels = folds[0]["y_true"]

        deployed = fit_calibrator_with_nested_cv(folds, Cfg())
        # Output should match what deployed.transform(raw) produces
        expected = deployed.transform(raw)
        np.testing.assert_array_almost_equal(folds[0]["y_prob"], expected)

    def test_none_config_returns_none_no_mutation(self):
        folds = self._make_folds(n_folds=3)
        snapshot = [f["y_prob"].copy() for f in folds]
        result = fit_calibrator_with_nested_cv(folds, None)
        assert result is None
        # No mutation
        for orig, after in zip(snapshot, folds, strict=True):
            np.testing.assert_array_equal(orig, after["y_prob"])

    def test_segmented_nested_cv_works(self):
        """Segmented variant goes through the segmented fit path with the per-fold df."""

        class Cfg:
            method = "isotonic"
            segments = ["circuit"]
            min_n = 100

        rng = np.random.RandomState(11)
        folds = []
        for k in range(4):
            n = 600
            probs = rng.uniform(0.1, 0.9, n)
            labels = (rng.uniform(0, 1, n) < probs).astype(int)
            circuits = rng.choice(["tour", "chal"], size=n)
            folds.append({
                "y_prob": probs.copy(),
                "y_true": labels,
                "df": pl.DataFrame({"circuit": circuits}),
            })

        deployed = fit_calibrator_with_nested_cv(folds, Cfg())
        assert isinstance(deployed, SegmentedIsotonicCalibrator)
        assert deployed.n_segments >= 1
        # All outputs in [0, 1]
        for f in folds:
            assert np.all(f["y_prob"] >= 0.0)
            assert np.all(f["y_prob"] <= 1.0)
