"""Tests for sample weighting utilities."""

from datetime import date

import numpy as np
import pytest

from mvp.model.config import SampleWeightConfig
from mvp.model.weighting import compute_sample_weights


class TestRecencyWeights:
    def test_most_recent_gets_weight_one(self):
        dates = np.array([date(2025, 1, 1), date(2025, 6, 1), date(2025, 12, 31)])
        config = SampleWeightConfig(type="recency", half_life_days=365)
        weights = compute_sample_weights(dates, config)
        assert weights[-1] == pytest.approx(1.0)

    def test_half_life_produces_half_weight(self):
        dates = np.array([date(2025, 1, 1), date(2026, 1, 1)])
        config = SampleWeightConfig(type="recency", half_life_days=365)
        weights = compute_sample_weights(dates, config)
        assert weights[0] == pytest.approx(0.5, abs=1e-6)
        assert weights[1] == pytest.approx(1.0)

    def test_newer_matches_get_higher_weight(self):
        dates = np.array([date(2023, 1, 1), date(2024, 1, 1), date(2025, 1, 1)])
        config = SampleWeightConfig(type="recency", half_life_days=365)
        weights = compute_sample_weights(dates, config)
        assert weights[0] < weights[1] < weights[2]

    def test_all_same_date_equal_weights(self):
        dates = np.array([date(2025, 6, 1)] * 5)
        config = SampleWeightConfig(type="recency", half_life_days=365)
        weights = compute_sample_weights(dates, config)
        np.testing.assert_array_almost_equal(weights, np.ones(5))

    def test_weights_always_positive(self):
        dates = np.array([date(2010, 1, 1), date(2015, 1, 1), date(2025, 1, 1)])
        config = SampleWeightConfig(type="recency", half_life_days=365)
        weights = compute_sample_weights(dates, config)
        assert np.all(weights > 0)

    def test_shorter_half_life_decays_faster(self):
        dates = np.array([date(2024, 1, 1), date(2025, 1, 1)])
        short = SampleWeightConfig(type="recency", half_life_days=180)
        long = SampleWeightConfig(type="recency", half_life_days=730)
        w_short = compute_sample_weights(dates, short)
        w_long = compute_sample_weights(dates, long)
        # Older match should have lower weight with shorter half-life
        assert w_short[0] < w_long[0]


class TestSampleWeightConfig:
    def test_config_parsing(self):
        config = SampleWeightConfig(type="recency", half_life_days=365)
        assert config.type == "recency"
        assert config.half_life_days == 365

    def test_invalid_type_rejected(self):
        with pytest.raises(ValueError):
            SampleWeightConfig(type="unknown", half_life_days=365)
