"""Tests for sample weighting utilities."""

from datetime import date

import numpy as np
import polars as pl
import pytest

from mvp.model.config import SampleWeightConfig
from mvp.model.weighting import compute_sample_weights, sample_weights_from_frame


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


class TestGroupWeights:
    def _cfg(self, rules, default_weight=0.3):
        return SampleWeightConfig(
            type="group", rules=rules, default_weight=default_weight
        )

    def test_single_column_rule_and_default(self):
        attrs = {"surface": np.array(["Clay", "Hard", "Grass", "Clay"])}
        cfg = self._cfg([{"where": {"surface": "Clay"}, "weight": 1.0}])
        weights = compute_sample_weights(None, cfg, attributes=attrs)
        np.testing.assert_array_equal(weights, [1.0, 0.3, 0.3, 1.0])

    def test_two_column_and_condition(self):
        # Hard + Outdoor (indoor False) gets full weight; other combos default.
        attrs = {
            "surface": np.array(["Hard", "Hard", "Clay", "Grass"]),
            "indoor": np.array([False, True, False, False]),
        }
        cfg = self._cfg(
            [{"where": {"surface": "Hard", "indoor": False}, "weight": 1.0}]
        )
        weights = compute_sample_weights(None, cfg, attributes=attrs)
        np.testing.assert_array_equal(weights, [1.0, 0.3, 0.3, 0.3])

    def test_first_matching_rule_wins(self):
        # Tiered: outdoor hard 1.0, any other hard 0.6, rest default 0.3.
        attrs = {
            "surface": np.array(["Hard", "Hard", "Clay"]),
            "indoor": np.array([False, True, False]),
        }
        cfg = self._cfg([
            {"where": {"surface": "Hard", "indoor": False}, "weight": 1.0},
            {"where": {"surface": "Hard"}, "weight": 0.6},
        ])
        weights = compute_sample_weights(None, cfg, attributes=attrs)
        np.testing.assert_array_equal(weights, [1.0, 0.6, 0.3])

    def test_null_value_gets_default(self):
        attrs = {"surface": np.array(["Clay", None, "Hard"], dtype=object)}
        cfg = self._cfg([{"where": {"surface": "Clay"}, "weight": 1.0}])
        weights = compute_sample_weights(None, cfg, attributes=attrs)
        np.testing.assert_array_equal(weights, [1.0, 0.3, 0.3])

    def test_default_weight_defaults_to_one(self):
        attrs = {"surface": np.array(["Clay", "Hard"])}
        cfg = SampleWeightConfig(
            type="group", rules=[{"where": {"surface": "Clay"}, "weight": 1.0}]
        )
        weights = compute_sample_weights(None, cfg, attributes=attrs)
        np.testing.assert_array_equal(weights, [1.0, 1.0])

    def test_missing_attributes_raises(self):
        cfg = self._cfg([{"where": {"surface": "Clay"}, "weight": 1.0}])
        with pytest.raises(ValueError, match="requires per-row column values"):
            compute_sample_weights(None, cfg, attributes=None)


class TestSampleWeightsFromFrame:
    def _frame(self):
        return pl.DataFrame(
            {
                "effective_match_date": [date(2025, 1, 1), date(2026, 1, 1)],
                "surface": ["Clay", "Hard"],
                "indoor": [False, True],
            }
        )

    def test_recency_reads_dates(self):
        cfg = SampleWeightConfig(type="recency", half_life_days=365)
        weights = sample_weights_from_frame(self._frame(), cfg)
        assert weights[0] == pytest.approx(0.5, abs=1e-6)
        assert weights[1] == pytest.approx(1.0)

    def test_group_reads_referenced_columns(self):
        cfg = SampleWeightConfig(
            type="group",
            rules=[{"where": {"surface": "Hard", "indoor": True}, "weight": 1.0}],
            default_weight=0.3,
        )
        weights = sample_weights_from_frame(self._frame(), cfg)
        np.testing.assert_array_equal(weights, [0.3, 1.0])

    def test_missing_column_raises(self):
        cfg = SampleWeightConfig(
            type="group",
            rules=[{"where": {"circuit": "atptour"}, "weight": 1.0}],
        )
        with pytest.raises(ValueError, match="not in the training frame"):
            sample_weights_from_frame(self._frame(), cfg)


class TestSampleWeightConfig:
    def test_config_parsing(self):
        config = SampleWeightConfig(type="recency", half_life_days=365)
        assert config.type == "recency"
        assert config.half_life_days == 365

    def test_invalid_type_rejected(self):
        with pytest.raises(ValueError):
            SampleWeightConfig(type="unknown", half_life_days=365)

    def test_recency_requires_half_life(self):
        with pytest.raises(ValueError, match="requires half_life_days"):
            SampleWeightConfig(type="recency")

    def test_group_config_parsing(self):
        config = SampleWeightConfig(
            type="group",
            rules=[{"where": {"surface": "Hard", "indoor": False}, "weight": 1.0}],
            default_weight=0.3,
        )
        assert config.type == "group"
        assert config.rules[0].where == {"surface": "Hard", "indoor": False}
        assert config.rules[0].weight == 1.0
        assert config.default_weight == 0.3
        assert config.referenced_columns() == ["surface", "indoor"]

    def test_group_requires_rules(self):
        with pytest.raises(ValueError, match="non-empty rules list"):
            SampleWeightConfig(type="group")
        with pytest.raises(ValueError, match="non-empty rules list"):
            SampleWeightConfig(type="group", rules=[])

    def test_group_rule_requires_where(self):
        with pytest.raises(ValueError, match="non-empty 'where'"):
            SampleWeightConfig(type="group", rules=[{"where": {}, "weight": 1.0}])
