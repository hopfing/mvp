"""Tests for apply_filters helper."""

import polars as pl
import pytest

from mvp.model.config import apply_filters


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "circuit": ["tour", "chal", "tour", "chal", "itf"],
        "draw_type": ["singles", "singles", "doubles", "singles", "singles"],
        "elo_diff": [100.0, -50.0, 30.0, 0.0, -200.0],
        "surface": ["hard", "clay", "grass", "hard", "clay"],
    })


class TestApplyFilters:
    def test_range_min_and_max(self, sample_df):
        result = apply_filters(sample_df, {"elo_diff": {"min": -50, "max": 30}})
        assert len(result) == 3
        assert result["elo_diff"].to_list() == [-50.0, 30.0, 0.0]

    def test_range_min_only(self, sample_df):
        result = apply_filters(sample_df, {"elo_diff": {"min": 0}})
        assert len(result) == 3
        assert set(result["elo_diff"].to_list()) == {100.0, 30.0, 0.0}

    def test_range_max_only(self, sample_df):
        result = apply_filters(sample_df, {"elo_diff": {"max": 0}})
        assert len(result) == 3
        assert set(result["elo_diff"].to_list()) == {-50.0, 0.0, -200.0}

    def test_equality_filter(self, sample_df):
        result = apply_filters(sample_df, {"draw_type": "singles"})
        assert len(result) == 4

    def test_list_filter(self, sample_df):
        result = apply_filters(sample_df, {"circuit": ["tour", "chal"]})
        assert len(result) == 4

    def test_mixed_filter_types(self, sample_df):
        result = apply_filters(sample_df, {
            "draw_type": "singles",
            "circuit": ["tour", "chal"],
            "elo_diff": {"min": -50, "max": 50},
        })
        assert len(result) == 2
        assert result["circuit"].to_list() == ["chal", "chal"]

    def test_empty_filters(self, sample_df):
        result = apply_filters(sample_df, {})
        assert len(result) == len(sample_df)

    def test_range_filters_all_rows(self, sample_df):
        result = apply_filters(sample_df, {"elo_diff": {"min": -999, "max": 999}})
        assert len(result) == len(sample_df)

    def test_range_filters_no_rows(self, sample_df):
        result = apply_filters(sample_df, {"elo_diff": {"min": 500, "max": 600}})
        assert len(result) == 0

    def test_abs_min_and_abs_max(self, sample_df):
        # elo_diff values: [100, -50, 30, 0, -200]
        # abs values: [100, 50, 30, 0, 200]
        # abs_min=50, abs_max=150 keeps abs in [50, 150] → 100, -50, 50
        result = apply_filters(sample_df, {"elo_diff": {"abs_min": 50, "abs_max": 150}})
        assert sorted(result["elo_diff"].to_list()) == [-50.0, 100.0]

    def test_abs_min_only(self, sample_df):
        # abs >= 100 keeps 100 and -200
        result = apply_filters(sample_df, {"elo_diff": {"abs_min": 100}})
        assert sorted(result["elo_diff"].to_list()) == [-200.0, 100.0]

    def test_abs_max_only(self, sample_df):
        # abs <= 50 keeps -50, 30, 0
        result = apply_filters(sample_df, {"elo_diff": {"abs_max": 50}})
        assert sorted(result["elo_diff"].to_list()) == [-50.0, 0.0, 30.0]

    def test_abs_combined_with_regular_range(self, sample_df):
        # min=0 keeps [100, 30, 0], then abs_max=50 keeps [30, 0]
        result = apply_filters(sample_df, {"elo_diff": {"min": 0, "abs_max": 50}})
        assert sorted(result["elo_diff"].to_list()) == [0.0, 30.0]
