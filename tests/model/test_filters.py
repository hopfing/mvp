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
