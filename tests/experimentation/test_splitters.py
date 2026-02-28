"""Tests for data splitting strategies."""

from datetime import date, timedelta

import polars as pl
import pytest

from mvp.experimentation.splitters import WalkForwardSplitter


class TestWalkForwardSplitter:
    """Tests for WalkForwardSplitter."""

    @pytest.fixture
    def sample_df(self) -> pl.DataFrame:
        """Create sample DataFrame with 1000 rows."""
        return pl.DataFrame({
            "match_uid": [f"M{i}" for i in range(1000)],
            "effective_match_date": [
                date(2024, 1, 1) + timedelta(days=i // 10) for i in range(1000)
            ],
            "player_id": ["A"] * 1000,
            "won": [i % 2 == 0 for i in range(1000)],
        })

    def test_generates_n_splits(self, sample_df: pl.DataFrame):
        """Generates correct number of splits."""
        splitter = WalkForwardSplitter(
            n_splits=5,
            min_train_size=100,
            test_size=100,
        )
        splits = list(splitter.split(sample_df))
        assert len(splits) == 5

    def test_train_before_test(self, sample_df: pl.DataFrame):
        """Training data comes before test data chronologically."""
        splitter = WalkForwardSplitter(
            n_splits=3,
            min_train_size=100,
            test_size=100,
        )
        for train_idx, test_idx in splitter.split(sample_df):
            train_dates = sample_df[train_idx]["effective_match_date"]
            test_dates = sample_df[test_idx]["effective_match_date"]
            assert train_dates.max() <= test_dates.min()

    def test_no_overlap(self, sample_df: pl.DataFrame):
        """Train and test sets don't overlap within a split."""
        splitter = WalkForwardSplitter(
            n_splits=3,
            min_train_size=100,
            test_size=100,
        )
        for train_idx, test_idx in splitter.split(sample_df):
            assert len(set(train_idx) & set(test_idx)) == 0

    def test_single_split_is_chronological(self, sample_df: pl.DataFrame):
        """n_splits=1 gives simple chronological split."""
        splitter = WalkForwardSplitter(
            n_splits=1,
            min_train_size=100,
            test_size=100,
        )
        splits = list(splitter.split(sample_df))
        assert len(splits) == 1
