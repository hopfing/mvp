"""Tests for data splitting strategies."""

from datetime import date, timedelta

import polars as pl
import pytest

from mvp.model.splitters import (
    ExpandingWindowSplitter,
    SlidingWindowSplitter,
    WalkForwardSplitter,
)


class TestWalkForwardSplitter:
    """Tests for WalkForwardSplitter."""

    @pytest.fixture
    def sample_df(self) -> pl.DataFrame:
        """Create sample DataFrame with 1000 rows."""
        return pl.DataFrame(
            {
                "match_uid": [f"M{i}" for i in range(1000)],
                "effective_match_date": [
                    date(2024, 1, 1) + timedelta(days=i // 10) for i in range(1000)
                ],
                "player_id": ["A"] * 1000,
                "won": [i % 2 == 0 for i in range(1000)],
            }
        )

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


class TestExpandingWindowSplitter:
    """Tests for ExpandingWindowSplitter."""

    @pytest.fixture
    def sample_df(self) -> pl.DataFrame:
        """Create sample DataFrame with 1000 rows."""
        return pl.DataFrame(
            {
                "match_uid": [f"M{i}" for i in range(1000)],
                "effective_match_date": [
                    date(2024, 1, 1) + timedelta(days=i // 10) for i in range(1000)
                ],
                "player_id": ["A"] * 1000,
                "won": [i % 2 == 0 for i in range(1000)],
            }
        )

    def test_train_grows_by_step_size(self, sample_df: pl.DataFrame):
        """Training set grows by step_size each fold."""
        splitter = ExpandingWindowSplitter(
            initial_train_size=200,
            step_size=100,
        )
        splits = list(splitter.split(sample_df))

        train_sizes = [len(train) for train, test in splits]
        for i in range(1, len(train_sizes)):
            assert train_sizes[i] == train_sizes[i - 1] + 100

    def test_no_overlap(self, sample_df: pl.DataFrame):
        """Train and test don't overlap."""
        splitter = ExpandingWindowSplitter(
            initial_train_size=200,
            step_size=100,
        )
        for train_idx, test_idx in splitter.split(sample_df):
            assert len(set(train_idx) & set(test_idx)) == 0


class TestSlidingWindowSplitter:
    """Tests for SlidingWindowSplitter."""

    @pytest.fixture
    def sample_df(self) -> pl.DataFrame:
        """Create sample DataFrame with 1000 rows."""
        return pl.DataFrame(
            {
                "match_uid": [f"M{i}" for i in range(1000)],
                "effective_match_date": [
                    date(2024, 1, 1) + timedelta(days=i // 10) for i in range(1000)
                ],
                "player_id": ["A"] * 1000,
                "won": [i % 2 == 0 for i in range(1000)],
            }
        )

    def test_train_size_is_fixed(self, sample_df: pl.DataFrame):
        """Training set maintains fixed size across folds."""
        splitter = SlidingWindowSplitter(
            train_size=200,
            test_size=100,
        )
        splits = list(splitter.split(sample_df))

        train_sizes = [len(train) for train, test in splits]
        assert all(size == 200 for size in train_sizes)

    def test_window_slides_forward(self, sample_df: pl.DataFrame):
        """Training window slides forward (drops old data)."""
        splitter = SlidingWindowSplitter(
            train_size=200,
            test_size=100,
            step_size=100,
        )
        splits = list(splitter.split(sample_df))

        # Each split should start later
        train_starts = [min(train) for train, test in splits]
        for i in range(1, len(train_starts)):
            assert train_starts[i] > train_starts[i - 1]

    def test_no_overlap(self, sample_df: pl.DataFrame):
        """Train and test don't overlap within a split."""
        splitter = SlidingWindowSplitter(
            train_size=200,
            test_size=100,
        )
        for train_idx, test_idx in splitter.split(sample_df):
            assert len(set(train_idx) & set(test_idx)) == 0

    def test_train_before_test(self, sample_df: pl.DataFrame):
        """Training data comes before test data chronologically."""
        splitter = SlidingWindowSplitter(
            train_size=200,
            test_size=100,
        )
        for train_idx, test_idx in splitter.split(sample_df):
            train_dates = sample_df[train_idx]["effective_match_date"]
            test_dates = sample_df[test_idx]["effective_match_date"]
            assert train_dates.max() <= test_dates.min()

    def test_step_size_defaults_to_test_size(self, sample_df: pl.DataFrame):
        """step_size defaults to test_size if not specified."""
        splitter = SlidingWindowSplitter(
            train_size=200,
            test_size=100,
        )
        splits = list(splitter.split(sample_df))

        # With 1000 rows, 200 train, 100 test, step=100
        # Split 1: 0-200 train, 200-300 test
        # Split 2: 100-300 train, 300-400 test
        # ...
        # Split 8: 700-900 train, 900-1000 test
        assert len(splits) == 8
