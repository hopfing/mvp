"""Tests for data splitting strategies."""

from datetime import date, timedelta

import polars as pl
import pytest

from mvp.model.splitters import (
    DateExpandingWindowSplitter,
    DateSlidingWindowSplitter,
    ExpandingWindowSplitter,
    SlidingWindowSplitter,
    WalkForwardSplitter,
    _add_months,
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


class TestAddMonths:
    """Tests for the _add_months calendar helper."""

    def test_forward_within_year(self):
        assert _add_months(date(2024, 1, 1), 3) == date(2024, 4, 1)

    def test_forward_across_year(self):
        assert _add_months(date(2024, 11, 1), 3) == date(2025, 2, 1)

    def test_backward(self):
        assert _add_months(date(2024, 3, 1), -6) == date(2023, 9, 1)

    def test_full_year_step(self):
        assert _add_months(date(2024, 1, 1), 12) == date(2025, 1, 1)


def _monthly_df(start: date, months: int, per_month: int = 100) -> pl.DataFrame:
    """Build a synthetic frame with `per_month` rows on the 1st of each month."""
    dates: list[date] = []
    for i in range(months):
        d = _add_months(start, i)
        dates.extend([d] * per_month)
    return pl.DataFrame(
        {
            "match_uid": [f"M{i}" for i in range(len(dates))],
            "effective_match_date": dates,
            "won": [i % 2 == 0 for i in range(len(dates))],
        }
    )


class TestDateSlidingWindowSplitter:
    """Tests for DateSlidingWindowSplitter."""

    def test_fold_count_and_boundaries(self):
        df = _monthly_df(date(2022, 1, 1), months=48)
        splitter = DateSlidingWindowSplitter(
            train_months=24,
            test_months=3,
            start_date=date(2024, 1, 1),
            end_date=date(2025, 1, 1),
        )
        splits = list(splitter.split(df))
        assert len(splits) == 4

        # First fold: train [2022-01, 2024-01), test [2024-01, 2024-04)
        train_idx, test_idx = splits[0]
        train_dates = df[train_idx]["effective_match_date"]
        test_dates = df[test_idx]["effective_match_date"]
        assert train_dates.min() == date(2022, 1, 1)
        assert train_dates.max() == date(2023, 12, 1)
        assert test_dates.min() == date(2024, 1, 1)
        assert test_dates.max() == date(2024, 3, 1)

    def test_train_shifts_by_test_months(self):
        df = _monthly_df(date(2022, 1, 1), months=48)
        splitter = DateSlidingWindowSplitter(
            train_months=24,
            test_months=3,
            start_date=date(2024, 1, 1),
            end_date=date(2025, 1, 1),
        )
        splits = list(splitter.split(df))
        train_mins = [df[t]["effective_match_date"].min() for t, _ in splits]
        for i in range(1, len(train_mins)):
            assert _add_months(train_mins[i - 1], 3) == train_mins[i]

    def test_no_overlap(self):
        df = _monthly_df(date(2022, 1, 1), months=48)
        splitter = DateSlidingWindowSplitter(
            train_months=24,
            test_months=3,
            start_date=date(2024, 1, 1),
            end_date=date(2025, 1, 1),
        )
        for train_idx, test_idx in splitter.split(df):
            assert len(set(train_idx) & set(test_idx)) == 0

    def test_start_date_must_be_first_of_month(self):
        with pytest.raises(ValueError, match="start_date"):
            DateSlidingWindowSplitter(
                train_months=12,
                test_months=3,
                start_date=date(2024, 1, 15),
            )

    def test_end_date_must_be_first_of_month(self):
        with pytest.raises(ValueError, match="end_date"):
            DateSlidingWindowSplitter(
                train_months=12,
                test_months=3,
                start_date=date(2024, 1, 1),
                end_date=date(2025, 6, 30),
            )

    def test_end_date_cutoff(self):
        df = _monthly_df(date(2022, 1, 1), months=48)
        splitter = DateSlidingWindowSplitter(
            train_months=12,
            test_months=6,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 7, 1),
        )
        splits = list(splitter.split(df))
        # Only one fold fits: [2024-01, 2024-07)
        assert len(splits) == 1

    def test_empty_fold_skipped(self):
        # Frame has data only in 2024; ask for folds starting 2025 → no test rows
        df = _monthly_df(date(2024, 1, 1), months=12)
        splitter = DateSlidingWindowSplitter(
            train_months=12,
            test_months=3,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 4, 1),
        )
        splits = list(splitter.split(df))
        assert splits == []


class TestDateExpandingWindowSplitter:
    """Tests for DateExpandingWindowSplitter."""

    def test_train_grows_each_fold(self):
        df = _monthly_df(date(2020, 1, 1), months=72)
        splitter = DateExpandingWindowSplitter(
            train_start_date=date(2020, 1, 1),
            start_date=date(2022, 1, 1),
            test_months=12,
            end_date=date(2025, 1, 1),
        )
        splits = list(splitter.split(df))
        assert len(splits) == 3

        prev_train_size = 0
        for train_idx, _ in splits:
            assert len(train_idx) > prev_train_size
            prev_train_size = len(train_idx)

    def test_fixed_train_start(self):
        df = _monthly_df(date(2020, 1, 1), months=72)
        splitter = DateExpandingWindowSplitter(
            train_start_date=date(2020, 1, 1),
            start_date=date(2022, 1, 1),
            test_months=12,
            end_date=date(2025, 1, 1),
        )
        for train_idx, _ in splitter.split(df):
            assert df[train_idx]["effective_match_date"].min() == date(2020, 1, 1)

    def test_train_start_defaults_to_data_min(self):
        df = _monthly_df(date(2021, 6, 1), months=36)
        splitter = DateExpandingWindowSplitter(
            start_date=date(2023, 1, 1),
            test_months=6,
            end_date=date(2024, 1, 1),
        )
        splits = list(splitter.split(df))
        assert len(splits) > 0
        first_train_idx, _ = splits[0]
        assert df[first_train_idx]["effective_match_date"].min() == date(2021, 6, 1)

    def test_start_date_must_be_first_of_month(self):
        with pytest.raises(ValueError, match="start_date"):
            DateExpandingWindowSplitter(
                start_date=date(2024, 1, 15),
                test_months=12,
            )

    def test_train_start_date_must_be_first_of_month(self):
        with pytest.raises(ValueError, match="train_start_date"):
            DateExpandingWindowSplitter(
                start_date=date(2024, 1, 1),
                train_start_date=date(2020, 1, 5),
                test_months=12,
            )

    def test_no_overlap(self):
        df = _monthly_df(date(2020, 1, 1), months=72)
        splitter = DateExpandingWindowSplitter(
            train_start_date=date(2020, 1, 1),
            start_date=date(2022, 1, 1),
            test_months=12,
            end_date=date(2025, 1, 1),
        )
        for train_idx, test_idx in splitter.split(df):
            assert len(set(train_idx) & set(test_idx)) == 0


class TestDateSplitterDatetimeColumn:
    """Both date splitters must handle a Datetime-typed date_col (real data shape)."""

    def _datetime_df(self) -> pl.DataFrame:
        # Real matches.parquet has effective_match_date as Datetime, not Date
        df = _monthly_df(date(2022, 1, 1), months=48)
        return df.with_columns(
            pl.col("effective_match_date").cast(pl.Datetime("us"))
        )

    def test_sliding_with_datetime_column(self):
        df = self._datetime_df()
        splitter = DateSlidingWindowSplitter(
            train_months=12,
            test_months=3,
            start_date=date(2024, 1, 1),
            end_date=date(2025, 1, 1),
        )
        splits = list(splitter.split(df))
        assert len(splits) == 4

    def test_expanding_with_datetime_column(self):
        df = self._datetime_df()
        splitter = DateExpandingWindowSplitter(
            train_start_date=date(2022, 1, 1),
            start_date=date(2024, 1, 1),
            test_months=3,
            end_date=date(2025, 1, 1),
        )
        splits = list(splitter.split(df))
        assert len(splits) == 4
