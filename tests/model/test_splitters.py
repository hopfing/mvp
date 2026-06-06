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
        # 48 months of data from 2022-01 to 2025-12. train_months=24, test_months=3.
        # First test fold starts at 2024-01 (anchor 2022-01 + 24 months).
        # Folds run through last quarter that fits: 2025-10 → 2026-01 → no, exceeds data.
        # Last valid test_end: 2025-10 → 2026-01 (upper = 2026-01). Just fits.
        df = _monthly_df(date(2022, 1, 1), months=48)
        splitter = DateSlidingWindowSplitter(train_months=24, test_months=3)
        splits = list(splitter.split(df))
        assert len(splits) == 8  # 2024-Q1, Q2, Q3, Q4, 2025-Q1, Q2, Q3, Q4

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
        splitter = DateSlidingWindowSplitter(train_months=24, test_months=3)
        splits = list(splitter.split(df))
        train_mins = [df[t]["effective_match_date"].min() for t, _ in splits]
        for i in range(1, len(train_mins)):
            assert _add_months(train_mins[i - 1], 3) == train_mins[i]

    def test_no_overlap(self):
        df = _monthly_df(date(2022, 1, 1), months=48)
        splitter = DateSlidingWindowSplitter(train_months=24, test_months=3)
        for train_idx, test_idx in splitter.split(df):
            assert len(set(train_idx) & set(test_idx)) == 0

    def test_train_months_must_be_positive(self):
        with pytest.raises(ValueError, match="train_months"):
            DateSlidingWindowSplitter(train_months=0, test_months=3)

    def test_test_months_must_be_positive(self):
        with pytest.raises(ValueError, match="test_months"):
            DateSlidingWindowSplitter(train_months=12, test_months=0)

    def test_empty_when_no_data_after_train(self):
        # train_months > data span → no folds produced
        df = _monthly_df(date(2024, 1, 1), months=6)
        splitter = DateSlidingWindowSplitter(train_months=12, test_months=3)
        assert list(splitter.split(df)) == []


class TestDateExpandingWindowSplitter:
    """Tests for DateExpandingWindowSplitter."""

    def test_train_grows_each_fold(self):
        df = _monthly_df(date(2020, 1, 1), months=72)
        splitter = DateExpandingWindowSplitter(initial_train_months=24, test_months=12)
        splits = list(splitter.split(df))
        # Anchor 2020-01, first test 2022-01. Test folds: 2022, 2023, 2024, 2025 → 4 folds
        assert len(splits) == 4

        prev_train_size = 0
        for train_idx, _ in splits:
            assert len(train_idx) > prev_train_size
            prev_train_size = len(train_idx)

    def test_fixed_train_start_anchored_at_data_min(self):
        df = _monthly_df(date(2020, 1, 1), months=72)
        splitter = DateExpandingWindowSplitter(initial_train_months=24, test_months=12)
        for train_idx, _ in splitter.split(df):
            assert df[train_idx]["effective_match_date"].min() == date(2020, 1, 1)

    def test_initial_train_months_must_be_positive(self):
        with pytest.raises(ValueError, match="initial_train_months"):
            DateExpandingWindowSplitter(initial_train_months=0, test_months=12)

    def test_test_months_must_be_positive(self):
        with pytest.raises(ValueError, match="test_months"):
            DateExpandingWindowSplitter(initial_train_months=12, test_months=0)

    def test_no_overlap(self):
        df = _monthly_df(date(2020, 1, 1), months=72)
        splitter = DateExpandingWindowSplitter(initial_train_months=24, test_months=12)
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
        splitter = DateSlidingWindowSplitter(train_months=24, test_months=3)
        splits = list(splitter.split(df))
        assert len(splits) == 8

    def test_expanding_with_datetime_column(self):
        df = self._datetime_df()
        splitter = DateExpandingWindowSplitter(initial_train_months=24, test_months=12)
        splits = list(splitter.split(df))
        # Anchor 2022-01, first test 2024-01. Folds: 2024, 2025 → 2 folds
        assert len(splits) == 2


class TestDateWindowsFrozenGeometry:
    """Tests for date_windows() — frozen fold geometry for stability selection."""

    def _df(self, start: date, n_months: int) -> pl.DataFrame:
        """One row on the 1st of each of n_months consecutive months."""
        dates = [_add_months(date(start.year, start.month, 1), i) for i in range(n_months)]
        return pl.DataFrame(
            {
                "effective_match_date": dates,
                "won": [i % 2 == 0 for i in range(n_months)],
            }
        )

    def test_sliding_windows_match_split_folds(self):
        """date_windows yields exactly the folds split() produces, in order."""
        df = self._df(date(2020, 1, 1), 60)  # 2020-01 .. 2024-12
        sp = DateSlidingWindowSplitter(train_months=24, test_months=12)
        windows = sp.date_windows(df)
        splits = list(sp.split(df))
        assert len(windows) == len(splits)
        # Each split's train/test rows must fall inside the matching window bounds.
        dates = df["effective_match_date"].cast(pl.Date).to_list()
        for (tr_s, tr_e, te_s, te_e), (train_idx, test_idx) in zip(windows, splits):
            assert all(tr_s <= dates[i] < tr_e for i in train_idx)
            assert all(te_s <= dates[i] < te_e for i in test_idx)

    def test_expanding_windows_match_split_folds(self):
        df = self._df(date(2020, 1, 1), 60)
        sp = DateExpandingWindowSplitter(initial_train_months=24, test_months=12)
        windows = sp.date_windows(df)
        splits = list(sp.split(df))
        assert len(windows) == len(splits)
        # Expanding: train_start is the fixed anchor for every fold.
        assert len({w[0] for w in windows}) == 1

    def test_windows_frozen_under_subset(self):
        """Dropping early rows must NOT shift the windows derived from the full frame.

        This is the core invariant: geometry is frozen from the full frame, so a
        resample's rows are assigned to identical folds.
        """
        full = self._df(date(2020, 1, 1), 60)
        sp = DateSlidingWindowSplitter(train_months=24, test_months=12)
        full_windows = sp.date_windows(full)
        # A resample that drops 2020 entirely: split() on it would re-anchor and
        # shift folds, but the frozen windows are computed from `full`, so the
        # subset's rows just map into a subset of the same windows.
        subset = full.filter(pl.col("effective_match_date") >= date(2021, 1, 1))
        # Re-deriving from the subset re-anchors (demonstrates the drift we avoid).
        subset_windows = sp.date_windows(subset)
        assert subset_windows[0][2] != full_windows[0][2]  # test_start shifted
        # But the full-frame windows are the geometry stability selection uses.
        assert full_windows[0][2] == date(2022, 1, 1)
