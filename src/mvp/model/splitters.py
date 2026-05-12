"""Data splitting strategies for experiments."""


from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import date

import polars as pl


class BaseSplitter(ABC):
    """Base class for data splitters."""

    @abstractmethod
    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        """Generate train/test index splits.

        Args:
            df: DataFrame to split.

        Yields:
            Tuples of (train_indices, test_indices).
        """
        pass


class ExpandingWindowSplitter(BaseSplitter):
    """Expanding window validation where training grows over time.

    Training always starts from the beginning of the data and expands forward.
    Two parameterization modes (mutually exclusive):

    1. n_splits mode: "Give me exactly N folds, figure out the spacing"
       - Set n_splits, min_train_size, test_size
       - step_size is calculated automatically

    2. step_size mode: "Step forward by X each time, give me as many folds as fit"
       - Set initial_train_size, step_size
       - n_splits is determined by data size
    """

    def __init__(
        self,
        n_splits: int | None = None,
        min_train_size: int | None = None,
        test_size: int | None = None,
        initial_train_size: int | None = None,
        step_size: int | None = None,
        date_col: str = "effective_match_date",
    ) -> None:
        self.date_col = date_col

        # Validate mutually exclusive modes
        n_splits_mode = n_splits is not None
        step_size_mode = step_size is not None

        if n_splits_mode and step_size_mode:
            raise ValueError("Cannot specify both n_splits and step_size")
        if not n_splits_mode and not step_size_mode:
            raise ValueError("Must specify either n_splits or step_size")

        if n_splits_mode:
            if min_train_size is None or test_size is None:
                raise ValueError("n_splits mode requires min_train_size and test_size")
            self._mode = "n_splits"
            self._n_splits = n_splits
            self._min_train_size = min_train_size
            self._test_size = test_size
        else:
            if initial_train_size is None:
                raise ValueError("step_size mode requires initial_train_size")
            self._mode = "step_size"
            self._initial_train_size = initial_train_size
            self._step_size = step_size

    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        """Generate expanding window splits."""
        sorted_df = df.with_row_index("_idx").sort(self.date_col)
        indices = sorted_df["_idx"].to_list()
        n_total = len(indices)

        if self._mode == "n_splits":
            yield from self._split_n_splits(indices, n_total)
        else:
            yield from self._split_step_size(indices, n_total)

    def _split_n_splits(
        self, indices: list[int], n_total: int
    ) -> Iterator[tuple[list[int], list[int]]]:
        """Generate splits for n_splits mode."""
        remaining = n_total - self._min_train_size - self._test_size

        if remaining < 0:
            raise ValueError(
                f"Not enough data: {n_total} rows, need at least "
                f"{self._min_train_size + self._test_size}"
            )

        if self._n_splits == 1:
            step_size = 0
        else:
            step_size = remaining // self._n_splits

        for i in range(self._n_splits):
            train_end = self._min_train_size + i * step_size
            test_start = train_end
            test_end = test_start + self._test_size

            if test_end > n_total:
                break

            train_idx = indices[:train_end]
            test_idx = indices[test_start:test_end]

            yield train_idx, test_idx

    def _split_step_size(
        self, indices: list[int], n_total: int
    ) -> Iterator[tuple[list[int], list[int]]]:
        """Generate splits for step_size mode."""
        train_end = self._initial_train_size

        while train_end + self._step_size <= n_total:
            test_start = train_end
            test_end = train_end + self._step_size

            train_idx = indices[:train_end]
            test_idx = indices[test_start:test_end]

            yield train_idx, test_idx

            train_end += self._step_size


class SlidingWindowSplitter(BaseSplitter):
    """Sliding window validation with fixed training size.

    Unlike ExpandingWindowSplitter, training window maintains a fixed size
    and slides forward, dropping old data as it advances. Useful for testing
    temporal drift (whether old data helps or hurts predictions).
    """

    def __init__(
        self,
        train_size: int,
        test_size: int,
        step_size: int | None = None,
        date_col: str = "effective_match_date",
    ) -> None:
        self.train_size = train_size
        self.test_size = test_size
        self.step_size = step_size if step_size is not None else test_size
        self.date_col = date_col

    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        """Generate sliding window splits."""
        sorted_df = df.with_row_index("_idx").sort(self.date_col)
        indices = sorted_df["_idx"].to_list()
        n_total = len(indices)

        train_start = 0
        while train_start + self.train_size + self.test_size <= n_total:
            train_end = train_start + self.train_size
            test_end = train_end + self.test_size

            train_idx = indices[train_start:train_end]
            test_idx = indices[train_end:test_end]

            yield train_idx, test_idx

            train_start += self.step_size


def _add_months(d: date, months: int) -> date:
    """Add (or subtract) calendar months to a first-of-month date."""
    total = d.year * 12 + (d.month - 1) + months
    new_year = total // 12
    new_month = total % 12 + 1
    return date(new_year, new_month, d.day)


def _validate_first_of_month(d: date, name: str) -> None:
    if d.day != 1:
        raise ValueError(f"{name} must be the 1st of a month, got {d.isoformat()}")


class DateSlidingWindowSplitter(BaseSplitter):
    """Sliding window validation expressed in calendar months.

    Training window is a fixed `train_months` and slides forward with the
    test window. The first test fold begins `train_months` after the first
    available data date (floored to the 1st of that month). Folds continue
    until the next test window would exceed the available data.

    Mirrors `SlidingWindowSplitter` semantically — `train_months` is the
    rolling window length used for every fold.
    """

    def __init__(
        self,
        train_months: int,
        test_months: int,
        date_col: str = "effective_match_date",
    ) -> None:
        if train_months <= 0:
            raise ValueError("train_months must be positive")
        if test_months <= 0:
            raise ValueError("test_months must be positive")
        self.train_months = train_months
        self.test_months = test_months
        self.date_col = date_col

    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        sorted_df = df.with_row_index("_idx").sort(self.date_col)
        dates = sorted_df[self.date_col].cast(pl.Date)
        idx = sorted_df["_idx"]

        min_date = dates.min()
        max_date = dates.max()
        anchor = date(min_date.year, min_date.month, 1)
        upper = _add_months(date(max_date.year, max_date.month, 1), 1)
        first_test_start = _add_months(anchor, self.train_months)

        i = 0
        while True:
            test_start = _add_months(first_test_start, i * self.test_months)
            test_end = _add_months(test_start, self.test_months)
            if test_end > upper:
                break

            train_start = _add_months(test_start, -self.train_months)
            train_end = test_start

            train_mask = (dates >= train_start) & (dates < train_end)
            test_mask = (dates >= test_start) & (dates < test_end)

            train_idx = idx.filter(train_mask).to_list()
            test_idx = idx.filter(test_mask).to_list()

            if train_idx and test_idx:
                yield train_idx, test_idx

            i += 1


class DateExpandingWindowSplitter(BaseSplitter):
    """Expanding window validation expressed in calendar months.

    Training begins at the first available data date and grows each fold.
    The first test fold begins `initial_train_months` after the data start
    (floored to the 1st of that month). Folds continue until the next test
    window would exceed the available data.

    Mirrors `ExpandingWindowSplitter` step_size mode semantically —
    `initial_train_months` only governs where fold 1's test begins; the
    training window expands to include all prior data each fold thereafter.
    """

    def __init__(
        self,
        initial_train_months: int,
        test_months: int,
        date_col: str = "effective_match_date",
    ) -> None:
        if initial_train_months <= 0:
            raise ValueError("initial_train_months must be positive")
        if test_months <= 0:
            raise ValueError("test_months must be positive")
        self.initial_train_months = initial_train_months
        self.test_months = test_months
        self.date_col = date_col

    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        sorted_df = df.with_row_index("_idx").sort(self.date_col)
        dates = sorted_df[self.date_col].cast(pl.Date)
        idx = sorted_df["_idx"]

        min_date = dates.min()
        max_date = dates.max()
        anchor = date(min_date.year, min_date.month, 1)
        upper = _add_months(date(max_date.year, max_date.month, 1), 1)
        first_test_start = _add_months(anchor, self.initial_train_months)

        i = 0
        while True:
            test_start = _add_months(first_test_start, i * self.test_months)
            test_end = _add_months(test_start, self.test_months)
            if test_end > upper:
                break

            train_mask = (dates >= anchor) & (dates < test_start)
            test_mask = (dates >= test_start) & (dates < test_end)

            train_idx = idx.filter(train_mask).to_list()
            test_idx = idx.filter(test_mask).to_list()

            if train_idx and test_idx:
                yield train_idx, test_idx

            i += 1


class DateWindowSplitter(BaseSplitter):
    """Single train/test split based on a date boundary.

    Everything before test_start is training, everything from test_start
    onward is test. Useful for precise temporal evaluation.
    """

    def __init__(
        self,
        test_start: date,
        date_col: str = "effective_match_date",
    ) -> None:
        self.test_start = test_start
        self.date_col = date_col

    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        """Generate a single train/test split at the date boundary."""
        sorted_df = df.with_row_index("_idx").sort(self.date_col)
        dates = sorted_df[self.date_col]

        train_mask = dates < self.test_start
        test_mask = dates >= self.test_start

        train_idx = sorted_df.filter(train_mask)["_idx"].to_list()
        test_idx = sorted_df.filter(test_mask)["_idx"].to_list()

        if not train_idx:
            raise ValueError(f"No training data before {self.test_start}")
        if not test_idx:
            raise ValueError(f"No test data from {self.test_start} onward")

        yield train_idx, test_idx


def make_splitter(
    val_type: str,
    n_splits: int = 5,
    min_train_size: int = 50000,
    test_size: int = 10000,
    initial_train_size: int | None = None,
    step_size: int | None = None,
    train_size: int | None = None,
    test_start: date | None = None,
    train_months: int | None = None,
    initial_train_months: int | None = None,
    test_months: int | None = None,
) -> BaseSplitter:
    """Create a splitter from validation parameters.

    Args:
        val_type: One of "walk_forward", "expanding_window", "sliding_window".
        n_splits: Number of folds (walk_forward mode).
        min_train_size: Minimum training size (walk_forward mode).
        test_size: Test set size.
        initial_train_size: Initial training size (expanding_window mode).
        step_size: Step size between folds.
        train_size: Fixed training window size (sliding_window mode).

    Returns:
        Configured splitter instance.
    """
    if val_type == "walk_forward":
        return ExpandingWindowSplitter(
            n_splits=n_splits,
            min_train_size=min_train_size,
            test_size=test_size,
        )
    elif val_type == "expanding_window":
        if initial_train_size is None or step_size is None:
            raise ValueError(
                "expanding_window requires initial_train_size and step_size"
            )
        return ExpandingWindowSplitter(
            initial_train_size=initial_train_size,
            step_size=step_size,
        )
    elif val_type == "sliding_window":
        if train_size is None:
            raise ValueError("sliding_window requires train_size")
        return SlidingWindowSplitter(
            train_size=train_size,
            test_size=test_size,
            step_size=step_size,
        )
    elif val_type == "date_window":
        if test_start is None:
            raise ValueError("date_window requires test_start")
        return DateWindowSplitter(test_start=test_start)
    elif val_type == "date_sliding":
        if train_months is None or test_months is None:
            raise ValueError(
                "date_sliding requires train_months and test_months"
            )
        return DateSlidingWindowSplitter(
            train_months=train_months,
            test_months=test_months,
        )
    elif val_type == "date_expanding":
        if initial_train_months is None or test_months is None:
            raise ValueError(
                "date_expanding requires initial_train_months and test_months"
            )
        return DateExpandingWindowSplitter(
            initial_train_months=initial_train_months,
            test_months=test_months,
        )
    else:
        raise ValueError(f"Unknown validation type: {val_type}")


# Backwards compatibility alias
WalkForwardSplitter = ExpandingWindowSplitter
