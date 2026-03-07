"""Data splitting strategies for experiments."""


from abc import ABC, abstractmethod
from collections.abc import Iterator

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


# Backwards compatibility alias
WalkForwardSplitter = ExpandingWindowSplitter
