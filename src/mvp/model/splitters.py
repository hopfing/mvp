"""Data splitting strategies for experiments."""

from __future__ import annotations

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


class WalkForwardSplitter(BaseSplitter):
    """Walk-forward validation with expanding training window."""

    def __init__(
        self,
        n_splits: int = 5,
        min_train_size: int = 50000,
        test_size: int = 10000,
        date_col: str = "effective_match_date",
    ) -> None:
        self.n_splits = n_splits
        self.min_train_size = min_train_size
        self.test_size = test_size
        self.date_col = date_col

    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        """Generate walk-forward splits."""
        # Sort by date and get indices
        sorted_df = df.with_row_index("_idx").sort(self.date_col)
        indices = sorted_df["_idx"].to_list()

        n_total = len(indices)

        # Calculate how much data is available after min_train and one test set
        remaining = n_total - self.min_train_size - self.test_size

        if remaining < 0:
            raise ValueError(
                f"Not enough data: {n_total} rows, need at least "
                f"{self.min_train_size + self.test_size}"
            )

        # Calculate step size to create n_splits
        if self.n_splits == 1:
            step_size = 0
        else:
            step_size = remaining // self.n_splits

        for i in range(self.n_splits):
            train_end = self.min_train_size + i * step_size
            test_start = train_end
            test_end = test_start + self.test_size

            if test_end > n_total:
                break

            train_idx = indices[:train_end]
            test_idx = indices[test_start:test_end]

            yield train_idx, test_idx


class ExpandingWindowSplitter(BaseSplitter):
    """Expanding window validation where training grows by fixed step."""

    def __init__(
        self,
        initial_train_size: int,
        step_size: int,
        date_col: str = "effective_match_date",
    ) -> None:
        self.initial_train_size = initial_train_size
        self.step_size = step_size
        self.date_col = date_col

    def split(self, df: pl.DataFrame) -> Iterator[tuple[list[int], list[int]]]:
        """Generate expanding window splits."""
        sorted_df = df.with_row_index("_idx").sort(self.date_col)
        indices = sorted_df["_idx"].to_list()

        n_total = len(indices)
        train_end = self.initial_train_size

        while train_end + self.step_size <= n_total:
            test_start = train_end
            test_end = train_end + self.step_size

            train_idx = indices[:train_end]
            test_idx = indices[test_start:test_end]

            yield train_idx, test_idx

            train_end += self.step_size
