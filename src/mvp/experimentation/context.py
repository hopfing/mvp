"""FeatureContext for feature computation.

Provides a wrapper around primitives with pre-configured parameters,
making feature definitions cleaner and more consistent.
"""

from __future__ import annotations

import polars as pl

from mvp.experimentation import primitives


class FeatureContext:
    """Context for feature computation, wrapping primitive access.

    Provides access to temporal-safe primitives with pre-configured
    group_by and date_col parameters.

    Attributes:
        group_by: Column(s) to group by (typically "player_id").
        date_col: Date column for temporal ordering.
    """

    def __init__(
        self,
        group_by: str | list[str] = "player_id",
        date_col: str = "effective_match_date",
    ) -> None:
        """Initialize feature context.

        Args:
            group_by: Column(s) to group by (default "player_id").
            date_col: Date column for temporal ordering.
        """
        self.group_by = group_by
        self.date_col = date_col

    def rolling_sum(self, col: str, days: int) -> pl.Expr:
        """Sum of column over past N days, excluding current row.

        Args:
            col: Column to sum.
            days: Window size in days.

        Returns:
            Polars expression computing the rolling sum.
        """
        return primitives.rolling_sum(
            col=col,
            days=days,
            group_by=self.group_by,
            date_col=self.date_col,
        )

    def rolling_mean(self, col: str, days: int) -> pl.Expr:
        """Mean of column over past N days, excluding current row.

        Args:
            col: Column to average.
            days: Window size in days.

        Returns:
            Polars expression computing the rolling mean.
        """
        return primitives.rolling_mean(
            col=col,
            days=days,
            group_by=self.group_by,
            date_col=self.date_col,
        )

    def rolling_count(self, days: int) -> pl.Expr:
        """Count of rows over past N days, excluding current row.

        Args:
            days: Window size in days.

        Returns:
            Polars expression computing the rolling count.
        """
        return primitives.rolling_count(
            days=days,
            group_by=self.group_by,
            date_col=self.date_col,
        )

    def cumulative_sum(self, col: str) -> pl.Expr:
        """Cumulative sum over all prior rows, excluding current row.

        Args:
            col: Column to sum.

        Returns:
            Polars expression computing the cumulative sum.
        """
        return primitives.cumulative_sum(
            col=col,
            group_by=self.group_by,
            date_col=self.date_col,
        )

    def cumulative_mean(self, col: str) -> pl.Expr:
        """Cumulative mean over all prior rows, excluding current row.

        Args:
            col: Column to average.

        Returns:
            Polars expression computing the cumulative mean.
        """
        return primitives.cumulative_mean(
            col=col,
            group_by=self.group_by,
            date_col=self.date_col,
        )
