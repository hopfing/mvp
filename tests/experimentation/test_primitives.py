"""Tests for temporal-safe primitives."""

from datetime import date

import polars as pl

from mvp.experimentation.primitives import rolling_sum


class TestRollingSum:
    """Tests for rolling_sum primitive."""

    def test_rolling_sum_basic(self):
        """Sum values over rolling window."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 15),
            ],
            "won": [1, 0, 1, 1],
        }).lazy()

        result = df.with_columns(
            rolling_sum("won", days=30, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 0: no prior matches → 0
        # Row 1: 1 prior match (won=1) → 1
        # Row 2: 2 prior matches (won=1,0) → 1
        # Row 3: 3 prior matches (won=1,0,1) → 2
        assert result["rolling_wins"].to_list() == [0, 1, 1, 2]

    def test_rolling_sum_excludes_current_row(self):
        """Current row must NOT be included in the sum."""
        df = pl.DataFrame({
            "player_id": ["A", "A"],
            "effective_match_date": [date(2024, 1, 1), date(2024, 1, 2)],
            "won": [1, 1],
        }).lazy()

        result = df.with_columns(
            rolling_sum("won", days=30, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 0: no prior → 0 (not 1)
        # Row 1: 1 prior (won=1) → 1 (not 2)
        assert result["rolling_wins"].to_list() == [0, 1]
