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

    def test_rolling_sum_respects_window_boundary(self):
        """Only include matches within the window period."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),   # Day 0
                date(2024, 1, 10),  # Day 9
                date(2024, 1, 20),  # Day 19
            ],
            "won": [1, 1, 1],
        }).lazy()

        result = df.with_columns(
            rolling_sum("won", days=7, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 0: no prior → 0
        # Row 1: day 1 is 9 days before day 10, outside 7-day window → 0
        # Row 2: day 10 is 10 days before day 20, outside 7-day window → 0
        assert result["rolling_wins"].to_list() == [0, 0, 0]

    def test_rolling_sum_includes_edge_of_window(self):
        """Match exactly at window boundary is included."""
        df = pl.DataFrame({
            "player_id": ["A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 8),  # Exactly 7 days later
            ],
            "won": [1, 1],
        }).lazy()

        result = df.with_columns(
            rolling_sum("won", days=7, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 1: day 1 is exactly 7 days before day 8, should be included
        assert result["rolling_wins"].to_list() == [0, 1]
