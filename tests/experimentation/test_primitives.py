"""Tests for temporal-safe primitives."""

from datetime import date

import polars as pl

from mvp.experimentation.primitives import rolling_count, rolling_mean, rolling_sum


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

    def test_rolling_sum_isolates_players(self):
        """Each player's rolling sum is independent."""
        df = pl.DataFrame({
            "player_id": ["A", "B", "A", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
            ],
            "won": [1, 1, 1, 0],
        }).lazy()

        result = df.with_columns(
            rolling_sum("won", days=30, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 0 (A): no prior A matches → 0
        # Row 1 (B): no prior B matches → 0
        # Row 2 (A): 1 prior A match (won=1) → 1
        # Row 3 (B): 1 prior B match (won=1) → 1
        assert result["rolling_wins"].to_list() == [0, 0, 1, 1]


class TestRollingMean:
    """Tests for rolling_mean primitive."""

    def test_rolling_mean_basic(self):
        """Mean values over rolling window."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 15),
            ],
            "score": [10.0, 20.0, 30.0, 40.0],
        }).lazy()

        result = df.with_columns(
            rolling_mean("score", days=30, group_by="player_id").alias("rolling_avg")
        ).collect()

        # Row 0: no prior matches → null
        # Row 1: 1 prior match (10) → 10.0
        # Row 2: 2 prior matches (10, 20) → 15.0
        # Row 3: 3 prior matches (10, 20, 30) → 20.0
        assert result["rolling_avg"].to_list() == [None, 10.0, 15.0, 20.0]

    def test_rolling_mean_excludes_current_row(self):
        """Current row must NOT be included in the mean."""
        df = pl.DataFrame({
            "player_id": ["A", "A"],
            "effective_match_date": [date(2024, 1, 1), date(2024, 1, 2)],
            "score": [100.0, 200.0],
        }).lazy()

        result = df.with_columns(
            rolling_mean("score", days=30, group_by="player_id").alias("rolling_avg")
        ).collect()

        # Row 0: no prior → null (not 100.0)
        # Row 1: 1 prior (100) → 100.0 (not 150.0)
        assert result["rolling_avg"].to_list() == [None, 100.0]


class TestRollingCount:
    """Tests for rolling_count primitive."""

    def test_rolling_count_basic(self):
        """Count rows over rolling window."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 15),
            ],
        }).lazy()

        result = df.with_columns(
            rolling_count(days=30, group_by="player_id").alias("match_count")
        ).collect()

        # Row 0: no prior matches → 0
        # Row 1: 1 prior match → 1
        # Row 2: 2 prior matches → 2
        # Row 3: 3 prior matches → 3
        assert result["match_count"].to_list() == [0, 1, 2, 3]

    def test_rolling_count_respects_window(self):
        """Only count matches within the window period."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),   # Day 0
                date(2024, 1, 10),  # Day 9
                date(2024, 1, 20),  # Day 19
            ],
        }).lazy()

        result = df.with_columns(
            rolling_count(days=7, group_by="player_id").alias("match_count")
        ).collect()

        # Row 0: no prior → 0
        # Row 1: day 1 is 9 days before day 10, outside 7-day window → 0
        # Row 2: day 10 is 10 days before day 20, outside 7-day window → 0
        assert result["match_count"].to_list() == [0, 0, 0]
