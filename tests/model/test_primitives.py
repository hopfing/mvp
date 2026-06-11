"""Tests for temporal-safe primitives."""

from datetime import date

import polars as pl

from mvp.model.primitives import (
    cumulative_mean,
    cumulative_sum,
    rolling_count,
    rolling_max,
    rolling_mean,
    rolling_sum,
)


class TestRollingSum:
    """Tests for rolling_sum primitive."""

    def test_rolling_sum_basic(self):
        """Sum values over rolling window."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "won": [1, 0, 1, 1],
            }
        ).lazy()

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
        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 2)],
                "won": [1, 1],
                "match_uid": ["m1", "m2"],
                "round_order": [12, 12],
            }
        ).lazy()

        result = df.with_columns(
            rolling_sum("won", days=30, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 0: no prior → 0 (not 1)
        # Row 1: 1 prior (won=1) → 1 (not 2)
        assert result["rolling_wins"].to_list() == [0, 1]

    def test_rolling_sum_respects_window_boundary(self):
        """Only include matches within the window period."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),  # Day 0
                    date(2024, 1, 10),  # Day 9
                    date(2024, 1, 20),  # Day 19
                ],
                "won": [1, 1, 1],
            }
        ).lazy()

        result = df.with_columns(
            rolling_sum("won", days=7, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 0: no prior → 0
        # Row 1: day 1 is 9 days before day 10, outside 7-day window → 0
        # Row 2: day 10 is 10 days before day 20, outside 7-day window → 0
        assert result["rolling_wins"].to_list() == [0, 0, 0]

    def test_rolling_sum_includes_edge_of_window(self):
        """Match exactly at window boundary is included."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 8),  # Exactly 7 days later
                ],
                "won": [1, 1],
            }
        ).lazy()

        result = df.with_columns(
            rolling_sum("won", days=7, group_by="player_id").alias("rolling_wins")
        ).collect()

        # Row 1: day 1 is exactly 7 days before day 8, should be included
        assert result["rolling_wins"].to_list() == [0, 1]

    def test_rolling_sum_isolates_players(self):
        """Each player's rolling sum is independent."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "B", "A", "B"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                    date(2024, 1, 4),
                ],
                "won": [1, 1, 1, 0],
                "match_uid": ["m1", "m2", "m3", "m4"],
                "round_order": [12, 12, 12, 12],
            }
        ).lazy()

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
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "score": [10.0, 20.0, 30.0, 40.0],
                "match_uid": ["m1", "m2", "m3", "m4"],
                "round_order": [12, 12, 12, 12],
            }
        ).lazy()

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
        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 2)],
                "score": [100.0, 200.0],
            }
        ).lazy()

        result = df.with_columns(
            rolling_mean("score", days=30, group_by="player_id").alias("rolling_avg")
        ).collect()

        # Row 0: no prior → null (not 100.0)
        # Row 1: 1 prior (100) → 100.0 (not 150.0)
        assert result["rolling_avg"].to_list() == [None, 100.0]


class TestRollingMax:
    """Tests for rolling_max primitive."""

    def test_rolling_max_basic(self):
        """Returns max value within window."""
        df = pl.DataFrame(
            {
                "player_id": ["A"] * 4,
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "speed": [200.0, 190.0, 210.0, 195.0],
            }
        ).lazy()

        result = df.with_columns(
            rolling_max("speed", days=30, group_by="player_id").alias("max_speed")
        ).collect()

        # Row 0: no prior → null
        # Row 1: [200] → 200
        # Row 2: [200, 190] → 200
        # Row 3: [200, 190, 210] → 210
        assert result["max_speed"][0] is None
        assert result["max_speed"][1] == 200.0
        assert result["max_speed"][2] == 200.0
        assert result["max_speed"][3] == 210.0

    def test_rolling_max_respects_window(self):
        """Only includes values within the window period."""
        df = pl.DataFrame(
            {
                "player_id": ["A"] * 3,
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 2, 10),  # 40 days after first
                ],
                "speed": [220.0, 190.0, 195.0],
            }
        ).lazy()

        result = df.with_columns(
            rolling_max("speed", days=7, group_by="player_id").alias("max_speed")
        ).collect()

        # Row 2: 7d window before Feb 10 = Feb 3 to Feb 9 → no matches → null
        assert result["max_speed"][2] is None


class TestRollingCount:
    """Tests for rolling_count primitive."""

    def test_rolling_count_basic(self):
        """Count rows over rolling window."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
            }
        ).lazy()

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
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),  # Day 0
                    date(2024, 1, 10),  # Day 9
                    date(2024, 1, 20),  # Day 19
                ],
            }
        ).lazy()

        result = df.with_columns(
            rolling_count(days=7, group_by="player_id").alias("match_count")
        ).collect()

        # Row 0: no prior → 0
        # Row 1: day 1 is 9 days before day 10, outside 7-day window → 0
        # Row 2: day 10 is 10 days before day 20, outside 7-day window → 0
        assert result["match_count"].to_list() == [0, 0, 0]


class TestCumulativeSum:
    """Tests for cumulative_sum primitive."""

    def test_cumulative_sum_basic(self):
        """Sum values over all prior rows."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "won": [1, 0, 1, 1],
                "match_uid": ["m1", "m2", "m3", "m4"],
                "round_order": [12, 12, 12, 12],
            }
        ).lazy()

        result = df.with_columns(
            cumulative_sum("won", group_by="player_id").alias("total_wins")
        ).collect()

        # Row 0: no prior matches → 0
        # Row 1: 1 prior match (won=1) → 1
        # Row 2: 2 prior matches (won=1,0) → 1
        # Row 3: 3 prior matches (won=1,0,1) → 2
        assert result["total_wins"].to_list() == [0, 1, 1, 2]

    def test_cumulative_sum_excludes_current_row(self):
        """Current row must NOT be included in the sum."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 2)],
                "won": [1, 1],
                "match_uid": ["m1", "m2"],
                "round_order": [12, 12],
            }
        ).lazy()

        result = df.with_columns(
            cumulative_sum("won", group_by="player_id").alias("total_wins")
        ).collect()

        # Row 0: no prior → 0 (not 1)
        # Row 1: 1 prior (won=1) → 1 (not 2)
        assert result["total_wins"].to_list() == [0, 1]

    def test_cumulative_sum_groups_by_matchup(self):
        """Cumulative sum respects group_by columns."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "B", "A", "B"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                    date(2024, 1, 4),
                ],
                "won": [1, 1, 1, 0],
                "match_uid": ["m1", "m2", "m3", "m4"],
                "round_order": [12, 12, 12, 12],
            }
        ).lazy()

        result = df.with_columns(
            cumulative_sum("won", group_by="player_id").alias("total_wins")
        ).collect()

        # Row 0 (A): no prior A matches → 0
        # Row 1 (B): no prior B matches → 0
        # Row 2 (A): 1 prior A match (won=1) → 1
        # Row 3 (B): 1 prior B match (won=1) → 1
        assert result["total_wins"].to_list() == [0, 0, 1, 1]


class TestCumulativeMean:
    """Tests for cumulative_mean primitive."""

    def test_cumulative_mean_basic(self):
        """Mean values over all prior rows."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "score": [10.0, 20.0, 30.0, 40.0],
                "match_uid": ["m1", "m2", "m3", "m4"],
                "round_order": [12, 12, 12, 12],
            }
        ).lazy()

        result = df.with_columns(
            cumulative_mean("score", group_by="player_id").alias("avg_score")
        ).collect()

        # Row 0: no prior matches → null
        # Row 1: 1 prior match (10) → 10.0
        # Row 2: 2 prior matches (10, 20) → 15.0
        # Row 3: 3 prior matches (10, 20, 30) → 20.0
        assert result["avg_score"].to_list() == [None, 10.0, 15.0, 20.0]

    def test_cumulative_mean_ignores_null_values(self):
        """A null source row must not dilute the mean: it sits out of both the
        sum and the count, rather than counting as a 0 in the denominator."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "score": [10.0, None, 20.0, 30.0],
                "match_uid": ["m1", "m2", "m3", "m4"],
                "round_order": [12, 12, 12, 12],
            }
        ).lazy()

        result = df.with_columns(
            cumulative_mean("score", group_by="player_id").alias("avg")
        ).collect()

        # Row 0: no prior → null
        # Row 1: prior=[10] → 10.0
        # Row 2: prior=[10, null] → null ignored → 10.0 (NOT 5.0)
        # Row 3: prior=[10, null, 20] → mean(10, 20) = 15.0 (NOT 10.0)
        assert result["avg"].to_list() == [None, 10.0, 10.0, 15.0]


