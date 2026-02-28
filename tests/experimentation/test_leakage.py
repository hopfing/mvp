"""Tests for temporal leakage prevention.

These tests verify that feature computations do not include:
1. Future data (data from matches that haven't happened yet)
2. Current match data (the match we're predicting)
3. Same-day data that could leak information

Leakage prevention is critical for model integrity.
"""

from datetime import date

import polars as pl
import pytest

from mvp.experimentation.features import win_rate as win_rate_module  # noqa: F401
from mvp.experimentation.primitives import (
    cumulative_mean,
    cumulative_sum,
    rolling_count,
    rolling_mean,
    rolling_sum,
)


class TestWinRateNoFutureLeakage:
    """Tests verifying win_rate does not include future matches."""

    def test_win_rate_excludes_future_matches(self):
        """Win rate at time T must not include matches after time T."""
        from mvp.experimentation.features.win_rate import win_rate

        # Player A: loses first 3 matches, then wins 3 matches
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),   # loss
                date(2024, 1, 2),   # loss
                date(2024, 1, 3),   # loss
                date(2024, 1, 10),  # win
                date(2024, 1, 11),  # win
                date(2024, 1, 12),  # win
            ],
            "won": [0, 0, 0, 1, 1, 1],
        }).sort("effective_match_date")

        result = df.with_columns(
            win_rate(days=365).alias("win_rate")
        )

        # At row 3 (first win), win_rate should be 0.0 (0/3 from losses)
        # NOT 0.5 (3/6) which would indicate leakage from future wins
        win_rate_at_first_win = result["win_rate"][3]
        assert win_rate_at_first_win == 0.0, (
            f"Expected win_rate=0.0 at first win, got {win_rate_at_first_win}. "
            "This indicates future leakage!"
        )

    def test_win_rate_no_same_day_leakage(self):
        """Win rate must not include other matches from the same day."""
        from mvp.experimentation.features.win_rate import win_rate

        # Multiple matches on the same day - only prior day data should count
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),   # Day 1: win
                date(2024, 1, 5),   # Day 5: win
                date(2024, 1, 5),   # Day 5: loss (same day)
                date(2024, 1, 5),   # Day 5: loss (same day)
            ],
            "won": [1, 1, 0, 0],
        }).sort("effective_match_date")

        result = df.with_columns(
            win_rate(days=365).alias("win_rate")
        )

        # All day-5 matches should see only the day-1 win (win_rate = 1.0)
        # They should NOT see each other's results
        day5_win_rates = result.filter(
            pl.col("effective_match_date") == date(2024, 1, 5)
        )["win_rate"].to_list()

        assert all(wr == 1.0 for wr in day5_win_rates), (
            f"Expected all day-5 win_rates to be 1.0, got {day5_win_rates}. "
            "Same-day leakage detected!"
        )


class TestH2HNoCurrentMatchLeakage:
    """Tests verifying H2H features don't include the current match."""

    def test_cumulative_sum_excludes_current_match(self):
        """Cumulative H2H wins must exclude the current match being predicted."""
        # Simulate H2H: A vs B, tracking A's wins against B
        # Columns represent rows from A's perspective against B
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A"],
            "opp_id": ["B", "B", "B", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 10),
                date(2024, 1, 20),
                date(2024, 1, 30),
            ],
            "won": [1, 0, 1, 1],  # A wins: match 1, 3, 4
        }).sort("effective_match_date")

        result = df.with_columns(
            cumulative_sum("won", group_by=["player_id", "opp_id"]).alias("h2h_wins")
        )

        # Match 1: no prior H2H -> 0 (not 1)
        # Match 2: 1 prior win -> 1 (not 1)
        # Match 3: 1 prior win (match 1 only) -> 1 (not 2)
        # Match 4: 2 prior wins (matches 1, 3) -> 2 (not 3)
        expected = [0, 1, 1, 2]
        actual = result["h2h_wins"].to_list()

        assert actual == expected, (
            f"Expected H2H wins {expected}, got {actual}. "
            "Current match result may be leaking into H2H count!"
        )

    def test_cumulative_mean_excludes_current_match(self):
        """Cumulative mean must exclude the current match."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 10),
                date(2024, 1, 20),
            ],
            "score": [10.0, 20.0, 30.0],
        }).sort("effective_match_date")

        result = df.with_columns(
            cumulative_mean("score", group_by="player_id").alias("avg_score")
        )

        # Match 1: no prior data -> null
        # Match 2: mean of [10] -> 10.0 (not 15.0)
        # Match 3: mean of [10, 20] -> 15.0 (not 20.0)
        expected = [None, 10.0, 15.0]
        actual = result["avg_score"].to_list()

        assert actual == expected, (
            f"Expected avg scores {expected}, got {actual}. "
            "Current match may be included in cumulative mean!"
        )


class TestRollingWindowExcludesFuture:
    """Tests verifying rolling windows only look backward."""

    def test_rolling_sum_excludes_future(self):
        """Rolling sum window must only include past data."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 15),
                date(2024, 1, 20),
            ],
            "points": [10, 20, 30, 40, 50],
        }).sort("effective_match_date")

        result = df.with_columns(
            rolling_sum("points", days=30, group_by="player_id").alias("rolling_points")
        )

        # Each row should only see prior rows:
        # Row 0: no prior -> 0
        # Row 1: 10 -> 10
        # Row 2: 10+20 -> 30
        # Row 3: 10+20+30 -> 60
        # Row 4: 10+20+30+40 -> 100
        expected = [0, 10, 30, 60, 100]
        actual = result["rolling_points"].to_list()

        assert actual == expected, (
            f"Expected rolling points {expected}, got {actual}. "
            "Future data may be included in rolling window!"
        )

    def test_rolling_mean_excludes_future(self):
        """Rolling mean window must only include past data."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
            ],
            "score": [100.0, 50.0, 0.0],  # Declining scores
        }).sort("effective_match_date")

        result = df.with_columns(
            rolling_mean("score", days=30, group_by="player_id").alias("avg_score")
        )

        # Row 0: no prior -> null
        # Row 1: mean of [100] -> 100.0 (not including 50 or 0)
        # Row 2: mean of [100, 50] -> 75.0 (not including 0)
        expected = [None, 100.0, 75.0]
        actual = result["avg_score"].to_list()

        assert actual == expected, (
            f"Expected avg scores {expected}, got {actual}. "
            "Future data may be included in rolling mean!"
        )

    def test_rolling_count_excludes_future(self):
        """Rolling count must only count past matches."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 15),
            ],
        }).sort("effective_match_date")

        result = df.with_columns(
            rolling_count(days=30, group_by="player_id").alias("match_count")
        )

        # Row 0: no prior -> 0
        # Row 1: 1 prior -> 1
        # Row 2: 2 prior -> 2
        # Row 3: 3 prior -> 3
        expected = [0, 1, 2, 3]
        actual = result["match_count"].to_list()

        assert actual == expected, (
            f"Expected match counts {expected}, got {actual}. "
            "Future matches may be included in rolling count!"
        )

    def test_rolling_window_boundary_excludes_current_date(self):
        """Rolling window with closed='left' excludes current date."""
        # This is a regression test for the window boundary behavior
        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 1, 2),  # Two matches on same day
            ],
            "won": [1, 1, 0],
        }).sort("effective_match_date")

        result = df.with_columns(
            rolling_sum("won", days=30, group_by="player_id").alias("rolling_wins")
        )

        # Both Jan 2 matches should see only Jan 1's win (value=1)
        jan2_wins = result.filter(
            pl.col("effective_match_date") == date(2024, 1, 2)
        )["rolling_wins"].to_list()

        assert jan2_wins == [1, 1], (
            f"Expected both Jan 2 matches to see rolling_wins=1, got {jan2_wins}. "
            "Same-day data may be leaking!"
        )


class TestNoLeakageAcrossPlayers:
    """Tests verifying player isolation in feature computation."""

    def test_rolling_sum_isolated_by_player(self):
        """Player A's history must not affect Player B's features."""
        df = pl.DataFrame({
            "player_id": ["A", "A", "A", "B", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 3),
                date(2024, 1, 8),
            ],
            "won": [1, 1, 1, 0, 0],
        }).sort("effective_match_date")

        result = df.with_columns(
            rolling_sum("won", days=30, group_by="player_id").alias("rolling_wins")
        )

        # Player A rows (dates 1, 5, 10): 0, 1, 2
        # Player B rows (dates 3, 8): 0, 0
        player_a = result.filter(pl.col("player_id") == "A").sort("effective_match_date")
        player_b = result.filter(pl.col("player_id") == "B").sort("effective_match_date")

        assert player_a["rolling_wins"].to_list() == [0, 1, 2], (
            "Player A's rolling wins incorrect"
        )
        assert player_b["rolling_wins"].to_list() == [0, 0], (
            "Player B's rolling wins should be 0 (all losses), "
            "but may be contaminated by Player A's wins!"
        )
