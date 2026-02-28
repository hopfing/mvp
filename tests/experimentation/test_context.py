"""Tests for FeatureContext."""

from datetime import date

import polars as pl

from mvp.experimentation.context import FeatureContext


class TestFeatureContext:
    """Tests for FeatureContext."""

    def test_rolling_sum_delegates_to_primitive(self):
        """FeatureContext.rolling_sum delegates to primitives.rolling_sum."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                ],
                "wins": [1, 1, 1],
            }
        ).sort("effective_match_date")

        ctx = FeatureContext(group_by="player_id")
        result = df.with_columns(ctx.rolling_sum("wins", days=30).alias("rolling_wins"))

        # Day 1: no prior data -> 0
        # Day 2: 1 win from day 1 -> 1
        # Day 3: 2 wins from days 1-2 -> 2
        assert result["rolling_wins"].to_list() == [0, 1, 2]

    def test_cumulative_mean_delegates_to_primitive(self):
        """FeatureContext.cumulative_mean delegates to primitives.cumulative_mean."""
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                ],
                "score": [10.0, 20.0, 30.0],
            }
        ).sort("effective_match_date")

        ctx = FeatureContext(group_by="player_id")
        result = df.with_columns(ctx.cumulative_mean("score").alias("avg_score"))

        # Day 1: no prior data -> null
        # Day 2: mean of [10] -> 10.0
        # Day 3: mean of [10, 20] -> 15.0
        assert result["avg_score"].to_list() == [None, 10.0, 15.0]
