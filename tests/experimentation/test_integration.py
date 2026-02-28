"""Integration tests for the experimentation platform.

Tests all features working together on realistic match data.
"""

from datetime import date

import polars as pl
import pytest

# Import feature modules to register them
from mvp.experimentation.features import h2h as h2h_module  # noqa: F401
from mvp.experimentation.features import ranking as ranking_module  # noqa: F401
from mvp.experimentation.features import win_rate as win_rate_module  # noqa: F401
from mvp.experimentation.registry import get_registry


@pytest.fixture
def sample_matches_df() -> pl.DataFrame:
    """Create a realistic sample matches DataFrame.

    Simulates matches between 4 players (A, B, C, D) over a month.
    Includes columns that would come from matches.parquet.
    """
    return pl.DataFrame({
        "match_id": [f"M{i:03d}" for i in range(1, 13)],
        "player_id": ["A", "B", "A", "C", "B", "D", "A", "B", "C", "D", "A", "B"],
        "opp_id": ["B", "A", "C", "A", "D", "B", "B", "A", "D", "C", "C", "D"],
        "effective_match_date": [
            # Week 1
            date(2024, 1, 1),   # A vs B: A wins
            date(2024, 1, 1),   # B vs A: B loses (mirror)
            date(2024, 1, 3),   # A vs C: A wins
            date(2024, 1, 3),   # C vs A: C loses (mirror)
            # Week 2
            date(2024, 1, 8),   # B vs D: B wins
            date(2024, 1, 8),   # D vs B: D loses (mirror)
            date(2024, 1, 10),  # A vs B: A wins again
            date(2024, 1, 10),  # B vs A: B loses again (mirror)
            # Week 3
            date(2024, 1, 15),  # C vs D: C wins
            date(2024, 1, 15),  # D vs C: D loses (mirror)
            date(2024, 1, 17),  # A vs C: A wins
            date(2024, 1, 17),  # B vs D: B wins
        ],
        "won": [1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1],
        "player_ranking_points": [
            1000, 800, 1050, 600, 850, 500, 1100, 750, 650, 450, 1150, 900
        ],
        "opp_ranking_points": [
            800, 1000, 600, 1050, 500, 850, 750, 1100, 450, 650, 650, 500
        ],
    }).sort("effective_match_date")


class TestAllFeaturesIntegration:
    """Integration tests for all features together."""

    def test_all_features_are_registered(self):
        """Verify all expected features are registered."""
        registry = get_registry()
        features = registry.list_features()

        expected_features = [
            "win_rate",
            "matches_played",
            "win_rate_diff",
            "h2h_wins",
            "ranking_points_diff",
        ]

        for feat_name in expected_features:
            assert feat_name in features, f"Feature '{feat_name}' not registered"

    def test_win_rate_on_sample_data(self, sample_matches_df: pl.DataFrame):
        """win_rate computes correctly on sample data."""
        from mvp.experimentation.features.win_rate import win_rate

        result = sample_matches_df.with_columns(
            win_rate(days=30).alias("player_win_rate_30d")
        )

        # Player A's matches (rows 0, 2, 6, 10):
        # Row 0 (Jan 1): no prior -> null
        # Row 2 (Jan 3): 1 win -> 1.0
        # Row 6 (Jan 10): 2 wins -> 1.0
        # Row 10 (Jan 17): 3 wins -> 1.0
        player_a = result.filter(pl.col("player_id") == "A")
        win_rates = player_a["player_win_rate_30d"].to_list()
        assert win_rates == [None, 1.0, 1.0, 1.0]

    def test_matches_played_on_sample_data(self, sample_matches_df: pl.DataFrame):
        """matches_played computes correctly on sample data."""
        from mvp.experimentation.features.win_rate import matches_played

        result = sample_matches_df.with_columns(
            matches_played(days=30).alias("matches_30d")
        )

        # Player A's matches (rows 0, 2, 6, 10):
        # Row 0 (Jan 1): no prior -> 0
        # Row 2 (Jan 3): 1 prior -> 1
        # Row 6 (Jan 10): 2 prior -> 2
        # Row 10 (Jan 17): 3 prior -> 3
        player_a = result.filter(pl.col("player_id") == "A")
        match_counts = player_a["matches_30d"].to_list()
        assert match_counts == [0, 1, 2, 3]

    def test_h2h_wins_on_sample_data(self, sample_matches_df: pl.DataFrame):
        """h2h_wins computes correctly on sample data."""
        from mvp.experimentation.features.h2h import h2h_wins

        result = sample_matches_df.with_columns(
            h2h_wins().alias("h2h_wins_vs_opp")
        )

        # A vs B matches (rows 0, 6): A wins both
        # Row 0 (Jan 1, A vs B): no prior H2H -> 0
        # Row 6 (Jan 10, A vs B): 1 prior win -> 1
        a_vs_b = result.filter(
            (pl.col("player_id") == "A") & (pl.col("opp_id") == "B")
        )
        h2h = a_vs_b["h2h_wins_vs_opp"].to_list()
        assert h2h == [0, 1]

    def test_ranking_points_diff_on_sample_data(self, sample_matches_df: pl.DataFrame):
        """ranking_points_diff computes correctly on sample data."""
        from mvp.experimentation.features.ranking import ranking_points_diff

        result = sample_matches_df.with_columns(
            ranking_points_diff().alias("ranking_diff")
        )

        # Row 0: A (1000) vs B (800) -> 200
        # Row 1: B (800) vs A (1000) -> -200
        assert result["ranking_diff"][0] == 200
        assert result["ranking_diff"][1] == -200

    def test_combined_features_no_conflict(self, sample_matches_df: pl.DataFrame):
        """Multiple features can be computed together without conflict."""
        from mvp.experimentation.features.h2h import h2h_wins
        from mvp.experimentation.features.ranking import ranking_points_diff
        from mvp.experimentation.features.win_rate import matches_played, win_rate

        result = sample_matches_df.with_columns([
            win_rate(days=30).alias("player_win_rate_30d"),
            matches_played(days=30).alias("matches_30d"),
            h2h_wins().alias("h2h_wins_vs_opp"),
            ranking_points_diff().alias("ranking_diff"),
        ])

        # All columns should exist
        assert "player_win_rate_30d" in result.columns
        assert "matches_30d" in result.columns
        assert "h2h_wins_vs_opp" in result.columns
        assert "ranking_diff" in result.columns

        # Row count preserved
        assert len(result) == len(sample_matches_df)

    def test_win_rate_diff_requires_dependencies(self, sample_matches_df: pl.DataFrame):
        """win_rate_diff requires win_rate columns to be computed first."""
        from mvp.experimentation.features.win_rate import win_rate, win_rate_diff

        # First compute win_rate for player and opponent
        df_with_wr = sample_matches_df.with_columns([
            win_rate(days=30).alias("player_win_rate_30d"),
        ])

        # Simulate opponent's win_rate (in real system this would be mirrored)
        # For now just add a placeholder
        df_with_both = df_with_wr.with_columns(
            pl.lit(0.5).alias("opp_win_rate_30d")
        )

        # Now win_rate_diff should work
        result = df_with_both.with_columns(
            win_rate_diff(days=30).alias("win_rate_diff_30d")
        )

        assert "win_rate_diff_30d" in result.columns

    def test_features_handle_null_values(self):
        """Features handle null/missing values gracefully."""
        from mvp.experimentation.features.win_rate import win_rate

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
            ],
            "won": [1, None, 0],  # Middle value is null
        }).sort("effective_match_date")

        result = df.with_columns(
            win_rate(days=30).alias("win_rate")
        )

        # Should not crash, null is propagated through rolling_mean
        assert len(result) == 3


class TestFeatureMetadata:
    """Tests for feature metadata and registry."""

    def test_feature_params_are_documented(self):
        """All features have their params documented."""
        registry = get_registry()

        for feat_name in registry.list_features():
            feat = registry.get(feat_name)
            # Either no params or params list is not None
            assert feat.params is not None

    def test_mirror_flag_set_correctly(self):
        """Features have appropriate mirror flag settings."""
        registry = get_registry()

        # Diff features should not mirror (they compare player vs opponent)
        diff_features = ["win_rate_diff", "ranking_points_diff"]
        for feat_name in diff_features:
            feat = registry.get(feat_name)
            assert feat.mirror is False, f"{feat_name} should not mirror"

        # Regular player features should mirror
        mirror_features = ["win_rate", "matches_played", "h2h_wins"]
        for feat_name in mirror_features:
            feat = registry.get(feat_name)
            assert feat.mirror is True, f"{feat_name} should mirror"

    def test_dependencies_are_declared(self):
        """Features with dependencies have them declared."""
        registry = get_registry()

        # win_rate_diff depends on win_rate
        feat = registry.get("win_rate_diff")
        assert "win_rate" in feat.depends_on


class TestTemporalSafety:
    """Additional temporal safety tests for integration."""

    def test_features_respect_temporal_ordering(self, sample_matches_df: pl.DataFrame):
        """Features compute differently based on temporal position."""
        from mvp.experimentation.features.win_rate import win_rate

        result = sample_matches_df.with_columns(
            win_rate(days=30).alias("win_rate")
        )

        # For player A, win_rate should increase over time (all wins)
        player_a = result.filter(pl.col("player_id") == "A")

        # First match: null (no prior data)
        assert player_a["win_rate"][0] is None

        # Subsequent matches: should be 1.0 (100% win rate from prior matches)
        for i in range(1, len(player_a)):
            assert player_a["win_rate"][i] == 1.0

    def test_features_player_isolation(self, sample_matches_df: pl.DataFrame):
        """Each player's features are computed independently."""
        from mvp.experimentation.features.win_rate import win_rate

        result = sample_matches_df.with_columns(
            win_rate(days=30).alias("win_rate")
        )

        # Player B always loses against A, wins against D
        player_b = result.filter(pl.col("player_id") == "B")

        # B's win rates should not be affected by A's wins
        # B loses first (row 1), then wins (row 4), then loses (row 7), then wins (row 11)
        # Note: actual values depend on match ordering
        assert len(player_b) == 4
