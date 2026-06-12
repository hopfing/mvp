"""Integration tests for the model platform.

Tests all features working together on realistic match data.
"""

from datetime import date
from importlib import reload

import polars as pl
import pytest

from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    registry = get_registry()
    saved = dict(registry._features)
    registry.clear()

    from mvp.model.features import h2h, ranking, win_rate

    reload(h2h)
    reload(ranking)
    reload(win_rate)

    yield

    registry.clear()
    registry._features.update(saved)


@pytest.fixture
def sample_matches_df() -> pl.DataFrame:
    """Create a realistic sample matches DataFrame.

    Simulates matches between 4 players (A, B, C, D) over a month.
    Includes columns that would come from matches.parquet.
    """
    return pl.DataFrame(
        {
            "match_id": [f"M{i:03d}" for i in range(1, 13)],
            "match_uid": [f"M{i:03d}" for i in range(1, 13)],
            "round_order": [7] * 12,
            "player_id": ["A", "B", "A", "C", "B", "D", "A", "B", "C", "D", "A", "B"],
            "opp_id": ["B", "A", "C", "A", "D", "B", "B", "A", "D", "C", "C", "D"],
            "effective_match_date": [
                # Week 1
                date(2024, 1, 1),  # A vs B: A wins
                date(2024, 1, 1),  # B vs A: B loses (mirror)
                date(2024, 1, 3),  # A vs C: A wins
                date(2024, 1, 3),  # C vs A: C loses (mirror)
                # Week 2
                date(2024, 1, 8),  # B vs D: B wins
                date(2024, 1, 8),  # D vs B: D loses (mirror)
                date(2024, 1, 10),  # A vs B: A wins again
                date(2024, 1, 10),  # B vs A: B loses again (mirror)
                # Week 3
                date(2024, 1, 15),  # C vs D: C wins
                date(2024, 1, 15),  # D vs C: D loses (mirror)
                date(2024, 1, 17),  # A vs C: A wins
                date(2024, 1, 17),  # B vs D: B wins
            ],
            "won": [1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1],
            "player_rankings_points": [
                1000,
                800,
                1050,
                600,
                850,
                500,
                1100,
                750,
                650,
                450,
                1150,
                900,
            ],
            "opp_rankings_points": [
                800,
                1000,
                600,
                1050,
                500,
                850,
                750,
                1100,
                450,
                650,
                650,
                500,
            ],
        }
    ).sort("effective_match_date")


class TestAllFeaturesIntegration:
    """Integration tests for all features together."""

    def test_all_features_are_registered(self):
        """Verify all expected features are registered."""
        registry = get_registry()
        features = registry.list_features()

        expected_features = [
            "win_pct",
            "matches_played",
            "win_pct_diff",
            "h2h_wins",
            "ranking_points_diff",
        ]

        for feat_name in expected_features:
            assert feat_name in features, f"Feature '{feat_name}' not registered"

    def test_win_pct_on_sample_data(self, sample_matches_df: pl.DataFrame):
        """win_pct computes correctly on sample data."""
        from mvp.model.features.win_rate import win_pct

        result = sample_matches_df.with_columns(
            win_pct(days=30).alias("player_win_pct_30d")
        )

        # Player A wins every match. Row 0 has no prior history -> null. The
        # rest are EB-shrunk toward the pooled rate, so they land between 0.5
        # and 1.0 and rise as the win count grows (shrinkage weakens).
        player_a = result.filter(pl.col("player_id") == "A")
        win_pcts = player_a["player_win_pct_30d"].to_list()
        assert win_pcts[0] is None
        later = win_pcts[1:]
        assert all(0.5 < v < 1.0 for v in later)
        assert later[-1] >= later[0], f"all-win win_pct should rise, got {later}"

    def test_matches_played_on_sample_data(self, sample_matches_df: pl.DataFrame):
        """matches_played computes correctly on sample data."""
        from mvp.model.features.win_rate import matches_played

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
        from mvp.model.features.h2h import h2h_wins

        result = sample_matches_df.with_columns(h2h_wins().alias("h2h_wins_vs_opp"))

        # A vs B matches (rows 0, 6): A wins both. h2h_wins is NaN-passthrough,
        # so the first encounter (no prior H2H) is null, not 0 — keeping "never
        # played" distinct from "played and won 0".
        # Row 0 (Jan 1, A vs B): no prior H2H -> None
        # Row 6 (Jan 10, A vs B): 1 prior win -> 1
        a_vs_b = result.filter((pl.col("player_id") == "A") & (pl.col("opp_id") == "B"))
        h2h = a_vs_b["h2h_wins_vs_opp"].to_list()
        assert h2h == [None, 1]

    def test_ranking_points_diff_on_sample_data(self, sample_matches_df: pl.DataFrame):
        """ranking_points_diff computes correctly on sample data."""
        from mvp.model.features.ranking import ranking_points_diff

        result = sample_matches_df.with_columns(
            ranking_points_diff().alias("ranking_diff")
        )

        # Row 0: A (1000) vs B (800) -> 200
        # Row 1: B (800) vs A (1000) -> -200
        assert result["ranking_diff"][0] == 200
        assert result["ranking_diff"][1] == -200

    def test_combined_features_no_conflict(self, sample_matches_df: pl.DataFrame):
        """Multiple features can be computed together without conflict."""
        from mvp.model.features.h2h import h2h_wins
        from mvp.model.features.ranking import ranking_points_diff
        from mvp.model.features.win_rate import matches_played, win_pct

        result = sample_matches_df.with_columns(
            [
                win_pct(days=30).alias("player_win_pct_30d"),
                matches_played(days=30).alias("matches_30d"),
                h2h_wins().alias("h2h_wins_vs_opp"),
                ranking_points_diff().alias("ranking_diff"),
            ]
        )

        # All columns should exist
        assert "player_win_pct_30d" in result.columns
        assert "matches_30d" in result.columns
        assert "h2h_wins_vs_opp" in result.columns
        assert "ranking_diff" in result.columns

        # Row count preserved
        assert len(result) == len(sample_matches_df)

    def test_win_pct_diff_requires_dependencies(self, sample_matches_df: pl.DataFrame):
        """win_pct_diff requires win_pct columns to be computed first."""
        from mvp.model.features.win_rate import win_pct
        win_pct_diff = get_registry().get("win_pct_diff").func

        # First compute win_pct for player and opponent
        df_with_wr = sample_matches_df.with_columns(
            [
                win_pct(days=30).alias("player_win_pct_30d"),
            ]
        )

        # Simulate opponent's win_pct (in real system this would be mirrored)
        # For now just add a placeholder
        df_with_both = df_with_wr.with_columns(pl.lit(0.5).alias("opp_win_pct_30d"))

        # Now win_pct_diff should work
        result = df_with_both.with_columns(
            win_pct_diff(days=30).alias("win_pct_diff_30d")
        )

        assert "win_pct_diff_30d" in result.columns

    def test_features_handle_null_values(self):
        """Features handle null/missing values gracefully."""
        from mvp.model.features.win_rate import win_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                ],
                "won": [1, None, 0],  # Middle value is null
            }
        ).sort("effective_match_date")

        result = df.with_columns(win_pct(days=30).alias("win_pct"))

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
        diff_features = ["win_pct_diff", "ranking_points_diff"]
        for feat_name in diff_features:
            feat = registry.get(feat_name)
            assert feat.mirror is False, f"{feat_name} should not mirror"

        # Regular player features should mirror
        mirror_features = ["win_pct", "matches_played", "h2h_wins"]
        for feat_name in mirror_features:
            feat = registry.get(feat_name)
            assert feat.mirror is True, f"{feat_name} should mirror"

    def test_dependencies_are_declared(self):
        """Features with dependencies have them declared."""
        registry = get_registry()

        # win_pct_diff depends on win_pct
        feat = registry.get("win_pct_diff")
        assert "win_pct" in feat.depends_on


class TestTemporalSafety:
    """Additional temporal safety tests for integration."""

    def test_features_respect_temporal_ordering(self, sample_matches_df: pl.DataFrame):
        """Features compute differently based on temporal position."""
        from mvp.model.features.win_rate import win_pct

        result = sample_matches_df.with_columns(win_pct(days=30).alias("win_pct"))

        # For player A, win_pct should increase over time (all wins)
        player_a = result.filter(pl.col("player_id") == "A")

        # First match: null (no prior data)
        assert player_a["win_pct"][0] is None

        # Subsequent matches: EB-shrunk toward pooled, so above 0.5 (all wins)
        # and rising as the win count accumulates.
        later = [player_a["win_pct"][i] for i in range(1, len(player_a))]
        assert all(0.5 < v < 1.0 for v in later)
        assert later[-1] >= later[0], f"all-win win_pct should rise, got {later}"

    def test_features_player_isolation(self, sample_matches_df: pl.DataFrame):
        """Each player's features are computed independently."""
        from mvp.model.features.win_rate import win_pct

        result = sample_matches_df.with_columns(win_pct(days=30).alias("win_pct"))

        # Player B always loses against A, wins against D
        player_b = result.filter(pl.col("player_id") == "B")

        # B's win rates should not be affected by A's wins
        # B loses (row 1), wins (row 4), loses (row 7), wins (row 11)
        # Note: actual values depend on match ordering
        assert len(player_b) == 4
