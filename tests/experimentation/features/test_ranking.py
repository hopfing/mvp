"""Tests for ranking feature module."""

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.experimentation.features import ranking as ranking_module  # noqa: F401
from mvp.experimentation.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


class TestRankingPointsDiffFeature:
    """Tests for ranking_points_diff feature."""

    def test_ranking_points_diff_registered(self):
        """ranking_points_diff is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("ranking_points_diff")
        assert feat.name == "ranking_points_diff"
        assert feat.params == []
        assert feat.mirror is False  # Diff features don't mirror

    def test_ranking_points_diff_computes_difference(self):
        """ranking_points_diff computes player_ranking_points - opp_ranking_points."""
        from mvp.experimentation.features.ranking import ranking_points_diff

        df = pl.DataFrame(
            {
                "player_ranking_points": [1000, 500, 800],
                "opp_ranking_points": [500, 500, 1200],
            }
        ).lazy()

        result = df.with_columns(
            ranking_points_diff().alias("ranking_points_diff")
        ).collect()

        # Row 0: 1000 - 500 = 500
        # Row 1: 500 - 500 = 0
        # Row 2: 800 - 1200 = -400
        assert result["ranking_points_diff"].to_list() == [500, 0, -400]

    def test_ranking_points_diff_handles_nulls(self):
        """ranking_points_diff handles null values gracefully."""
        from mvp.experimentation.features.ranking import ranking_points_diff

        df = pl.DataFrame(
            {
                "player_ranking_points": [1000, None, 800],
                "opp_ranking_points": [500, 500, None],
            }
        ).lazy()

        result = df.with_columns(
            ranking_points_diff().alias("ranking_points_diff")
        ).collect()

        # Row 0: 1000 - 500 = 500
        # Row 1: None - 500 = None
        # Row 2: 800 - None = None
        assert result["ranking_points_diff"][0] == 500
        assert result["ranking_points_diff"][1] is None
        assert result["ranking_points_diff"][2] is None
