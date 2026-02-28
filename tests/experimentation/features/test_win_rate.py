"""Tests for win_rate feature module."""

from datetime import date

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.experimentation.features import win_rate as win_rate_module  # noqa: F401
from mvp.experimentation.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    # Features are registered at import time via decorators
    yield


class TestWinRateFeature:
    """Tests for win_rate feature."""

    def test_win_rate_registered(self):
        """win_rate is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("win_rate")
        assert feat.name == "win_rate"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_win_rate_computes_rolling_mean(self):
        """win_rate computes rolling mean of won column."""
        from mvp.experimentation.features.win_rate import win_rate

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

        result = df.with_columns(win_rate(days=30).alias("win_rate")).collect()

        # Row 0: no prior matches -> null
        # Row 1: 1 prior match (won=1) -> 1.0
        # Row 2: 2 prior matches (won=1,0) -> 0.5
        # Row 3: 3 prior matches (won=1,0,1) -> 0.666...
        assert result["win_rate"][0] is None
        assert result["win_rate"][1] == 1.0
        assert result["win_rate"][2] == 0.5
        assert abs(result["win_rate"][3] - 2 / 3) < 0.001


class TestMatchesPlayedFeature:
    """Tests for matches_played feature."""

    def test_matches_played_registered(self):
        """matches_played is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("matches_played")
        assert feat.name == "matches_played"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_matches_played_computes_rolling_count(self):
        """matches_played computes rolling count."""
        from mvp.experimentation.features.win_rate import matches_played

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
            matches_played(days=30).alias("matches_played")
        ).collect()

        # Row 0: no prior matches -> 0
        # Row 1: 1 prior match -> 1
        # Row 2: 2 prior matches -> 2
        # Row 3: 3 prior matches -> 3
        assert result["matches_played"].to_list() == [0, 1, 2, 3]


class TestWinRateDiffFeature:
    """Tests for win_rate_diff feature."""

    def test_win_rate_diff_registered(self):
        """win_rate_diff is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("win_rate_diff")
        assert feat.name == "win_rate_diff"
        assert feat.params == ["days"]
        assert feat.depends_on == ["win_rate"]
        assert feat.mirror is False  # Diff features don't mirror

    def test_win_rate_diff_computes_difference(self):
        """win_rate_diff computes player_win_rate - opp_win_rate."""
        from mvp.experimentation.features.win_rate import win_rate_diff

        df = pl.DataFrame(
            {
                "player_win_rate_30d": [0.8, 0.6, 0.5],
                "opp_win_rate_30d": [0.4, 0.6, 0.7],
            }
        ).lazy()

        result = df.with_columns(
            win_rate_diff(days=30).alias("win_rate_diff")
        ).collect()

        # Row 0: 0.8 - 0.4 = 0.4
        # Row 1: 0.6 - 0.6 = 0.0
        # Row 2: 0.5 - 0.7 = -0.2
        assert abs(result["win_rate_diff"][0] - 0.4) < 0.001
        assert abs(result["win_rate_diff"][1] - 0.0) < 0.001
        assert abs(result["win_rate_diff"][2] - (-0.2)) < 0.001
