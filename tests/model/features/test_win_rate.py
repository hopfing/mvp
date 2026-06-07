"""Tests for win_rate feature module."""

from datetime import date

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.model.features import win_rate as win_rate_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    # Features are registered at import time via decorators
    yield


class TestWinPctFeature:
    """Tests for win_pct feature."""

    def test_win_pct_registered(self):
        """win_pct is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("win_pct")
        assert feat.name == "win_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_win_pct_computes_rolling_mean(self):
        """win_pct computes rolling mean of won column."""
        from mvp.model.features.win_rate import win_pct

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

        result = df.with_columns(win_pct(days=30).alias("win_pct")).collect()

        # win_pct is empirical-Bayes shrunk (k=13) and never fabricates at no
        # history: row 0 (no priors) is null; later rows are interior, pulled
        # toward the pooled mean (a perfect 1-0 no longer reads as a flat 1.0).
        vals = result["win_pct"].to_list()
        assert vals[0] is None
        assert all(v is not None and 0.0 < v < 1.0 for v in vals[1:])
        assert vals[1] < 1.0  # 1-0 shrunk below a confident 100%


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
        from mvp.model.features.win_rate import matches_played

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


class TestWinPctDiffFeature:
    """Tests for win_pct_diff feature."""

    def test_win_pct_diff_registered(self):
        """win_pct_diff is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("win_pct_diff")
        assert feat.name == "win_pct_diff"
        assert feat.params == ["days"]
        assert feat.depends_on == ["win_pct"]
        assert feat.mirror is False  # Diff features don't mirror

    def test_win_pct_diff_computes_difference(self):
        """win_pct_diff computes player_win_pct - opp_win_pct."""
        win_pct_diff = get_registry().get("win_pct_diff").func

        df = pl.DataFrame(
            {
                "player_win_pct_30d": [0.8, 0.6, 0.5],
                "opp_win_pct_30d": [0.4, 0.6, 0.7],
            }
        ).lazy()

        result = df.with_columns(
            win_pct_diff(days=30).alias("win_pct_diff")
        ).collect()

        # Row 0: 0.8 - 0.4 = 0.4
        # Row 1: 0.6 - 0.6 = 0.0
        # Row 2: 0.5 - 0.7 = -0.2
        assert abs(result["win_pct_diff"][0] - 0.4) < 0.001
        assert abs(result["win_pct_diff"][1] - 0.0) < 0.001
        assert abs(result["win_pct_diff"][2] - (-0.2)) < 0.001
