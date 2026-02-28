"""Tests for serve feature module."""

from datetime import date

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.model.features import serve as serve_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


class TestSvcFirstWinPctFeature:
    """Tests for svc_first_win_pct feature."""

    def test_svc_first_win_pct_registered(self):
        """svc_first_win_pct is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("svc_first_win_pct")
        assert feat.name == "svc_first_win_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_svc_first_win_pct_computes_rolling_percentage(self):
        """svc_first_win_pct computes rolling percentage of first serve points won."""
        from mvp.model.features.serve import svc_first_win_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "svc_first_serve_pts_won": [30, 40, 35, 45],
                "svc_first_serve_pts_played": [40, 50, 50, 60],
            }
        ).lazy()

        result = df.with_columns(
            svc_first_win_pct(days=30).alias("svc_first_win_pct")
        ).collect()

        # Row 0: no prior matches -> null
        # Row 1: 1 prior match: 30/40 = 0.75
        # Row 2: 2 prior matches: (30+40)/(40+50) = 70/90 = 0.777...
        # Row 3: 3 prior matches: (30+40+35)/(40+50+50) = 105/140 = 0.75
        assert result["svc_first_win_pct"][0] is None
        assert abs(result["svc_first_win_pct"][1] - 0.75) < 0.001
        assert abs(result["svc_first_win_pct"][2] - 70 / 90) < 0.001
        assert abs(result["svc_first_win_pct"][3] - 105 / 140) < 0.001

    def test_svc_first_win_pct_respects_window(self):
        """svc_first_win_pct only includes data within window period."""
        from mvp.model.features.serve import svc_first_win_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),  # Day 0
                    date(2024, 1, 10),  # Day 9
                    date(2024, 1, 20),  # Day 19
                ],
                "svc_first_serve_pts_won": [30, 40, 50],
                "svc_first_serve_pts_played": [40, 50, 60],
            }
        ).lazy()

        # Use a 7-day window
        result = df.with_columns(
            svc_first_win_pct(days=7).alias("svc_first_win_pct")
        ).collect()

        # Row 0: no prior matches -> null
        # Row 1: day 1 is 9 days before, outside 7-day window -> null
        # Row 2: day 10 is 10 days before, outside 7-day window -> null
        assert result["svc_first_win_pct"][0] is None
        assert result["svc_first_win_pct"][1] is None
        assert result["svc_first_win_pct"][2] is None
