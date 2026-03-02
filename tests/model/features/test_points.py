"""Tests for points feature module."""

from datetime import date

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.model.features import points as points_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


class TestPtsTotalWonPctFeature:
    """Tests for pts_total_won_pct feature."""

    def test_registered(self):
        """pts_total_won_pct is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("pts_total_won_pct")
        assert feat.name == "pts_total_won_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_computes_rolling_percentage(self):
        """pts_total_won_pct computes rolling percentage of total points won."""
        from mvp.model.features.points import pts_total_won_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                ],
                "pts_total_pts_won": [50, 60, 55],
                "pts_total_pts_played": [100, 120, 110],
            }
        ).lazy()

        result = df.with_columns(
            pts_total_won_pct(days=30).alias("pts_total_won_pct")
        ).collect()

        # Row 0: no prior matches -> null
        # Row 1: 50/100 = 0.50
        # Row 2: (50+60)/(100+120) = 110/220 = 0.50
        assert result["pts_total_won_pct"][0] is None
        assert abs(result["pts_total_won_pct"][1] - 0.50) < 0.001
        assert abs(result["pts_total_won_pct"][2] - 0.50) < 0.001


class TestPtsDiffFeatures:
    """Tests for points diff features."""

    def test_diff_features_registered(self):
        """All points diff features are registered."""
        registry = get_registry()
        diff_features = [
            "pts_total_won_pct_diff",
            "pts_service_won_pct_diff",
            "pts_return_won_pct_diff",
        ]
        for name in diff_features:
            feat = registry.get(name)
            assert feat.name == name
            assert feat.mirror is False
            assert len(feat.depends_on) > 0


class TestPtsMatchupFeatures:
    """Tests for points matchup features."""

    def test_matchup_features_registered(self):
        """Points matchup features are registered with correct metadata."""
        registry = get_registry()

        svc_matchup = registry.get("svc_pts_won_pct_matchup")
        assert svc_matchup.name == "svc_pts_won_pct_matchup"
        assert svc_matchup.mirror is False
        assert "pts_service_won_pct" in svc_matchup.depends_on
        assert "pts_return_won_pct" in svc_matchup.depends_on

        ret_matchup = registry.get("ret_pts_won_pct_matchup")
        assert ret_matchup.name == "ret_pts_won_pct_matchup"
        assert ret_matchup.mirror is False

    def test_svc_matchup_computes_serve_vs_return(self):
        """svc_pts_won_pct_matchup computes player serve - opp return."""
        from mvp.model.features.points import svc_pts_won_pct_matchup

        df = pl.DataFrame(
            {
                "player_pts_service_won_pct": [0.70, 0.65],
                "opp_pts_return_won_pct": [0.35, 0.40],
            }
        ).lazy()

        result = df.with_columns(
            svc_pts_won_pct_matchup().alias("matchup")
        ).collect()

        # 0.70 - 0.35 = 0.35, 0.65 - 0.40 = 0.25
        assert abs(result["matchup"][0] - 0.35) < 0.001
        assert abs(result["matchup"][1] - 0.25) < 0.001

    def test_ret_matchup_computes_return_vs_serve(self):
        """ret_pts_won_pct_matchup computes player return - opp serve."""
        from mvp.model.features.points import ret_pts_won_pct_matchup

        df = pl.DataFrame(
            {
                "player_pts_return_won_pct": [0.35, 0.40],
                "opp_pts_service_won_pct": [0.70, 0.65],
            }
        ).lazy()

        result = df.with_columns(
            ret_pts_won_pct_matchup().alias("matchup")
        ).collect()

        # 0.35 - 0.70 = -0.35, 0.40 - 0.65 = -0.25
        assert abs(result["matchup"][0] - (-0.35)) < 0.001
        assert abs(result["matchup"][1] - (-0.25)) < 0.001
