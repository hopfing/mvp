"""Tests for return feature module."""

from datetime import date

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.model.features import returns as returns_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


class TestRetFirstServeWinPctFeature:
    """Tests for ret_first_serve_win_pct feature."""

    def test_registered(self):
        """ret_first_serve_win_pct is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("ret_first_serve_win_pct")
        assert feat.name == "ret_first_serve_win_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_computes_rolling_percentage(self):
        """ret_first_serve_win_pct computes rolling percentage of return points won."""
        from mvp.model.features.returns import ret_first_serve_win_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                    date(2024, 1, 15),
                ],
                "ret_first_serve_pts_won": [20, 25, 30, 35],
                "ret_first_serve_pts_played": [50, 50, 60, 70],
            }
        ).lazy()

        result = df.with_columns(
            ret_first_serve_win_pct(days=30).alias("ret_first_serve_win_pct")
        ).collect()

        # Row 0: no prior matches -> null
        # Row 1: 20/50 = 0.40
        # Row 2: (20+25)/(50+50) = 45/100 = 0.45
        # Row 3: (20+25+30)/(50+50+60) = 75/160 = 0.46875
        assert result["ret_first_serve_win_pct"][0] is None
        assert abs(result["ret_first_serve_win_pct"][1] - 0.40) < 0.001
        assert abs(result["ret_first_serve_win_pct"][2] - 0.45) < 0.001
        assert abs(result["ret_first_serve_win_pct"][3] - 75 / 160) < 0.001


class TestRetRatingFeature:
    """Tests for ret_rating feature (ATP return rating average)."""

    def test_registered(self):
        """ret_rating is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("ret_rating")
        assert feat.name == "ret_rating"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_computes_average_rating(self):
        """ret_rating computes average of ATP return rating."""
        from mvp.model.features.returns import ret_rating

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                ],
                "ret_return_rating": [150.0, 180.0, 200.0],
            }
        ).lazy()

        result = df.with_columns(ret_rating(days=30).alias("ret_rating")).collect()

        # Row 0: no prior matches -> null
        # Row 1: 150.0 (only prior match)
        # Row 2: (150 + 180) / 2 = 165.0
        assert result["ret_rating"][0] is None
        assert abs(result["ret_rating"][1] - 150.0) < 0.001
        assert abs(result["ret_rating"][2] - 165.0) < 0.001


class TestRetDiffFeatures:
    """Tests for return diff features."""

    def test_diff_features_registered(self):
        """All return diff features are registered."""
        registry = get_registry()
        diff_features = [
            "ret_first_serve_win_pct_diff",
            "ret_second_serve_win_pct_diff",
            "ret_bp_convert_pct_diff",
            "ret_rating_diff",
        ]
        for name in diff_features:
            feat = registry.get(name)
            assert feat.name == name
            assert feat.mirror is False
            assert len(feat.depends_on) > 0

    def test_diff_computes_player_minus_opp(self):
        """Diff feature computes player stat minus opponent stat."""
        from mvp.model.features.returns import ret_first_serve_win_pct_diff

        df = pl.DataFrame(
            {
                "player_ret_first_serve_win_pct": [0.35, 0.40],
                "opp_ret_first_serve_win_pct": [0.30, 0.45],
            }
        ).lazy()

        result = df.with_columns(
            ret_first_serve_win_pct_diff().alias("diff")
        ).collect()

        assert abs(result["diff"][0] - 0.05) < 0.001
        assert abs(result["diff"][1] - (-0.05)) < 0.001
