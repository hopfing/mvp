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


class TestSvcFirstServeWinPctFeature:
    """Tests for svc_first_serve_win_pct feature."""

    def test_registered(self):
        """svc_first_serve_win_pct is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("svc_first_serve_win_pct")
        assert feat.name == "svc_first_serve_win_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_computes_rolling_percentage(self):
        """svc_first_serve_win_pct computes rolling percentage of first serve points won."""
        from mvp.model.features.serve import svc_first_serve_win_pct

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
            svc_first_serve_win_pct(days=30).alias("svc_first_serve_win_pct")
        ).collect()

        # Empirical-Bayes shrunk (k=56): row 0 has no prior -> null (no-fabricate);
        # later rows are interior, regularized toward the pooled mean.
        vals = result["svc_first_serve_win_pct"].to_list()
        assert vals[0] is None
        assert all(v is not None and 0.0 < v < 1.0 for v in vals[1:])

    def test_respects_window(self):
        """svc_first_serve_win_pct only includes data within window period."""
        from mvp.model.features.serve import svc_first_serve_win_pct

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
            svc_first_serve_win_pct(days=7).alias("svc_first_serve_win_pct")
        ).collect()

        # Row 0: no prior matches -> null
        # Row 1: day 1 is 9 days before, outside 7-day window -> null
        # Row 2: day 10 is 10 days before, outside 7-day window -> null
        assert result["svc_first_serve_win_pct"][0] is None
        assert result["svc_first_serve_win_pct"][1] is None
        assert result["svc_first_serve_win_pct"][2] is None


class TestSvcAcePctFeature:
    """Tests for svc_ace_pct feature (formula fix: uses first_serve_att)."""

    def test_registered(self):
        """svc_ace_pct is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("svc_ace_pct")
        assert feat.name == "svc_ace_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_uses_first_serve_att_denominator(self):
        """svc_ace_pct uses first serve attempts as denominator, not pts_played."""
        from mvp.model.features.serve import svc_ace_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 5)],
                "svc_aces": [10, 8],
                "svc_first_serve_att": [100, 80],  # Correct denominator
            }
        ).lazy()

        result = df.with_columns(svc_ace_pct(days=30).alias("svc_ace_pct")).collect()

        # Row 0: no prior matches -> null
        # Row 1: 10/100 = 0.10
        assert result["svc_ace_pct"][0] is None
        assert abs(result["svc_ace_pct"][1] - 0.10) < 0.001


class TestSvcFirstServeInPctFeature:
    """Tests for svc_first_serve_in_pct feature (new from audit)."""

    def test_registered(self):
        """svc_first_serve_in_pct is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("svc_first_serve_in_pct")
        assert feat.name == "svc_first_serve_in_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_computes_first_serve_in_percentage(self):
        """svc_first_serve_in_pct computes first serves in / attempts."""
        from mvp.model.features.serve import svc_first_serve_in_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 5)],
                "svc_first_serve_in": [60, 70],
                "svc_first_serve_att": [100, 100],
            }
        ).lazy()

        result = df.with_columns(
            svc_first_serve_in_pct(days=30).alias("svc_first_serve_in_pct")
        ).collect()

        # Shrunk (k=95): row 0 has no prior -> null; row 1 interior in (0,1).
        vals = result["svc_first_serve_in_pct"].to_list()
        assert vals[0] is None
        assert vals[1] is not None and 0.0 < vals[1] < 1.0


class TestSvcRatingFeature:
    """Tests for svc_rating feature (ATP serve rating average)."""

    def test_registered(self):
        """svc_rating is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("svc_rating")
        assert feat.name == "svc_rating"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_computes_average_rating(self):
        """svc_rating computes average of ATP serve rating."""
        from mvp.model.features.serve import svc_rating

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 5),
                    date(2024, 1, 10),
                ],
                "svc_serve_rating": [200.0, 250.0, 300.0],
            }
        ).lazy()

        result = df.with_columns(svc_rating(days=30).alias("svc_rating")).collect()

        # Row 0: no prior matches -> null
        # Row 1: 200.0 (only prior match)
        # Row 2: (200 + 250) / 2 = 225.0
        assert result["svc_rating"][0] is None
        assert abs(result["svc_rating"][1] - 200.0) < 0.001
        assert abs(result["svc_rating"][2] - 225.0) < 0.001


class TestHoldPctFeature:
    """Tests for hold_pct feature."""

    def test_registered(self):
        registry = get_registry()
        feat = registry.get("hold_pct")
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_computes_rolling_hold_percentage(self):
        from mvp.model.features.serve import hold_pct

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 10),
            ],
            # Match 1: 10 svc games, faced 3 BP saved 1 -> 2 breaks -> 8 holds -> 80%
            # Match 2: 12 svc games, faced 0 BP saved 0 -> 0 breaks -> 12 holds -> 100%
            # Match 3: 11 svc games, faced 5 BP saved 3 -> 2 breaks -> 9 holds -> 81.8%
            "svc_games_played": [10, 12, 11],
            "svc_bp_faced": [3, 0, 5],
            "svc_bp_saved": [1, 0, 3],
        }).lazy()

        result = df.with_columns(hold_pct(days=365).alias("val")).collect()
        # Shrunk (k=12): row 0 no prior -> null; later rows interior in (0,1).
        vals = result["val"].to_list()
        assert vals[0] is None
        assert all(v is not None and 0.0 < v < 1.0 for v in vals[1:])

    def test_diff_and_sum_registered(self):
        registry = get_registry()
        registry.get("hold_pct_diff")
        registry.get("hold_pct_sum")


class TestSvcDiffFeatures:
    """Tests for serve diff features."""

    def test_diff_features_registered(self):
        """All serve diff features are registered."""
        registry = get_registry()
        diff_features = [
            "svc_first_serve_win_pct_diff",
            "svc_second_serve_win_pct_diff",
            "svc_ace_pct_diff",
            "svc_df_pct_diff",
            "svc_bp_save_pct_diff",
            "svc_first_serve_in_pct_diff",
            "svc_rating_diff",
        ]
        for name in diff_features:
            feat = registry.get(name)
            assert feat.name == name
            assert feat.mirror is False
            assert len(feat.depends_on) > 0

    def test_diff_computes_player_minus_opp(self):
        """Diff feature computes player stat minus opponent stat."""
        svc_first_serve_win_pct_diff = get_registry().get("svc_first_serve_win_pct_diff").func

        df = pl.DataFrame(
            {
                "player_svc_first_serve_win_pct": [0.70, 0.65],
                "opp_svc_first_serve_win_pct": [0.60, 0.70],
            }
        ).lazy()

        result = df.with_columns(
            svc_first_serve_win_pct_diff().alias("diff")
        ).collect()

        assert abs(result["diff"][0] - 0.10) < 0.001
        assert abs(result["diff"][1] - (-0.05)) < 0.001
