"""Tests for Elo feature module."""

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.model.features import elo as elo_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


class TestSurfaceEloExpr:
    """Test the surface_elo_expr helper."""

    def _base_df(self, surface: str) -> pl.DataFrame:
        return pl.DataFrame({
            "player_elo": [1500.0],
            "player_hard_adj": [30.0],
            "player_clay_adj": [20.0],
            "player_grass_adj": [10.0],
            "opp_elo": [1400.0],
            "opp_hard_adj": [15.0],
            "opp_clay_adj": [25.0],
            "opp_grass_adj": [5.0],
            "surface": [surface],
        })

    def test_hard_surface_player(self):
        from mvp.model.features.elo import surface_elo_expr

        df = self._base_df("Hard")
        result = df.select(surface_elo_expr("player").alias("val"))
        assert result["val"].to_list() == [1530.0]

    def test_hard_surface_opp(self):
        from mvp.model.features.elo import surface_elo_expr

        df = self._base_df("Hard")
        result = df.select(surface_elo_expr("opp").alias("val"))
        assert result["val"].to_list() == [1415.0]

    def test_clay_surface(self):
        from mvp.model.features.elo import surface_elo_expr

        df = self._base_df("Clay")
        result = df.select(surface_elo_expr("player").alias("val"))
        assert result["val"].to_list() == [1520.0]

    def test_grass_surface(self):
        from mvp.model.features.elo import surface_elo_expr

        df = self._base_df("Grass")
        result = df.select(surface_elo_expr("player").alias("val"))
        assert result["val"].to_list() == [1510.0]

    def test_unknown_surface(self):
        from mvp.model.features.elo import surface_elo_expr

        df = self._base_df("Carpet")
        result = df.select(surface_elo_expr("player").alias("val"))
        # Unknown surface gets no adjustment (otherwise 0.0)
        assert result["val"].to_list() == [1500.0]

    def test_elo_surface_diff_uses_helper(self):
        """elo_surface_diff should produce the same result as manual helper diff."""
        from mvp.model.features.elo import elo_surface_diff, surface_elo_expr

        df = self._base_df("Clay")
        diff_result = df.select(elo_surface_diff().alias("diff"))
        manual_result = df.select(
            (surface_elo_expr("player") - surface_elo_expr("opp")).alias("diff")
        )
        assert diff_result["diff"].to_list() == manual_result["diff"].to_list()


class TestAbsoluteLevelFeatures:
    """Test absolute Elo level features."""

    def _base_df(self) -> pl.DataFrame:
        return pl.DataFrame({
            "player_elo": [1500.0, 1800.0],
            "opp_elo": [1400.0, 1600.0],
            "player_elo_rd": [80.0, 60.0],
            "opp_elo_rd": [100.0, 70.0],
            "player_hard_adj": [30.0, 20.0],
            "player_clay_adj": [20.0, 10.0],
            "player_grass_adj": [10.0, 5.0],
            "opp_hard_adj": [15.0, 10.0],
            "opp_clay_adj": [25.0, 15.0],
            "opp_grass_adj": [5.0, 0.0],
            "surface": ["Hard", "Clay"],
        })

    def test_elo_avg(self):
        from mvp.model.features.elo import elo_avg

        df = self._base_df()
        result = df.select(elo_avg().alias("val"))
        assert result["val"].to_list() == [1450.0, 1700.0]

    def test_elo_min(self):
        from mvp.model.features.elo import elo_min

        df = self._base_df()
        result = df.select(elo_min().alias("val"))
        assert result["val"].to_list() == [1400.0, 1600.0]

    def test_elo_diff_x_elo_avg(self):
        from mvp.model.features.elo import elo_diff_x_elo_avg

        df = self._base_df()
        result = df.select(elo_diff_x_elo_avg().alias("val"))
        # Row 0: surface_diff = (1500+30) - (1400+15) = 115, avg = 1450 → 115 * 1450 = 166750
        # Row 1: surface_diff = (1800+10) - (1600+15) = 195, avg = 1700 → 195 * 1700 = 331500
        assert result["val"].to_list() == [pytest.approx(166750.0), pytest.approx(331500.0)]

    def test_elo_avg_sq(self):
        from mvp.model.features.elo import elo_avg_sq

        df = self._base_df()
        result = df.select(elo_avg_sq().alias("val"))
        # Row 0: avg = 1450 → 1450² = 2102500
        # Row 1: avg = 1700 → 1700² = 2890000
        assert result["val"].to_list() == [pytest.approx(2102500.0), pytest.approx(2890000.0)]

    def test_elo_diff_x_rd_sum(self):
        from mvp.model.features.elo import elo_diff_x_rd_sum

        df = self._base_df()
        result = df.select(elo_diff_x_rd_sum().alias("val"))
        # Row 0: surface_diff = (1500+30) - (1400+15) = 115, rd_sum = 80+100 = 180 → 115 * 180 = 20700
        # Row 1: surface_diff = (1800+10) - (1600+15) = 195, rd_sum = 60+70 = 130 → 195 * 130 = 25350
        assert result["val"].to_list() == [pytest.approx(20700.0), pytest.approx(25350.0)]

    def test_elo_surface_diff_abs(self):
        from mvp.model.features.elo import elo_surface_diff_abs

        df = self._base_df()
        result = df.select(elo_surface_diff_abs().alias("val"))
        # Row 0: |(1500+30) - (1400+15)| = 115
        # Row 1: |(1800+10) - (1600+15)| = 195
        assert result["val"].to_list() == [pytest.approx(115.0), pytest.approx(195.0)]

    def test_elo_surface_diff_sq(self):
        from mvp.model.features.elo import elo_surface_diff_sq

        df = self._base_df()
        result = df.select(elo_surface_diff_sq().alias("val"))
        # Row 0: 115² = 13225
        # Row 1: 195² = 38025
        assert result["val"].to_list() == [pytest.approx(13225.0), pytest.approx(38025.0)]

    def test_all_registered(self):
        registry = get_registry()
        for name in [
            "elo_avg", "elo_avg_sq", "elo_min", "elo_diff_x_elo_avg",
            "elo_diff_x_rd_sum", "elo_surface_diff_abs", "elo_surface_diff_sq",
        ]:
            feat = registry.get(name)
            assert feat is not None, f"Feature {name} not registered"
            assert feat.mirror is False


class TestStyleDimensionFeatures:
    """Test style dimension derived features."""

    def test_svc_first_serve_power_diff_registered(self):
        """svc_first_serve_power_diff is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("svc_first_serve_power_diff")
        assert feat.name == "svc_first_serve_power_diff"
        assert feat.mirror is False

    def test_svc_first_serve_power_diff(self):
        from mvp.model.features.elo import svc_first_serve_power_diff

        df = pl.DataFrame({
            "player_first_serve_power": [1600.0, 1500.0],
            "opp_first_serve_power": [1500.0, 1550.0],
        })
        result = df.select(svc_first_serve_power_diff().alias("diff"))
        assert result["diff"].to_list() == [100.0, -50.0]

    def test_svc_clutch_diff(self):
        from mvp.model.features.elo import svc_clutch_diff

        df = pl.DataFrame({
            "player_serve_clutch": [1550.0, 1400.0],
            "opp_serve_clutch": [1600.0, 1400.0],
        })
        result = df.select(svc_clutch_diff().alias("diff"))
        assert result["diff"].to_list() == [-50.0, 0.0]

    def test_elo_clutch_diff(self):
        from mvp.model.features.elo import elo_clutch_diff

        df = pl.DataFrame({
            "player_overall_clutch": [1580.0, 1520.0],
            "opp_overall_clutch": [1520.0, 1580.0],
        })
        result = df.select(elo_clutch_diff().alias("diff"))
        assert result["diff"].to_list() == [60.0, -60.0]

    def test_elo_indoor_adj_diff(self):
        from mvp.model.features.elo import elo_indoor_adj_diff

        df = pl.DataFrame({
            "player_indoor_adj": [25.0, -10.0],
            "opp_indoor_adj": [10.0, 15.0],
        })
        result = df.select(elo_indoor_adj_diff().alias("diff"))
        assert result["diff"].to_list() == [15.0, -25.0]

    def test_all_style_features_registered(self):
        """All style dimension features are registered."""
        registry = get_registry()
        expected = [
            "svc_first_serve_power_diff",
            "svc_second_serve_reliability_diff",
            "ret_ace_resistance_diff",
            "svc_clutch_diff",
            "ret_clutch_diff",
            "elo_tb_clutch_diff",
            "elo_clutch_diff",
            "elo_indoor_adj_diff",
        ]
        for name in expected:
            feat = registry.get(name)
            assert feat is not None, f"Feature {name} not registered"
            assert feat.mirror is False

    def test_matchup_features_registered(self):
        """All matchup features are registered."""
        registry = get_registry()
        expected = [
            "svc_first_serve_power_matchup",
            "svc_clutch_matchup",
            "ret_clutch_matchup",
        ]
        for name in expected:
            feat = registry.get(name)
            assert feat is not None, f"Feature {name} not registered"
            assert feat.mirror is False

    def test_svc_clutch_matchup(self):
        from mvp.model.features.elo import svc_clutch_matchup

        df = pl.DataFrame({
            "player_serve_clutch": [1600.0, 1500.0],
            "opp_return_clutch": [1550.0, 1550.0],
        })
        result = df.select(svc_clutch_matchup().alias("matchup"))
        assert result["matchup"].to_list() == [50.0, -50.0]

    def test_ret_clutch_matchup(self):
        from mvp.model.features.elo import ret_clutch_matchup

        df = pl.DataFrame({
            "player_return_clutch": [1550.0, 1450.0],
            "opp_serve_clutch": [1500.0, 1500.0],
        })
        result = df.select(ret_clutch_matchup().alias("matchup"))
        assert result["matchup"].to_list() == [50.0, -50.0]


class TestRawEloFeatures:
    """Test raw Elo column passthrough features."""

    def test_elo_raw_registered(self):
        registry = get_registry()
        for name in ["elo", "elo_surface", "serve_elo", "return_elo"]:
            feat = registry.get(name)
            assert feat is not None, f"Feature {name} not registered"
            assert feat.mirror is True

    def test_elo_expr(self):
        from mvp.model.features.elo import elo

        df = pl.DataFrame({"player_elo": [1500.0, 1800.0]})
        result = df.select(elo().alias("val"))
        assert result["val"].to_list() == [1500.0, 1800.0]

    def test_elo_surface_expr(self):
        from mvp.model.features.elo import elo_surface

        df = pl.DataFrame({
            "player_elo": [1500.0],
            "player_hard_adj": [30.0],
            "player_clay_adj": [20.0],
            "player_grass_adj": [10.0],
            "surface": ["Hard"],
        })
        result = df.select(elo_surface().alias("val"))
        assert result["val"].to_list() == [1530.0]

    def test_serve_elo_expr(self):
        from mvp.model.features.elo import serve_elo

        df = pl.DataFrame({"player_serve_elo": [1600.0, 1700.0]})
        result = df.select(serve_elo().alias("val"))
        assert result["val"].to_list() == [1600.0, 1700.0]

    def test_return_elo_expr(self):
        from mvp.model.features.elo import return_elo

        df = pl.DataFrame({"player_return_elo": [1550.0, 1650.0]})
        result = df.select(return_elo().alias("val"))
        assert result["val"].to_list() == [1550.0, 1650.0]
