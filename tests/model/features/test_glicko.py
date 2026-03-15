"""Tests for Glicko-2 feature module."""

import polars as pl
import pytest

from mvp.model.features import glicko as glicko_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _base_df(surface: str = "Hard") -> pl.DataFrame:
    return pl.DataFrame({
        "player_glicko_mu": [1600.0],
        "player_glicko_rd": [100.0],
        "player_glicko_sigma": [0.05],
        "player_glicko_hard_adj": [30.0],
        "player_glicko_hard_rd": [80.0],
        "player_glicko_hard_sigma": [0.04],
        "player_glicko_clay_adj": [-20.0],
        "player_glicko_clay_rd": [150.0],
        "player_glicko_clay_sigma": [0.06],
        "player_glicko_grass_adj": [10.0],
        "player_glicko_grass_rd": [200.0],
        "player_glicko_grass_sigma": [0.07],
        "opp_glicko_mu": [1500.0],
        "opp_glicko_rd": [120.0],
        "opp_glicko_sigma": [0.06],
        "opp_glicko_hard_adj": [-10.0],
        "opp_glicko_hard_rd": [90.0],
        "opp_glicko_hard_sigma": [0.05],
        "opp_glicko_clay_adj": [40.0],
        "opp_glicko_clay_rd": [100.0],
        "opp_glicko_clay_sigma": [0.04],
        "opp_glicko_grass_adj": [0.0],
        "opp_glicko_grass_rd": [250.0],
        "opp_glicko_grass_sigma": [0.08],
        "surface": [surface],
    })


class TestGlickoDiff:
    def test_basic_diff(self):
        from mvp.model.features.glicko import glicko_diff
        df = _base_df()
        result = df.select(glicko_diff().alias("val"))
        assert result["val"][0] == pytest.approx(100.0)


class TestGlickoSurfaceDiff:
    def test_hard_surface(self):
        from mvp.model.features.glicko import glicko_surface_diff
        df = _base_df("Hard")
        result = df.select(glicko_surface_diff().alias("val"))
        # (1600+30) - (1500-10) = 140
        assert result["val"][0] == pytest.approx(140.0)

    def test_clay_surface(self):
        from mvp.model.features.glicko import glicko_surface_diff
        df = _base_df("Clay")
        result = df.select(glicko_surface_diff().alias("val"))
        # (1600-20) - (1500+40) = 40
        assert result["val"][0] == pytest.approx(40.0)


class TestGlickoRdSum:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_rd_sum
        df = _base_df()
        result = df.select(glicko_rd_sum().alias("val"))
        assert result["val"][0] == pytest.approx(220.0)


class TestGlickoRdDiff:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_rd_diff
        df = _base_df()
        result = df.select(glicko_rd_diff().alias("val"))
        assert result["val"][0] == pytest.approx(-20.0)


class TestGlickoSigmaDiff:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_sigma_diff
        df = _base_df()
        result = df.select(glicko_sigma_diff().alias("val"))
        assert result["val"][0] == pytest.approx(-0.01)


class TestGlickoSurfaceRdSum:
    def test_hard(self):
        from mvp.model.features.glicko import glicko_surface_rd_sum
        df = _base_df("Hard")
        result = df.select(glicko_surface_rd_sum().alias("val"))
        assert result["val"][0] == pytest.approx(170.0)  # 80 + 90

    def test_carpet_falls_back_to_base(self):
        from mvp.model.features.glicko import glicko_surface_rd_sum
        df = _base_df("Carpet")
        result = df.select(glicko_surface_rd_sum().alias("val"))
        assert result["val"][0] == pytest.approx(220.0)  # 100 + 120


class TestGlickoDiffXRdSum:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_diff_x_rd_sum
        df = _base_df("Hard")
        result = df.select(glicko_diff_x_rd_sum().alias("val"))
        # surface diff = 140, rd_sum = 220 => 140 * 220 = 30800
        assert result["val"][0] == pytest.approx(30800.0)


class TestGlickoFeaturesRegistered:
    def test_all_features_in_registry(self):
        registry = get_registry()
        expected = [
            "glicko_diff", "glicko_surface_diff",
            "glicko_rd_sum", "glicko_rd_diff", "glicko_sigma_diff",
            "glicko_surface_rd_sum", "glicko_diff_x_rd_sum",
        ]
        registered = registry.list_features()
        for name in expected:
            assert name in registered, f"Feature '{name}' not registered"
