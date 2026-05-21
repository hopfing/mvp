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
        "player_glicko_hard_rd": [80.0],
        "player_glicko_clay_rd": [150.0],
        "player_glicko_grass_rd": [200.0],
        "opp_glicko_mu": [1500.0],
        "opp_glicko_rd": [120.0],
        "opp_glicko_sigma": [0.06],
        "opp_glicko_hard_rd": [90.0],
        "opp_glicko_clay_rd": [100.0],
        "opp_glicko_grass_rd": [250.0],
        "surface": [surface],
    })


class TestGlickoDiff:
    def test_basic_diff(self):
        from mvp.model.features.glicko import glicko_diff
        df = _base_df()
        result = df.select(glicko_diff().alias("val"))
        assert result["val"][0] == pytest.approx(100.0)


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


class TestGlickoDiffAbs:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_diff_abs
        df = _base_df()
        result = df.select(glicko_diff_abs().alias("val"))
        assert result["val"][0] == pytest.approx(100.0)

    def test_symmetric(self):
        """Abs diff is the same regardless of who is stronger."""
        from mvp.model.features.glicko import glicko_diff_abs
        df = pl.DataFrame({
            "player_glicko_mu": [1400.0], "opp_glicko_mu": [1600.0],
            **{c: [0.0] for c in [
                "player_glicko_rd", "opp_glicko_rd",
                "player_glicko_sigma", "opp_glicko_sigma",
            ]},
            "surface": ["Hard"],
        })
        result = df.select(glicko_diff_abs().alias("val"))
        assert result["val"][0] == pytest.approx(200.0)


class TestGlickoDiffSq:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_diff_sq
        df = _base_df()
        result = df.select(glicko_diff_sq().alias("val"))
        # diff = 100 → 100² = 10000
        assert result["val"][0] == pytest.approx(10000.0)


class TestGlickoDiffXRdSum:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_diff_x_rd_sum
        df = _base_df("Hard")
        result = df.select(glicko_diff_x_rd_sum().alias("val"))
        # base diff = 100, rd_sum = 220 => 100 * 220 = 22000
        assert result["val"][0] == pytest.approx(22000.0)


class TestGlickoMuRaw:
    def test_expr(self):
        from mvp.model.features.glicko import glicko_mu
        df = _base_df()
        result = df.select(glicko_mu().alias("val"))
        assert result["val"][0] == pytest.approx(1600.0)

    def test_registered(self):
        registry = get_registry()
        feat = registry.get("glicko_mu")
        assert feat is not None
        assert feat.mirror is True


class TestGlickoRdXMatchCount:
    def test_alltime(self):
        from mvp.model.features.glicko import glicko_rd_x_match_count
        df = pl.DataFrame({
            "player_glicko_rd": [100.0],
            "opp_glicko_rd": [120.0],
            "player_match_count": [20.0],
            "opp_match_count": [15.0],
        })
        result = df.select(glicko_rd_x_match_count().alias("val"))
        # (100 + 120) * (20 + 15) = 220 * 35 = 7700
        assert result["val"][0] == pytest.approx(7700.0)

    def test_windowed(self):
        from mvp.model.features.glicko import glicko_rd_x_match_count
        df = pl.DataFrame({
            "player_glicko_rd": [100.0],
            "opp_glicko_rd": [120.0],
            "player_match_count_90d": [5.0],
            "opp_match_count_90d": [3.0],
        })
        result = df.select(glicko_rd_x_match_count(days=90).alias("val"))
        # (100 + 120) * (5 + 3) = 220 * 8 = 1760
        assert result["val"][0] == pytest.approx(1760.0)


class TestGlickoRdXDaysSinceLastMatch:
    def test_basic(self):
        from mvp.model.features.glicko import glicko_rd_x_days_since_last_match
        df = pl.DataFrame({
            "player_glicko_rd": [100.0],
            "opp_glicko_rd": [120.0],
            "player_days_since_last_match": [10.0],
            "opp_days_since_last_match": [5.0],
        })
        result = df.select(glicko_rd_x_days_since_last_match().alias("val"))
        # (100 + 120) * (10 + 5) = 220 * 15 = 3300
        assert result["val"][0] == pytest.approx(3300.0)


class TestGlickoFeaturesRegistered:
    def test_all_features_in_registry(self):
        registry = get_registry()
        expected = [
            "glicko_mu",
            "glicko_diff", "glicko_diff_abs", "glicko_diff_sq",
            "glicko_rd_sum", "glicko_rd_diff", "glicko_sigma_diff",
            "glicko_surface_rd_sum", "glicko_diff_x_rd_sum",
            "glicko_rd_x_match_count", "glicko_rd_x_days_since_last_match",
        ]
        registered = registry.list_features()
        for name in expected:
            assert name in registered, f"Feature '{name}' not registered"
