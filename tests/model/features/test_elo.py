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


class TestStyleDimensionFeatures:
    """Test style dimension derived features."""

    def test_first_serve_power_diff_registered(self):
        """first_serve_power_diff is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("first_serve_power_diff")
        assert feat.name == "first_serve_power_diff"
        assert feat.mirror is False

    def test_first_serve_power_diff(self):
        from mvp.model.features.elo import first_serve_power_diff

        df = pl.DataFrame({
            "player_first_serve_power": [1600.0, 1500.0],
            "opp_first_serve_power": [1500.0, 1550.0],
        })
        result = df.select(first_serve_power_diff().alias("diff"))
        assert result["diff"].to_list() == [100.0, -50.0]

    def test_serve_clutch_diff(self):
        from mvp.model.features.elo import serve_clutch_diff

        df = pl.DataFrame({
            "player_serve_clutch": [1550.0, 1400.0],
            "opp_serve_clutch": [1600.0, 1400.0],
        })
        result = df.select(serve_clutch_diff().alias("diff"))
        assert result["diff"].to_list() == [-50.0, 0.0]

    def test_overall_clutch_diff(self):
        from mvp.model.features.elo import overall_clutch_diff

        df = pl.DataFrame({
            "player_overall_clutch": [1580.0, 1520.0],
            "opp_overall_clutch": [1520.0, 1580.0],
        })
        result = df.select(overall_clutch_diff().alias("diff"))
        assert result["diff"].to_list() == [60.0, -60.0]

    def test_indoor_adj_diff(self):
        from mvp.model.features.elo import indoor_adj_diff

        df = pl.DataFrame({
            "player_indoor_adj": [25.0, -10.0],
            "opp_indoor_adj": [10.0, 15.0],
        })
        result = df.select(indoor_adj_diff().alias("diff"))
        assert result["diff"].to_list() == [15.0, -25.0]

    def test_all_style_features_registered(self):
        """All style dimension features are registered."""
        registry = get_registry()
        expected = [
            "first_serve_power_diff",
            "second_serve_reliability_diff",
            "ace_resistance_diff",
            "serve_clutch_diff",
            "return_clutch_diff",
            "tb_clutch_diff",
            "overall_clutch_diff",
            "indoor_adj_diff",
        ]
        for name in expected:
            feat = registry.get(name)
            assert feat is not None, f"Feature {name} not registered"
            assert feat.mirror is False
