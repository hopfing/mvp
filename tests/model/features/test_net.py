"""Tests for net-play surface-conditioned features (Tier B)."""

from datetime import date

import polars as pl

from mvp.model.features import net as net_module  # noqa: F401
from mvp.model.registry import get_registry


def test_surface_net_features_registered():
    reg = get_registry()
    for name in ["surface_net_points_won", "surface_net_points_lost"]:
        feat = reg.get(name)
        assert feat.params == ["days"]
        assert feat.mirror is True
        assert feat.impute is None
        reg.get(f"{name}_diff")


def test_surface_net_points_won_within_surface():
    fn = get_registry().get("surface_net_points_won").func
    df = pl.DataFrame({
        "player_id": ["P", "P", "P"],
        "surface": ["Hard", "Clay", "Hard"],
        "effective_match_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
        "player_sp_net_points_won": [10.0, 30.0, 20.0],
    }).sort("effective_match_date")
    vals = df.with_columns(fn(days=365).alias("v"))["v"].to_list()
    assert vals[0] is None             # first Hard: no prior
    assert vals[1] is None             # first Clay: no prior
    assert abs(vals[2] - 10.0) < 1e-9  # prior Hard {10}; clay 30 excluded


def test_surface_net_points_won_pct_registered():
    reg = get_registry()
    feat = reg.get("surface_net_points_won_pct")
    assert feat.params == ["days"]
    assert feat.mirror is True
    assert feat.impute is None
    reg.get("surface_net_points_won_pct_diff")
