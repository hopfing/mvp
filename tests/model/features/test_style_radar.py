"""Tests for surface-conditioned style-radar raw axes (Tier B)."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import style_radar as style_radar_module  # noqa: F401
from mvp.model.registry import get_registry

_SURFACE_AXES = [
    "surface_style_ace_rate",
    "surface_style_net_rate",
    "surface_style_sp_winner_rate",
    "surface_style_sp_ue_rate",
    "surface_style_rally_lean",
]


def test_surface_axes_registered():
    reg = get_registry()
    for name in _SURFACE_AXES:
        feat = reg.get(name)
        assert feat.params == ["days"]
        assert feat.mirror is True
        assert feat.impute is None
        reg.get(f"{name}_diff")


def test_z_axes_not_surfaced():
    """The LOO z-axes need a surface-partitioned field population — deferred."""
    reg = get_registry()
    for name in ["surface_style_z_serve", "surface_style_z_net"]:
        with pytest.raises(KeyError):
            reg.get(name)


def test_ace_rate_within_surface_only():
    fn = get_registry().get("surface_style_ace_rate").func
    df = pl.DataFrame({
        "player_id": ["P", "P", "P"],
        "surface": ["Hard", "Clay", "Hard"],
        "effective_match_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
        "svc_aces": [5, 5, 5],
        "pts_service_pts_played": [100, 100, 100],
    }).sort("effective_match_date")
    vals = df.with_columns(fn(days=365).alias("v"))["v"].to_list()
    assert vals[0] is None       # first Hard: no prior
    assert vals[1] is None       # first Clay: no prior
    assert vals[2] is not None   # 2nd Hard: prior Hard exists (Clay excluded)
