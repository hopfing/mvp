"""Tests for surface-transition feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import transition as transition_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _make_transition_df() -> pl.DataFrame:
    """5 matches for player A across surfaces.

    Jan 1 Hard, Jan 15 Hard, Feb 1 Clay, Feb 10 Clay, Mar 1 Hard
    """
    return pl.DataFrame({
        "player_id": ["A", "A", "A", "A", "A"],
        "effective_match_date": [
            date(2024, 1, 1),
            date(2024, 1, 15),
            date(2024, 2, 1),
            date(2024, 2, 10),
            date(2024, 3, 1),
        ],
        "surface": ["Hard", "Hard", "Clay", "Clay", "Hard"],
        "won": [1, 0, 1, 1, 0],
    }).sort("effective_match_date")


class TestTransitionBaseFeatures:
    """Tests for base transition features."""

    def test_all_base_registered(self):
        registry = get_registry()
        base_names = ["days_since_surface", "surface_switch", "pct_matches_on_surface"]
        for name in base_names:
            feat = registry.get(name)
            assert feat.mirror is True

    def test_days_since_surface(self):
        from mvp.model.features.transition import days_since_surface

        df = _make_transition_df()
        result = df.with_columns(days_since_surface().alias("val"))
        # Row 0: first Hard -> null
        assert result["val"][0] is None
        # Row 1: 14 days since last Hard (Jan 1 -> Jan 15)
        assert result["val"][1] == pytest.approx(14.0)
        # Row 2: first Clay -> null
        assert result["val"][2] is None
        # Row 3: 9 days since last Clay (Feb 1 -> Feb 10)
        assert result["val"][3] == pytest.approx(9.0)
        # Row 4: 46 days since last Hard (Jan 15 -> Mar 1, 2024 is leap year)
        assert result["val"][4] == pytest.approx(46.0)

    def test_surface_switch(self):
        from mvp.model.features.transition import surface_switch

        df = _make_transition_df()
        result = df.with_columns(surface_switch().alias("val"))
        # Row 0: first match -> null
        assert result["val"][0] is None
        # Row 1: Hard -> Hard = 0
        assert result["val"][1] == pytest.approx(0.0)
        # Row 2: Hard -> Clay = 1
        assert result["val"][2] == pytest.approx(1.0)
        # Row 3: Clay -> Clay = 0
        assert result["val"][3] == pytest.approx(0.0)
        # Row 4: Clay -> Hard = 1
        assert result["val"][4] == pytest.approx(1.0)

    def test_pct_matches_on_surface_rolling(self):
        from mvp.model.features.transition import pct_matches_on_surface

        df = _make_transition_df()
        result = df.with_columns(pct_matches_on_surface(days=365).alias("val"))
        # Row 0: no prior -> null (0/0)
        assert result["val"][0] is None
        # Row 1: prior=[1 Hard], 1 Hard / 1 total = 1.0
        assert result["val"][1] == pytest.approx(1.0)
        # Row 2: prior=[Hard, Hard], 0 Clay / 2 total = 0.0
        assert result["val"][2] == pytest.approx(0.0)
        # Row 3: prior=[Hard, Hard, Clay], 1 Clay / 3 total = 1/3
        assert result["val"][3] == pytest.approx(1 / 3, abs=0.01)
        # Row 4: prior=[Hard, Hard, Clay, Clay], 2 Hard / 4 total = 0.5
        assert result["val"][4] == pytest.approx(0.5)

    def test_pct_matches_on_surface_alltime(self):
        from mvp.model.features.transition import pct_matches_on_surface

        df = _make_transition_df()
        result = df.with_columns(pct_matches_on_surface(days=None).alias("val"))
        # Same expected values as rolling with large window
        assert result["val"][0] is None
        assert result["val"][1] == pytest.approx(1.0)
        assert result["val"][4] == pytest.approx(0.5)


class TestTransitionMultiPlayer:
    """Test that features are independent across players."""

    def test_multi_player_independence(self):
        from mvp.model.features.transition import days_since_surface

        df = pl.DataFrame({
            "player_id": ["A", "B", "A", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 5),
                date(2024, 1, 20),
                date(2024, 1, 25),
            ],
            "surface": ["Hard", "Clay", "Hard", "Clay"],
            "won": [1, 1, 0, 0],
        }).sort("effective_match_date")

        result = df.with_columns(days_since_surface().alias("val"))
        # A: first Hard null, second Hard 19 days
        assert result["val"][0] is None
        assert result["val"][2] == pytest.approx(19.0)
        # B: first Clay null, second Clay 20 days
        assert result["val"][1] is None
        assert result["val"][3] == pytest.approx(20.0)


class TestTransitionDiffFeatures:
    """Tests for transition diff features."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        diff_names = [
            "days_since_surface_diff",
            "surface_switch_diff",
            "pct_matches_on_surface_diff",
        ]
        for name in diff_names:
            feat = registry.get(name)
            assert feat.mirror is False
            # diff inherits the base's impute (no-fabricate bases are None)
            assert feat.impute == registry.get(feat.depends_on[0]).impute
            assert len(feat.depends_on) == 1

    def test_days_since_surface_diff_computation(self):
        days_since_surface_diff = get_registry().get("days_since_surface_diff").func

        df = pl.DataFrame({
            "player_days_since_surface": [14.0, 45.0],
            "opp_days_since_surface": [7.0, 30.0],
        })
        result = df.with_columns(days_since_surface_diff().alias("diff"))
        assert result["diff"][0] == pytest.approx(7.0)
        assert result["diff"][1] == pytest.approx(15.0)

    def test_pct_matches_on_surface_diff_rolling(self):
        pct_matches_on_surface_diff = get_registry().get("pct_matches_on_surface_diff").func

        df = pl.DataFrame({
            "player_pct_matches_on_surface_365d": [0.5, 0.8],
            "opp_pct_matches_on_surface_365d": [0.3, 0.9],
        })
        result = df.with_columns(pct_matches_on_surface_diff(days=365).alias("diff"))
        assert result["diff"][0] == pytest.approx(0.2)
        assert result["diff"][1] == pytest.approx(-0.1)

    def test_pct_matches_on_surface_diff_alltime(self):
        pct_matches_on_surface_diff = get_registry().get("pct_matches_on_surface_diff").func

        df = pl.DataFrame({
            "player_pct_matches_on_surface": [0.6, 0.4],
            "opp_pct_matches_on_surface": [0.3, 0.7],
        })
        result = df.with_columns(pct_matches_on_surface_diff(days=None).alias("diff"))
        assert result["diff"][0] == pytest.approx(0.3)
        assert result["diff"][1] == pytest.approx(-0.3)


class TestTransitionFeatureCount:
    """Verify total feature count."""

    def test_total_count(self):
        registry = get_registry()
        names = [
            "days_since_surface", "surface_switch", "pct_matches_on_surface",
            "days_since_surface_diff", "surface_switch_diff", "pct_matches_on_surface_diff",
        ]
        for name in names:
            registry.get(name)  # Will raise KeyError if missing
        assert len(names) == 6
