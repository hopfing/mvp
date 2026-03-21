"""Tests for context feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import context as context_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


class TestTourMatchPctFeature:
    """Tests for tour_match_pct feature."""

    def test_registered(self):
        registry = get_registry()
        feat = registry.get("tour_match_pct")
        assert feat.name == "tour_match_pct"
        assert feat.params == ["days"]
        assert feat.mirror is True

    def test_rolling_365d(self):
        from mvp.model.features.context import tour_match_pct

        df = pl.DataFrame({
            "player_id": ["A"] * 5,
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
                date(2024, 4, 1),
                date(2024, 5, 1),
            ],
            "circuit": ["tour", "chal", "tour", "chal", "tour"],
        }).sort("effective_match_date")

        result = df.with_columns(tour_match_pct(days=365).alias("pct"))

        # Row 0: no prior -> null
        assert result["pct"][0] is None
        # Row 1: 1 prior (tour) -> 1.0
        assert result["pct"][1] == pytest.approx(1.0)
        # Row 2: 2 prior (tour, chal) -> 0.5
        assert result["pct"][2] == pytest.approx(0.5)
        # Row 3: 3 prior (tour, chal, tour) -> 2/3
        assert result["pct"][3] == pytest.approx(2 / 3, abs=0.001)
        # Row 4: 4 prior (tour, chal, tour, chal) -> 0.5
        assert result["pct"][4] == pytest.approx(0.5)

    def test_all_tour(self):
        from mvp.model.features.context import tour_match_pct

        df = pl.DataFrame({
            "player_id": ["A"] * 3,
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
            ],
            "circuit": ["tour", "tour", "tour"],
        }).sort("effective_match_date")

        result = df.with_columns(tour_match_pct(days=365).alias("pct"))
        assert result["pct"][1] == pytest.approx(1.0)
        assert result["pct"][2] == pytest.approx(1.0)

    def test_all_chal(self):
        from mvp.model.features.context import tour_match_pct

        df = pl.DataFrame({
            "player_id": ["A"] * 3,
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
            ],
            "circuit": ["chal", "chal", "chal"],
        }).sort("effective_match_date")

        result = df.with_columns(tour_match_pct(days=365).alias("pct"))
        assert result["pct"][1] == pytest.approx(0.0)
        assert result["pct"][2] == pytest.approx(0.0)

    def test_window_expiry(self):
        """Matches older than the window are excluded."""
        from mvp.model.features.context import tour_match_pct

        df = pl.DataFrame({
            "player_id": ["A"] * 3,
            "effective_match_date": [
                date(2023, 1, 1),  # outside 365d window for row 2
                date(2023, 7, 1),  # inside 365d window for row 2 (chal)
                date(2024, 6, 1),  # only row 1 (chal) in window
            ],
            "circuit": ["tour", "chal", "tour"],
        }).sort("effective_match_date")

        result = df.with_columns(tour_match_pct(days=365).alias("pct"))
        # Row 2: only row 1 (chal, Jul 2023) is in 365d window from Jun 2024
        # Row 0 (tour, Jan 2023) is >365 days before Jun 2024
        assert result["pct"][2] == pytest.approx(0.0)

    def test_multiple_players(self):
        """Each player's pct is independent."""
        from mvp.model.features.context import tour_match_pct

        df = pl.DataFrame({
            "player_id": ["A", "A", "B", "B"],
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 1, 1),
                date(2024, 2, 1),
            ],
            "circuit": ["tour", "tour", "chal", "chal"],
        }).sort("effective_match_date")

        result = df.with_columns(tour_match_pct(days=365).alias("pct"))
        a_rows = result.filter(pl.col("player_id") == "A")
        b_rows = result.filter(pl.col("player_id") == "B")
        assert a_rows["pct"][1] == pytest.approx(1.0)
        assert b_rows["pct"][1] == pytest.approx(0.0)

    def test_alltime(self):
        from mvp.model.features.context import tour_match_pct

        df = pl.DataFrame({
            "player_id": ["A"] * 4,
            "effective_match_date": [
                date(2020, 1, 1),
                date(2021, 1, 1),
                date(2022, 1, 1),
                date(2023, 1, 1),
            ],
            "circuit": ["tour", "chal", "tour", "chal"],
        }).sort("effective_match_date")

        result = df.with_columns(tour_match_pct(days=None).alias("pct"))
        # Row 0: no prior -> null
        assert result["pct"][0] is None
        # Row 3: 3 prior (tour, chal, tour) -> 2/3
        assert result["pct"][3] == pytest.approx(2 / 3, abs=0.001)


class TestTourMatchPctDiffFeature:
    """Tests for tour_match_pct_diff feature."""

    def test_registered(self):
        registry = get_registry()
        feat = registry.get("tour_match_pct_diff")
        assert feat.name == "tour_match_pct_diff"
        assert feat.depends_on == ["tour_match_pct"]
        assert feat.mirror is False

    def test_diff_computation(self):
        tour_match_pct_diff = get_registry().get("tour_match_pct_diff").func

        df = pl.DataFrame({
            "player_tour_match_pct_365d": [0.8, 0.3],
            "opp_tour_match_pct_365d": [0.2, 0.9],
        })
        result = df.with_columns(tour_match_pct_diff(days=365).alias("diff"))
        assert result["diff"][0] == pytest.approx(0.6)
        assert result["diff"][1] == pytest.approx(-0.6)
