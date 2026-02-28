"""Tests for h2h feature module."""

from datetime import date

import polars as pl
import pytest

# Import the module to ensure features are registered
from mvp.model.features import h2h as h2h_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    """Ensure features are registered for all tests in this module."""
    yield


class TestH2HWinsFeature:
    """Tests for h2h_wins feature."""

    def test_h2h_wins_registered(self):
        """h2h_wins is registered with correct metadata."""
        registry = get_registry()
        feat = registry.get("h2h_wins")
        assert feat.name == "h2h_wins"
        assert feat.params == []
        assert feat.mirror is True

    def test_h2h_wins_computes_cumulative_sum(self):
        """h2h_wins computes cumulative sum of wins against specific opponent."""
        from mvp.model.features.h2h import h2h_wins

        # Player A plays opponent B three times, winning 1st and 3rd
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "opp_id": ["B", "B", "B"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 10),
                    date(2024, 1, 20),
                ],
                "won": [1, 0, 1],
            }
        ).lazy()

        result = df.with_columns(h2h_wins().alias("h2h_wins")).collect()

        # Row 0: no prior matches vs B -> 0
        # Row 1: 1 prior match vs B (won=1) -> 1
        # Row 2: 2 prior matches vs B (won=1,0) -> 1
        assert result["h2h_wins"].to_list() == [0, 1, 1]

    def test_h2h_wins_groups_by_matchup(self):
        """h2h_wins is computed per player-opponent pair."""
        from mvp.model.features.h2h import h2h_wins

        # Player A plays B twice, then C once
        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "opp_id": ["B", "B", "C"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 10),
                    date(2024, 1, 20),
                ],
                "won": [1, 1, 1],
            }
        ).lazy()

        result = df.with_columns(h2h_wins().alias("h2h_wins")).collect()

        # Row 0 (A vs B): no prior -> 0
        # Row 1 (A vs B): 1 prior A vs B match (won=1) -> 1
        # Row 2 (A vs C): no prior A vs C matches -> 0
        assert result["h2h_wins"].to_list() == [0, 1, 0]
