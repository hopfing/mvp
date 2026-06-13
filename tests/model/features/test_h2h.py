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
        assert feat.params == ["days"]
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
                "match_uid": ["m1", "m2", "m3"],
                "round_order": [12, 12, 12],
                "tournament_start_date": date(2020, 1, 1),
            }
        ).lazy()

        result = df.with_columns(h2h_wins().alias("h2h_wins")).collect()

        # Row 0: no prior matches vs B -> None (NaN passthrough; "never played"
        # must be distinguished from "played and won 0")
        # Row 1: 1 prior match vs B (won=1) -> 1
        # Row 2: 2 prior matches vs B (won=1,0) -> 1
        assert result["h2h_wins"].to_list() == [None, 1, 1]

    def test_h2h_wins_zero_after_h2h_loss(self):
        """REAL 0 case: player on 2nd H2H match having lost the 1st must
        show h2h_wins = 0, not None. Regression guard for fill_with=None."""
        from mvp.model.features.h2h import h2h_wins

        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "opp_id": ["B", "B"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 10)],
                "won": [0, 1],
                "match_uid": ["m1", "m2"],
                "round_order": [12, 12],
                "tournament_start_date": date(2020, 1, 1),
            }
        ).lazy()

        result = df.with_columns(h2h_wins().alias("h2h_wins")).collect()
        assert result["h2h_wins"][0] is None  # no prior
        assert result["h2h_wins"][1] == 0     # 1 prior, lost it (REAL 0)

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
                "match_uid": ["m1", "m2", "m3"],
                "round_order": [12, 12, 12],
                "tournament_start_date": date(2020, 1, 1),
            }
        ).lazy()

        result = df.with_columns(h2h_wins().alias("h2h_wins")).collect()

        # Row 0 (A vs B): no prior -> None
        # Row 1 (A vs B): 1 prior A vs B match (won=1) -> 1
        # Row 2 (A vs C): no prior A vs C matches -> None (different pair)
        assert result["h2h_wins"].to_list() == [None, 1, None]


class TestH2HImputeContract:
    """All four h2h features must declare impute=None so the NaN they emit
    on first-encounter survives to a NaN-tolerant model. Counts and rates
    both flipped to passthrough — "never played them" should not collapse
    to 0 (or 0.5) and be indistinguishable from a low-sample real value."""

    def test_h2h_features_impute_none(self):
        registry = get_registry()
        for name in [
            "h2h_wins", "h2h_surface_wins", "h2h_win_pct", "h2h_surface_win_pct",
        ]:
            feat = registry.get(name)
            assert feat.impute is None, f"{name} expected impute=None, got {feat.impute}"


class TestH2HSurfaceWinsFeature:
    """Same NaN-passthrough contract as h2h_wins, partitioned by surface."""

    def test_surface_wins_first_encounter_is_none(self):
        from mvp.model.features.h2h import h2h_surface_wins

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "opp_id": ["B", "B", "B"],
                "surface": ["Hard", "Hard", "Clay"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 3, 1),
                ],
                "won": [1, 0, 1],
                "match_uid": ["m1", "m2", "m3"],
                "round_order": [12, 12, 12],
                "tournament_start_date": date(2020, 1, 1),
            }
        ).lazy()

        result = df.with_columns(h2h_surface_wins().alias("v")).collect()
        # Row 0: first Hard match vs B -> None
        # Row 1: 1 prior Hard match vs B (won) -> 1
        # Row 2: first Clay match vs B -> None (different surface partition)
        assert result["v"].to_list() == [None, 1, None]

    def test_surface_wins_zero_after_surface_loss(self):
        """REAL 0: 2nd same-surface H2H match after losing the 1st."""
        from mvp.model.features.h2h import h2h_surface_wins

        df = pl.DataFrame(
            {
                "player_id": ["A", "A"],
                "opp_id": ["B", "B"],
                "surface": ["Hard", "Hard"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 2, 1)],
                "won": [0, 1],
                "match_uid": ["m1", "m2"],
                "round_order": [12, 12],
                "tournament_start_date": date(2020, 1, 1),
            }
        ).lazy()

        result = df.with_columns(h2h_surface_wins().alias("v")).collect()
        assert result["v"][0] is None
        assert result["v"][1] == 0


class TestH2HWinPctFeature:
    """win_pct emits NaN via the engine's otherwise(None) branch when no
    prior matches exist; impute=None preserves it rather than filling 0.5."""

    def test_win_pct_first_encounter_is_none(self):
        from mvp.model.features.h2h import h2h_win_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "opp_id": ["B", "B", "B"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 3, 1),
                ],
                "won": [1, 0, 1],
                "match_uid": ["m1", "m2", "m3"],
                "round_order": [12, 12, 12],
                "tournament_start_date": date(2020, 1, 1),
            }
        ).lazy()

        result = df.with_columns(h2h_win_pct().alias("v")).collect()
        # Row 0: no prior -> None (not 0.5)
        # Row 1: 1 prior (1 win / 1 match) -> 1.0
        # Row 2: 2 prior (1 win / 2 matches) -> 0.5  (REAL 0.5, not the fill)
        assert result["v"][0] is None
        assert result["v"][1] == pytest.approx(1.0)
        assert result["v"][2] == pytest.approx(0.5)


class TestH2HSurfaceWinPctFeature:
    def test_surface_win_pct_first_same_surface_is_none(self):
        from mvp.model.features.h2h import h2h_surface_win_pct

        df = pl.DataFrame(
            {
                "player_id": ["A", "A", "A"],
                "opp_id": ["B", "B", "B"],
                "surface": ["Hard", "Hard", "Clay"],
                "effective_match_date": [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 3, 1),
                ],
                "won": [1, 0, 1],
                "match_uid": ["m1", "m2", "m3"],
                "round_order": [12, 12, 12],
                "tournament_start_date": date(2020, 1, 1),
            }
        ).lazy()

        result = df.with_columns(h2h_surface_win_pct().alias("v")).collect()
        # Row 0: first Hard vs B -> None
        # Row 1: 1 prior Hard vs B (won) -> 1.0
        # Row 2: first Clay vs B -> None (different partition)
        assert result["v"][0] is None
        assert result["v"][1] == pytest.approx(1.0)
        assert result["v"][2] is None
