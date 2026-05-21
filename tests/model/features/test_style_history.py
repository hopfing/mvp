"""Tests for style_history feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import style_history as style_history_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


UNIVERSAL_LABELS = [
    "power_server",
    "placement_server",
    "counterpuncher",
    "aggressive_baseliner",
    "net_rusher",
    "clutch_player",
]
STATS = ["matches", "wins", "losses", "winpct"]


class TestStyleHistoryRegistration:
    def test_universal_labels_all_four_metrics(self):
        registry = get_registry()
        for label in UNIVERSAL_LABELS:
            for stat in STATS:
                name = f"{stat}_vs_{label}"
                feat = registry.get(name)
                assert feat.mirror is True
                assert feat.params == []
                assert f"is_{label}" in feat.depends_on
                if stat == "winpct":
                    assert feat.impute == 0.5
                else:
                    assert feat.impute == 0

    def test_surface_specialist_composite_features(self):
        registry = get_registry()
        for stat in STATS:
            name = f"{stat}_vs_surface_specialists"
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == []
            assert "is_hard_specialist" in feat.depends_on
            assert "is_clay_specialist" in feat.depends_on
            if stat == "winpct":
                assert feat.impute == 0.5
            else:
                assert feat.impute == 0

    def test_diffs_registered(self):
        registry = get_registry()
        for label in UNIVERSAL_LABELS:
            for stat in STATS:
                feat = registry.get(f"{stat}_vs_{label}_diff")
                assert feat.mirror is False
                assert feat.params == []
        for stat in STATS:
            feat = registry.get(f"{stat}_vs_surface_specialists_diff")
            assert feat.mirror is False
            assert feat.params == []

    def test_total_count(self):
        registry = get_registry()
        # 6 universal × 4 metrics = 24
        # 1 surface-aligned composite × 4 metrics = 4
        # = 28 base + 28 diffs = 56 total
        count = sum(
            1 for n in registry.list_features()
            if (n.startswith(("matches_vs_", "wins_vs_", "losses_vs_", "winpct_vs_")))
        )
        assert count == 56, f"Expected 56 style_history features, got {count}"


class TestUniversalLabelComputation:
    """Verify universal-label matchup features compute correctly."""

    def _df(self) -> pl.DataFrame:
        """Player A plays 5 matches; opp_is_power_server varies; A's results vary."""
        return pl.DataFrame({
            "player_id": ["A"] * 5,
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
            ],
            "won": [1, 1, 0, 1, 1],
            "opp_is_power_server": [1, 0, 1, 1, 1],
        }).sort("effective_match_date")

    def test_matches_vs(self):
        matches_fn = get_registry().get("matches_vs_power_server").func
        df = self._df()
        result = df.with_columns(matches_fn().alias("val"))
        assert result["val"].to_list() == [0, 1, 1, 2, 3]

    def test_wins_vs(self):
        wins_fn = get_registry().get("wins_vs_power_server").func
        df = self._df()
        result = df.with_columns(wins_fn().alias("val"))
        assert result["val"].to_list() == [0, 1, 1, 1, 2]

    def test_losses_vs(self):
        losses_fn = get_registry().get("losses_vs_power_server").func
        df = self._df()
        result = df.with_columns(losses_fn().alias("val"))
        assert result["val"].to_list() == [0, 0, 0, 1, 1]

    def test_winpct_vs(self):
        winpct_fn = get_registry().get("winpct_vs_power_server").func
        df = self._df()
        result = df.with_columns(winpct_fn().alias("val"))
        assert result["val"][0] == pytest.approx(0.5)
        assert result["val"][1] == pytest.approx(1.0)
        assert result["val"][2] == pytest.approx(1.0)
        assert result["val"][3] == pytest.approx(0.5)
        assert result["val"][4] == pytest.approx(2 / 3)


class TestSurfaceSpecialistComposite:
    """Verify the surface-aligned composite feature gates on current-match surface."""

    def _df(self) -> pl.DataFrame:
        """Player A plays a mix of surfaces against opponents of varying specialty.

        Row | Surface | opp_is_clay_spec | opp_is_hard_spec | won
        m1  | Clay    | 1                | 0                | 1   (clay win vs clay spec)
        m2  | Hard    | 0                | 1                | 0   (hard loss vs hard spec)
        m3  | Clay    | 1                | 0                | 0   (clay loss vs clay spec)
        m4  | Hard    | 0                | 1                | 1   (hard win vs hard spec)
        m5  | Grass   | 0                | 0                | 1   (irrelevant; should not count)
        m6  | Clay    | 1                | 0                | 1   (focal clay row)
        m7  | Hard    | 0                | 1                | 1   (focal hard row)
        m8  | Grass   | 0                | 0                | 1   (focal grass row — feature returns 0)
        """
        return pl.DataFrame({
            "player_id": ["A"] * 8,
            "effective_match_date": [date(2024, 1, i) for i in range(1, 9)],
            "won": [1, 0, 0, 1, 1, 1, 1, 1],
            "surface": ["Clay", "Hard", "Clay", "Hard", "Grass", "Clay", "Hard", "Grass"],
            "opp_is_clay_specialist": [1, 0, 1, 0, 0, 1, 0, 0],
            "opp_is_hard_specialist": [0, 1, 0, 1, 0, 0, 1, 0],
        }).sort("effective_match_date")

    def test_matches_vs_surface_specialists(self):
        matches_fn = get_registry().get("matches_vs_surface_specialists").func
        df = self._df()
        result = df.with_columns(matches_fn().alias("val"))
        # m1 (Clay): no prior clay-matches-vs-clay-spec → 0
        # m2 (Hard): no prior hard-matches-vs-hard-spec → 0
        # m3 (Clay): prior clay matches vs clay spec = {m1} → 1
        # m4 (Hard): prior hard matches vs hard spec = {m2} → 1
        # m5 (Grass): always 0
        # m6 (Clay): prior clay matches vs clay spec = {m1, m3} → 2
        # m7 (Hard): prior hard matches vs hard spec = {m2, m4} → 2
        # m8 (Grass): 0
        assert result["val"].to_list() == [0, 0, 1, 1, 0, 2, 2, 0]

    def test_wins_vs_surface_specialists(self):
        wins_fn = get_registry().get("wins_vs_surface_specialists").func
        df = self._df()
        result = df.with_columns(wins_fn().alias("val"))
        # m6 (Clay): prior clay wins vs clay spec = m1 (won=1) → 1; m3 (won=0) doesn't count
        # m7 (Hard): prior hard wins vs hard spec = m4 (won=1) → 1; m2 (won=0) doesn't count
        # m8 (Grass): 0
        assert result["val"].to_list() == [0, 0, 1, 0, 0, 1, 1, 0]

    def test_losses_vs_surface_specialists(self):
        losses_fn = get_registry().get("losses_vs_surface_specialists").func
        df = self._df()
        result = df.with_columns(losses_fn().alias("val"))
        # m6 (Clay): prior clay losses vs clay spec = {m3} → 1
        # m7 (Hard): prior hard losses vs hard spec = {m2} → 1
        # m8 (Grass): 0
        assert result["val"].to_list() == [0, 0, 0, 1, 0, 1, 1, 0]

    def test_winpct_vs_surface_specialists(self):
        winpct_fn = get_registry().get("winpct_vs_surface_specialists").func
        df = self._df()
        result = df.with_columns(winpct_fn().alias("val"))
        # m6 (Clay): 1 win / 2 matches = 0.5
        # m7 (Hard): 1 win / 2 matches = 0.5
        # m8 (Grass): cum_n = 0 → impute 0.5
        assert result["val"][5] == pytest.approx(0.5)
        assert result["val"][6] == pytest.approx(0.5)
        assert result["val"][7] == pytest.approx(0.5)

    def test_grass_match_returns_zero_count(self):
        """On a grass match, the count features must be 0 (no specialist label for grass)."""
        matches_fn = get_registry().get("matches_vs_surface_specialists").func
        df = self._df()
        result = df.with_columns(matches_fn().alias("val"))
        # m5 and m8 are grass — must be 0 regardless of prior history
        assert result["val"][4] == 0
        assert result["val"][7] == 0


class TestTemporalSafety:
    """Current row must not contribute to its own feature value."""

    def test_single_match_returns_zero_count_and_impute(self):
        registry = get_registry()
        df = pl.DataFrame({
            "player_id": ["A"],
            "effective_match_date": [date(2024, 1, 1)],
            "won": [1],
            "opp_is_power_server": [1],
        })
        assert df.with_columns(
            registry.get("matches_vs_power_server").func().alias("v")
        )["v"][0] == 0
        assert df.with_columns(
            registry.get("wins_vs_power_server").func().alias("v")
        )["v"][0] == 0
        assert df.with_columns(
            registry.get("losses_vs_power_server").func().alias("v")
        )["v"][0] == 0
        assert df.with_columns(
            registry.get("winpct_vs_power_server").func().alias("v")
        )["v"][0] == pytest.approx(0.5)
