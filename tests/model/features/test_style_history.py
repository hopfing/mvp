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
SURFACE_SPECIALIST_BASES = [
    "clay_specialist_on_clay", "clay_specialist_off_clay",
    "hard_specialist_on_hard", "hard_specialist_off_hard",
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

    def test_surface_specialists_all_four_metrics(self):
        registry = get_registry()
        for base in SURFACE_SPECIALIST_BASES:
            for stat in STATS:
                name = f"{stat}_vs_{base}"
                feat = registry.get(name)
                assert feat.mirror is True
                assert feat.params == []
                # depends_on references the underlying label without surface suffix
                label_root = base.split("_on_")[0].split("_off_")[0]
                assert f"is_{label_root}" in feat.depends_on
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
        for base in SURFACE_SPECIALIST_BASES:
            for stat in STATS:
                feat = registry.get(f"{stat}_vs_{base}_diff")
                assert feat.mirror is False
                assert feat.params == []

    def test_total_count(self):
        registry = get_registry()
        # 6 universal × 4 metrics = 24
        # 2 specialists × 2 (on/off) × 4 metrics = 16
        # = 40 base + 40 diffs = 80 total
        count = sum(
            1 for n in registry.list_features()
            if (n.startswith(("matches_vs_", "wins_vs_", "losses_vs_", "winpct_vs_")))
        )
        assert count == 80, f"Expected 80 style_history features, got {count}"


class TestUniversalLabelComputation:
    """Verify universal-label matchup features compute correctly."""

    def _df(self) -> pl.DataFrame:
        """Player A plays 5 matches; opp_is_power_server varies; A's results vary.

        Chronological:
          m1: opp_is_power_server=1, won=1   -> count win vs power server
          m2: opp_is_power_server=0, won=1   -> NOT counted (opp not power server)
          m3: opp_is_power_server=1, won=0   -> count loss vs power server
          m4: opp_is_power_server=1, won=1   -> count win vs power server
          m5 (focal): opp_is_power_server=1, won=1

        At m5: prior matches vs power servers = 3 (m1, m3, m4); wins = 2 (m1, m4); losses = 1 (m3).
        """
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
        # m1 not yet counted (current), m2 prior wins =1 (m1 was win vs ps)
        # m3 prior wins =1, m4 prior wins =1 (m1 + m3 loss), m5 prior wins =2 (m1+m4)
        assert result["val"].to_list() == [0, 1, 1, 1, 2]

    def test_losses_vs(self):
        losses_fn = get_registry().get("losses_vs_power_server").func
        df = self._df()
        result = df.with_columns(losses_fn().alias("val"))
        # m1: 0 prior losses. m2: 0 (m1 was win). m3: 0 (m2 not ps). m4: 1 (m3 was loss). m5: 1.
        assert result["val"].to_list() == [0, 0, 0, 1, 1]

    def test_winpct_vs(self):
        winpct_fn = get_registry().get("winpct_vs_power_server").func
        df = self._df()
        result = df.with_columns(winpct_fn().alias("val"))
        # m1: 0 prior → 0.5 impute
        # m2: 1 prior match (m1, win) → 1/1 = 1.0
        # m3: 1 prior ps match (m1 win) → 1/1 = 1.0
        # m4: 2 prior ps matches (m1 win, m3 loss) → 1/2 = 0.5
        # m5: 3 prior ps matches (m1, m3, m4) → 2/3 ≈ 0.6667
        assert result["val"][0] == pytest.approx(0.5)
        assert result["val"][1] == pytest.approx(1.0)
        assert result["val"][2] == pytest.approx(1.0)
        assert result["val"][3] == pytest.approx(0.5)
        assert result["val"][4] == pytest.approx(2 / 3)


class TestSurfaceConditionedComputation:
    """Verify on_clay / off_clay matchup features filter on the surface column."""

    def _df(self) -> pl.DataFrame:
        """Player A plays clay specialists on different surfaces; only clay matches engage."""
        return pl.DataFrame({
            "player_id": ["A"] * 5,
            "effective_match_date": [
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
            ],
            "won": [1, 0, 1, 0, 1],
            "opp_is_clay_specialist": [1, 1, 1, 1, 1],  # all opp are clay specialists
            "surface": ["Clay", "Hard", "Clay", "Hard", "Clay"],  # alternating
        }).sort("effective_match_date")

    def test_matches_on_clay(self):
        """Only counts matches where surface == Clay."""
        matches_fn = get_registry().get("matches_vs_clay_specialist_on_clay").func
        df = self._df()
        result = df.with_columns(matches_fn().alias("val"))
        # Prior matches by row:
        # m1: 0; m2: 1 (m1 Clay); m3: 1 (m2 Hard skipped); m4: 2 (m1, m3); m5: 2 (m4 Hard skipped)
        assert result["val"].to_list() == [0, 1, 1, 2, 2]

    def test_matches_off_clay(self):
        """Counts matches where surface != Clay."""
        matches_fn = get_registry().get("matches_vs_clay_specialist_off_clay").func
        df = self._df()
        result = df.with_columns(matches_fn().alias("val"))
        # m1: 0; m2: 0 (m1 Clay → skipped); m3: 1 (m2 Hard); m4: 1; m5: 2 (m2 + m4)
        assert result["val"].to_list() == [0, 0, 1, 1, 2]

    def test_wins_on_clay(self):
        wins_fn = get_registry().get("wins_vs_clay_specialist_on_clay").func
        df = self._df()
        result = df.with_columns(wins_fn().alias("val"))
        # Prior wins on Clay vs clay specialists by row:
        # m1: 0; m2: 1 (m1 won on Clay); m3: 1; m4: 2 (m1 win + m3 win); m5: 2
        assert result["val"].to_list() == [0, 1, 1, 2, 2]

    def test_winpct_on_clay_imputes_when_no_prior(self):
        winpct_fn = get_registry().get("winpct_vs_clay_specialist_on_clay").func
        df = self._df()
        result = df.with_columns(winpct_fn().alias("val"))
        # m1: 0 prior → 0.5 impute
        assert result["val"][0] == pytest.approx(0.5)


class TestTemporalSafety:
    """Current row must not contribute to its own feature value."""

    def test_single_match_returns_zero_count_and_impute(self):
        """One match (currently being predicted): everything reflects 'no prior'."""
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
