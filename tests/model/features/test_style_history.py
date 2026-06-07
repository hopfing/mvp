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
                assert feat.params == ["days"]
                assert f"is_{label}" in feat.depends_on
                if stat == "winpct":
                    assert feat.impute is None  # no-fabricate: null when no prior
                else:
                    assert feat.impute == 0

    def test_surface_specialist_composite_features(self):
        registry = get_registry()
        for stat in STATS:
            name = f"{stat}_vs_surface_specialists"
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == ["days"]
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
                assert feat.params == ["days"]
        for stat in STATS:
            feat = registry.get(f"{stat}_vs_surface_specialists_diff")
            assert feat.mirror is False
            assert feat.params == ["days"]

    def test_total_count(self):
        registry = get_registry()
        # 6 universal × 4 metrics = 24
        # 1 surface-aligned composite × 4 metrics = 4
        # 3 vs-opp-type axes × 4 metrics = 12
        # 3 surface-gated vs-opp-type axes × 4 metrics = 12
        # = 52 base + 52 diffs = 104 total
        count = sum(
            1 for n in registry.list_features()
            if (n.startswith((
                "matches_vs_", "wins_vs_", "losses_vs_", "winpct_vs_",
                "surface_matches_vs_", "surface_wins_vs_",
                "surface_losses_vs_", "surface_winpct_vs_",
            )))
        )
        assert count == 104, f"Expected 104 style_history features, got {count}"


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
        assert result["val"][0] is None  # no prior vs this type -> null (no-fabricate)
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
        # m8 (Grass): cum_n = 0 → null (no-fabricate)
        assert result["val"][5] == pytest.approx(0.5)
        assert result["val"][6] == pytest.approx(0.5)
        assert result["val"][7] is None

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
        )["v"][0] is None  # no prior -> null (no-fabricate)


class TestVsOppType:
    """vs-current-opponent's-type composites (serve / rally / net axes)."""

    def _df(self) -> "pl.DataFrame":
        # Player A history; serve-type axis via opp_is_power_server / opp_is_placement_server.
        #   m1 power  (won)   m2 placement (lost)  m3 power (lost)
        #   m4 neutral (won)  m5 power (won)       m6 opp type unknown (null)
        return pl.DataFrame({
            "player_id": ["A"] * 6,
            "effective_match_date": [date(2024, 1, i) for i in range(1, 7)],
            "won": [1, 0, 0, 1, 1, 1],
            "opp_is_power_server": pl.Series([1, 0, 1, 0, 1, None], dtype=pl.Int8),
            "opp_is_placement_server": pl.Series([0, 1, 0, 0, 0, None], dtype=pl.Int8),
        }).sort("effective_match_date")

    def test_registration(self):
        reg = get_registry()
        for axis in ("serve_type", "rally_type", "net_type"):
            for stat in ("matches", "wins", "losses", "winpct"):
                feat = reg.get(f"{stat}_vs_opp_{axis}")
                assert feat.mirror is True
                assert feat.params == ["days"]
                assert feat.impute is None  # unknown stays null, not imputed

    def test_matches_selects_current_opp_bucket(self):
        fn = get_registry().get("matches_vs_opp_serve_type").func
        result = self._df().with_columns(fn().alias("v"))
        # prior matches sharing the current opp's serve bucket:
        # m1 power:0  m2 placement:0  m3 power:{m1}=1  m4 neutral:0  m5 power:{m1,m3}=2  m6 unknown:null
        assert result["v"].to_list() == [0, 0, 1, 0, 2, None]

    def test_winpct_and_unknown_null(self):
        fn = get_registry().get("winpct_vs_opp_serve_type").func
        result = self._df().with_columns(fn().alias("v"))
        assert result["v"][0] is None          # m1: no prior power → null (not 0.5)
        assert result["v"][2] == pytest.approx(1.0)   # m3: power priors {m1 won} → 1/1
        assert result["v"][4] == pytest.approx(0.5)   # m5: power priors {m1 won, m3 lost} → 1/2
        assert result["v"][5] is None          # m6: opp type unknown → null

    def test_surface_gating(self):
        # surface-gated variant counts only prior SAME-surface matches in the bucket
        df = pl.DataFrame({
            "player_id": ["A"] * 4,
            "effective_match_date": [date(2024, 1, i) for i in range(1, 5)],
            "won": [1, 1, 0, 1],
            "surface": ["Hard", "Clay", "Hard", "Hard"],
            "opp_is_power_server": pl.Series([1, 1, 1, 1], dtype=pl.Int8),
            "opp_is_placement_server": pl.Series([0, 0, 0, 0], dtype=pl.Int8),
        }).sort("effective_match_date")
        xsurf = get_registry().get("matches_vs_opp_serve_type").func
        surf = get_registry().get("surface_matches_vs_opp_serve_type").func
        rx = df.with_columns(xsurf().alias("v"))["v"].to_list()
        rs = df.with_columns(surf().alias("v"))["v"].to_list()
        # all opps power. m4 (Hard): cross-surface prior = {m1,m2,m3}=3; same-surface Hard = {m1,m3}=2
        assert rx == [0, 1, 2, 3]
        assert rs == [0, 0, 1, 2]


class TestSurfSpecRatios:
    """Surface rate stats / quality, gated to prior same-surface vs-specialist matches."""

    _NAMES = [
        "surface_first_serve_win_pct_vs_surf_spec",
        "surface_second_serve_win_pct_vs_surf_spec",
        "surface_ace_pct_vs_surf_spec",
        "surface_df_pct_vs_surf_spec",
        "surface_first_serve_in_pct_vs_surf_spec",
        "surface_bp_save_pct_vs_surf_spec",
        "surface_hold_pct_vs_surf_spec",
        "surface_ret_first_serve_win_pct_vs_surf_spec",
        "surface_ret_second_serve_win_pct_vs_surf_spec",
        "surface_ret_bp_convert_pct_vs_surf_spec",
        "surface_pts_service_won_pct_vs_surf_spec",
        "surface_pts_return_won_pct_vs_surf_spec",
        "quality_win_rate_vs_surf_spec",
    ]

    def test_registration(self):
        reg = get_registry()
        for name in self._NAMES:
            feat = reg.get(name)
            assert feat.mirror is True
            assert feat.params == ["days"]
            assert feat.impute is None
            assert feat.depends_on == ["is_hard_specialist", "is_clay_specialist"]

    def test_first_serve_win_pct_gating(self):
        fn = get_registry().get("surface_first_serve_win_pct_vs_surf_spec").func
        df = pl.DataFrame({
            "player_id": ["A"] * 5,
            "effective_match_date": [date(2024, 1, i) for i in range(1, 6)],
            "won": [1, 1, 0, 1, 1],
            "surface": ["Hard", "Hard", "Clay", "Hard", "Grass"],
            "opp_is_hard_specialist": pl.Series([1, 0, 0, 1, 0], dtype=pl.Int8),
            "opp_is_clay_specialist": pl.Series([0, 0, 1, 0, 0], dtype=pl.Int8),
            "svc_first_serve_pts_won": [30, 40, 20, 10, 25],
            "svc_first_serve_pts_played": [50, 50, 50, 50, 50],
        }).sort("effective_match_date")
        v = df.with_columns(fn().alias("v"))["v"].to_list()
        # m1 Hard: no prior Hard-vs-hardspec -> null
        # m2 Hard: prior = {m1} -> 30/50 = 0.6
        # m3 Clay: no prior Clay-vs-clayspec -> null
        # m4 Hard: prior Hard-vs-hardspec = {m1} (m2 not spec) -> 30/50 = 0.6
        # m5 Grass: no specialist label for grass -> null
        assert v[0] is None
        assert v[1] == pytest.approx(0.6)
        assert v[2] is None
        assert v[3] == pytest.approx(0.6)
        assert v[4] is None
