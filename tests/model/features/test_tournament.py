"""Tests for tournament-context feature module."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import tournament as tournament_module  # noqa: F401
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


def _make_tournament_df() -> pl.DataFrame:
    """Player A plays 3 rounds in one tournament: R32, R16, QF.

    Round 1 (R32): Won 6-3, 6-4 (2 sets won, 0 lost, 9 games won, 7 lost)
    Round 2 (R16): Won 7-6, 3-6, 6-3 (2 sets won, 1 lost, 16 games won, 15 lost)
    Round 3 (QF): Lost 4-6, 6-3, 3-6 (1 set won, 2 lost, 13 games won, 15 lost)
    """
    return pl.DataFrame({
        "player_id": ["A", "A", "A"],
        "tournament_id": ["T1", "T1", "T1"],
        "year": [2024, 2024, 2024],
        "draw_type": ["singles", "singles", "singles"],
        "effective_match_date": [
            date(2024, 6, 10),
            date(2024, 6, 11),
            date(2024, 6, 12),
        ],
        "won": [1, 1, 0],
        "match_uid": ["m1", "m2", "m3"],
        "round_order": [7, 9, 10],
        "player_set1_games": [6, 7, 4],
        "opp_set1_games": [3, 6, 6],
        "player_set2_games": [6, 3, 6],
        "opp_set2_games": [4, 6, 3],
        "player_set3_games": [None, 6, 3],
        "opp_set3_games": [None, 3, 6],
        "player_set4_games": [None, None, None],
        "opp_set4_games": [None, None, None],
        "player_set5_games": [None, None, None],
        "opp_set5_games": [None, None, None],
    }).sort("effective_match_date")


class TestTournamentBaseFeatures:
    """Tests for cumulative tournament features."""

    def test_all_base_features_registered(self):
        registry = get_registry()
        # Result counts and margins use impute=None so first-occurrence rows
        # remain NaN (distinguishable from "had data, result was 0"). Only
        # tourn_matches_played stays at 0 (opportunity count).
        passthrough_bases = [
            "tourn_sets_won", "tourn_sets_lost", "tourn_sets_margin",
            "tourn_games_won", "tourn_games_lost", "tourn_games_margin",
            "tourn_matches_won",
        ]
        for name in passthrough_bases:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.impute is None
            assert feat.params == []

        played = registry.get("tourn_matches_played")
        assert played.impute == 0

    def test_sets_won_cumulative(self):
        from mvp.model.features.tournament import tourn_sets_won

        df = _make_tournament_df()
        result = df.with_columns(tourn_sets_won().alias("val"))
        # R32: first match -> None (no prior; "haven't played" distinguished
        # from "played and won 0")
        assert result["val"][0] is None
        # R16: prior = R32 (2 sets won) -> 2
        assert result["val"][1] == 2
        # QF: prior = R32 + R16 (2 + 2 = 4) -> 4
        assert result["val"][2] == 4

    def test_sets_lost_cumulative(self):
        from mvp.model.features.tournament import tourn_sets_lost

        df = _make_tournament_df()
        result = df.with_columns(tourn_sets_lost().alias("val"))
        # R32: None (no prior)
        assert result["val"][0] is None
        # R16: prior = R32 (won straight sets) -> 0 (REAL 0, not from no-data fill)
        assert result["val"][1] == 0
        # QF: prior = R32 + R16 (0 + 1 = 1) -> 1
        assert result["val"][2] == 1

    def test_sets_lost_zero_after_straight_sets_win(self):
        """A player on match 2 who won match 1 in straight sets must show
        tourn_sets_lost = 0 (REAL 0), not None.

        Regression guard for the impute=None + fill_with=None change: shift(1)
        on row 2 returns the cumsum value through row 1 (which is 0 for a
        player who lost 0 sets), and that 0 must NOT be conflated with the
        first-row NaN.
        """
        from mvp.model.features.tournament import tourn_sets_lost

        df = _make_tournament_df()
        result = df.with_columns(tourn_sets_lost().alias("val"))
        # Match 1 → no prior (NaN). Match 2 → 0 prior sets lost (REAL 0).
        assert result["val"][0] is None
        assert result["val"][1] == 0
        assert result["val"][1] is not None

    def test_sets_margin_cumulative(self):
        from mvp.model.features.tournament import tourn_sets_margin

        df = _make_tournament_df()
        result = df.with_columns(tourn_sets_margin().alias("val"))
        # R32: None (no prior)
        assert result["val"][0] is None
        # R16: prior = R32 (2 - 0 = 2) -> 2
        assert result["val"][1] == 2
        # QF: prior = R32 + R16 ((2-0) + (2-1) = 3) -> 3
        assert result["val"][2] == 3

    def test_games_won_cumulative(self):
        from mvp.model.features.tournament import tourn_games_won

        df = _make_tournament_df()
        result = df.with_columns(tourn_games_won().alias("val"))
        # R32: None (no prior)
        assert result["val"][0] is None
        # R16: prior = R32 (6+6 = 12) -> 12
        assert result["val"][1] == 12
        # QF: prior = R32 + R16 (12 + 7+3+6 = 12+16 = 28) -> 28
        assert result["val"][2] == 28

    def test_games_lost_cumulative(self):
        from mvp.model.features.tournament import tourn_games_lost

        df = _make_tournament_df()
        result = df.with_columns(tourn_games_lost().alias("val"))
        # R32: None (no prior)
        assert result["val"][0] is None
        # R16: prior = R32 (3+4 = 7) -> 7
        assert result["val"][1] == 7
        # QF: prior = R32 + R16 (7 + 6+6+3 = 7+15 = 22) -> 22
        assert result["val"][2] == 22

    def test_games_margin_cumulative(self):
        from mvp.model.features.tournament import tourn_games_margin

        df = _make_tournament_df()
        result = df.with_columns(tourn_games_margin().alias("val"))
        # R32: None (no prior)
        assert result["val"][0] is None
        # R16: prior = R32 (12-7 = 5) -> 5
        assert result["val"][1] == 5
        # QF: prior = R32 + R16 (5 + 16-15 = 6) -> 6
        assert result["val"][2] == 6

    def test_matches_won_cumulative(self):
        from mvp.model.features.tournament import tourn_matches_won

        df = _make_tournament_df()
        result = df.with_columns(tourn_matches_won().alias("val"))
        # R32: None (no prior)
        assert result["val"][0] is None
        # R16: prior = R32 (1 win) -> 1
        assert result["val"][1] == 1
        # QF: prior = R32 + R16 (1 + 1 = 2) -> 2
        assert result["val"][2] == 2

    def test_different_tournaments_independent(self):
        """Features don't leak across tournament boundaries."""
        from mvp.model.features.tournament import tourn_games_won

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "tournament_id": ["T1", "T1", "T2"],
            "year": [2024, 2024, 2024],
            "draw_type": ["singles", "singles", "singles"],
            "effective_match_date": [
                date(2024, 6, 10),
                date(2024, 6, 11),
                date(2024, 6, 17),
            ],
            "won": [1, 1, 0],
            "match_uid": ["m1", "m2", "m3"],
            "round_order": [7, 9, 7],
            "player_set1_games": [6, 6, 3],
            "opp_set1_games": [3, 4, 6],
            "player_set2_games": [6, 6, 4],
            "opp_set2_games": [4, 3, 6],
            "player_set3_games": [None, None, None],
            "opp_set3_games": [None, None, None],
            "player_set4_games": [None, None, None],
            "opp_set4_games": [None, None, None],
            "player_set5_games": [None, None, None],
            "opp_set5_games": [None, None, None],
        }).sort("effective_match_date")

        result = df.with_columns(tourn_games_won().alias("val"))
        # T2 match: first in that tournament -> None (not 24, not 0)
        assert result["val"][2] is None


class TestTournamentDiffFeatures:
    """Tests for tournament diff features."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        # All seven diffs inherit impute=None from their bases (result counts +
        # margins). matches_played_diff stays at 0 — same opportunity-count
        # rationale as its base.
        passthrough_diffs = [
            "tourn_sets_won_diff", "tourn_sets_lost_diff", "tourn_sets_margin_diff",
            "tourn_games_won_diff", "tourn_games_lost_diff", "tourn_games_margin_diff",
            "tourn_matches_won_diff",
        ]
        for name in passthrough_diffs:
            feat = registry.get(name)
            assert feat.mirror is False
            assert feat.impute is None
            assert len(feat.depends_on) == 1

        played_diff = registry.get("tourn_matches_played_diff")
        assert played_diff.impute == 0

    def test_sets_won_diff_computation(self):
        tourn_sets_won_diff = get_registry().get("tourn_sets_won_diff").func

        df = pl.DataFrame({
            "player_tourn_sets_won": [4, 2, 0],
            "opp_tourn_sets_won": [2, 2, 3],
        })
        result = df.with_columns(tourn_sets_won_diff().alias("diff"))
        assert result["diff"][0] == 2
        assert result["diff"][1] == 0
        assert result["diff"][2] == -3

    def test_matches_won_diff_computation(self):
        tourn_matches_won_diff = get_registry().get("tourn_matches_won_diff").func

        df = pl.DataFrame({
            "player_tourn_matches_won": [2, 0],
            "opp_tourn_matches_won": [1, 3],
        })
        result = df.with_columns(tourn_matches_won_diff().alias("diff"))
        assert result["diff"][0] == 1
        assert result["diff"][1] == -3


_HISTORY_BASES = [
    # Counts
    "tourn_history_matches_played",
    "tourn_history_year_instances_completed",
    "tourn_history_matches_won",
    "tourn_history_matches_lost",
    "tourn_history_sets_won",
    "tourn_history_sets_lost",
    "tourn_history_games_won",
    "tourn_history_games_lost",
    # Margin sums
    "tourn_history_matches_margin_sum",
    "tourn_history_sets_margin_sum",
    "tourn_history_games_margin_sum",
    # Per-match rate
    "tourn_history_win_pct",
    # Per-prior-match avgs
    "tourn_history_sets_won_avg_per_match",
    "tourn_history_sets_lost_avg_per_match",
    "tourn_history_games_won_avg_per_match",
    "tourn_history_games_lost_avg_per_match",
    "tourn_history_sets_margin_avg_per_match",
    "tourn_history_games_margin_avg_per_match",
    # Per-prior-year-instance avgs
    "tourn_history_matches_won_avg_per_year",
    "tourn_history_matches_lost_avg_per_year",
    "tourn_history_matches_margin_avg_per_year",
    "tourn_history_sets_won_avg_per_year",
    "tourn_history_sets_lost_avg_per_year",
    "tourn_history_sets_margin_avg_per_year",
    "tourn_history_games_won_avg_per_year",
    "tourn_history_games_lost_avg_per_year",
    "tourn_history_games_margin_avg_per_year",
]


class TestTournamentFeatureCount:
    """Verify total feature count."""

    def test_total_count(self):
        registry = get_registry()
        tourn = [n for n in registry.list_features() if n.startswith("tourn_")]
        # 8 within-tournament base + 8 diffs = 16
        # 27 cross-year history base + 27 diffs = 54
        # Total = 70
        assert len(tourn) == 70, f"Expected 70 tournament features, got {len(tourn)}: {sorted(tourn)}"


class TestTournamentHistoryFeatures:
    """Cross-year tournament history (groups by player_id + tournament_id + draw_type)."""

    def test_history_features_registered(self):
        registry = get_registry()
        assert len(_HISTORY_BASES) == 27
        for name in _HISTORY_BASES:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.params == []

    def test_history_diffs_registered(self):
        registry = get_registry()
        for base_name in _HISTORY_BASES:
            diff_name = f"{base_name}_diff"
            feat = registry.get(diff_name)
            assert feat.mirror is False
            assert feat.params == []

    def _make_cross_year_df(self) -> pl.DataFrame:
        """Player A plays one match at T1 in 2022, one in 2023, one in 2024.

        2022 match: Won 6-3, 6-4 -> 2 sets won, 0 lost; 12 games won, 7 lost
        2023 match: Lost 4-6, 3-6 -> 0 sets won, 2 lost; 7 games won, 12 lost
        2024 match: Won 6-2, 6-3 -> 2 sets won, 0 lost; 12 games won, 5 lost
        """
        return pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "tournament_id": ["T1", "T1", "T1"],
            "year": [2022, 2023, 2024],
            "draw_type": ["singles", "singles", "singles"],
            "effective_match_date": [
                date(2022, 6, 10),
                date(2023, 6, 11),
                date(2024, 6, 12),
            ],
            "won": [1, 0, 1],
            "match_uid": ["m1", "m2", "m3"],
            "round_order": [12, 12, 12],
            "player_set1_games": [6, 4, 6],
            "opp_set1_games": [3, 6, 2],
            "player_set2_games": [6, 3, 6],
            "opp_set2_games": [4, 6, 3],
            "player_set3_games": [None, None, None],
            "opp_set3_games": [None, None, None],
            "player_set4_games": [None, None, None],
            "opp_set4_games": [None, None, None],
            "player_set5_games": [None, None, None],
            "opp_set5_games": [None, None, None],
        }).sort("effective_match_date")

    def test_matches_played_crosses_years(self):
        from mvp.model.features.tournament import tourn_history_matches_played

        df = self._make_cross_year_df()
        result = df.with_columns(tourn_history_matches_played().alias("val"))
        # 2022: no prior at T1 -> 0
        assert result["val"][0] == 0
        # 2023: 1 prior at T1 (2022) -> 1
        assert result["val"][1] == 1
        # 2024: 2 prior at T1 (2022 + 2023) -> 2
        assert result["val"][2] == 2

    def test_matches_won_crosses_years(self):
        from mvp.model.features.tournament import tourn_history_matches_won

        df = self._make_cross_year_df()
        result = df.with_columns(tourn_history_matches_won().alias("val"))
        # 2022: no prior appearance at T1 -> None
        assert result["val"][0] is None
        # 2023: 1 prior win (the 2022 match)
        assert result["val"][1] == 1
        # 2024: 1 prior win (2022 won, 2023 lost)
        assert result["val"][2] == 1

    def test_matches_lost_zero_after_undefeated_history(self):
        """Cross-year history: a player whose only prior appearance was a win
        must show tourn_history_matches_lost = 0 (REAL 0), not None.

        Regression guard for fill_with=None on cumulative_sum: shift(1) on row
        2 returns the cumsum value through row 1 (= 0 losses), and that 0
        must survive the no-fill path.
        """
        from mvp.model.features.tournament import tourn_history_matches_lost

        df = self._make_cross_year_df()
        result = df.with_columns(tourn_history_matches_lost().alias("val"))
        # 2022: first appearance -> None
        assert result["val"][0] is None
        # 2023: 1 prior appearance, won it -> 0 losses (REAL 0)
        assert result["val"][1] == 0
        assert result["val"][1] is not None
        # 2024: 1 prior loss (2023)
        assert result["val"][2] == 1

    def test_win_pct_crosses_years(self):
        from mvp.model.features.tournament import tourn_history_win_pct

        df = self._make_cross_year_df()
        result = df.with_columns(tourn_history_win_pct().alias("val"))
        # 2022: no prior -> null (cumulative_mean returns null/None when no prior)
        # 2023: 1/1 = 1.0
        assert result["val"][1] == pytest.approx(1.0)
        # 2024: 1/2 = 0.5
        assert result["val"][2] == pytest.approx(0.5)

    def test_sets_margin_sum_crosses_years(self):
        from mvp.model.features.tournament import tourn_history_sets_margin_sum

        df = self._make_cross_year_df()
        result = df.with_columns(tourn_history_sets_margin_sum().alias("val"))
        # 2022: first appearance -> None (impute=None + fill_with=None)
        assert result["val"][0] is None
        # 2023: prior = 2022 (2-0 = 2)
        assert result["val"][1] == 2
        # 2024: prior = 2022 + 2023 (2 + -2 = 0)  ← REAL 0, not first-row None
        assert result["val"][2] == 0

    def test_sets_margin_avg_per_match_crosses_years(self):
        """Per-prior-match average — denominator is total prior matches."""
        from mvp.model.features.tournament import tourn_history_sets_margin_avg_per_match

        df = self._make_cross_year_df()
        result = df.with_columns(tourn_history_sets_margin_avg_per_match().alias("val"))
        # 2023: 2/1 = 2.0
        assert result["val"][1] == pytest.approx(2.0)
        # 2024: 0/2 = 0.0
        assert result["val"][2] == pytest.approx(0.0)

    def _add_year_instances_completed(self, df: pl.DataFrame) -> pl.DataFrame:
        """Manually compute player_tourn_history_year_instances_completed for tests.

        In production this is auto-loaded via the depends_on chain in the feature engine.
        In tests we call the feature function directly, so the dep must be pre-populated.
        """
        from mvp.model.features.tournament import tourn_history_year_instances_completed
        return df.with_columns(
            tourn_history_year_instances_completed()
            .alias("player_tourn_history_year_instances_completed")
        )

    def test_sets_margin_avg_per_year_single_match_per_year(self):
        """With 1 match per year-instance, per-year and per-match coincide."""
        from mvp.model.features.tournament import tourn_history_sets_margin_avg_per_year

        df = self._add_year_instances_completed(self._make_cross_year_df())
        result = df.with_columns(tourn_history_sets_margin_avg_per_year().alias("val"))
        # 2023: avg of 1 prior year (2022 margin = +2) = 2.0
        assert result["val"][1] == pytest.approx(2.0)
        # 2024: avg of 2 prior years (2022 +2, 2023 -2) = 0.0
        assert result["val"][2] == pytest.approx(0.0)

    def test_sets_margin_avg_per_year_vs_per_match_differ_with_multiple_matches(self):
        """When a year-instance has multiple matches, per-year and per-match diverge."""
        from mvp.model.features.tournament import (
            tourn_history_sets_margin_avg_per_match,
            tourn_history_sets_margin_avg_per_year,
        )

        # Year 1: 1 match (margin +2). Year 2: 3 matches (each margin +2, total +6).
        # Year 3 R1: focal match — averaging over prior years.
        # Per-match avg: (2 + 2+2+2) / 4 = 8/4 = 2.0
        # Per-year avg:  (2 + 6) / 2 = 8/2 = 4.0
        df = pl.DataFrame({
            "player_id": ["A"] * 5,
            "tournament_id": ["T1"] * 5,
            "year": [2022, 2023, 2023, 2023, 2024],
            "draw_type": ["singles"] * 5,
            "effective_match_date": [
                date(2022, 6, 1),
                date(2023, 6, 1), date(2023, 6, 2), date(2023, 6, 3),
                date(2024, 6, 1),
            ],
            "won": [1, 1, 1, 1, 1],
            "match_uid": ["m1", "m2", "m3", "m4", "m5"],
            "round_order": [12, 9, 10, 11, 12],
            "player_set1_games": [6, 6, 6, 6, 6],
            "opp_set1_games": [3, 3, 3, 3, 3],
            "player_set2_games": [6, 6, 6, 6, 6],
            "opp_set2_games": [4, 4, 4, 4, 4],
            "player_set3_games": [None] * 5,
            "opp_set3_games": [None] * 5,
            "player_set4_games": [None] * 5,
            "opp_set4_games": [None] * 5,
            "player_set5_games": [None] * 5,
            "opp_set5_games": [None] * 5,
        }).sort("effective_match_date")
        df = self._add_year_instances_completed(df)
        result = df.with_columns(
            tourn_history_sets_margin_avg_per_match().alias("per_match"),
            tourn_history_sets_margin_avg_per_year().alias("per_year"),
        )
        # Focal row is the 2024 match (index 4)
        assert result["per_match"][4] == pytest.approx(2.0)
        assert result["per_year"][4] == pytest.approx(4.0)

    def test_different_draw_types_independent(self):
        """Singles and doubles history at the same tournament don't pool."""
        from mvp.model.features.tournament import tourn_history_matches_played

        df = pl.DataFrame({
            "player_id": ["A", "A"],
            "tournament_id": ["T1", "T1"],
            "year": [2023, 2024],
            "draw_type": ["doubles", "singles"],
            "effective_match_date": [date(2023, 6, 11), date(2024, 6, 12)],
            "won": [1, 1],
            "match_uid": ["m1", "m2"],
            "round_order": [12, 12],
            "player_set1_games": [6, 6],
            "opp_set1_games": [3, 4],
            "player_set2_games": [6, 6],
            "opp_set2_games": [4, 3],
            "player_set3_games": [None, None],
            "opp_set3_games": [None, None],
            "player_set4_games": [None, None],
            "opp_set4_games": [None, None],
            "player_set5_games": [None, None],
            "opp_set5_games": [None, None],
        }).sort("effective_match_date")
        result = df.with_columns(tourn_history_matches_played().alias("val"))
        # 2024 singles match: 0 prior singles appearances (the 2023 doubles doesn't count)
        assert result["val"][1] == 0
