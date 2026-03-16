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
        base_names = [
            "tourn_sets_won", "tourn_sets_lost", "tourn_sets_margin",
            "tourn_games_won", "tourn_games_lost", "tourn_games_margin",
            "tourn_matches_won",
        ]
        for name in base_names:
            feat = registry.get(name)
            assert feat.mirror is True
            assert feat.impute == 0
            assert feat.params == []

    def test_sets_won_cumulative(self):
        from mvp.model.features.tournament import tourn_sets_won

        df = _make_tournament_df()
        result = df.with_columns(tourn_sets_won().alias("val"))
        # R32: first match -> 0 (no prior)
        assert result["val"][0] == 0
        # R16: prior = R32 (2 sets won) -> 2
        assert result["val"][1] == 2
        # QF: prior = R32 + R16 (2 + 2 = 4) -> 4
        assert result["val"][2] == 4

    def test_sets_lost_cumulative(self):
        from mvp.model.features.tournament import tourn_sets_lost

        df = _make_tournament_df()
        result = df.with_columns(tourn_sets_lost().alias("val"))
        # R32: 0 (no prior)
        assert result["val"][0] == 0
        # R16: prior = R32 (0 sets lost) -> 0
        assert result["val"][1] == 0
        # QF: prior = R32 + R16 (0 + 1 = 1) -> 1
        assert result["val"][2] == 1

    def test_sets_margin_cumulative(self):
        from mvp.model.features.tournament import tourn_sets_margin

        df = _make_tournament_df()
        result = df.with_columns(tourn_sets_margin().alias("val"))
        # R32: 0
        assert result["val"][0] == 0
        # R16: prior = R32 (2 - 0 = 2) -> 2
        assert result["val"][1] == 2
        # QF: prior = R32 + R16 ((2-0) + (2-1) = 3) -> 3
        assert result["val"][2] == 3

    def test_games_won_cumulative(self):
        from mvp.model.features.tournament import tourn_games_won

        df = _make_tournament_df()
        result = df.with_columns(tourn_games_won().alias("val"))
        # R32: 0
        assert result["val"][0] == 0
        # R16: prior = R32 (6+6 = 12) -> 12
        assert result["val"][1] == 12
        # QF: prior = R32 + R16 (12 + 7+3+6 = 12+16 = 28) -> 28
        assert result["val"][2] == 28

    def test_games_lost_cumulative(self):
        from mvp.model.features.tournament import tourn_games_lost

        df = _make_tournament_df()
        result = df.with_columns(tourn_games_lost().alias("val"))
        # R32: 0
        assert result["val"][0] == 0
        # R16: prior = R32 (3+4 = 7) -> 7
        assert result["val"][1] == 7
        # QF: prior = R32 + R16 (7 + 6+6+3 = 7+15 = 22) -> 22
        assert result["val"][2] == 22

    def test_games_margin_cumulative(self):
        from mvp.model.features.tournament import tourn_games_margin

        df = _make_tournament_df()
        result = df.with_columns(tourn_games_margin().alias("val"))
        # R32: 0
        assert result["val"][0] == 0
        # R16: prior = R32 (12-7 = 5) -> 5
        assert result["val"][1] == 5
        # QF: prior = R32 + R16 (5 + 16-15 = 6) -> 6
        assert result["val"][2] == 6

    def test_matches_won_cumulative(self):
        from mvp.model.features.tournament import tourn_matches_won

        df = _make_tournament_df()
        result = df.with_columns(tourn_matches_won().alias("val"))
        # R32: 0
        assert result["val"][0] == 0
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
        # T2 match: first in that tournament -> 0 (not 24)
        assert result["val"][2] == 0


class TestTournamentDiffFeatures:
    """Tests for tournament diff features."""

    def test_all_diffs_registered(self):
        registry = get_registry()
        diff_names = [
            "tourn_sets_won_diff", "tourn_sets_lost_diff", "tourn_sets_margin_diff",
            "tourn_games_won_diff", "tourn_games_lost_diff", "tourn_games_margin_diff",
            "tourn_matches_won_diff",
        ]
        for name in diff_names:
            feat = registry.get(name)
            assert feat.mirror is False
            assert feat.impute == 0
            assert len(feat.depends_on) == 1

    def test_sets_won_diff_computation(self):
        from mvp.model.features.tournament import tourn_sets_won_diff

        df = pl.DataFrame({
            "player_tourn_sets_won": [4, 2, 0],
            "opp_tourn_sets_won": [2, 2, 3],
        })
        result = df.with_columns(tourn_sets_won_diff().alias("diff"))
        assert result["diff"][0] == 2
        assert result["diff"][1] == 0
        assert result["diff"][2] == -3

    def test_matches_won_diff_computation(self):
        from mvp.model.features.tournament import tourn_matches_won_diff

        df = pl.DataFrame({
            "player_tourn_matches_won": [2, 0],
            "opp_tourn_matches_won": [1, 3],
        })
        result = df.with_columns(tourn_matches_won_diff().alias("diff"))
        assert result["diff"][0] == 1
        assert result["diff"][1] == -3


class TestTournamentFeatureCount:
    """Verify total feature count."""

    def test_total_count(self):
        registry = get_registry()
        tourn = [n for n in registry.list_features() if n.startswith("tourn_")]
        assert len(tourn) == 14, f"Expected 14 tournament features, got {len(tourn)}: {sorted(tourn)}"
