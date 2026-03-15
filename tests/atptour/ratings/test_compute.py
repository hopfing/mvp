"""Tests for the shared rating orchestrator."""

from datetime import date

import polars as pl
import pytest

from mvp.atptour.elo.compute import ELO_COLUMNS, compute_elo_ratings
from mvp.atptour.glicko.constants import INITIAL_MU, INITIAL_RD, INITIAL_SIGMA
from mvp.atptour.ratings.compute import ALL_RATING_COLUMNS, compute_all_ratings


def _make_match_df() -> pl.DataFrame:
    """Create a multi-match DataFrame for testing."""
    return pl.DataFrame({
        "match_uid": ["m1", "m1", "m2", "m2", "m3", "m3"],
        "player_id": ["A", "B", "C", "A", "B", "C"],
        "opp_id": ["B", "A", "A", "C", "C", "B"],
        "won": [True, False, True, False, True, False],
        "surface": ["Hard", "Hard", "Clay", "Clay", "Grass", "Grass"],
        "round": ["F", "F", "R32", "R32", "QF", "QF"],
        "tournament_level": ["GS", "GS", "250", "250", "500", "500"],
        "effective_match_date": [
            date(2024, 1, 1), date(2024, 1, 1),
            date(2024, 2, 1), date(2024, 2, 1),
            date(2024, 3, 1), date(2024, 3, 1),
        ],
        "player_rank": [10, 20, 30, 10, 20, 30],
        "opp_rank": [20, 10, 10, 30, 30, 20],
        "pts_service_pts_won": [50, 40, None, None, None, None],
        "pts_service_pts_played": [80, 80, None, None, None, None],
        "opp_pts_service_pts_won": [40, 50, None, None, None, None],
        "opp_pts_service_pts_played": [80, 80, None, None, None, None],
        "pts_return_pts_won": [None] * 6,
        "pts_return_pts_played": [None] * 6,
        "indoor": [False] * 6,
    })


class TestEloRegression:
    """Orchestrator must produce identical Elo columns to standalone compute."""

    def test_elo_columns_match_standalone(self):
        df = _make_match_df()
        standalone = compute_elo_ratings(df)
        combined = compute_all_ratings(df)

        for col in ELO_COLUMNS:
            standalone_vals = standalone[col].to_list()
            combined_vals = combined[col].to_list()
            for i, (s, c) in enumerate(zip(standalone_vals, combined_vals)):
                if s is None and c is None:
                    continue
                assert s == pytest.approx(c, abs=1e-10), (
                    f"Column {col} row {i}: standalone={s}, combined={c}"
                )

    def test_all_rating_columns_present(self):
        df = _make_match_df()
        result = compute_all_ratings(df)
        for col in ALL_RATING_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"


class TestGlickoColumnsPresent:
    def test_glicko_columns_in_output(self):
        df = _make_match_df()
        result = compute_all_ratings(df)
        expected_cols = [
            "player_glicko_mu",
            "player_glicko_rd",
            "player_glicko_sigma",
            "player_glicko_hard_adj",
            "player_glicko_hard_rd",
            "player_glicko_hard_sigma",
            "player_glicko_clay_adj",
            "player_glicko_clay_rd",
            "player_glicko_clay_sigma",
            "player_glicko_grass_adj",
            "player_glicko_grass_rd",
            "player_glicko_grass_sigma",
            "opp_glicko_mu",
            "opp_glicko_rd",
            "opp_glicko_sigma",
            "opp_glicko_hard_adj",
            "opp_glicko_hard_rd",
            "opp_glicko_hard_sigma",
            "opp_glicko_clay_adj",
            "opp_glicko_clay_rd",
            "opp_glicko_clay_sigma",
            "opp_glicko_grass_adj",
            "opp_glicko_grass_rd",
            "opp_glicko_grass_sigma",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"


class TestGlickoPreMatchCaching:
    def test_both_rows_same_match_consistent(self):
        df = _make_match_df()
        result = compute_all_ratings(df)
        a_row = result.filter(
            (pl.col("player_id") == "A")
            & (pl.col("match_uid") == "m1")
        )
        b_row = result.filter(
            (pl.col("player_id") == "B")
            & (pl.col("match_uid") == "m1")
        )
        assert (
            a_row["player_glicko_mu"][0]
            == b_row["opp_glicko_mu"][0]
        )
        assert (
            b_row["player_glicko_mu"][0]
            == a_row["opp_glicko_mu"][0]
        )


class TestGlickoConvergence:
    def test_rd_decreases_over_matches(self):
        df = _make_match_df()
        result = compute_all_ratings(df)
        a_m1 = result.filter(
            (pl.col("player_id") == "A")
            & (pl.col("match_uid") == "m1")
        )["player_glicko_rd"][0]
        a_m2 = result.filter(
            (pl.col("player_id") == "A")
            & (pl.col("match_uid") == "m2")
        )["player_glicko_rd"][0]
        assert a_m2 < a_m1

    def test_new_player_starts_at_defaults(self):
        df = _make_match_df()
        result = compute_all_ratings(df)
        a_m1 = result.filter(
            (pl.col("player_id") == "A")
            & (pl.col("match_uid") == "m1")
        )
        assert a_m1["player_glicko_mu"][0] == INITIAL_MU
        assert a_m1["player_glicko_rd"][0] == INITIAL_RD
        assert a_m1["player_glicko_sigma"][0] == INITIAL_SIGMA


class TestGlickoEloIndependence:
    def test_elo_unchanged_after_glicko_added(self):
        df = _make_match_df()
        standalone = compute_elo_ratings(df)
        combined = compute_all_ratings(df)
        for col in ELO_COLUMNS:
            standalone_vals = standalone[col].to_list()
            combined_vals = combined[col].to_list()
            for i, (s, c) in enumerate(
                zip(standalone_vals, combined_vals)
            ):
                if s is None and c is None:
                    continue
                assert s == pytest.approx(c, abs=1e-10), (
                    f"Column {col} row {i}: "
                    f"standalone={s}, combined={c}"
                )
