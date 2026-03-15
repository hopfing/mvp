"""Tests for the shared rating orchestrator."""

from datetime import date

import polars as pl
import pytest

from mvp.atptour.elo.compute import ELO_COLUMNS, compute_elo_ratings
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
