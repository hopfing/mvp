"""Tests for Elo computation on DataFrames."""

from datetime import date

import polars as pl

from mvp.atptour.elo.compute import ELO_COLUMNS, STYLE_COLUMNS, compute_elo_ratings


class TestComputeEloRatings:
    def test_adds_all_elo_columns(self):
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "opp_id": ["B", "A"],
                "won": [True, False],
                "surface": ["Hard", "Hard"],
                "round": ["F", "F"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "player_rank": [10, 20],
                "opp_rank": [20, 10],
                "pts_service_pts_won": [50, 40],
                "pts_service_pts_played": [80, 80],
                "pts_return_pts_won": [40, 30],
                "pts_return_pts_played": [80, 80],
            }
        )
        result = compute_elo_ratings(df)
        for col in ELO_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_elo_values_are_pre_match(self):
        # Two matches: A vs B (A wins), then A vs C (A wins)
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1", "m2", "m2"],
                "player_id": ["A", "B", "A", "C"],
                "opp_id": ["B", "A", "C", "A"],
                "won": [True, False, True, False],
                "surface": ["Hard"] * 4,
                "round": ["R32"] * 4,
                "effective_match_date": [date(2024, 1, 1)] * 2 + [date(2024, 1, 2)] * 2,
                "player_rank": [100, 100, 100, 100],
                "opp_rank": [100, 100, 100, 100],
                "pts_service_pts_won": [None] * 4,
                "pts_service_pts_played": [None] * 4,
                "pts_return_pts_won": [None] * 4,
                "pts_return_pts_played": [None] * 4,
            }
        )
        result = compute_elo_ratings(df)

        # A's Elo in match 2 should be higher than match 1 (won match 1)
        a_m1 = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m1")
        )["player_elo"][0]
        a_m2 = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]
        assert a_m2 > a_m1

    def test_both_rows_same_match_get_same_elo(self):
        # Rankings must be consistent: A is rank 10, B is rank 50
        # Row 1: A's perspective - A is player (rank 10), B is opp (rank 50)
        # Row 2: B's perspective - B is player (rank 50), A is opp (rank 10)
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "opp_id": ["B", "A"],
                "won": [True, False],
                "surface": ["Hard", "Hard"],
                "round": ["F", "F"],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "player_rank": [10, 50],
                "opp_rank": [50, 10],
                "pts_service_pts_won": [None, None],
                "pts_service_pts_played": [None, None],
                "pts_return_pts_won": [None, None],
                "pts_return_pts_played": [None, None],
            }
        )
        result = compute_elo_ratings(df)

        # A's row should have A's Elo as player_elo and B's as opp_elo
        a_row = result.filter(pl.col("player_id") == "A")
        b_row = result.filter(pl.col("player_id") == "B")

        # A's player_elo should match B's opp_elo (both are A's rating)
        assert a_row["player_elo"][0] == b_row["opp_elo"][0]
        # B's player_elo should match A's opp_elo (both are B's rating)
        assert b_row["player_elo"][0] == a_row["opp_elo"][0]


class TestStyleDimensionColumns:
    """Test that style dimension columns are added."""

    def test_style_columns_exist(self):
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "opp_id": ["B", "A"],
                "won": [True, False],
                "surface": ["Hard", "Hard"],
                "round": ["F", "F"],
                "indoor": [False, False],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "player_rank": [10, 20],
                "opp_rank": [20, 10],
                "pts_service_pts_won": [50, 40],
                "pts_service_pts_played": [80, 80],
                "pts_return_pts_won": [40, 30],
                "pts_return_pts_played": [80, 80],
                "svc_aces": [8, 5],
                "opp_svc_aces": [5, 8],
                "svc_first_serve_pts_won": [40, 35],
                "svc_double_faults": [2, 3],
                "svc_second_serve_pts_played": [30, 35],
                "svc_bp_saved": [5, 3],
                "svc_bp_faced": [8, 6],
                "ret_bp_converted": [3, 2],
                "ret_bp_opportunities": [6, 8],
                "ret_first_serve_pts_played": [60, 55],
                "ret_first_serve_pts_won": [20, 18],
                "player_set1_tiebreak": [None, None],
                "opp_set1_tiebreak": [None, None],
                "player_set2_tiebreak": [None, None],
                "opp_set2_tiebreak": [None, None],
                "player_set3_tiebreak": [None, None],
                "opp_set3_tiebreak": [None, None],
                "player_set4_tiebreak": [None, None],
                "opp_set4_tiebreak": [None, None],
                "player_set5_tiebreak": [None, None],
                "opp_set5_tiebreak": [None, None],
            }
        )
        result = compute_elo_ratings(df)
        for col in STYLE_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_style_columns_have_values(self):
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "player_id": ["A", "B"],
                "opp_id": ["B", "A"],
                "won": [True, False],
                "surface": ["Hard", "Hard"],
                "round": ["F", "F"],
                "indoor": [False, False],
                "effective_match_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "player_rank": [10, 20],
                "opp_rank": [20, 10],
                "pts_service_pts_won": [50, 40],
                "pts_service_pts_played": [80, 80],
                "pts_return_pts_won": [40, 30],
                "pts_return_pts_played": [80, 80],
                "svc_aces": [8, 5],
                "opp_svc_aces": [5, 8],
                "svc_first_serve_pts_won": [40, 35],
                "svc_double_faults": [2, 3],
                "svc_second_serve_pts_played": [30, 35],
                "svc_bp_saved": [5, 3],
                "svc_bp_faced": [8, 6],
                "ret_bp_converted": [3, 2],
                "ret_bp_opportunities": [6, 8],
                "ret_first_serve_pts_played": [60, 55],
                "ret_first_serve_pts_won": [20, 18],
                "player_set1_tiebreak": [7, 5],
                "opp_set1_tiebreak": [5, 7],
                "player_set2_tiebreak": [None, None],
                "opp_set2_tiebreak": [None, None],
                "player_set3_tiebreak": [None, None],
                "opp_set3_tiebreak": [None, None],
                "player_set4_tiebreak": [None, None],
                "opp_set4_tiebreak": [None, None],
                "player_set5_tiebreak": [None, None],
                "opp_set5_tiebreak": [None, None],
            }
        )
        result = compute_elo_ratings(df)
        # All style columns should have non-null values (initial 1500.0)
        for col in STYLE_COLUMNS:
            assert result[col].null_count() == 0, f"Column {col} has nulls"
