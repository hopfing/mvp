"""Tests for Elo computation on DataFrames."""

from datetime import date

import polars as pl

from mvp.atptour.elo.compute import ELO_COLUMNS, STYLE_COLUMNS, compute_elo_ratings
from mvp.atptour.elo.constants import BASE_K, DEFAULT_ELO, REVERSION_RATE


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
                "round_order": [12, 12],
                "tournament_start_date": date(2020, 1, 1),
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
                "round_order": [12] * 4,
                "tournament_start_date": date(2020, 1, 1),
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
                "round_order": [12, 12],
                "tournament_start_date": date(2020, 1, 1),
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
                "round_order": [12, 12],
                "tournament_start_date": date(2020, 1, 1),
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
                "round_order": [12, 12],
                "tournament_start_date": date(2020, 1, 1),
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


def _make_match_df(
    match_uid: str = "m1",
    player_a: str = "A",
    player_b: str = "B",
    rank_a: int | None = 100,
    rank_b: int | None = 100,
    surface: str = "Hard",
    round_name: str = "R32",
    match_date: date = date(2024, 1, 1),
) -> pl.DataFrame:
    """Helper to create a minimal two-row match DataFrame."""
    return pl.DataFrame(
        {
            "match_uid": [match_uid, match_uid],
            "player_id": [player_a, player_b],
            "opp_id": [player_b, player_a],
            "won": [True, False],
            "surface": [surface, surface],
            "round": [round_name, round_name],
            "round_order": [12, 12],
            "tournament_start_date": date(2020, 1, 1),
            "effective_match_date": [match_date, match_date],
            "player_rank": [rank_a, rank_b],
            "opp_rank": [rank_b, rank_a],
            "pts_service_pts_won": [None, None],
            "pts_service_pts_played": [None, None],
            "pts_return_pts_won": [None, None],
            "pts_return_pts_played": [None, None],
        }
    )


class TestBaseEloZeroSum:
    """Verify base Elo updates are zero-sum (no inflation)."""

    def test_zero_sum_single_match_equal_k(self):
        """Same-K players: Elo update is zero-sum, reversion adds expected pull."""
        df = _make_match_df(rank_a=100, rank_b=100)
        result = compute_elo_ratings(df)

        a_pre = result.filter(pl.col("player_id") == "A")["player_elo"][0]
        b_pre = result.filter(pl.col("player_id") == "B")["player_elo"][0]

        # Both start at same seeded Elo
        assert a_pre == b_pre

        # After one match, check the rating dict via a second match
        df2 = pl.concat([
            _make_match_df("m1", rank_a=100, rank_b=100),
            _make_match_df("m2", match_date=date(2024, 1, 2)),
        ])
        result2 = compute_elo_ratings(df2)

        a_post = result2.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]
        b_post = result2.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]

        a_delta = a_post - a_pre
        b_delta = b_post - b_pre
        # Elo update is zero-sum; reversion adds a pull toward DEFAULT_ELO
        expected_reversion = REVERSION_RATE * (2 * DEFAULT_ELO - a_pre - b_pre)
        assert abs(a_delta + b_delta - expected_reversion) < 1e-6, (
            f"Unexpected sum: A delta={a_delta}, B delta={b_delta}, "
            f"expected reversion={expected_reversion}"
        )

    def test_ordering_bug_regression(self):
        """Two players at different ratings — no leak from ordering beyond reversion."""
        df = pl.concat([
            _make_match_df("m1", rank_a=10, rank_b=200),
            _make_match_df("m2", rank_a=10, rank_b=200, match_date=date(2024, 1, 2)),
        ])
        result = compute_elo_ratings(df)

        a_pre = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m1")
        )["player_elo"][0]
        b_pre = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m1")
        )["player_elo"][0]

        a_post = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]
        b_post = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]

        a_delta = a_post - a_pre
        b_delta = b_post - b_pre
        expected_reversion = REVERSION_RATE * (2 * DEFAULT_ELO - a_pre - b_pre)
        assert abs(a_delta + b_delta - expected_reversion) < 1e-6, (
            f"Ordering leak: A delta={a_delta}, B delta={b_delta}, "
            f"sum={a_delta + b_delta}, expected_reversion={expected_reversion}"
        )


class TestPerPlayerKFactor:
    """Verify each player gets their own K-factor."""

    def test_newcomer_vs_veteran(self):
        """Newcomer (high K) and veteran (low K) get different updates."""
        # First give veteran 30+ matches so they're past NEW_PLAYER_THRESHOLD
        matches = []
        for i in range(31):
            matches.append(
                _make_match_df(
                    f"setup_{i}", player_a="VET", player_b=f"X{i}",
                    match_date=date(2024, 1, 1 + i),
                )
            )
        # Now match newcomer vs veteran
        matches.append(
            _make_match_df(
                "test_match", player_a="NEW", player_b="VET",
                match_date=date(2024, 3, 1),
            )
        )
        df = pl.concat(matches)
        result = compute_elo_ratings(df)

        # Get pre-match Elos for the test match
        new_pre = result.filter(
            (pl.col("player_id") == "NEW") & (pl.col("match_uid") == "test_match")
        )["player_elo"][0]
        vet_pre = result.filter(
            (pl.col("player_id") == "VET") & (pl.col("match_uid") == "test_match")
        )["player_elo"][0]

        # NEW wins with higher K -> gains more than VET loses
        # (because per-player K means different K for each side)
        new_delta = None
        vet_delta = None

        # We need a follow-up match to read post-match values
        matches.append(
            _make_match_df(
                "readout_new", player_a="NEW", player_b="DUMMY",
                match_date=date(2024, 3, 2),
            )
        )
        matches.append(
            _make_match_df(
                "readout_vet", player_a="VET", player_b="DUMMY2",
                match_date=date(2024, 3, 2),
            )
        )
        df2 = pl.concat(matches)
        result2 = compute_elo_ratings(df2)

        new_post = result2.filter(
            (pl.col("player_id") == "NEW") & (pl.col("match_uid") == "readout_new")
        )["player_elo"][0]
        vet_post = result2.filter(
            (pl.col("player_id") == "VET") & (pl.col("match_uid") == "readout_vet")
        )["player_elo"][0]

        new_delta = abs(new_post - new_pre)
        vet_delta = abs(vet_post - vet_pre)
        # Newcomer has higher K -> larger absolute delta
        assert new_delta > vet_delta, (
            f"Expected newcomer delta ({new_delta}) > veteran delta ({vet_delta})"
        )


class TestRowOrderIndependence:
    """Verify that row order within a match doesn't affect results."""

    def test_swapped_rows_same_elos(self):
        """Same match with A-first vs B-first row order gives identical Elos."""
        # Order 1: A row first
        df1 = pl.concat([
            _make_match_df("m1", rank_a=10, rank_b=200),
            _make_match_df("m2", rank_a=10, rank_b=200, match_date=date(2024, 1, 2)),
        ])

        # Order 2: B row first (swap rows within each match)
        rows = df1.to_dicts()
        swapped = [rows[1], rows[0], rows[3], rows[2]]
        df2 = pl.DataFrame(swapped)

        result1 = compute_elo_ratings(df1)
        result2 = compute_elo_ratings(df2)

        # Post-match Elos should be identical regardless of row order
        for pid in ["A", "B"]:
            elo1 = result1.filter(
                (pl.col("player_id") == pid) & (pl.col("match_uid") == "m2")
            )["player_elo"][0]
            elo2 = result2.filter(
                (pl.col("player_id") == pid) & (pl.col("match_uid") == "m2")
            )["player_elo"][0]
            assert abs(elo1 - elo2) < 1e-10, (
                f"Player {pid} Elo differs by row order: {elo1} vs {elo2}"
            )


class TestMeanReversion:
    """Verify per-match mean reversion toward DEFAULT_ELO."""

    def test_pulls_toward_default(self):
        """Player above 1500 gets pulled down, player below gets pulled up."""
        # A seeded high (rank 1 → ~2360), B unranked (→ 1300, below default)
        df = pl.concat([
            _make_match_df("m1", rank_a=1, rank_b=None),
            _make_match_df("m2", rank_a=1, rank_b=None, match_date=date(2024, 1, 2)),
        ])
        result = compute_elo_ratings(df)

        a_pre = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m1")
        )["player_elo"][0]
        a_post = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]

        b_pre = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m1")
        )["player_elo"][0]
        b_post = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]

        assert a_pre > DEFAULT_ELO, "A should be seeded above default"
        assert b_pre < DEFAULT_ELO, "B should be seeded below default"

        # The sum of deltas should reflect reversion pull
        a_delta = a_post - a_pre
        b_delta = b_post - b_pre
        expected_reversion = REVERSION_RATE * (2 * DEFAULT_ELO - a_pre - b_pre)
        assert abs(a_delta + b_delta - expected_reversion) < 1e-6

    def test_symmetric_both_players(self):
        """Both players in a match get reversion applied."""
        # Two matches: check that both players' Elos change by reversion
        df = pl.concat([
            _make_match_df("m1", rank_a=50, rank_b=50),
            _make_match_df("m2", rank_a=50, rank_b=50, match_date=date(2024, 1, 2)),
        ])
        result = compute_elo_ratings(df)

        a_pre = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m1")
        )["player_elo"][0]
        a_post = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]
        b_pre = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m1")
        )["player_elo"][0]
        b_post = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m2")
        )["player_elo"][0]

        # Both start above DEFAULT_ELO (rank 50 seeds ~2117)
        assert a_pre > DEFAULT_ELO
        assert b_pre > DEFAULT_ELO
        assert a_pre == b_pre

        # Winner gains less than K/2 (reversion drags down)
        # Loser loses more than K/2 effectively (Elo loss + reversion loss)
        # But both get reversion applied — verify the sum accounts for both
        expected_reversion = REVERSION_RATE * (2 * DEFAULT_ELO - a_pre - b_pre)
        a_delta = a_post - a_pre
        b_delta = b_post - b_pre
        assert abs(a_delta + b_delta - expected_reversion) < 1e-6

    def test_surface_adj_reverts_toward_zero(self):
        """Surface adjustments shrink toward zero after each match."""
        # Play two Clay matches to build up clay_adj, then check reversion
        df = pl.concat([
            _make_match_df("m1", surface="Clay", rank_a=100, rank_b=100),
            _make_match_df("m2", surface="Clay", rank_a=100, rank_b=100,
                           match_date=date(2024, 1, 2)),
            _make_match_df("m3", surface="Clay", rank_a=100, rank_b=100,
                           match_date=date(2024, 1, 3)),
        ])
        result = compute_elo_ratings(df)

        # After m1, A (winner) should have positive clay_adj
        a_m2_clay = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_clay_adj"][0]

        a_m3_clay = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m3")
        )["player_clay_adj"][0]

        # The adj from m1 should be non-zero
        assert a_m2_clay != 0.0

        # Hard adj was never played — should be 0 after m1 (no update),
        # but still gets reversion (which is 0 * (1-R) = 0)
        a_m2_hard = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_hard_adj"][0]
        assert a_m2_hard == 0.0, "Hard adj should stay 0 when only Clay played"
