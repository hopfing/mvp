"""Tests for the shared rating orchestrator."""

from datetime import date, timedelta

import polars as pl
import pytest

from mvp.atptour.elo.constants import SERVE_SEED_SHRINK
from mvp.atptour.elo.ratings import initialize_player
from mvp.atptour.glicko.constants import (
    GLICKO_REVERSION_RATE,
    INITIAL_MU,
    INITIAL_RD,
    INITIAL_SIGMA,
    TAU,
)
from mvp.atptour.glicko.ratings import glicko2_update
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
        "round_order": [12, 12, 7, 7, 9, 9],
        "tournament_start_date": date(2020, 1, 1),
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


class TestRatingColumns:
    """Column contract of the orchestrator's output.

    Previously this class also pinned the orchestrator equal to a duplicate
    implementation in mvp.atptour.elo.compute. That module has been deleted --
    it had drifted (flat vs RD-scaled reversion, no config threading) and its
    remaining behavioural tests were repointed here rather than dropped.
    """

    def test_all_rating_columns_present(self):
        df = _make_match_df()
        result = compute_all_ratings(df)
        for col in ALL_RATING_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"


class TestIndoorAdjustment:
    """Reconstructed indoor_adj: additive, opponent-adjusted, indoor-only."""

    @staticmethod
    def _indoor_df(indoor: bool) -> pl.DataFrame:
        # A (rank 50) is the stronger seed; plays B (rank 60) twice.
        return pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m2"],
            "player_id": ["A", "B", "A", "B"],
            "opp_id": ["B", "A", "B", "A"],
            "won": [True, False, True, False],
            "surface": ["Hard"] * 4,
            "round": ["R32"] * 4,
            "round_order": [7] * 4,
            "tournament_start_date": date(2020, 1, 1),
            "tournament_level": ["250"] * 4,
            "effective_match_date": [
                date(2024, 1, 1), date(2024, 1, 1),
                date(2024, 2, 1), date(2024, 2, 1),
            ],
            "player_rank": [50, 60, 50, 60],
            "opp_rank": [60, 50, 60, 50],
            "indoor": [indoor] * 4,
        })

    def test_indoor_win_moves_adjustment(self):
        # A wins the indoor m1; recorded pre-m2, A's indoor_adj is positive and
        # B's (the opponent) is negative.
        result = compute_all_ratings(self._indoor_df(indoor=True))
        m2_a = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )
        assert m2_a["player_indoor_adj"][0] > 0.0
        assert m2_a["opp_indoor_adj"][0] < 0.0

    def test_outdoor_leaves_indoor_adj_zero(self):
        # No indoor matches → indoor_adj is never updated, stays 0 everywhere.
        result = compute_all_ratings(self._indoor_df(indoor=False))
        assert all(v == 0.0 for v in result["player_indoor_adj"].to_list())
        assert all(v == 0.0 for v in result["opp_indoor_adj"].to_list())


class TestGlickoColumnsPresent:
    def test_glicko_columns_in_output(self):
        df = _make_match_df()
        result = compute_all_ratings(df)
        expected_cols = [
            "player_glicko_mu", "player_glicko_rd", "player_glicko_sigma",
            "player_glicko_hard_rd", "player_glicko_clay_rd", "player_glicko_grass_rd",
            "opp_glicko_mu", "opp_glicko_rd", "opp_glicko_sigma",
            "opp_glicko_hard_rd", "opp_glicko_clay_rd", "opp_glicko_grass_rd",
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

    def test_new_player_seeds_mu_from_rank(self):
        # Glicko mu is seeded from the rank-based Elo seed (not flat INITIAL_MU),
        # mirroring Elo; rd/sigma still start at defaults. A enters at rank 10.
        df = _make_match_df()
        result = compute_all_ratings(df)
        a_m1 = result.filter(
            (pl.col("player_id") == "A")
            & (pl.col("match_uid") == "m1")
        )
        assert a_m1["player_glicko_mu"][0] == initialize_player(10).elo
        assert a_m1["player_glicko_mu"][0] != INITIAL_MU
        assert a_m1["player_glicko_rd"][0] == INITIAL_RD
        assert a_m1["player_glicko_sigma"][0] == INITIAL_SIGMA


class TestGlickoMeanReversion:
    """Verify per-match RD-scaled mu reversion toward INITIAL_MU (mirrors Elo's
    TestMeanReversion). Guards the constant, the anchor, and the RD-scaling."""

    def test_reversion_applied_to_both_players_exactly(self):
        # m1: A (rank 10) beats B (rank 20); both new, so pre-match rd == INITIAL_RD
        # (reversion factor 1.0). Each player's post-m1 mu — recorded as their
        # pre-match mu at their NEXT appearance — must equal glicko2_update()
        # followed by the RD-scaled reversion toward INITIAL_MU.
        df = _make_match_df()
        result = compute_all_ratings(df)

        a_seed = initialize_player(10).elo
        b_seed = initialize_player(20).elo
        a_post, _, _ = glicko2_update(
            a_seed, INITIAL_RD, INITIAL_SIGMA, b_seed, INITIAL_RD, True, TAU)
        b_post, _, _ = glicko2_update(
            b_seed, INITIAL_RD, INITIAL_SIGMA, a_seed, INITIAL_RD, False, TAU)
        factor = INITIAL_RD / INITIAL_RD  # both new -> 1.0
        a_expected = a_post + GLICKO_REVERSION_RATE * factor * (INITIAL_MU - a_post)
        b_expected = b_post + GLICKO_REVERSION_RATE * factor * (INITIAL_MU - b_post)

        a_next = result.filter(
            (pl.col("player_id") == "A") & (pl.col("match_uid") == "m2")
        )["player_glicko_mu"][0]  # A's next match after m1
        b_next = result.filter(
            (pl.col("player_id") == "B") & (pl.col("match_uid") == "m3")
        )["player_glicko_mu"][0]  # B's next match after m1

        assert a_next == pytest.approx(a_expected, abs=1e-9)
        assert b_next == pytest.approx(b_expected, abs=1e-9)
        # reversion must actually move mu (guards against a no-op / zero constant)
        assert a_next != pytest.approx(a_post, abs=1e-9)


def _serve_history_df(stats_present: list[bool]) -> pl.DataFrame:
    """A vs B every 7 days; `stats_present[i]` says if match i carries serve stats.

    Dates stay tight on purpose. RD is capped at MAX_RD, so a long gap pushes
    both the base and the serve RD back to the ceiling and hides exactly the
    divergence these tests exist to detect.
    """
    rows = []
    for n, present in enumerate(stats_present):
        d = date(2020, 1, 6) + timedelta(days=7 * n)
        won, played = (60, 100) if present else (None, None)
        for pid, oid, w in (("A", "B", True), ("B", "A", False)):
            rows.append({
                "match_uid": f"m{n}", "player_id": pid, "opp_id": oid,
                "won": w, "surface": "Hard", "round": "R32", "round_order": 7,
                "tournament_start_date": d, "tournament_level": "250",
                "effective_match_date": d, "player_rank": 50, "opp_rank": 60,
                "pts_service_pts_won": won, "pts_service_pts_played": played,
                "opp_pts_service_pts_won": won,
                "opp_pts_service_pts_played": played,
                "pts_return_pts_won": None, "pts_return_pts_played": None,
                "indoor": False,
            })
    return pl.DataFrame(rows)


class TestServeRDIsIndependentOfBaseRD:
    """serve_rd tracks what the SERVE rating has learned, not match count.

    Before this, serve_rd and return_rd were decayed by the same unconditional
    rule as the base rd on every match — including matches carrying no serve
    stats, where the serve rating did not move at all. The three columns were
    bit-identical across the entire corpus, so nothing could distinguish a
    measured serve rating from an untouched one.
    """

    def _serve_rds(self, df: pl.DataFrame) -> list[float]:
        out = compute_all_ratings(df)
        a = out.filter(pl.col("player_id") == "A").sort("match_uid")
        return a["player_serve_elo_rd"].to_list()

    def _base_rds(self, df: pl.DataFrame) -> list[float]:
        out = compute_all_ratings(df)
        a = out.filter(pl.col("player_id") == "A").sort("match_uid")
        return a["player_elo_rd"].to_list()

    def test_tracks_base_rd_while_every_match_has_stats(self):
        """Control: with stats throughout, the two should still agree."""
        df = _serve_history_df([True] * 5)
        assert self._serve_rds(df) == pytest.approx(self._base_rds(df), abs=1e-9)

    def test_stops_decaying_once_serve_stats_stop(self):
        df = _serve_history_df([True, True, False, False, False, False])
        serve = self._serve_rds(df)
        base = self._base_rds(df)

        # Identical while both are learning.
        assert serve[:3] == pytest.approx(base[:3], abs=1e-9)
        # Base keeps falling; serve does not.
        assert base[-1] < base[2]
        assert serve[-1] > serve[2]

    def test_grows_on_its_own_clock_during_a_stats_drought(self):
        """The player keeps playing, so last_match_date keeps refreshing.

        Keyed to that clock, serve_rd would freeze rather than grow — the same
        lie as decaying it, relocated to the inactivity side.
        """
        df = _serve_history_df([True, True, False, False, False, False])
        serve = self._serve_rds(df)
        assert serve[3] < serve[4] < serve[5], serve


class TestServeSeedAndReversion:
    def test_serve_elo_seeded_from_rank_not_flat(self):
        """The rank seed reaches serve Elo, scaled by SERVE_SEED_SHRINK.

        Flat 1500 was the old behaviour and left a quarter of singles rows with
        a serve rating carrying no information at all.
        """
        df = _serve_history_df([True])
        out = compute_all_ratings(df)
        a = out.filter(pl.col("player_id") == "A")
        serve_elo = a["player_serve_elo"][0]
        elo = a["player_elo"][0]
        assert serve_elo > 1500.0
        assert serve_elo == pytest.approx(
            1500.0 + SERVE_SEED_SHRINK * (elo - 1500.0), abs=1e-6
        )

    def test_unmeasured_serve_rating_is_not_reverted(self):
        """Reversion must not erase a seed before serve data can test it.

        Reversion scales with rd and a fresh seed sits at the rd ceiling, so
        ungated it would pull hardest exactly when the rating knows least.
        """
        df = _serve_history_df([False, False, False])
        out = compute_all_ratings(df)
        a = out.filter(pl.col("player_id") == "A").sort("match_uid")
        vals = a["player_serve_elo"].to_list()
        assert vals[0] == pytest.approx(vals[-1], abs=1e-9), vals
