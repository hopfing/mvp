"""MOV-Elo variants (elo/mov.py + the compute_all_ratings seam).

Plan: mvp-docs/plans/2026-09-01-mov-elo-ratings.md rev 3."""

from datetime import date

import polars as pl
import pytest

from mvp.atptour.elo import mov
from mvp.atptour.elo.mov import (
    MovTracker,
    games_share,
    kmov_multiplier,
    margin_is_valid,
)
from mvp.atptour.ratings.compute import ALL_RATING_COLUMNS, compute_all_ratings


class TestUpdateMath:
    def test_shares_sum_to_one_including_clipped(self):
        for gw, gl in ((13, 11), (12, 0), (18, 6), (1, 1)):
            assert games_share(gw, gl) + games_share(gl, gw) == pytest.approx(1.0)

    def test_share_clipped_off_extremes(self):
        assert games_share(12, 0) == 1.0 - mov.SHARE_CLIP
        assert games_share(0, 12) == mov.SHARE_CLIP

    def test_multiplier_bounded_monotone_and_unit_at_even(self):
        assert kmov_multiplier(0.5) == 1.0
        vals = [kmov_multiplier(0.5 + x / 100) for x in range(0, 51, 5)]
        assert vals == sorted(vals)
        assert max(vals) <= mov.KMOV_CAP
        assert kmov_multiplier(0.2) == kmov_multiplier(0.8)  # symmetric

    def test_margin_validity(self):
        assert margin_is_valid(24, None, None)
        assert not margin_is_valid(0, None, None)  # the literal 0/0
        assert not margin_is_valid(None, None, None)
        assert not margin_is_valid(18, "RET", None)
        assert not margin_is_valid(24, "W/O", None)
        # result_type-only walkover (reason null) — the completeness.py case
        assert not margin_is_valid(4, None, "walkover")
        # unflagged legacy row with games present is valid; with zero games not
        assert not margin_is_valid(0, None, None)

    def _fresh_pair(self):
        t = MovTracker()
        t.ensure_player("A", 1500.0)
        t.ensure_player("B", 1500.0)
        return t

    def _delta(self, tracker, gw, gl, valid=True):
        tracker.update_match(
            "A", "B", True, "R32", "250", gw, gl, valid, date(2024, 1, 1)
        )
        return {v: tracker._state[v]["A"].rating - 1500.0 for v in tracker.variants}

    def test_double_bagel_moves_more_than_tight_win(self):
        blowout = self._delta(self._fresh_pair(), 12, 0)
        tight = self._delta(self._fresh_pair(), 13, 11)
        assert blowout["melo"] > tight["melo"] > 0
        assert blowout["kmov"] > tight["kmov"] > 0

    def test_loser_with_many_games_loses_little_under_melo(self):
        near = self._fresh_pair()
        near.update_match(
            "A", "B", False, "R32", "250", 11, 13, True, date(2024, 1, 1)
        )
        crushed = self._fresh_pair()
        crushed.update_match(
            "A", "B", False, "R32", "250", 0, 12, True, date(2024, 1, 1)
        )
        assert near._state["melo"]["A"].rating > crushed._state["melo"]["A"].rating

    def test_invalid_margin_falls_back_to_identical_binary_update(self):
        """ALL variants (kflat included) reduce to the SAME standard binary
        update on an invalid margin — so their ratings coincide exactly."""
        t = self._fresh_pair()
        t.update_match("A", "B", True, "F", "GS", 0, 0, False, date(2024, 1, 1))
        assert (
            t._state["melo"]["A"].rating
            == t._state["kmov"]["A"].rating
            == t._state["kflat"]["A"].rating
        )
        assert t._state["melo"]["A"].rating > 1500.0  # it still updates

    def test_melo_scale_exact_on_valid_margin_only(self):
        """The rescale applies to margin-valid updates exactly (manual
        recomputation with the same primitives) and NOT to the fallback —
        which the fallback-identity test above already pins at unscaled."""
        from mvp.atptour.elo.constants import DEFAULT_ELO, DEFAULT_RD, REVERSION_RATE
        from mvp.atptour.elo.ratings import k_factor_from

        t = self._fresh_pair()
        t.update_match("A", "B", True, "R32", "250", 13, 11, True, date(2024, 1, 1))
        k = k_factor_from(DEFAULT_RD, 0, "R32", "250")
        raw = 1500.0 + k * mov.MELO_K_SCALE * (games_share(13, 11) - 0.5)
        rev = REVERSION_RATE * 1.0  # rd == DEFAULT_RD at first update
        expected = raw + rev * (DEFAULT_ELO - raw)
        assert t._state["melo"]["A"].rating == pytest.approx(expected)

    def test_kflat_boost_is_margin_independent(self):
        """The ablation's whole point: kflat's delta is identical for a
        blowout and a tight win (same K, same E) — only kmov's varies."""
        blowout = self._delta(self._fresh_pair(), 12, 0)
        tight = self._delta(self._fresh_pair(), 13, 11)
        assert blowout["kflat"] == pytest.approx(tight["kflat"])
        assert blowout["kmov"] > tight["kmov"]
        # and it IS boosted relative to the binary fallback on valid rows
        binary = self._delta(self._fresh_pair(), 12, 0, valid=False)
        assert tight["kflat"] > binary["kflat"]


def _mov_match_df() -> pl.DataFrame:
    base = {
        "match_uid": ["m1", "m1", "m2", "m2", "m3", "m3", "m4", "m4"],
        "player_id": ["A", "B", "C", "A", "B", "C", "A", "B"],
        "opp_id": ["B", "A", "A", "C", "C", "B", "B", "A"],
        "won": [True, False, True, False, True, False, True, False],
        "surface": ["Hard"] * 8,
        "round": ["F", "F", "R32", "R32", "QF", "QF", "R16", "R16"],
        "round_order": [12, 12, 7, 7, 9, 9, 8, 8],
        "tournament_start_date": date(2020, 1, 1),
        "tournament_level": ["250"] * 8,
        "effective_match_date": [
            date(2024, 1, 1), date(2024, 1, 1),
            date(2024, 2, 1), date(2024, 2, 1),
            date(2024, 3, 1), date(2024, 3, 1),
            date(2024, 4, 1), date(2024, 4, 1),
        ],
        "player_rank": [10, 20, 30, 10, 20, 30, 10, 20],
        "opp_rank": [20, 10, 10, 30, 30, 20, 20, 10],
        "indoor": [False] * 8,
        # m1 complete blowout; m2 tight; m3 result_type-only walkover with
        # zero games; m4 unflagged legacy zero-games row.
        "player_set1_games": [6, 0, 7, 5, 0, None, None, None],
        "player_set2_games": [6, 0, 6, 7, None, None, None, None],
        "opp_set1_games": [0, 6, 5, 7, None, 0, None, None],
        "opp_set2_games": [0, 6, 7, 6, None, None, None, None],
        "reason": [None] * 8,
        "result_type": [None, None, None, None, "walkover", "walkover", None, None],
    }
    return pl.DataFrame(base)


class TestSeam:
    def test_default_path_untouched_by_tracker(self):
        """The pipeline-safety invariant: every EXISTING rating column is
        byte-identical whether or not a tracker runs alongside."""
        df = _mov_match_df()
        plain = compute_all_ratings(df)
        with_mov = compute_all_ratings(df, mov_tracker=MovTracker())
        for col in ALL_RATING_COLUMNS:
            assert plain[col].to_list() == with_mov[col].to_list(), col

    def test_mov_columns_present_prematch_and_consistent(self):
        df = _mov_match_df()
        out = compute_all_ratings(df, mov_tracker=MovTracker())
        for col in ("player_melo", "opp_melo", "player_kmov", "opp_kmov"):
            assert col in out.columns
        # PRE-match: first appearance carries the rank-based seed, and the two
        # orientation rows of one match agree.
        m1 = out.filter(pl.col("match_uid") == "m1")
        a_row = m1.filter(pl.col("player_id") == "A")
        b_row = m1.filter(pl.col("player_id") == "B")
        assert a_row["player_melo"][0] == b_row["opp_melo"][0]
        assert a_row["opp_kmov"][0] == b_row["player_kmov"][0]

    def test_zero_game_rows_stay_finite_and_update(self):
        df = _mov_match_df()
        out = compute_all_ratings(df, mov_tracker=MovTracker())
        assert out["player_melo"].is_not_null().all()
        assert out["player_kmov"].is_not_null().all()
        # A appears in m1 (seed), m2, and m4; the m4 pre-match value must
        # differ from the seed (earlier matches updated) and be finite.
        a_vals = out.filter(pl.col("player_id") == "A")["player_melo"].to_list()
        assert a_vals[0] != a_vals[-1]

    def test_deterministic_rerun(self):
        df = _mov_match_df()
        a = compute_all_ratings(df, mov_tracker=MovTracker())
        b = compute_all_ratings(df, mov_tracker=MovTracker())
        assert a["player_melo"].to_list() == b["player_melo"].to_list()
        assert a["player_kmov"].to_list() == b["player_kmov"].to_list()

    def test_missing_games_columns_refused(self):
        df = _mov_match_df().drop(
            [f"player_set{s}_games" for s in (1, 2)]
            + [f"opp_set{s}_games" for s in (1, 2)]
        )
        with pytest.raises(ValueError, match="per-set games"):
            compute_all_ratings(df, mov_tracker=MovTracker())

    def test_missing_incomplete_guard_columns_refused(self):
        """reason/result_type absent must raise, not silently degrade the
        guard to zero-games-only (a retirement's partial margin would feed
        the update)."""
        with pytest.raises(ValueError, match="result_type"):
            compute_all_ratings(
                _mov_match_df().drop("result_type"), mov_tracker=MovTracker()
            )
        with pytest.raises(ValueError, match="reason"):
            compute_all_ratings(
                _mov_match_df().drop("reason"), mov_tracker=MovTracker()
            )

    def test_single_variant_tracker_is_the_production_shape(self):
        """The pipeline runs MovTracker(variants=("melo",)) — assert that
        shape end-to-end: only melo columns, existing columns untouched,
        melo values identical to a full tracker's melo."""
        df = _mov_match_df()
        plain = compute_all_ratings(df)
        solo = compute_all_ratings(
            df, mov_tracker=MovTracker(variants=("melo",))
        )
        both = compute_all_ratings(df, mov_tracker=MovTracker())
        assert "player_melo" in solo.columns and "opp_melo" in solo.columns
        assert "player_kmov" not in solo.columns
        for col in ALL_RATING_COLUMNS:
            assert plain[col].to_list() == solo[col].to_list(), col
        assert solo["player_melo"].to_list() == both["player_melo"].to_list()

    def test_diagnostics_populated(self):
        tracker = MovTracker()
        compute_all_ratings(_mov_match_df(), mov_tracker=tracker)
        diag = tracker.diagnostics()
        assert diag["n_margin_valid"] == 2  # m1 and m2 only
        assert 0 < diag["melo_mean_abs_s_minus_e"] < 1
