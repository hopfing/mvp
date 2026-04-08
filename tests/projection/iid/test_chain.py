"""Tests for IID/Markov tennis chain math.

Reference values for hold probability come from the standard tennis-math
literature (e.g., O'Malley 2008). Set/match invariants are checked via
symmetry, monotonicity, and conservation of probability mass.
"""

import numpy as np
import pytest

from mvp.model.features.iid import _iid_hold_probability
from mvp.projection.iid.chain import (
    SET_SCORE_LABELS,
    MatchDistribution,
    _scalar_tiebreak_win_prob_a_first,
    match_distribution,
    p_service_game_win,
    p_set_win,
    p_tiebreak_game_win,
    set_score_distribution,
)


# =============================================================================
# Game-level: hold probability
# =============================================================================


class TestHoldProbability:
    """Tests for p_service_game_win — must match the existing scalar implementation."""

    @pytest.mark.parametrize(
        "p,expected",
        [
            (0.0, 0.0),
            (0.5, 0.5),
            (0.6, 0.7357),
            (0.65, 0.8296),
            (0.70, 0.9008),
            (1.0, 1.0),
        ],
    )
    def test_reference_values(self, p, expected):
        actual = float(p_service_game_win(np.array([p]))[0])
        assert actual == pytest.approx(expected, abs=1e-3)

    def test_matches_existing_scalar(self):
        for p in [0.0, 0.1, 0.3, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.8, 0.9, 1.0]:
            expected = _iid_hold_probability(p)
            actual = float(p_service_game_win(np.array([p]))[0])
            assert actual == pytest.approx(expected, abs=1e-12)

    def test_vectorized(self):
        p = np.array([0.5, 0.6, 0.65, 0.7])
        result = p_service_game_win(p)
        assert result.shape == (4,)
        assert result[0] == pytest.approx(0.5, abs=1e-6)
        assert result[1] == pytest.approx(0.7357, abs=1e-3)

    def test_monotonic(self):
        p = np.linspace(0.0, 1.0, 50)
        h = p_service_game_win(p)
        assert np.all(np.diff(h) >= -1e-12)

    def test_endpoints_no_div_by_zero(self):
        # Both p=0 and p=1 should compute without nan/inf
        for p in [0.0, 1.0]:
            result = float(p_service_game_win(np.array([p]))[0])
            assert np.isfinite(result)


# =============================================================================
# Game-level: tiebreak game win prob
# =============================================================================


class TestTiebreakWinProb:
    """Tests for p_tiebreak_game_win — averaged over both first-server cases."""

    def test_equal_players_is_half(self):
        for p in [0.5, 0.55, 0.6, 0.65, 0.7]:
            actual = float(p_tiebreak_game_win(np.array([p]), np.array([p]))[0])
            assert actual == pytest.approx(0.5, abs=1e-12)

    def test_extremes(self):
        # A wins every point on serve, B wins zero → A wins
        assert float(p_tiebreak_game_win(np.array([1.0]), np.array([0.0]))[0]) == pytest.approx(1.0, abs=1e-9)
        # B wins every point on serve, A wins zero → B wins
        assert float(p_tiebreak_game_win(np.array([0.0]), np.array([1.0]))[0]) == pytest.approx(0.0, abs=1e-9)

    def test_anti_symmetry(self):
        # P(A wins | p_a, p_b) + P(A wins | p_b, p_a) == 1 (averaged over first-server)
        rng = np.random.default_rng(0)
        p_a = rng.uniform(0.4, 0.8, 20)
        p_b = rng.uniform(0.4, 0.8, 20)
        forward = p_tiebreak_game_win(p_a, p_b)
        backward = p_tiebreak_game_win(p_b, p_a)
        np.testing.assert_allclose(forward + backward, 1.0, atol=1e-12)

    def test_better_player_wins_more(self):
        # Holding p_b fixed at 0.6, increasing p_a non-strictly increases
        # P(A wins tiebreak). Use exact 0.01-grid values to avoid lookup-table
        # quantization artifacts.
        p_a = np.array([0.40, 0.50, 0.55, 0.58, 0.60, 0.62, 0.65, 0.70, 0.80])
        p_b = np.full_like(p_a, 0.60)
        result = p_tiebreak_game_win(p_a, p_b)
        assert np.all(np.diff(result) >= -1e-9)
        # Exactly at p_a = p_b = 0.60 the averaged tiebreak prob is 0.5
        assert float(p_tiebreak_game_win(np.array([0.60]), np.array([0.60]))[0]) == pytest.approx(0.5, abs=1e-12)
        # Asymmetric: A meaningfully better than B → A wins more often
        better = float(p_tiebreak_game_win(np.array([0.70]), np.array([0.60]))[0])
        assert better > 0.55

    def test_scalar_a_first_lookup_finite(self):
        # The scalar "A serves first" lookup is well-defined and ∈ [0, 1] for
        # several representative inputs. (We do NOT assert that A serving first
        # gives an edge with equal serve rates — over the relevant tiebreak
        # lengths the per-player serve fractions balance out.)
        for p_a, p_b in [(0.5, 0.5), (0.6, 0.6), (0.7, 0.5), (0.5, 0.7), (1.0, 0.0), (0.0, 1.0)]:
            v = _scalar_tiebreak_win_prob_a_first(p_a, p_b)
            assert 0.0 <= v <= 1.0


# =============================================================================
# Set-level: set score distribution and set win probability
# =============================================================================


class TestSetScoreDistribution:
    """Tests for set_score_distribution and p_set_win."""

    def test_label_count(self):
        assert len(SET_SCORE_LABELS) == 14

    def test_pmf_sums_to_one(self):
        rng = np.random.default_rng(1)
        h_a = rng.uniform(0.5, 0.95, 50)
        h_b = rng.uniform(0.5, 0.95, 50)
        t_ab = rng.uniform(0.2, 0.8, 50)
        pmf = set_score_distribution(h_a, h_b, t_ab)
        np.testing.assert_allclose(pmf.sum(axis=1), 1.0, atol=1e-12)

    def test_pmf_nonnegative(self):
        rng = np.random.default_rng(2)
        h_a = rng.uniform(0.5, 0.95, 50)
        h_b = rng.uniform(0.5, 0.95, 50)
        t_ab = rng.uniform(0.2, 0.8, 50)
        pmf = set_score_distribution(h_a, h_b, t_ab)
        assert (pmf >= -1e-15).all()

    def test_equal_players_set_win_half(self):
        # Equal h, fair tiebreak → P(A wins set) = 0.5
        h = np.array([0.6, 0.7, 0.8, 0.9])
        pmf = set_score_distribution(h, h, np.full_like(h, 0.5))
        p_a_wins = pmf[:, :7].sum(axis=1)
        np.testing.assert_allclose(p_a_wins, 0.5, atol=1e-12)

    def test_equal_players_mirror_symmetric(self):
        # Equal players → P(6-X) == P(X-6) for each X, P(7-5) == P(5-7), P(7-6) == P(6-7)
        h = np.array([0.7])
        pmf = set_score_distribution(h, h, np.array([0.5]))[0]
        # 6-0 ↔ 0-6
        assert pmf[0] == pytest.approx(pmf[7], abs=1e-12)
        # 6-1 ↔ 1-6
        assert pmf[1] == pytest.approx(pmf[8], abs=1e-12)
        # 6-2 ↔ 2-6
        assert pmf[2] == pytest.approx(pmf[9], abs=1e-12)
        # 6-3 ↔ 3-6
        assert pmf[3] == pytest.approx(pmf[10], abs=1e-12)
        # 6-4 ↔ 4-6
        assert pmf[4] == pytest.approx(pmf[11], abs=1e-12)
        # 7-5 ↔ 5-7
        assert pmf[5] == pytest.approx(pmf[12], abs=1e-12)
        # 7-6 ↔ 6-7
        assert pmf[6] == pytest.approx(pmf[13], abs=1e-12)

    def test_p_set_win_anti_symmetry(self):
        rng = np.random.default_rng(3)
        h_a = rng.uniform(0.5, 0.95, 30)
        h_b = rng.uniform(0.5, 0.95, 30)
        # If we swap players, t_ab also swaps to 1 - t_ab
        t_ab = rng.uniform(0.2, 0.8, 30)
        forward = p_set_win(h_a, h_b, t_ab)
        backward = p_set_win(h_b, h_a, 1.0 - t_ab)
        np.testing.assert_allclose(forward + backward, 1.0, atol=1e-12)

    def test_better_holder_wins_more_sets(self):
        h_a = np.linspace(0.6, 0.95, 20)
        h_b = np.full_like(h_a, 0.75)
        t_ab = np.full_like(h_a, 0.5)
        result = p_set_win(h_a, h_b, t_ab)
        assert np.all(np.diff(result) >= -1e-9)

    def test_empty_input(self):
        pmf = set_score_distribution(np.array([]), np.array([]), np.array([]))
        assert pmf.shape == (0, 14)


# =============================================================================
# Match-level: match distribution
# =============================================================================


class TestMatchDistribution:
    """Tests for match_distribution covering Bo3 and Bo5."""

    def _equal_players(self, n=5, h=0.75):
        h_a = np.full(n, h)
        h_b = np.full(n, h)
        t_ab = np.full(n, 0.5)
        return h_a, h_b, t_ab

    def test_pmfs_sum_to_one_bo3(self):
        h_a, h_b, t_ab = self._equal_players(n=10)
        best_of = np.full(10, 3, dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        np.testing.assert_allclose(dist.total_games_pmf.sum(axis=1), 1.0, atol=1e-12)
        np.testing.assert_allclose(dist.spread_pmf.sum(axis=1), 1.0, atol=1e-12)

    def test_pmfs_sum_to_one_bo5(self):
        h_a, h_b, t_ab = self._equal_players(n=10)
        best_of = np.full(10, 5, dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        np.testing.assert_allclose(dist.total_games_pmf.sum(axis=1), 1.0, atol=1e-12)
        np.testing.assert_allclose(dist.spread_pmf.sum(axis=1), 1.0, atol=1e-12)

    def test_set_outcomes_sum_to_one(self):
        h_a, h_b, t_ab = self._equal_players(n=10)
        best_of = np.array([3, 3, 3, 3, 3, 5, 5, 5, 5, 5], dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        total = np.zeros(10)
        for vec in dist.set_outcome_probs.values():
            total += vec
        np.testing.assert_allclose(total, 1.0, atol=1e-12)

    def test_match_win_prob_complementary(self):
        # For any match, P(A wins) + P(B wins) == 1
        rng = np.random.default_rng(4)
        n = 50
        p_a = rng.uniform(0.55, 0.75, n)
        p_b = rng.uniform(0.55, 0.75, n)
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        best_of = rng.choice([3, 5], n).astype(np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)

        p_b_wins = np.zeros(n)
        for (sa, sb), vec in dist.set_outcome_probs.items():
            if sb > sa:
                p_b_wins += vec
        np.testing.assert_allclose(dist.p_match_win_a + p_b_wins, 1.0, atol=1e-12)

    def test_equal_players_match_win_half_bo3(self):
        h_a, h_b, t_ab = self._equal_players(n=4)
        best_of = np.full(4, 3, dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        np.testing.assert_allclose(dist.p_match_win_a, 0.5, atol=1e-12)

    def test_equal_players_match_win_half_bo5(self):
        h_a, h_b, t_ab = self._equal_players(n=4)
        best_of = np.full(4, 5, dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        np.testing.assert_allclose(dist.p_match_win_a, 0.5, atol=1e-12)

    def test_compounding_bo3(self):
        # P(A wins Bo3 set) = p_set; P(A wins Bo3 match) = p_set^2 * (3 - 2*p_set)
        # When p_set > 0.5, the match prob should exceed p_set (compounding).
        p = 0.7
        p_a = np.array([p])
        p_b = np.array([1 - p])  # Asymmetric for clarity
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        p_set_a = p_set_win(h_a, h_b, t_ab)
        dist = match_distribution(h_a, h_b, t_ab, np.array([3]))
        # The match-level prob compounds vs the set-level prob
        assert dist.p_match_win_a[0] > p_set_a[0]
        # And the analytic form holds
        analytic_bo3 = p_set_a[0] ** 2 * (3 - 2 * p_set_a[0])
        assert dist.p_match_win_a[0] == pytest.approx(analytic_bo3, abs=1e-9)

    def test_bo5_amplifies_bo3(self):
        # The favorite should win Bo5 more often than Bo3 (longer chain compounds more)
        p_a = np.array([0.65])
        p_b = np.array([0.55])
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        bo3 = match_distribution(h_a, h_b, t_ab, np.array([3]))
        bo5 = match_distribution(h_a, h_b, t_ab, np.array([5]))
        assert bo5.p_match_win_a[0] > bo3.p_match_win_a[0]

    def test_p_over_total_monotone_in_line(self):
        h_a, h_b, t_ab = self._equal_players(n=3)
        best_of = np.full(3, 3, dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        lines = [10.5, 14.5, 18.5, 22.5, 26.5, 30.5]
        probs = [dist.p_over_total(line)[0] for line in lines]
        # As line increases, P(over) decreases
        assert all(probs[i] >= probs[i + 1] - 1e-12 for i in range(len(probs) - 1))
        # Very low line: near 1 (always over). Very high line: near 0.
        assert dist.p_over_total(0.5)[0] > 0.99
        assert dist.p_over_total(60.5)[0] < 1e-9

    def test_p_a_spread_cover_monotone(self):
        h_a, h_b, t_ab = self._equal_players(n=3)
        best_of = np.full(3, 3, dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        lines = [-10.5, -2.5, -0.5, 0.5, 2.5, 10.5]
        probs = [dist.p_a_spread_cover(line)[0] for line in lines]
        assert all(probs[i] >= probs[i + 1] - 1e-12 for i in range(len(probs) - 1))

    def test_expected_total_games_reasonable(self):
        # Two equal strong holders → most sets are long, many tiebreaks. Bo3
        # expected total should be in a sensible tennis-watching range.
        h_a, h_b, t_ab = self._equal_players(n=1, h=0.85)
        best_of = np.array([3], dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        assert 18.0 < dist.expected_total_games[0] < 32.0
        # And weaker holders → fewer tiebreaks, lower total
        h_low, _, _ = self._equal_players(n=1, h=0.55)
        dist_low = match_distribution(h_low, h_low, np.full(1, 0.5), best_of)
        assert dist_low.expected_total_games[0] < dist.expected_total_games[0]

    def test_expected_games_a_plus_b_equals_total(self):
        rng = np.random.default_rng(5)
        n = 20
        p_a = rng.uniform(0.55, 0.75, n)
        p_b = rng.uniform(0.55, 0.75, n)
        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        best_of = rng.choice([3, 5], n).astype(np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        np.testing.assert_allclose(
            dist.expected_games_a + dist.expected_games_b,
            dist.expected_total_games,
            atol=1e-12,
        )

    def test_mixed_best_of_routing(self):
        # Mix Bo3 and Bo5; ensure each row gets the right shape of distribution
        n = 10
        h_a = np.full(n, 0.78)
        h_b = np.full(n, 0.78)
        t_ab = np.full(n, 0.5)
        best_of = np.array([3, 5, 3, 5, 3, 5, 3, 5, 3, 5], dtype=np.int64)
        dist = match_distribution(h_a, h_b, t_ab, best_of)
        # Equal players → all match win probs are 0.5 regardless of best_of
        np.testing.assert_allclose(dist.p_match_win_a, 0.5, atol=1e-12)
        # Bo5 average total games > Bo3 average for the same players (more sets)
        bo3_idx = np.where(best_of == 3)[0]
        bo5_idx = np.where(best_of == 5)[0]
        assert dist.expected_total_games[bo5_idx].mean() > dist.expected_total_games[bo3_idx].mean()

    def test_invalid_best_of_raises(self):
        h_a, h_b, t_ab = self._equal_players(n=2)
        best_of = np.array([3, 4], dtype=np.int64)
        with pytest.raises(ValueError, match="best_of"):
            match_distribution(h_a, h_b, t_ab, best_of)

    def test_shape_mismatch_raises(self):
        with pytest.raises(ValueError, match="Shape mismatch"):
            match_distribution(
                np.array([0.7, 0.7]),
                np.array([0.7]),
                np.array([0.5, 0.5]),
                np.array([3, 3], dtype=np.int64),
            )

    def test_empty_input(self):
        dist = match_distribution(
            np.array([]), np.array([]), np.array([]), np.array([], dtype=np.int64),
        )
        assert isinstance(dist, MatchDistribution)
        assert dist.p_match_win_a.shape == (0,)
        assert dist.total_games_pmf.shape[0] == 0

    def test_single_match(self):
        dist = match_distribution(
            np.array([0.85]),
            np.array([0.7]),
            np.array([0.65]),
            np.array([3], dtype=np.int64),
        )
        assert dist.p_match_win_a.shape == (1,)
        assert 0.0 < dist.p_match_win_a[0] < 1.0
        # Better holder + better tiebreak → A is favored
        assert dist.p_match_win_a[0] > 0.5
