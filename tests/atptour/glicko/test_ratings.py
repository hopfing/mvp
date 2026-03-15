import math
from datetime import date

import pytest

from mvp.atptour.glicko.constants import (
    INITIAL_MU,
    INITIAL_RD,
    INITIAL_SIGMA,
    MAX_RD,
    MIN_RD,
    SCALE,
    TAU,
)
from mvp.atptour.glicko.ratings import (
    GlickoRating,
    apply_glicko_inactivity,
    expected_score,
    from_glicko2,
    g,
    glicko2_update,
    glicko2_update_surface,
    to_glicko2,
)


class TestGlickoRatingDefaults:
    def test_initial_values(self):
        r = GlickoRating()
        assert r.mu == INITIAL_MU
        assert r.rd == INITIAL_RD
        assert r.sigma == INITIAL_SIGMA
        assert r.match_count == 0
        assert r.last_match_date is None

    def test_surface_defaults(self):
        r = GlickoRating()
        assert r.hard_adj == 0.0
        assert r.hard_rd == INITIAL_RD
        assert r.hard_sigma == INITIAL_SIGMA
        assert r.clay_adj == 0.0
        assert r.grass_adj == 0.0

    def test_get_surface_adj(self):
        r = GlickoRating(hard_adj=50.0, clay_adj=-30.0)
        assert r.get_surface_adj("Hard") == 50.0
        assert r.get_surface_adj("Clay") == -30.0
        assert r.get_surface_adj("Carpet") == 0.0

    def test_effective_surface_mu(self):
        r = GlickoRating(mu=1600.0, hard_adj=50.0)
        assert r.effective_surface_mu("Hard") == 1650.0
        assert r.effective_surface_mu("Carpet") == 1600.0


class TestScaleConversion:
    def test_round_trip(self):
        """Converting to Glicko-2 scale and back recovers original values."""
        mu, rd = 1700.0, 200.0
        mu_g2, rd_g2 = to_glicko2(mu, rd)
        mu_back, rd_back = from_glicko2(mu_g2, rd_g2)
        assert abs(mu_back - mu) < 1e-6
        assert abs(rd_back - rd) < 1e-6

    def test_1500_maps_to_zero(self):
        mu_g2, _ = to_glicko2(1500.0, 350.0)
        assert abs(mu_g2) < 1e-6

    def test_rd_scaling(self):
        _, rd_g2 = to_glicko2(1500.0, SCALE)
        assert abs(rd_g2 - 1.0) < 1e-6


class TestGFunction:
    def test_g_zero_rd_is_one(self):
        """g(0) = 1 (certain opponent has no discounting)."""
        assert abs(g(0.0) - 1.0) < 1e-6

    def test_g_decreases_with_rd(self):
        """Higher RD means more discounting."""
        assert g(0.5) < g(0.1)

    def test_g_is_positive(self):
        assert g(2.0) > 0.0


class TestExpectedScore:
    def test_equal_ratings_gives_half(self):
        """Equal ratings should give expected score of 0.5."""
        e = expected_score(0.0, 0.0, 1.0)
        assert abs(e - 0.5) < 1e-6

    def test_higher_rating_favored(self):
        """Higher-rated player should have E > 0.5."""
        e = expected_score(1.0, 0.0, 1.0)
        assert e > 0.5

    def test_matches_elo_at_zero_rd(self):
        """At phi_opp=0, g=1, so E reduces to standard logistic (Elo-like)."""
        mu, opp_mu = 1.0, 0.0
        e = expected_score(mu, opp_mu, 0.0)
        elo_e = 1.0 / (1.0 + math.exp(-(mu - opp_mu)))
        assert abs(e - elo_e) < 1e-6


class TestGlicko2Update:
    """Tests for the core Glicko-2 single-match update."""

    def test_winner_gains_loser_loses(self):
        mu, rd, sigma = 1500.0, 200.0, 0.06
        opp_mu, opp_rd = 1500.0, 200.0

        w_mu, w_rd, w_sigma = glicko2_update(mu, rd, sigma, opp_mu, opp_rd, True, TAU)
        l_mu, l_rd, l_sigma = glicko2_update(mu, rd, sigma, opp_mu, opp_rd, False, TAU)

        assert w_mu > mu, "Winner should gain rating"
        assert l_mu < mu, "Loser should lose rating"

    def test_rd_decreases_after_match(self):
        mu, rd, sigma = 1500.0, 200.0, 0.06
        _, new_rd, _ = glicko2_update(mu, rd, sigma, 1500.0, 200.0, True, TAU)
        assert new_rd < rd, "RD should decrease after a match"

    def test_rd_bounded(self):
        """RD stays within [MIN_RD, MAX_RD]."""
        mu, rd, sigma = 1500.0, 350.0, 0.06
        _, new_rd, _ = glicko2_update(mu, rd, sigma, 1500.0, 350.0, True, TAU)
        assert MIN_RD <= new_rd <= MAX_RD

    def test_upset_increases_volatility(self):
        """Strong favorite losing should increase sigma."""
        _, _, sigma_expected = glicko2_update(
            1800.0, 100.0, 0.06, 1200.0, 100.0, True, TAU
        )
        _, _, sigma_upset = glicko2_update(
            1800.0, 100.0, 0.06, 1200.0, 100.0, False, TAU
        )
        assert sigma_upset > sigma_expected, "Upset should increase volatility"

    def test_expected_result_decreases_volatility(self):
        """Strong favorite winning should decrease or maintain sigma."""
        _, _, sigma_new = glicko2_update(
            1800.0, 100.0, 0.06, 1200.0, 100.0, True, TAU
        )
        assert sigma_new <= 0.06 + 1e-6, (
            "Expected result should not increase volatility"
        )

    def test_approximately_zero_sum(self):
        """Mu updates should be approximately zero-sum (not exactly due to RD)."""
        mu, rd, sigma = 1500.0, 200.0, 0.06
        w_mu, _, _ = glicko2_update(mu, rd, sigma, mu, rd, True, TAU)
        l_mu, _, _ = glicko2_update(mu, rd, sigma, mu, rd, False, TAU)
        gain = w_mu - mu
        loss = mu - l_mu
        assert abs(gain - loss) / gain < 0.05

    def test_high_rd_means_larger_update(self):
        """Player with higher RD should get a larger mu update."""
        low_rd_mu, _, _ = glicko2_update(
            1500.0, 100.0, 0.06, 1500.0, 200.0, True, TAU
        )
        high_rd_mu, _, _ = glicko2_update(
            1500.0, 300.0, 0.06, 1500.0, 200.0, True, TAU
        )
        assert (high_rd_mu - 1500.0) > (low_rd_mu - 1500.0)

    def test_reference_values(self):
        """Verify exact output against known-correct Glicko-2 computation.

        For a symmetric match (1500/200 vs 1500/200), the Glicko-2 math gives
        phi=200/173.7178=1.1513, g(phi)=0.8443, E=0.5, v=5.6116, delta=2.3689.
        After the Illinois step and scale conversion, new_mu≈1578.8 and new_rd≈180.1.
        """
        new_mu, new_rd, new_sigma = glicko2_update(
            1500.0, 200.0, 0.06, 1500.0, 200.0, True, TAU
        )
        assert new_mu == pytest.approx(1578.8, abs=1.0)
        assert new_rd == pytest.approx(180.1, abs=2.0)
        assert new_sigma == pytest.approx(0.06, abs=0.005)


class TestGlicko2UpdateSurface:
    def test_winner_adj_increases(self):
        """Winning on surface should increase surface adjustment."""
        new_adj, _, _ = glicko2_update_surface(
            adj=0.0, adj_rd=200.0, adj_sigma=0.06,
            player_mu=1500.0, opp_mu=1500.0, opp_rd=200.0,
            won=True, tau=TAU,
        )
        assert new_adj > 0.0

    def test_loser_adj_decreases(self):
        new_adj, _, _ = glicko2_update_surface(
            adj=0.0, adj_rd=200.0, adj_sigma=0.06,
            player_mu=1500.0, opp_mu=1500.0, opp_rd=200.0,
            won=False, tau=TAU,
        )
        assert new_adj < 0.0

    def test_adj_rd_decreases(self):
        """Surface RD should decrease after a match."""
        _, new_rd, _ = glicko2_update_surface(
            adj=0.0, adj_rd=200.0, adj_sigma=0.06,
            player_mu=1500.0, opp_mu=1500.0, opp_rd=200.0,
            won=True, tau=TAU,
        )
        assert new_rd < 200.0

    def test_uses_base_mu_not_effective(self):
        """Expected score should be based on base mus, not effective mus."""
        adj1, _, _ = glicko2_update_surface(
            adj=0.0, adj_rd=200.0, adj_sigma=0.06,
            player_mu=1600.0, opp_mu=1400.0, opp_rd=200.0,
            won=True, tau=TAU,
        )
        adj2, _, _ = glicko2_update_surface(
            adj=100.0, adj_rd=200.0, adj_sigma=0.06,
            player_mu=1600.0, opp_mu=1400.0, opp_rd=200.0,
            won=True, tau=TAU,
        )
        assert adj1 > 0.0
        assert (adj2 - 100.0) == pytest.approx(adj1, abs=1e-6)


class TestGlickoInactivity:
    def test_rd_grows_with_inactivity(self):
        new_rd = apply_glicko_inactivity(
            rd=100.0, sigma=0.06,
            last_date=date(2024, 1, 1), current_date=date(2024, 4, 1),
        )
        assert new_rd > 100.0

    def test_capped_at_max_rd(self):
        new_rd = apply_glicko_inactivity(
            rd=300.0, sigma=0.06,
            last_date=date(2020, 1, 1), current_date=date(2024, 1, 1),
        )
        assert new_rd == MAX_RD

    def test_no_change_for_none_last_date(self):
        new_rd = apply_glicko_inactivity(
            rd=100.0, sigma=0.06,
            last_date=None, current_date=date(2024, 1, 1),
        )
        assert new_rd == 100.0

    def test_no_change_for_same_day(self):
        new_rd = apply_glicko_inactivity(
            rd=100.0, sigma=0.06,
            last_date=date(2024, 1, 1), current_date=date(2024, 1, 1),
        )
        assert new_rd == 100.0

    def test_higher_sigma_means_faster_growth(self):
        """Player with higher volatility should have faster RD growth."""
        rd1 = apply_glicko_inactivity(
            rd=100.0, sigma=0.04,
            last_date=date(2024, 1, 1), current_date=date(2024, 7, 1),
        )
        rd2 = apply_glicko_inactivity(
            rd=100.0, sigma=0.10,
            last_date=date(2024, 1, 1), current_date=date(2024, 7, 1),
        )
        assert rd2 > rd1
