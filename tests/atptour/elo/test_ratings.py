from datetime import date

from mvp.atptour.elo.constants import (
    BASE_K,
    DEFAULT_ELO,
    DEFAULT_RD,
    HIGH_RD_K_MULT,
    MIN_RD,
    NEW_PLAYER_K_MULT,
)
from mvp.atptour.elo.ratings import (
    PlayerRating,
    apply_inactivity_rd,
    expected_score,
    get_k_factor,
    update_elo,
    update_rd,
)


class TestPlayerRatingDefaults:
    """Test default values for PlayerRating."""

    def test_default_elo(self):
        rating = PlayerRating()
        assert rating.elo == DEFAULT_ELO

    def test_default_rd(self):
        rating = PlayerRating()
        assert rating.rd == DEFAULT_RD

    def test_default_surface_adjustments(self):
        rating = PlayerRating()
        assert rating.hard_adj == 0.0
        assert rating.clay_adj == 0.0
        assert rating.grass_adj == 0.0

    def test_default_serve_return_elo(self):
        rating = PlayerRating()
        assert rating.serve_elo == DEFAULT_ELO
        assert rating.serve_rd == DEFAULT_RD
        assert rating.return_elo == DEFAULT_ELO
        assert rating.return_rd == DEFAULT_RD

    def test_default_match_count(self):
        rating = PlayerRating()
        assert rating.match_count == 0

    def test_default_last_match_date(self):
        rating = PlayerRating()
        assert rating.last_match_date is None


class TestEffectiveSurfaceElo:
    """Test effective_surface_elo calculation."""

    def test_hard_surface(self):
        rating = PlayerRating(elo=1600.0, hard_adj=50.0)
        assert rating.effective_surface_elo("Hard") == 1650.0

    def test_clay_surface(self):
        rating = PlayerRating(elo=1600.0, clay_adj=-30.0)
        assert rating.effective_surface_elo("Clay") == 1570.0

    def test_grass_surface(self):
        rating = PlayerRating(elo=1600.0, grass_adj=25.0)
        assert rating.effective_surface_elo("Grass") == 1625.0

    def test_no_adjustment(self):
        rating = PlayerRating(elo=1600.0)
        assert rating.effective_surface_elo("Hard") == 1600.0


class TestGetSurfaceAdj:
    """Test get_surface_adj for various surfaces."""

    def test_known_surfaces(self):
        rating = PlayerRating(hard_adj=10.0, clay_adj=20.0, grass_adj=30.0)
        assert rating.get_surface_adj("Hard") == 10.0
        assert rating.get_surface_adj("Clay") == 20.0
        assert rating.get_surface_adj("Grass") == 30.0

    def test_unknown_surface_returns_zero(self):
        rating = PlayerRating(hard_adj=10.0, clay_adj=20.0, grass_adj=30.0)
        assert rating.get_surface_adj("Carpet") == 0.0
        assert rating.get_surface_adj("Indoor") == 0.0
        assert rating.get_surface_adj("Unknown") == 0.0
        assert rating.get_surface_adj("") == 0.0


class TestPlayerRatingCustomValues:
    """Test PlayerRating with custom initialization values."""

    def test_custom_values(self):
        rating = PlayerRating(
            elo=1800.0,
            rd=100.0,
            hard_adj=50.0,
            clay_adj=-25.0,
            grass_adj=10.0,
            serve_elo=1700.0,
            serve_rd=150.0,
            return_elo=1650.0,
            return_rd=175.0,
            match_count=50,
            last_match_date=date(2026, 1, 15),
        )
        assert rating.elo == 1800.0
        assert rating.rd == 100.0
        assert rating.hard_adj == 50.0
        assert rating.clay_adj == -25.0
        assert rating.grass_adj == 10.0
        assert rating.serve_elo == 1700.0
        assert rating.serve_rd == 150.0
        assert rating.return_elo == 1650.0
        assert rating.return_rd == 175.0
        assert rating.match_count == 50
        assert rating.last_match_date == date(2026, 1, 15)


class TestKFactor:
    """Test dynamic K-factor calculation."""

    def test_base_k_for_established_player(self):
        """Established player (match_count >= 30, rd <= 200, R32) gets base K."""
        player = PlayerRating(match_count=30, rd=200.0)
        k = get_k_factor(player, "R32")
        assert k == BASE_K

    def test_new_player_multiplier(self):
        """New player (match_count < 30) gets 1.5x multiplier."""
        player = PlayerRating(match_count=29, rd=200.0)
        k = get_k_factor(player, "R32")
        assert k == BASE_K * NEW_PLAYER_K_MULT

    def test_high_rd_multiplier(self):
        """High RD (rd > 200) gets 1.2x multiplier."""
        player = PlayerRating(match_count=30, rd=201.0)
        k = get_k_factor(player, "R32")
        assert k == BASE_K * HIGH_RD_K_MULT

    def test_finals_importance(self):
        """Finals gets 1.3x multiplier."""
        player = PlayerRating(match_count=30, rd=200.0)
        k = get_k_factor(player, "F")
        assert k == BASE_K * 1.3

    def test_qualifying_importance(self):
        """Qualifying rounds get 0.85x multiplier."""
        player = PlayerRating(match_count=30, rd=200.0)
        k = get_k_factor(player, "Q1")
        assert k == BASE_K * 0.85

    def test_combined_multipliers(self):
        """New player + high RD + finals combines all multipliers."""
        player = PlayerRating(match_count=10, rd=250.0)
        k = get_k_factor(player, "F")
        expected = BASE_K * NEW_PLAYER_K_MULT * HIGH_RD_K_MULT * 1.3
        assert k == expected

    def test_unknown_round_uses_default_importance(self):
        """Unknown round name uses default importance of 1.0."""
        player = PlayerRating(match_count=30, rd=200.0)
        k = get_k_factor(player, "UNKNOWN")
        assert k == BASE_K


class TestExpectedScore:
    """Test expected_score calculation."""

    def test_equal_ratings(self):
        assert expected_score(1500, 1500) == 0.5

    def test_higher_rating_favored(self):
        exp = expected_score(1700, 1500)
        assert 0.75 < exp < 0.77  # ~76% for 200 point diff

    def test_lower_rating_underdog(self):
        exp = expected_score(1500, 1700)
        assert 0.23 < exp < 0.25


class TestUpdateElo:
    """Test update_elo calculation."""

    def test_winner_gains_points(self):
        player = PlayerRating(elo=1500.0, rd=100.0, match_count=50)
        opponent = PlayerRating(elo=1500.0)
        new_elo = update_elo(player, opponent, won=True, k=32.0)
        assert new_elo > 1500.0

    def test_loser_loses_points(self):
        player = PlayerRating(elo=1500.0, rd=100.0, match_count=50)
        opponent = PlayerRating(elo=1500.0)
        new_elo = update_elo(player, opponent, won=False, k=32.0)
        assert new_elo < 1500.0

    def test_upset_larger_swing(self):
        # Underdog wins - should gain more than favorite winning
        underdog = PlayerRating(elo=1400.0)
        favorite = PlayerRating(elo=1600.0)
        underdog_win_gain = update_elo(underdog, favorite, won=True, k=32.0) - 1400.0
        favorite_win_gain = update_elo(favorite, underdog, won=True, k=32.0) - 1600.0
        assert underdog_win_gain > favorite_win_gain

    def test_surface_elo_used_when_provided(self):
        # Player has clay advantage
        player = PlayerRating(elo=1500.0, clay_adj=100.0)
        opponent = PlayerRating(elo=1600.0)
        # Without surface: player is underdog
        # With clay: player has 1600 vs 1600 - even match
        gain_no_surface = update_elo(player, opponent, won=True, k=32.0) - 1500.0
        gain_clay = (
            update_elo(player, opponent, won=True, k=32.0, surface="Clay") - 1500.0
        )
        assert gain_no_surface > gain_clay  # Upset gives more points


class TestUpdateSurfaceAdj:
    """Test update_surface_adj calculation."""

    def test_clay_specialist_gains_on_clay_win(self):
        from mvp.atptour.elo.ratings import update_surface_adj

        player = PlayerRating(elo=1600.0, clay_adj=50.0)
        opponent = PlayerRating(elo=1600.0, clay_adj=0.0)
        new_adj = update_surface_adj(player, opponent, won=True, surface="Clay", k=16.0)
        assert new_adj > 50.0

    def test_clay_specialist_loses_on_hard(self):
        from mvp.atptour.elo.ratings import update_surface_adj

        player = PlayerRating(elo=1600.0, hard_adj=-20.0)
        opponent = PlayerRating(elo=1600.0)
        new_adj = update_surface_adj(player, opponent, won=False, surface="Hard", k=16.0)
        assert new_adj < -20.0

    def test_unknown_surface_returns_zero(self):
        from mvp.atptour.elo.ratings import update_surface_adj

        player = PlayerRating()
        opponent = PlayerRating()
        new_adj = update_surface_adj(player, opponent, won=True, surface="Carpet", k=16.0)
        assert new_adj == 0.0

    def test_all_surfaces_work(self):
        from mvp.atptour.elo.ratings import update_surface_adj

        player = PlayerRating()
        opponent = PlayerRating()
        for surface in ["Hard", "Clay", "Grass"]:
            adj = update_surface_adj(player, opponent, won=True, surface=surface, k=16.0)
            assert adj > 0.0  # Win from even position should increase adj


class TestUpdateRD:
    """Test update_rd calculation."""

    def test_rd_decreases_after_match(self):
        new_rd = update_rd(200.0)
        assert new_rd < 200.0
        assert new_rd == 200.0 * 0.95

    def test_rd_has_minimum(self):
        new_rd = update_rd(50.0)
        assert new_rd == MIN_RD  # can't go below minimum

    def test_rd_respects_minimum_on_decay(self):
        new_rd = update_rd(52.0)
        # 52.0 * 0.95 = 49.4, but clamped to MIN_RD (50)
        assert new_rd == MIN_RD


class TestApplyInactivityRD:
    """Test apply_inactivity_rd calculation."""

    def test_rd_increases_with_inactivity(self):
        last_date = date(2024, 1, 1)
        current_date = date(2024, 2, 1)  # 31 days later
        new_rd = apply_inactivity_rd(100.0, last_date, current_date)
        assert new_rd > 100.0
        assert new_rd == 100.0 + 31 * 0.5

    def test_rd_has_maximum(self):
        last_date = date(2022, 1, 1)
        current_date = date(2024, 1, 1)  # 730 days later (100 + 730*0.5 = 465 > 350)
        new_rd = apply_inactivity_rd(100.0, last_date, current_date)
        assert new_rd == 350.0  # capped at max

    def test_no_last_date_returns_unchanged(self):
        new_rd = apply_inactivity_rd(200.0, None, date(2024, 1, 1))
        assert new_rd == 200.0
