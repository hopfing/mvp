from datetime import date

from mvp.atptour.elo.constants import (
    BASE_K,
    DEFAULT_ELO,
    DEFAULT_RD,
    HIGH_RD_K_MULT,
    NEW_PLAYER_K_MULT,
)
from mvp.atptour.elo.ratings import (
    PlayerRating,
    expected_score,
    get_k_factor,
    update_elo,
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
