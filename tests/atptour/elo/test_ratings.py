from datetime import date

import pytest

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
    normalize_serve_score,
    update_elo,
    update_rd,
    update_return_elo,
    update_serve_elo,
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
    """Test update_elo calculation (pure function)."""

    def test_winner_gains_points(self):
        new_elo = update_elo(
            player_elo=1500.0, player_effective_elo=1500.0,
            opponent_effective_elo=1500.0, won=True, k=32.0,
        )
        assert new_elo > 1500.0

    def test_loser_loses_points(self):
        new_elo = update_elo(
            player_elo=1500.0, player_effective_elo=1500.0,
            opponent_effective_elo=1500.0, won=False, k=32.0,
        )
        assert new_elo < 1500.0

    def test_upset_larger_swing(self):
        underdog_win_gain = update_elo(
            player_elo=1400.0, player_effective_elo=1400.0,
            opponent_effective_elo=1600.0, won=True, k=32.0,
        ) - 1400.0
        favorite_win_gain = update_elo(
            player_elo=1600.0, player_effective_elo=1600.0,
            opponent_effective_elo=1400.0, won=True, k=32.0,
        ) - 1600.0
        assert underdog_win_gain > favorite_win_gain

    def test_surface_elo_used_when_provided(self):
        # Without surface adj: player is underdog (1500 vs 1600)
        gain_no_surface = update_elo(
            player_elo=1500.0, player_effective_elo=1500.0,
            opponent_effective_elo=1600.0, won=True, k=32.0,
        ) - 1500.0
        # With clay adj: player effective is 1600 vs 1600 - even match
        gain_clay = update_elo(
            player_elo=1500.0, player_effective_elo=1600.0,
            opponent_effective_elo=1600.0, won=True, k=32.0,
        ) - 1500.0
        assert gain_no_surface > gain_clay  # Upset gives more points


class TestUpdateSurfaceAdj:
    """Test update_surface_adj calculation (pure function)."""

    def test_clay_specialist_gains_on_clay_win(self):
        from mvp.atptour.elo.ratings import update_surface_adj

        # Player: elo=1600 + clay_adj=50 -> effective=1650
        # Opponent: elo=1600 + clay_adj=0 -> effective=1600
        new_adj = update_surface_adj(
            current_adj=50.0, player_effective_elo=1650.0,
            opponent_effective_elo=1600.0, won=True, k=16.0,
        )
        assert new_adj > 50.0

    def test_clay_specialist_loses_on_hard(self):
        from mvp.atptour.elo.ratings import update_surface_adj

        # Player: elo=1600 + hard_adj=-20 -> effective=1580
        # Opponent: elo=1600 + hard_adj=0 -> effective=1600
        new_adj = update_surface_adj(
            current_adj=-20.0, player_effective_elo=1580.0,
            opponent_effective_elo=1600.0, won=False, k=16.0,
        )
        assert new_adj < -20.0

    def test_equal_players_win_increases_adj(self):
        from mvp.atptour.elo.ratings import update_surface_adj

        new_adj = update_surface_adj(
            current_adj=0.0, player_effective_elo=1500.0,
            opponent_effective_elo=1500.0, won=True, k=16.0,
        )
        assert new_adj > 0.0


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


class TestNormalizeServeScore:
    """Test normalization of serve% to [0,1] score."""

    def test_at_baseline_returns_half(self):
        assert normalize_serve_score(0.62, "Hard") == 0.5

    def test_above_baseline_returns_above_half(self):
        # (0.645 - 0.62) / 0.10 + 0.5 = 0.75
        score = normalize_serve_score(0.645, "Hard")
        assert score == pytest.approx(0.75)

    def test_below_baseline_returns_below_half(self):
        # (0.595 - 0.62) / 0.10 + 0.5 = 0.25
        score = normalize_serve_score(0.595, "Hard")
        assert score == pytest.approx(0.25)

    def test_clamped_at_one(self):
        score = normalize_serve_score(0.85, "Hard")
        assert score == 1.0

    def test_clamped_at_zero(self):
        score = normalize_serve_score(0.40, "Hard")
        assert score == 0.0

    def test_clay_baseline(self):
        assert normalize_serve_score(0.60, "Clay") == 0.5

    def test_grass_baseline(self):
        assert normalize_serve_score(0.64, "Grass") == 0.5

    def test_unknown_surface_uses_default(self):
        score = normalize_serve_score(0.62, "Carpet")
        assert score == 0.5


class TestUpdateServeElo:
    """Test opponent-relative serve Elo update."""

    def test_above_expected_increases_serve_elo(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=0.70,
            surface="Hard",
            k=12.8,
        )
        assert server_elo > 1500.0
        assert returner_elo < 1500.0

    def test_below_expected_decreases_serve_elo(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=0.55,
            surface="Hard",
            k=12.8,
        )
        assert server_elo < 1500.0
        assert returner_elo > 1500.0

    def test_at_baseline_equal_ratings_no_change(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=0.62,
            surface="Hard",
            k=12.8,
        )
        assert server_elo == 1500.0
        assert returner_elo == 1500.0

    def test_zero_sum(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1600.0,
            returner_return_elo=1400.0,
            serve_pct=0.68,
            surface="Hard",
            k=12.8,
        )
        server_delta = server_elo - 1600.0
        returner_delta = returner_elo - 1400.0
        assert abs(server_delta + returner_delta) < 1e-10

    def test_strong_server_vs_weak_returner_expected_high(self):
        server_elo, _ = update_serve_elo(
            server_serve_elo=1700.0,
            returner_return_elo=1300.0,
            serve_pct=0.62,
            surface="Hard",
            k=12.8,
        )
        assert server_elo < 1700.0

    def test_none_serve_pct_returns_unchanged(self):
        server_elo, returner_elo = update_serve_elo(
            server_serve_elo=1500.0,
            returner_return_elo=1500.0,
            serve_pct=None,
            surface="Hard",
            k=12.8,
        )
        assert server_elo == 1500.0
        assert returner_elo == 1500.0

    def test_clay_baseline_different(self):
        hard_serve, _ = update_serve_elo(1500.0, 1500.0, 0.62, "Hard", 12.8)
        clay_serve, _ = update_serve_elo(1500.0, 1500.0, 0.62, "Clay", 12.8)
        assert hard_serve == 1500.0
        assert clay_serve > 1500.0


class TestUpdateReturnElo:
    """Test opponent-relative return Elo update."""

    def test_good_return_increases_return_elo(self):
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1500.0,
            server_serve_elo=1500.0,
            opp_serve_pct=0.55,
            surface="Hard",
            k=12.8,
        )
        assert returner_elo > 1500.0
        assert server_elo < 1500.0

    def test_poor_return_decreases_return_elo(self):
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1500.0,
            server_serve_elo=1500.0,
            opp_serve_pct=0.70,
            surface="Hard",
            k=12.8,
        )
        assert returner_elo < 1500.0
        assert server_elo > 1500.0

    def test_none_returns_unchanged(self):
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1500.0,
            server_serve_elo=1500.0,
            opp_serve_pct=None,
            surface="Hard",
            k=12.8,
        )
        assert returner_elo == 1500.0
        assert server_elo == 1500.0

    def test_zero_sum(self):
        returner_elo, server_elo = update_return_elo(
            returner_return_elo=1600.0,
            server_serve_elo=1400.0,
            opp_serve_pct=0.58,
            surface="Hard",
            k=12.8,
        )
        returner_delta = returner_elo - 1600.0
        server_delta = server_elo - 1400.0
        assert abs(returner_delta + server_delta) < 1e-10


class TestInitializePlayer:
    """Test initialize_player function for seeding from ATP ranking."""

    def test_ranked_player_seeded_from_ranking(self):
        from mvp.atptour.elo.ratings import initialize_player

        rating = initialize_player(ranking=50)
        assert rating.elo > 1500.0  # better than default
        assert rating.elo < 2400.0  # not top Elo

    def test_top_player_high_elo(self):
        from mvp.atptour.elo.ratings import initialize_player

        rating = initialize_player(ranking=1)
        assert rating.elo > 2300.0

    def test_low_ranked_player_low_elo(self):
        from mvp.atptour.elo.ratings import initialize_player

        rating = initialize_player(ranking=500)
        assert rating.elo < 1600.0  # well below top players
        assert rating.elo >= 1200.0  # minimum

    def test_unranked_player_default(self):
        from mvp.atptour.elo.ratings import initialize_player

        rating = initialize_player(ranking=None)
        assert rating.elo == 1300.0

    def test_new_player_high_rd(self):
        from mvp.atptour.elo.ratings import initialize_player

        rating = initialize_player(ranking=100)
        assert rating.rd == 350.0

    def test_surface_adj_zero(self):
        from mvp.atptour.elo.ratings import initialize_player

        rating = initialize_player(ranking=100)
        assert rating.hard_adj == 0.0
        assert rating.clay_adj == 0.0
        assert rating.grass_adj == 0.0

    def test_serve_return_default(self):
        from mvp.atptour.elo.ratings import initialize_player

        rating = initialize_player(ranking=100)
        assert rating.serve_elo == 1500.0
        assert rating.return_elo == 1500.0


class TestStyleDimensionConstants:
    """Test style dimension constants are defined."""

    def test_first_serve_power_baseline_exists(self):
        from mvp.atptour.elo.constants import FIRST_SERVE_POWER_BASELINE

        assert isinstance(FIRST_SERVE_POWER_BASELINE, dict)
        assert "Hard" in FIRST_SERVE_POWER_BASELINE

    def test_second_serve_reliability_baseline_exists(self):
        from mvp.atptour.elo.constants import SECOND_SERVE_RELIABILITY_BASELINE

        assert isinstance(SECOND_SERVE_RELIABILITY_BASELINE, dict)

    def test_serve_clutch_baseline_exists(self):
        from mvp.atptour.elo.constants import SERVE_CLUTCH_BASELINE

        assert isinstance(SERVE_CLUTCH_BASELINE, dict)

    def test_return_clutch_baseline_exists(self):
        from mvp.atptour.elo.constants import RETURN_CLUTCH_BASELINE

        assert isinstance(RETURN_CLUTCH_BASELINE, dict)

    def test_tb_clutch_baseline_exists(self):
        from mvp.atptour.elo.constants import TB_CLUTCH_BASELINE

        assert isinstance(TB_CLUTCH_BASELINE, float)

    def test_ace_resistance_baseline_exists(self):
        from mvp.atptour.elo.constants import ACE_RESISTANCE_BASELINE

        assert isinstance(ACE_RESISTANCE_BASELINE, dict)
        assert "Hard" in ACE_RESISTANCE_BASELINE

    def test_style_k_mult_exists(self):
        from mvp.atptour.elo.constants import STYLE_K_MULT

        assert isinstance(STYLE_K_MULT, float)

    def test_style_scale_exists(self):
        from mvp.atptour.elo.constants import STYLE_SCALE

        assert isinstance(STYLE_SCALE, float)


class TestPlayerRatingStyleDimensions:
    """Test style dimension fields on PlayerRating."""

    def test_default_style_dimensions(self):
        from mvp.atptour.elo.constants import DEFAULT_ELO
        from mvp.atptour.elo.ratings import PlayerRating

        rating = PlayerRating()
        assert rating.first_serve_power == DEFAULT_ELO
        assert rating.second_serve_reliability == DEFAULT_ELO
        assert rating.ace_resistance == DEFAULT_ELO
        assert rating.serve_clutch == DEFAULT_ELO
        assert rating.return_clutch == DEFAULT_ELO
        assert rating.tb_clutch == DEFAULT_ELO
        assert rating.overall_clutch == DEFAULT_ELO
        assert rating.indoor_adj == 0.0

    def test_custom_style_dimensions(self):
        from mvp.atptour.elo.ratings import PlayerRating

        rating = PlayerRating(
            first_serve_power=1600.0,
            second_serve_reliability=1550.0,
            ace_resistance=1570.0,
            serve_clutch=1580.0,
            return_clutch=1520.0,
            tb_clutch=1540.0,
            indoor_adj=25.0,
        )
        assert rating.first_serve_power == 1600.0
        assert rating.second_serve_reliability == 1550.0
        assert rating.ace_resistance == 1570.0
        assert rating.serve_clutch == 1580.0
        assert rating.return_clutch == 1520.0
        assert rating.tb_clutch == 1540.0
        assert rating.indoor_adj == 25.0


class TestUpdateFirstServePower:
    """Test first serve power update based on ace rate."""

    def test_above_baseline_increases(self):
        from mvp.atptour.elo.ratings import update_first_serve_power

        # 0.25 ace rate on Hard (baseline 0.176) should increase
        new_elo = update_first_serve_power(1500.0, 0.25, "Hard", 16.0)
        assert new_elo > 1500.0

    def test_below_baseline_decreases(self):
        from mvp.atptour.elo.ratings import update_first_serve_power

        # 0.10 ace rate on Hard (baseline 0.176) should decrease
        new_elo = update_first_serve_power(1500.0, 0.10, "Hard", 16.0)
        assert new_elo < 1500.0

    def test_missing_stats_unchanged(self):
        from mvp.atptour.elo.ratings import update_first_serve_power

        new_elo = update_first_serve_power(1500.0, None, "Hard", 16.0)
        assert new_elo == 1500.0


class TestUpdateSecondServeReliability:
    """Test second serve reliability update."""

    def test_above_baseline_increases(self):
        from mvp.atptour.elo.ratings import update_second_serve_reliability

        # 0.95 reliability on Hard (baseline 0.893) should increase
        new_elo = update_second_serve_reliability(1500.0, 0.95, "Hard", 16.0)
        assert new_elo > 1500.0

    def test_below_baseline_decreases(self):
        from mvp.atptour.elo.ratings import update_second_serve_reliability

        # 0.85 reliability on Hard (baseline 0.893) should decrease
        new_elo = update_second_serve_reliability(1500.0, 0.85, "Hard", 16.0)
        assert new_elo < 1500.0


class TestUpdateAceResistance:
    """Test ace resistance update."""

    def test_above_baseline_increases(self):
        from mvp.atptour.elo.ratings import update_ace_resistance

        # 0.90 resistance on Hard (baseline 0.824) should increase
        new_elo = update_ace_resistance(1500.0, 0.90, "Hard", 16.0)
        assert new_elo > 1500.0

    def test_below_baseline_decreases(self):
        from mvp.atptour.elo.ratings import update_ace_resistance

        # 0.75 resistance on Hard (baseline 0.824) should decrease
        new_elo = update_ace_resistance(1500.0, 0.75, "Hard", 16.0)
        assert new_elo < 1500.0

    def test_missing_stats_unchanged(self):
        from mvp.atptour.elo.ratings import update_ace_resistance

        new_elo = update_ace_resistance(1500.0, None, "Hard", 16.0)
        assert new_elo == 1500.0


class TestUpdateServeClutch:
    """Test serve clutch update based on break points saved."""

    def test_above_baseline_increases(self):
        from mvp.atptour.elo.ratings import update_serve_clutch

        # 0.70 save rate on Hard (baseline 0.597) should increase
        new_elo = update_serve_clutch(1500.0, 0.70, "Hard", 16.0)
        assert new_elo > 1500.0

    def test_below_baseline_decreases(self):
        from mvp.atptour.elo.ratings import update_serve_clutch

        # 0.50 save rate on Hard (baseline 0.597) should decrease
        new_elo = update_serve_clutch(1500.0, 0.50, "Hard", 16.0)
        assert new_elo < 1500.0


class TestUpdateReturnClutch:
    """Test return clutch update based on break points converted."""

    def test_above_baseline_increases(self):
        from mvp.atptour.elo.ratings import update_return_clutch

        # 0.50 conversion rate on Hard (baseline 0.404) should increase
        new_elo = update_return_clutch(1500.0, 0.50, "Hard", 16.0)
        assert new_elo > 1500.0

    def test_below_baseline_decreases(self):
        from mvp.atptour.elo.ratings import update_return_clutch

        # 0.30 conversion rate on Hard (baseline 0.404) should decrease
        new_elo = update_return_clutch(1500.0, 0.30, "Hard", 16.0)
        assert new_elo < 1500.0


class TestUpdateTbClutch:
    """Test tiebreak clutch update."""

    def test_tb_win_increases(self):
        from mvp.atptour.elo.ratings import update_tb_clutch

        # Won 2 of 2 TBs (100%) vs baseline 50% should increase
        new_elo = update_tb_clutch(1500.0, 2, 2, 16.0)
        assert new_elo > 1500.0

    def test_tb_loss_decreases(self):
        from mvp.atptour.elo.ratings import update_tb_clutch

        # Won 0 of 2 TBs (0%) vs baseline 50% should decrease
        new_elo = update_tb_clutch(1500.0, 0, 2, 16.0)
        assert new_elo < 1500.0

    def test_no_tbs_unchanged(self):
        from mvp.atptour.elo.ratings import update_tb_clutch

        # No TBs played - unchanged
        new_elo = update_tb_clutch(1500.0, 0, 0, 16.0)
        assert new_elo == 1500.0


class TestUpdateIndoorAdj:
    """Test indoor adjustment update."""

    def test_indoor_win_increases(self):
        from mvp.atptour.elo.ratings import update_indoor_adj

        new_adj = update_indoor_adj(0.0, won=True, k=16.0)
        assert new_adj > 0.0

    def test_indoor_loss_decreases(self):
        from mvp.atptour.elo.ratings import update_indoor_adj

        new_adj = update_indoor_adj(0.0, won=False, k=16.0)
        assert new_adj < 0.0


class TestEMAConvergence:
    """Test that EMA-based ratings converge to stable values."""

    def test_serve_elo_converges(self):
        """Applying the same serve observation vs fixed opponent converges."""
        server_elo = DEFAULT_ELO
        opp_return_elo = DEFAULT_ELO
        # Use 0.655 (within non-clamped range for DEVIATION_SCALE=0.10)
        for _ in range(500):
            server_elo, opp_return_elo = update_serve_elo(
                server_elo, opp_return_elo, 0.655, "Hard", 16.0
            )
        # Strong serve% pushes server up, returner down; zero-sum
        assert server_elo > DEFAULT_ELO
        assert opp_return_elo < DEFAULT_ELO
        # Check convergence: last update should be tiny
        prev_server = server_elo
        server_elo, opp_return_elo = update_serve_elo(
            server_elo, opp_return_elo, 0.655, "Hard", 16.0
        )
        assert abs(server_elo - prev_server) < 0.1

    def test_return_elo_converges(self):
        """Applying the same return observation vs fixed opponent converges."""
        returner_elo = DEFAULT_ELO
        server_elo = DEFAULT_ELO
        # Use 0.585 (within non-clamped range for DEVIATION_SCALE=0.10)
        for _ in range(500):
            returner_elo, server_elo = update_return_elo(
                returner_elo, server_elo, 0.585, "Hard", 16.0
            )
        # Low opp serve% means returner did well
        assert returner_elo > DEFAULT_ELO
        assert server_elo < DEFAULT_ELO
        # Check convergence: last update should be tiny
        prev_returner = returner_elo
        returner_elo, server_elo = update_return_elo(
            returner_elo, server_elo, 0.585, "Hard", 16.0
        )
        assert abs(returner_elo - prev_returner) < 0.1

    def test_first_serve_power_converges(self):
        from mvp.atptour.elo.ratings import update_first_serve_power

        elo = DEFAULT_ELO
        for _ in range(200):
            elo = update_first_serve_power(elo, 0.25, "Hard", 16.0)
        # Target = 1500 + (0.25 - 0.176) * 3000 = 1722
        assert abs(elo - 1722.0) < 0.01

    def test_tb_clutch_converges(self):
        from mvp.atptour.elo.ratings import update_tb_clutch

        elo = DEFAULT_ELO
        for _ in range(200):
            elo = update_tb_clutch(elo, 2, 3, 16.0)
        # Target = 1500 + (0.6667 - 0.50) * 3000 = 2000
        assert abs(elo - 2000.0) < 1.0

    def test_indoor_adj_converges_winner(self):
        from mvp.atptour.elo.ratings import update_indoor_adj

        adj = 0.0
        for _ in range(200):
            adj = update_indoor_adj(adj, won=True, k=16.0)
        # Target = INDOOR_EMA_SCALE * 1.0 = 500.0
        assert abs(adj - 500.0) < 0.01

    def test_indoor_adj_converges_loser(self):
        from mvp.atptour.elo.ratings import update_indoor_adj

        adj = 0.0
        for _ in range(200):
            adj = update_indoor_adj(adj, won=False, k=16.0)
        # Target = INDOOR_EMA_SCALE * -1.0 = -500.0
        assert abs(adj - (-500.0)) < 0.01

    def test_different_match_counts_same_stats_converge(self):
        """Two players with identical stats but different match counts
        end up at similar ratings."""
        # Use 0.645 (within non-clamped range for DEVIATION_SCALE=0.10)
        # Player A: 200 matches vs fixed opponent
        server_a = DEFAULT_ELO
        opp_a = DEFAULT_ELO
        for _ in range(200):
            server_a, opp_a = update_serve_elo(server_a, opp_a, 0.645, "Hard", 16.0)

        # Player B: 500 matches vs fixed opponent
        server_b = DEFAULT_ELO
        opp_b = DEFAULT_ELO
        for _ in range(500):
            server_b, opp_b = update_serve_elo(server_b, opp_b, 0.645, "Hard", 16.0)

        # Both converge toward the same equilibrium
        assert abs(server_a - server_b) < 5.0

    def test_below_baseline_converges_down(self):
        """Rating converges below DEFAULT_ELO for below-baseline performance."""
        server_elo = DEFAULT_ELO
        opp_return_elo = DEFAULT_ELO
        for _ in range(200):
            server_elo, opp_return_elo = update_serve_elo(
                server_elo, opp_return_elo, 0.55, "Hard", 16.0
            )
        # Below-baseline serve% pushes server Elo down
        assert server_elo < DEFAULT_ELO

    def test_serve_elo_bounded_after_many_matches(self):
        """Opponent-relative Elo stays bounded even after
        hundreds of above-baseline observations."""
        server_elo = DEFAULT_ELO
        opp_return_elo = DEFAULT_ELO
        for _ in range(1000):
            server_elo, opp_return_elo = update_serve_elo(
                server_elo, opp_return_elo, 0.70, "Hard", 16.0
            )
        # Should be bounded, not diverging to infinity
        assert 1500.0 < server_elo < 2500.0
