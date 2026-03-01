from datetime import date

from mvp.atptour.elo.constants import DEFAULT_ELO, DEFAULT_RD
from mvp.atptour.elo.ratings import PlayerRating


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
