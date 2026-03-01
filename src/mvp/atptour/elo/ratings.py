from dataclasses import dataclass
from datetime import date

from mvp.atptour.elo.constants import DEFAULT_ELO, DEFAULT_RD


@dataclass
class PlayerRating:
    """Holds multi-dimensional Elo rating state for a player."""

    elo: float = DEFAULT_ELO
    rd: float = DEFAULT_RD
    hard_adj: float = 0.0
    clay_adj: float = 0.0
    grass_adj: float = 0.0
    serve_elo: float = DEFAULT_ELO
    serve_rd: float = DEFAULT_RD
    return_elo: float = DEFAULT_ELO
    return_rd: float = DEFAULT_RD
    match_count: int = 0
    last_match_date: date | None = None

    def get_surface_adj(self, surface: str) -> float:
        """Return surface adjustment for the given surface.

        Returns 0.0 for unknown surfaces.
        """
        surface_map = {
            "Hard": self.hard_adj,
            "Clay": self.clay_adj,
            "Grass": self.grass_adj,
        }
        return surface_map.get(surface, 0.0)

    def effective_surface_elo(self, surface: str) -> float:
        """Return overall Elo plus surface adjustment."""
        return self.elo + self.get_surface_adj(surface)
