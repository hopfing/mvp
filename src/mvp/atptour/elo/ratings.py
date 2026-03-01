from dataclasses import dataclass
from datetime import date

from mvp.atptour.elo.constants import (
    BASE_K,
    DEFAULT_ELO,
    DEFAULT_RD,
    HIGH_RD_K_MULT,
    HIGH_RD_THRESHOLD,
    NEW_PLAYER_K_MULT,
    NEW_PLAYER_THRESHOLD,
    ROUND_IMPORTANCE,
)


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


def get_k_factor(player: PlayerRating, round_name: str) -> float:
    """Calculate dynamic K-factor based on player state and match importance."""
    k = BASE_K

    # New player multiplier
    if player.match_count < NEW_PLAYER_THRESHOLD:
        k *= NEW_PLAYER_K_MULT

    # High uncertainty multiplier
    if player.rd > HIGH_RD_THRESHOLD:
        k *= HIGH_RD_K_MULT

    # Match importance multiplier
    importance = ROUND_IMPORTANCE.get(round_name, 1.0)
    k *= importance

    return k


def expected_score(player_elo: float, opponent_elo: float) -> float:
    """Calculate expected score (win probability) using Elo formula."""
    return 1.0 / (1.0 + 10.0 ** ((opponent_elo - player_elo) / 400.0))


def update_elo(
    player: PlayerRating,
    opponent: PlayerRating,
    won: bool,
    k: float,
    surface: str | None = None,
) -> float:
    """Calculate new Elo after a match.

    If surface is provided, uses effective surface Elo for calculation.
    Returns the new overall Elo value.
    """
    if surface:
        player_elo = player.effective_surface_elo(surface)
        opponent_elo = opponent.effective_surface_elo(surface)
    else:
        player_elo = player.elo
        opponent_elo = opponent.elo

    expected = expected_score(player_elo, opponent_elo)
    outcome = 1.0 if won else 0.0

    return player.elo + k * (outcome - expected)
