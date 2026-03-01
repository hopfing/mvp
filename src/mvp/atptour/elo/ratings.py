import math
from dataclasses import dataclass
from datetime import date

from mvp.atptour.elo.constants import (
    BASE_K,
    DEFAULT_ELO,
    DEFAULT_RD,
    HIGH_RD_K_MULT,
    HIGH_RD_THRESHOLD,
    MAX_RD,
    MIN_RD,
    NEW_PLAYER_K_MULT,
    NEW_PLAYER_THRESHOLD,
    RD_DECAY_FACTOR,
    RD_GROWTH_PER_DAY,
    RETURN_BASELINE,
    ROUND_IMPORTANCE,
    SEED_ELO_MAX,
    SEED_ELO_MIN,
    SEED_RANK_COEFF,
    SEED_UNRANKED,
    SERVE_BASELINE,
    SERVE_RETURN_SCALE,
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


def update_surface_adj(
    player: PlayerRating,
    opponent: PlayerRating,
    won: bool,
    surface: str,
    k: float,
) -> float:
    """Calculate new surface adjustment after a match.

    Returns the new adjustment value for the given surface.
    Returns 0.0 for unknown surfaces.
    """
    if surface not in ("Hard", "Clay", "Grass"):
        return 0.0

    player_effective = player.effective_surface_elo(surface)
    opponent_effective = opponent.effective_surface_elo(surface)

    expected = expected_score(player_effective, opponent_effective)
    outcome = 1.0 if won else 0.0

    current_adj = player.get_surface_adj(surface)
    return current_adj + k * (outcome - expected)


def update_rd(current_rd: float) -> float:
    """Decrease RD after a match (we learned something)."""
    return max(MIN_RD, current_rd * RD_DECAY_FACTOR)


def apply_inactivity_rd(
    current_rd: float,
    last_match_date: date | None,
    current_match_date: date,
) -> float:
    """Increase RD based on inactivity period."""
    if last_match_date is None:
        return current_rd

    days_inactive = (current_match_date - last_match_date).days
    new_rd = current_rd + days_inactive * RD_GROWTH_PER_DAY
    return min(MAX_RD, new_rd)


def update_serve_elo(
    current_elo: float,
    serve_pct: float | None,
    surface: str,
    k: float,
) -> float:
    """Update serve Elo based on serve points won percentage.

    Returns unchanged elo if serve_pct is None.
    """
    if serve_pct is None:
        return current_elo

    baseline = SERVE_BASELINE.get(surface, 0.62)
    diff = serve_pct - baseline
    return current_elo + k * diff * SERVE_RETURN_SCALE


def update_return_elo(
    current_elo: float,
    return_pct: float | None,
    surface: str,
    k: float,
) -> float:
    """Update return Elo based on return points won percentage.

    Returns unchanged elo if return_pct is None.
    """
    if return_pct is None:
        return current_elo

    baseline = RETURN_BASELINE.get(surface, 0.38)
    diff = return_pct - baseline
    return current_elo + k * diff * SERVE_RETURN_SCALE


def initialize_player(ranking: int | None) -> PlayerRating:
    """Initialize a new player's rating, optionally seeded from ranking.

    Mapping: #1 -> ~2400, #100 -> ~1800, #500 -> ~1400, unranked -> 1300
    """
    if ranking is not None:
        elo = SEED_ELO_MAX - math.sqrt(ranking) * SEED_RANK_COEFF
        elo = max(SEED_ELO_MIN, min(SEED_ELO_MAX, elo))
    else:
        elo = SEED_UNRANKED

    return PlayerRating(
        elo=elo,
        rd=DEFAULT_RD,
        hard_adj=0.0,
        clay_adj=0.0,
        grass_adj=0.0,
        serve_elo=DEFAULT_ELO,
        serve_rd=DEFAULT_RD,
        return_elo=DEFAULT_ELO,
        return_rd=DEFAULT_RD,
        match_count=0,
        last_match_date=None,
    )
