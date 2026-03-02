import math
from dataclasses import dataclass
from datetime import date

from mvp.atptour.elo.constants import (
    ACE_RESISTANCE_BASELINE,
    BASE_K,
    DEFAULT_ELO,
    DEFAULT_RD,
    FIRST_SERVE_POWER_BASELINE,
    HIGH_RD_K_MULT,
    HIGH_RD_THRESHOLD,
    MAX_RD,
    MIN_RD,
    NEW_PLAYER_K_MULT,
    NEW_PLAYER_THRESHOLD,
    RD_DECAY_FACTOR,
    RD_GROWTH_PER_DAY,
    RETURN_BASELINE,
    RETURN_CLUTCH_BASELINE,
    ROUND_IMPORTANCE,
    SECOND_SERVE_RELIABILITY_BASELINE,
    SEED_ELO_MAX,
    SEED_ELO_MIN,
    SEED_RANK_COEFF,
    SEED_UNRANKED,
    SERVE_BASELINE,
    SERVE_CLUTCH_BASELINE,
    SERVE_RETURN_SCALE,
    STYLE_K_MULT,
    STYLE_SCALE,
    TB_CLUTCH_BASELINE,
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
    # Style dimensions
    first_serve_power: float = DEFAULT_ELO
    second_serve_reliability: float = DEFAULT_ELO
    ace_resistance: float = DEFAULT_ELO
    serve_clutch: float = DEFAULT_ELO
    return_clutch: float = DEFAULT_ELO
    tb_clutch: float = DEFAULT_ELO
    overall_clutch: float = DEFAULT_ELO
    indoor_adj: float = 0.0
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
        first_serve_power=DEFAULT_ELO,
        second_serve_reliability=DEFAULT_ELO,
        ace_resistance=DEFAULT_ELO,
        serve_clutch=DEFAULT_ELO,
        return_clutch=DEFAULT_ELO,
        tb_clutch=DEFAULT_ELO,
        overall_clutch=DEFAULT_ELO,
        indoor_adj=0.0,
        match_count=0,
        last_match_date=None,
    )


def update_first_serve_power(
    current_elo: float,
    ace_rate: float | None,
    surface: str,
    k: float,
) -> float:
    """Update first serve power based on ace rate.

    ace_rate = aces / first_serve_pts_won
    """
    if ace_rate is None:
        return current_elo

    baseline = FIRST_SERVE_POWER_BASELINE.get(surface, 0.176)
    diff = ace_rate - baseline
    return current_elo + k * STYLE_K_MULT * diff * STYLE_SCALE


def update_second_serve_reliability(
    current_elo: float,
    reliability: float | None,
    surface: str,
    k: float,
) -> float:
    """Update second serve reliability.

    reliability = 1 - (double_faults / second_serve_pts_played)
    """
    if reliability is None:
        return current_elo

    baseline = SECOND_SERVE_RELIABILITY_BASELINE.get(surface, 0.893)
    diff = reliability - baseline
    return current_elo + k * STYLE_K_MULT * diff * STYLE_SCALE


def update_ace_resistance(
    current_elo: float,
    resistance: float | None,
    surface: str,
    k: float,
) -> float:
    """Update ace resistance based on opponent's ace rate against us.

    resistance = 1 - (opp_svc_aces / ret_first_serve_pts_lost)
    """
    if resistance is None:
        return current_elo

    baseline = ACE_RESISTANCE_BASELINE.get(surface, 0.824)
    diff = resistance - baseline
    return current_elo + k * STYLE_K_MULT * diff * STYLE_SCALE


def update_serve_clutch(
    current_elo: float,
    save_rate: float | None,
    surface: str,
    k: float,
) -> float:
    """Update serve clutch based on break points saved.

    save_rate = bp_saved / bp_faced
    """
    if save_rate is None:
        return current_elo

    baseline = SERVE_CLUTCH_BASELINE.get(surface, 0.597)
    diff = save_rate - baseline
    return current_elo + k * STYLE_K_MULT * diff * STYLE_SCALE


def update_return_clutch(
    current_elo: float,
    conversion_rate: float | None,
    surface: str,
    k: float,
) -> float:
    """Update return clutch based on break points converted.

    conversion_rate = bp_converted / bp_opportunities
    """
    if conversion_rate is None:
        return current_elo

    baseline = RETURN_CLUTCH_BASELINE.get(surface, 0.404)
    diff = conversion_rate - baseline
    return current_elo + k * STYLE_K_MULT * diff * STYLE_SCALE


def update_tb_clutch(
    current_elo: float,
    tb_won: int,
    tb_played: int,
    k: float,
) -> float:
    """Update tiebreak clutch based on TB win rate."""
    if tb_played == 0:
        return current_elo

    win_rate = tb_won / tb_played
    diff = win_rate - TB_CLUTCH_BASELINE
    return current_elo + k * STYLE_K_MULT * diff * STYLE_SCALE


def update_indoor_adj(
    current_adj: float,
    won: bool,
    k: float,
) -> float:
    """Update indoor adjustment based on match result.

    Works like surface adjustment - win/loss based.
    """
    outcome = 1.0 if won else 0.0
    return current_adj + k * STYLE_K_MULT * (outcome - 0.5)
