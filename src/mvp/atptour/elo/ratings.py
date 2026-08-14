import math
from dataclasses import dataclass
from datetime import date

from mvp.atptour.elo.constants import (
    ACE_RESISTANCE_BASELINE,
    BASE_K,
    DEFAULT_ELO,
    DEFAULT_RD,
    DEFAULT_SERVE_ELO_CONFIG,
    EMA_ALPHA,
    FIRST_SERVE_POWER_BASELINE,
    HIGH_RD_K_MULT,
    HIGH_RD_THRESHOLD,
    MAX_RD,
    MIN_RD,
    NEW_PLAYER_K_MULT,
    NEW_PLAYER_THRESHOLD,
    RD_DECAY_FACTOR,
    RD_GROWTH_PER_DAY,
    RETURN_CLUTCH_BASELINE,
    ROUND_IMPORTANCE,
    SECOND_SERVE_RELIABILITY_BASELINE,
    SEED_ELO_MAX,
    SEED_ELO_MIN,
    SEED_RANK_COEFF,
    SEED_UNRANKED,
    SERVE_BASELINE,
    SERVE_CLUTCH_BASELINE,
    INDOOR_SERVE_BOOST,
    STYLE_SCALE,
    TB_CLUTCH_BASELINE,
    TOURNAMENT_IMPORTANCE,
    ServeEloConfig,
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
    # Serve and return carry their own experience counters and clocks because
    # they learn from a different event than the base rating does. A match with
    # no point-level serve stats still moves `elo` and still refreshes
    # `last_match_date`, but leaves serve and return untouched — so keying their
    # decay or their inactivity growth off the match-level fields reports
    # confidence the serve rating has not earned. The two sides are tracked
    # separately, not as one "serve/return" pair: a player's RETURN rating
    # updates from the OPPONENT'S serve stats, so the two can land on different
    # matches. Mirrors the per-surface clocks GlickoRating already keeps.
    serve_match_count: int = 0
    return_match_count: int = 0
    last_serve_update_date: date | None = None
    last_return_update_date: date | None = None

    # Surface and venue residuals on top of the global serve/return ratings.
    # Their existence was gated on measurement rather than assumed: split-half
    # reliability says a player's surface-specific serve history alone predicts
    # their next performance on that surface WORSE than their overall history
    # does (hard -0.014, clay -0.016), but a blend of the two beats either
    # (+0.006 hard, +0.007 clay, +0.061 grass, +0.011 indoor). An adjustment on
    # top of a base rating IS that blend, which is why the residual form is the
    # right one and an independent per-surface rating would not be.
    #
    # Indoor is Hard-only. Indoor clay and indoor grass are too rare to support
    # a measured population baseline, and without one the adjustment would
    # absorb a venue constant as player skill.
    svc_hard_adj: float = 0.0
    svc_clay_adj: float = 0.0
    svc_grass_adj: float = 0.0
    svc_indoor_adj: float = 0.0
    ret_hard_adj: float = 0.0
    ret_clay_adj: float = 0.0
    ret_grass_adj: float = 0.0
    ret_indoor_adj: float = 0.0
    # Own rd per axis, on the same principle as serve_rd: confidence has to
    # reflect what THAT axis has seen, not how often the player appeared.
    svc_hard_rd: float = DEFAULT_RD
    svc_clay_rd: float = DEFAULT_RD
    svc_grass_rd: float = DEFAULT_RD
    svc_indoor_rd: float = DEFAULT_RD
    ret_hard_rd: float = DEFAULT_RD
    ret_clay_rd: float = DEFAULT_RD
    ret_grass_rd: float = DEFAULT_RD
    ret_indoor_rd: float = DEFAULT_RD
    # Own clocks. Internal only — never emitted, since their effect is already
    # fully expressed in the rd they drive.
    last_svc_hard_date: date | None = None
    last_svc_clay_date: date | None = None
    last_svc_grass_date: date | None = None
    last_svc_indoor_date: date | None = None
    last_ret_hard_date: date | None = None
    last_ret_clay_date: date | None = None
    last_ret_grass_date: date | None = None
    last_ret_indoor_date: date | None = None

    def serve_axes(self, surface: str, indoor: bool) -> tuple[str, ...]:
        """Which adjustment axes this match updates.

        A surface the base ratings do not model (Carpet) contributes to neither,
        matching get_surface_adj's own map. Indoor rides alongside the surface
        axis rather than replacing it, so an indoor hard match trains both.
        """
        axes = []
        if surface in ("Hard", "Clay", "Grass"):
            axes.append(surface.lower())
        if indoor and surface == "Hard":
            axes.append("indoor")
        return tuple(axes)

    def effective_serve_elo(self, surface: str, indoor: bool = False) -> float:
        """Serve rating plus every adjustment this match's conditions engage."""
        return self.serve_elo + sum(
            getattr(self, f"svc_{a}_adj") for a in self.serve_axes(surface, indoor)
        )

    def effective_return_elo(self, surface: str, indoor: bool = False) -> float:
        """Return rating plus every adjustment this match's conditions engage."""
        return self.return_elo + sum(
            getattr(self, f"ret_{a}_adj") for a in self.serve_axes(surface, indoor)
        )

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


def k_factor_from(
    rd: float,
    match_count: int,
    round_name: str,
    tournament_level: str = "250",
) -> float:
    """K-factor for one rating dimension, from that dimension's own state.

    Split out of `get_k_factor` so serve and return can use their own rd and
    experience count without a second copy of the thresholds and multipliers.
    A fork would let the two drift apart the next time these are retuned.
    """
    k = BASE_K

    # New player multiplier
    if match_count < NEW_PLAYER_THRESHOLD:
        k *= NEW_PLAYER_K_MULT

    # High uncertainty multiplier
    if rd > HIGH_RD_THRESHOLD:
        k *= HIGH_RD_K_MULT

    # Match importance multipliers
    k *= ROUND_IMPORTANCE.get(round_name, 1.0)
    k *= TOURNAMENT_IMPORTANCE.get(tournament_level, 1.0)

    return k


def get_k_factor(player: PlayerRating, round_name: str, tournament_level: str = "250") -> float:
    """K-factor for the BASE rating, from overall rd and match count."""
    return k_factor_from(
        player.rd, player.match_count, round_name, tournament_level
    )


def expected_score(player_elo: float, opponent_elo: float) -> float:
    """Calculate expected score (win probability) using Elo formula."""
    return 1.0 / (1.0 + 10.0 ** ((opponent_elo - player_elo) / 400.0))


def serve_baseline_for(surface: str, indoor: bool = False) -> float:
    """Population serve% for these conditions, which the update differences out.

    Indoor is a correction ON TOP of the surface baseline rather than a surface
    of its own: it only has a measured value for Hard, because indoor clay and
    indoor grass are too rare to support one.
    """
    base = SERVE_BASELINE.get(surface, 0.62)
    if indoor and surface == "Hard":
        base += INDOOR_SERVE_BOOST
    return base


def normalize_serve_score(
    serve_pct: float, surface: str, cfg: ServeEloConfig | None = None,
    indoor: bool = False,
) -> float:
    """Map raw serve% to a (0,1) score centred at 0.5 for the surface baseline.

    Logistic rather than a clipped linear ramp, so the map is strictly
    increasing everywhere and no two performances of different quality collapse
    onto the same score. Under the old ramp 59.8% of matches sat at 0.0 or 1.0
    and the update could not tell a good serving day from an overwhelming one.
    """
    slope = (cfg or DEFAULT_SERVE_ELO_CONFIG).score_slope
    baseline = serve_baseline_for(surface, indoor)
    return 1.0 / (1.0 + math.exp(-(serve_pct - baseline) / slope))


def denormalize_serve_score(
    score: float, surface: str, cfg: ServeEloConfig | None = None,
    indoor: bool = False,
) -> float:
    """Inverse of `normalize_serve_score` — a score back to an implied serve%.

    Exists so evaluation can compare `expected_score` against an observed serve
    percentage on one scale. Kept next to the forward map so the two cannot
    drift; a saturated score has no finite inverse, hence the guard.
    """
    slope = (cfg or DEFAULT_SERVE_ELO_CONFIG).score_slope
    baseline = serve_baseline_for(surface, indoor)
    eps = 1e-12
    s = min(max(score, eps), 1.0 - eps)
    return baseline + slope * math.log(s / (1.0 - s))


def update_elo(
    player_elo: float,
    player_effective_elo: float,
    opponent_effective_elo: float,
    won: bool,
    k: float,
) -> float:
    """Calculate new Elo after a match.

    Uses pre-computed effective Elos (base + surface adj) for expected score,
    but applies the delta to the base Elo. Pure function — no mutable state.

    Args:
        player_elo: Player's base Elo (before match).
        player_effective_elo: Player's effective Elo (base + surface adj).
        opponent_effective_elo: Opponent's effective Elo (base + surface adj).
        won: Whether the player won.
        k: K-factor for this update.

    Returns:
        New base Elo value.
    """
    expected = expected_score(player_effective_elo, opponent_effective_elo)
    outcome = 1.0 if won else 0.0

    return player_elo + k * (outcome - expected)


def update_surface_adj(
    current_adj: float,
    player_effective_elo: float,
    opponent_effective_elo: float,
    won: bool,
    k: float,
) -> float:
    """Calculate new surface adjustment after a match.

    Pure function — caller provides pre-computed effective Elos and current adj.

    Args:
        current_adj: Current surface adjustment value.
        player_effective_elo: Player's effective Elo (base + surface adj).
        opponent_effective_elo: Opponent's effective Elo (base + surface adj).
        won: Whether the player won.
        k: K-factor for this update.

    Returns:
        New surface adjustment value.
    """
    expected = expected_score(player_effective_elo, opponent_effective_elo)
    outcome = 1.0 if won else 0.0

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


def serve_surprise(
    serve_pct: float | None,
    surface: str,
    effective_serve_elo: float,
    effective_return_elo: float,
    cfg: ServeEloConfig | None = None,
    indoor: bool = False,
) -> float | None:
    """Signed surprise for one service sub-game, or None if it cannot be scored.

    Split out so the caller can apply one surprise to several places at once —
    the base rating and whichever surface/venue adjustments the conditions
    engage — each at its own step size. Mirrors how update_elo takes an
    effective rating for the expectation but returns a delta for the base.
    """
    if serve_pct is None:
        return None
    score = normalize_serve_score(serve_pct, surface, cfg, indoor)
    return score - expected_score(effective_serve_elo, effective_return_elo)


def update_serve_elo(
    server_serve_elo: float,
    returner_return_elo: float,
    serve_pct: float | None,
    surface: str,
    k: float,
    cfg: ServeEloConfig | None = None,
    indoor: bool = False,
) -> tuple[float, float]:
    """Update serve and return Elo for a serve sub-game.

    Uses opponent-relative Elo: normalizes serve% to a [0,1] score,
    computes expected score from serve Elo vs return Elo, and updates
    both ratings (zero-sum).

    Returns:
        Tuple of (new_server_serve_elo, new_returner_return_elo).
    """
    surprise = serve_surprise(
        serve_pct, surface, server_serve_elo, returner_return_elo, cfg, indoor,
    )
    if surprise is None:
        return server_serve_elo, returner_return_elo

    return (
        server_serve_elo + k * surprise,
        returner_return_elo - k * surprise,
    )


def update_return_elo(
    returner_return_elo: float,
    server_serve_elo: float,
    opp_serve_pct: float | None,
    surface: str,
    k: float,
    cfg: ServeEloConfig | None = None,
    indoor: bool = False,
) -> tuple[float, float]:
    """Update return and serve Elo for a return sub-game.

    This is the returner's perspective of a serve sub-game. The opponent's
    serve% is the input — low opp_serve_pct means the returner did well.

    Returns:
        Tuple of (new_returner_return_elo, new_server_serve_elo).
    """
    if opp_serve_pct is None:
        return returner_return_elo, server_serve_elo

    new_server, new_returner = update_serve_elo(
        server_serve_elo, returner_return_elo, opp_serve_pct, surface, k, cfg,
        indoor,
    )
    return new_returner, new_server


def initialize_player(
    ranking: int | None, cfg: ServeEloConfig | None = None,
) -> PlayerRating:
    """Initialize a new player's rating, optionally seeded from ranking.

    Mapping: #1 -> ~2400, #100 -> ~1800, #500 -> ~1400, unranked -> 1300
    """
    if ranking is not None and ranking > 0:
        elo = SEED_ELO_MAX - math.sqrt(ranking) * SEED_RANK_COEFF
        elo = max(SEED_ELO_MIN, min(SEED_ELO_MAX, elo))
    else:
        elo = SEED_UNRANKED

    # Serve and return start from a SHRUNK version of the rank seed rather than
    # flat DEFAULT_ELO. Flat left a quarter of singles rows carrying a serve
    # rating of exactly 1500 — no information at all for precisely the new and
    # challenger players a downstream model most needs a number for. Shrunk
    # rather than transplanted because rank measures overall strength, and a
    # full copy would import more general skill than serving evidence supports.
    #
    # Note this fires once, at a player's first appearance anywhere, which for
    # an ITF-graduate is their ITF debut when their rank is weak or absent. The
    # seed is correspondingly weak for that population; it is not re-anchored
    # when they reach tour level, because that initialization point is shared
    # with every other rating dimension.
    shrink = (cfg or DEFAULT_SERVE_ELO_CONFIG).seed_shrink
    serve_seed = DEFAULT_ELO + shrink * (elo - DEFAULT_ELO)

    return PlayerRating(
        elo=elo,
        rd=DEFAULT_RD,
        hard_adj=0.0,
        clay_adj=0.0,
        grass_adj=0.0,
        serve_elo=serve_seed,
        serve_rd=DEFAULT_RD,
        return_elo=serve_seed,
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
) -> float:
    """Update first serve power based on ace rate.

    ace_rate = aces / first_serve_pts_won
    Uses EMA toward a target derived from observed ace rate.
    """
    if ace_rate is None:
        return current_elo

    baseline = FIRST_SERVE_POWER_BASELINE.get(surface, 0.176)
    target = DEFAULT_ELO + (ace_rate - baseline) * STYLE_SCALE
    return current_elo + EMA_ALPHA * (target - current_elo)


def update_second_serve_reliability(
    current_elo: float,
    reliability: float | None,
    surface: str,
) -> float:
    """Update second serve reliability.

    reliability = 1 - (double_faults / second_serve_pts_played)
    Uses EMA toward a target derived from observed reliability.
    """
    if reliability is None:
        return current_elo

    baseline = SECOND_SERVE_RELIABILITY_BASELINE.get(surface, 0.893)
    target = DEFAULT_ELO + (reliability - baseline) * STYLE_SCALE
    return current_elo + EMA_ALPHA * (target - current_elo)


def update_ace_resistance(
    current_elo: float,
    resistance: float | None,
    surface: str,
) -> float:
    """Update ace resistance based on opponent's ace rate against us.

    resistance = 1 - (opp_svc_aces / ret_first_serve_pts_lost)
    Uses EMA toward a target derived from observed resistance.
    """
    if resistance is None:
        return current_elo

    baseline = ACE_RESISTANCE_BASELINE.get(surface, 0.824)
    target = DEFAULT_ELO + (resistance - baseline) * STYLE_SCALE
    return current_elo + EMA_ALPHA * (target - current_elo)


def update_serve_clutch(
    current_elo: float,
    save_rate: float | None,
    surface: str,
) -> float:
    """Update serve clutch based on break points saved.

    save_rate = bp_saved / bp_faced
    Uses EMA toward a target derived from observed save rate.
    """
    if save_rate is None:
        return current_elo

    baseline = SERVE_CLUTCH_BASELINE.get(surface, 0.597)
    target = DEFAULT_ELO + (save_rate - baseline) * STYLE_SCALE
    return current_elo + EMA_ALPHA * (target - current_elo)


def update_return_clutch(
    current_elo: float,
    conversion_rate: float | None,
    surface: str,
) -> float:
    """Update return clutch based on break points converted.

    conversion_rate = bp_converted / bp_opportunities
    Uses EMA toward a target derived from observed conversion rate.
    """
    if conversion_rate is None:
        return current_elo

    baseline = RETURN_CLUTCH_BASELINE.get(surface, 0.404)
    target = DEFAULT_ELO + (conversion_rate - baseline) * STYLE_SCALE
    return current_elo + EMA_ALPHA * (target - current_elo)


def update_tb_clutch(
    current_elo: float,
    tb_won: int,
    tb_played: int,
) -> float:
    """Update tiebreak clutch based on TB win rate.

    Uses EMA toward a target derived from observed TB win rate.
    """
    if tb_played == 0:
        return current_elo

    win_rate = tb_won / tb_played
    target = DEFAULT_ELO + (win_rate - TB_CLUTCH_BASELINE) * STYLE_SCALE
    return current_elo + EMA_ALPHA * (target - current_elo)


def update_indoor_adj(
    current_adj: float,
    player_effective_elo: float,
    opponent_effective_elo: float,
    won: bool,
    k: float,
) -> float:
    """Calculate new indoor adjustment after an indoor match.

    Mirrors ``update_surface_adj``: an additive, opponent-adjusted Elo modifier,
    applied only on indoor matches. The caller supplies pre-match effective Elos
    (base + surface adj + pre-match indoor adj) and the current indoor
    adjustment. Centered at 0.

    Args:
        current_adj: Current indoor adjustment value.
        player_effective_elo: Player's effective Elo (base + surface + indoor).
        opponent_effective_elo: Opponent's effective Elo (base + surface + indoor).
        won: Whether the player won.
        k: K-factor for this update.

    Returns:
        New indoor adjustment value.
    """
    expected = expected_score(player_effective_elo, opponent_effective_elo)
    outcome = 1.0 if won else 0.0

    return current_adj + k * (outcome - expected)
