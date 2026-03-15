import math
from dataclasses import dataclass
from datetime import date

from mvp.atptour.glicko.constants import (
    EPSILON,
    INITIAL_MU,
    INITIAL_RD,
    INITIAL_SIGMA,
    MAX_RD,
    MAX_SIGMA,
    MIN_RD,
    MIN_SIGMA,
    SCALE,
)


@dataclass
class GlickoRating:
    """Holds Glicko-2 rating state for a player."""

    mu: float = INITIAL_MU
    rd: float = INITIAL_RD
    sigma: float = INITIAL_SIGMA

    hard_adj: float = 0.0
    hard_rd: float = INITIAL_RD
    hard_sigma: float = INITIAL_SIGMA
    clay_adj: float = 0.0
    clay_rd: float = INITIAL_RD
    clay_sigma: float = INITIAL_SIGMA
    grass_adj: float = 0.0
    grass_rd: float = INITIAL_RD
    grass_sigma: float = INITIAL_SIGMA

    match_count: int = 0
    last_match_date: date | None = None
    last_hard_date: date | None = None
    last_clay_date: date | None = None
    last_grass_date: date | None = None

    def get_surface_adj(self, surface: str) -> float:
        """Return surface adjustment for the given surface."""
        return {
            "Hard": self.hard_adj,
            "Clay": self.clay_adj,
            "Grass": self.grass_adj,
        }.get(surface, 0.0)

    def effective_surface_mu(self, surface: str) -> float:
        """Return mu plus surface adjustment."""
        return self.mu + self.get_surface_adj(surface)


def to_glicko2(mu: float, rd: float) -> tuple[float, float]:
    """Convert from Glicko scale to Glicko-2 internal scale."""
    return (mu - 1500.0) / SCALE, rd / SCALE


def from_glicko2(mu_prime: float, rd_prime: float) -> tuple[float, float]:
    """Convert from Glicko-2 internal scale to Glicko scale."""
    return mu_prime * SCALE + 1500.0, rd_prime * SCALE


def g(phi: float) -> float:
    """Opponent RD weighting: 1 / sqrt(1 + 3*phi^2 / pi^2)."""
    return 1.0 / math.sqrt(1.0 + 3.0 * phi**2 / math.pi**2)


def expected_score(mu: float, opp_mu: float, opp_phi: float) -> float:
    """Expected score E(mu, mu_j, phi_j) in Glicko-2 scale."""
    return 1.0 / (1.0 + math.exp(-g(opp_phi) * (mu - opp_mu)))


def _f(
    x: float, delta_sq: float, phi_sq: float, v: float, a: float, tau: float,
) -> float:
    """Illinois method target function f(x) — Glickman's Equation A7.

    Note: `a` here is the constant ln(sigma^2), NOT the bracket endpoint.
    """
    ex = math.exp(x)
    num = ex * (delta_sq - phi_sq - v - ex)
    denom = 2.0 * (phi_sq + v + ex) ** 2
    return num / denom - (x - a) / tau**2


_MAX_ILLINOIS_ITERATIONS = 100


def _compute_new_sigma(
    sigma: float, phi: float, v: float, delta: float, tau: float,
) -> float:
    """Compute new volatility via Illinois method (Glickman Step 5).

    IMPORTANT: `a_orig` is the constant ln(sigma^2) passed to _f().
    `bracket_a` / `bracket_b` are the mutable bracket endpoints.
    These MUST be kept separate — mixing them produces wrong results.
    """
    a_orig = math.log(sigma**2)
    phi_sq = phi**2
    delta_sq = delta**2

    # Step 5b: determine initial bounds
    bracket_a = a_orig
    if delta_sq > phi_sq + v:
        bracket_b = math.log(delta_sq - phi_sq - v)
    else:
        k = 1
        while _f(a_orig - k * tau, delta_sq, phi_sq, v, a_orig, tau) < 0:
            k += 1
        bracket_b = a_orig - k * tau

    # Step 5c
    f_a = _f(bracket_a, delta_sq, phi_sq, v, a_orig, tau)
    f_b = _f(bracket_b, delta_sq, phi_sq, v, a_orig, tau)

    # Step 5d: iterate with max-iteration guard
    for _ in range(_MAX_ILLINOIS_ITERATIONS):
        if abs(bracket_b - bracket_a) <= EPSILON:
            break
        c = bracket_a + (bracket_a - bracket_b) * f_a / (f_b - f_a)
        f_c = _f(c, delta_sq, phi_sq, v, a_orig, tau)
        if f_c * f_b <= 0:
            bracket_a = bracket_b
            f_a = f_b
        else:
            f_a /= 2.0
        bracket_b = c
        f_b = f_c

    # Step 5e
    new_sigma = math.exp(bracket_a / 2.0)
    return max(MIN_SIGMA, min(MAX_SIGMA, new_sigma))


def glicko2_update(
    player_mu: float,
    player_rd: float,
    player_sigma: float,
    opp_mu: float,
    opp_rd: float,
    won: bool,
    tau: float,
) -> tuple[float, float, float]:
    """Full Glicko-2 single-match update.

    Returns (new_mu, new_rd, new_sigma) in Glicko scale.
    """
    # Step 1: convert to Glicko-2 scale
    mu, phi = to_glicko2(player_mu, player_rd)
    opp_mu_g2, opp_phi = to_glicko2(opp_mu, opp_rd)

    # Step 2-3: g, E
    g_val = g(opp_phi)
    e_val = expected_score(mu, opp_mu_g2, opp_phi)

    # Step 4: estimated variance
    v = 1.0 / (g_val**2 * e_val * (1.0 - e_val))

    # Delta
    outcome = 1.0 if won else 0.0
    delta = v * g_val * (outcome - e_val)

    # Step 5: new sigma
    new_sigma = _compute_new_sigma(player_sigma, phi, v, delta, tau)

    # Step 6a: phi_star (pre-update RD widened by volatility)
    phi_star = math.sqrt(phi**2 + new_sigma**2)

    # Step 6b: phi_new (new RD incorporating match info)
    phi_new = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)

    # Step 7: mu_new
    mu_new = mu + phi_new**2 * g_val * (outcome - e_val)

    # Step 8: convert back
    final_mu, final_rd = from_glicko2(mu_new, phi_new)
    final_rd = max(MIN_RD, min(MAX_RD, final_rd))

    return final_mu, final_rd, new_sigma


def glicko2_update_surface(
    adj: float,
    adj_rd: float,
    adj_sigma: float,
    player_mu: float,
    opp_mu: float,
    opp_rd: float,
    won: bool,
    tau: float,
) -> tuple[float, float, float]:
    """Glicko-2 update for a surface adjustment dimension.

    Uses base mus for expected score to avoid double-counting.
    The adjustment has its own RD and sigma.

    Returns (new_adj, new_adj_rd, new_adj_sigma).
    """
    # Convert base mus to Glicko-2 scale for expected score
    mu_p, _ = to_glicko2(player_mu, 0.0)
    mu_o, phi_o = to_glicko2(opp_mu, opp_rd)

    # Adjustment RD in Glicko-2 scale
    phi_adj = adj_rd / SCALE

    # Expected score from base mus
    g_val = g(phi_o)
    e_val = expected_score(mu_p, mu_o, phi_o)

    # Estimated variance
    v = 1.0 / (g_val**2 * e_val * (1.0 - e_val))

    outcome = 1.0 if won else 0.0
    delta = v * g_val * (outcome - e_val)

    # New sigma for the adjustment
    new_sigma = _compute_new_sigma(adj_sigma, phi_adj, v, delta, tau)

    # phi_star and phi_new for the adjustment
    phi_star = math.sqrt(phi_adj**2 + new_sigma**2)
    phi_new = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)

    # Update adjustment (centered at 0, not 1500)
    adj_g2 = adj / SCALE
    new_adj_g2 = adj_g2 + phi_new**2 * g_val * (outcome - e_val)

    new_adj = new_adj_g2 * SCALE
    new_adj_rd = phi_new * SCALE
    new_adj_rd = max(MIN_RD, min(MAX_RD, new_adj_rd))

    return new_adj, new_adj_rd, new_sigma


def apply_glicko_inactivity(
    rd: float,
    sigma: float,
    last_date: date | None,
    current_date: date,
) -> float:
    """Grow RD based on inactivity period.

    All math operates in Glicko-2 scale internally.
    Sigma is unchanged (only changes through match updates).
    """
    if last_date is None:
        return rd

    days_inactive = (current_date - last_date).days
    if days_inactive <= 0:
        return rd

    # Convert to Glicko-2 scale
    phi = rd / SCALE

    # Grow: phi_new = sqrt(phi^2 + sigma^2 * days_inactive)
    phi_new = math.sqrt(phi**2 + sigma**2 * days_inactive)

    # Convert back and cap
    new_rd = phi_new * SCALE
    return min(MAX_RD, new_rd)
