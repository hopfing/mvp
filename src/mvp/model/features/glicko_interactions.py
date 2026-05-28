"""Glicko-2 interaction features (H58 family).

Engineered interaction features over the (μ, RD, σ) triple, designed to
surface uncertainty-aware constructions that XGB cannot efficiently
reconstruct from the independent scalars already in `glicko.py`.

Nine mechanistic categories:
  1. Per-player ratios/products (mirror=True; auto-generates opp_*)
  2. Pair-level differentials and sums (mirror=False, match_level=True for symmetric)
  3. Joint uncertainty (mirror=False, match_level=True)
  4. z-scores — Bayesian-correct directional inversions of `glicko_diff_x_rd_sum`
     (anti-symmetric, so mirror=False, match_level=False — engine adds player_ prefix)
  5. TrueSkill-style P(win) — sigmoid approximation of Φ(z), `1/(1+exp(-1.702z))`
  6. Asymmetric uncertainty (per-player uncertainty × differential)
  7. Shrinkage forms (μ_diff discounted by uncertainty)
  8. Logistic-saturated (Elo-style P(win) at Glicko-2 standard scale 173.7178 —
     base-e equivalent of Elo's base-10 `400`; works in Elo-point units which is
     this codebase's storage convention per glicko_mu test fixture values)
  9. Distribution overlap (Bhattacharyya exact closed-form; Overlap Coefficient
     uses the equal-variance approximation — see docstring on that feature)

All raw-column references are inline — no `depends_on` declarations and no
references to other registered feature names (those columns don't exist on the
source DataFrame; the feature function receives raw columns only).

See `mvp-docs/experiments/model-exploration-log.md` H58 for the hypothesis.
"""

import polars as pl

from mvp.model.registry import feature

# Glicko-2 / Elo base-e scaling constant — conversion factor between Glicko-2
# internal units and Elo-equivalent points. `400 / ln(10) ≈ 173.7178`. This is
# the natural scale for the logistic feature given that ratings are stored in
# Elo-point units in this codebase (player_glicko_mu values like 1500, 1600).
# Domain-grounded, not empirical — does not drift, does not need refresh.
GLICKO_SCALE = 173.7178

# Sigmoid coefficient for the Φ(z) approximation `1/(1+exp(-c*z))`. The value
# 1.702 is the standard logistic-Gaussian matching constant; max approximation
# error ~1% in the tails. Tree splits are invariant to monotone transforms, so
# the approximation is functionally equivalent to exact Φ for FS / split use.
PHI_APPROX_COEF = 1.702


# ============================================================================
# 1. PER-PLAYER RATIOS / PRODUCTS (mirror=True — engine generates opp_* variant)
# ============================================================================

@feature(
    name="glicko_mu_over_rd",
    description="Glicko mu / RD — signal-to-estimation-noise per player",
    mirror=True,
    impute=None,
)
def glicko_mu_over_rd() -> pl.Expr:
    return pl.col("player_glicko_mu") / pl.col("player_glicko_rd")


@feature(
    name="glicko_mu_over_sigma",
    description="Glicko mu / sigma — signal-to-volatility per player",
    mirror=True,
    impute=None,
)
def glicko_mu_over_sigma() -> pl.Expr:
    return pl.col("player_glicko_mu") / pl.col("player_glicko_sigma")


@feature(
    name="glicko_mu_x_rd",
    description="Glicko mu × RD — joint magnitude per player",
    mirror=True,
    impute=None,
)
def glicko_mu_x_rd() -> pl.Expr:
    return pl.col("player_glicko_mu") * pl.col("player_glicko_rd")


@feature(
    name="glicko_mu_x_sigma",
    description="Glicko mu × sigma — joint magnitude with volatility per player",
    mirror=True,
    impute=None,
)
def glicko_mu_x_sigma() -> pl.Expr:
    return pl.col("player_glicko_mu") * pl.col("player_glicko_sigma")


@feature(
    name="glicko_rd_x_sigma",
    description="Glicko RD × sigma — combined uncertainty score per player",
    mirror=True,
    impute=None,
)
def glicko_rd_x_sigma() -> pl.Expr:
    return pl.col("player_glicko_rd") * pl.col("player_glicko_sigma")


@feature(
    name="glicko_rd_over_sigma",
    description="Glicko RD / sigma — which uncertainty type dominates per player",
    mirror=True,
    impute=None,
)
def glicko_rd_over_sigma() -> pl.Expr:
    return pl.col("player_glicko_rd") / pl.col("player_glicko_sigma")


@feature(
    name="glicko_log_rd",
    description="log(Glicko RD) — log-scaled estimation uncertainty (heavy tail)",
    mirror=True,
    impute=None,
)
def glicko_log_rd() -> pl.Expr:
    return pl.col("player_glicko_rd").log()


@feature(
    name="glicko_log_sigma",
    description="log(Glicko sigma) — log-scaled volatility",
    mirror=True,
    impute=None,
)
def glicko_log_sigma() -> pl.Expr:
    return pl.col("player_glicko_sigma").log()


@feature(
    name="glicko_precision",
    description="1 / Glicko RD — inverse estimation uncertainty per player",
    mirror=True,
    impute=None,
)
def glicko_precision() -> pl.Expr:
    return 1.0 / pl.col("player_glicko_rd")


@feature(
    name="glicko_precision_sigma",
    description="1 / Glicko sigma — inverse volatility per player",
    mirror=True,
    impute=None,
)
def glicko_precision_sigma() -> pl.Expr:
    return 1.0 / pl.col("player_glicko_sigma")


# ============================================================================
# 2. PAIR-LEVEL DIFFERENTIALS AND SUMS (symmetric → match_level=True)
# ============================================================================

@feature(
    name="glicko_sigma_sum",
    description="Combined Glicko sigma (total volatility)",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_sigma_sum() -> pl.Expr:
    return pl.col("player_glicko_sigma") + pl.col("opp_glicko_sigma")


@feature(
    name="glicko_rd_max",
    description="Max of player and opponent Glicko RDs",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_rd_max() -> pl.Expr:
    return pl.max_horizontal(
        pl.col("player_glicko_rd"), pl.col("opp_glicko_rd"),
    )


@feature(
    name="glicko_rd_min",
    description="Min of player and opponent Glicko RDs",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_rd_min() -> pl.Expr:
    return pl.min_horizontal(
        pl.col("player_glicko_rd"), pl.col("opp_glicko_rd"),
    )


@feature(
    name="glicko_rd_ratio",
    description="max(RD) / min(RD) — asymmetry magnitude (guarded against div-by-0)",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_rd_ratio() -> pl.Expr:
    # Belt-and-suspenders guard: Glicko-2 invariant says RD > 0, but inf in a
    # ratio behaves differently in trees than NaN-passthrough. Default to 1.0
    # (perfect symmetry) if min_rd somehow hits 0.
    max_rd = pl.max_horizontal(
        pl.col("player_glicko_rd"), pl.col("opp_glicko_rd"),
    )
    min_rd = pl.min_horizontal(
        pl.col("player_glicko_rd"), pl.col("opp_glicko_rd"),
    )
    return pl.when(min_rd > 0).then(max_rd / min_rd).otherwise(1.0)


@feature(
    name="glicko_sigma_max",
    description="Max of player and opponent Glicko sigmas",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_sigma_max() -> pl.Expr:
    return pl.max_horizontal(
        pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
    )


@feature(
    name="glicko_sigma_min",
    description="Min of player and opponent Glicko sigmas",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_sigma_min() -> pl.Expr:
    return pl.min_horizontal(
        pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
    )


@feature(
    name="glicko_sigma_ratio",
    description="max(sigma) / min(sigma) — volatility asymmetry magnitude",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_sigma_ratio() -> pl.Expr:
    max_s = pl.max_horizontal(
        pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
    )
    min_s = pl.min_horizontal(
        pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
    )
    return pl.when(min_s > 0).then(max_s / min_s).otherwise(1.0)


# ============================================================================
# 3. JOINT UNCERTAINTY (symmetric → match_level=True)
# ============================================================================

@feature(
    name="glicko_joint_rd",
    description="sqrt(rd_p^2 + rd_o^2) — joint estimation uncertainty",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_joint_rd() -> pl.Expr:
    return (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
    ).sqrt()


@feature(
    name="glicko_joint_sigma",
    description="sqrt(sigma_p^2 + sigma_o^2) — joint volatility",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_joint_sigma() -> pl.Expr:
    return (
        pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
    ).sqrt()


@feature(
    name="glicko_joint_total",
    description="sqrt(rd_p^2 + rd_o^2 + sigma_p^2 + sigma_o^2) — combined joint uncertainty",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_joint_total() -> pl.Expr:
    return (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
        + pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
    ).sqrt()


# ============================================================================
# 4. Z-SCORES — Bayesian-correct directional inversions (anti-symmetric)
# ============================================================================

@feature(
    name="glicko_zscore_rd",
    description="mu_diff / sqrt(rd_p^2 + rd_o^2) — separation in joint-RD units",
    mirror=False,
    impute=None,
)
def glicko_zscore_rd() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    joint_rd = (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
    ).sqrt()
    return mu_diff / joint_rd


@feature(
    name="glicko_zscore_sigma",
    description="mu_diff / sqrt(sigma_p^2 + sigma_o^2) — separation in joint-sigma units",
    mirror=False,
    impute=None,
)
def glicko_zscore_sigma() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    joint_s = (
        pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
    ).sqrt()
    return mu_diff / joint_s


@feature(
    name="glicko_zscore_total",
    description="mu_diff / sqrt(rd_p^2 + rd_o^2 + sigma_p^2 + sigma_o^2) — combined-uncertainty z",
    mirror=False,
    impute=None,
)
def glicko_zscore_total() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    joint = (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
        + pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
    ).sqrt()
    return mu_diff / joint


@feature(
    name="glicko_diff_over_rd_sum",
    description="mu_diff / (rd_p + rd_o) — simpler shrunk-by-sum inversion",
    mirror=False,
    impute=None,
)
def glicko_diff_over_rd_sum() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    rd_sum = pl.col("player_glicko_rd") + pl.col("opp_glicko_rd")
    return mu_diff / rd_sum


@feature(
    name="glicko_diff_over_sigma_sum",
    description="mu_diff / (sigma_p + sigma_o) — sigma-based simple inversion",
    mirror=False,
    impute=None,
)
def glicko_diff_over_sigma_sum() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    sigma_sum = pl.col("player_glicko_sigma") + pl.col("opp_glicko_sigma")
    return mu_diff / sigma_sum


# ============================================================================
# 5. TRUESKILL-STYLE P(WIN) — sigmoid approximation of Φ(z)
# ============================================================================

@feature(
    name="glicko_truesk_pwin_rd",
    description="Φ(zscore_rd) ≈ 1/(1+exp(-1.702 z)) — TrueSkill-style P(win) on joint RD",
    mirror=False,
    impute=None,
)
def glicko_truesk_pwin_rd() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    joint_rd = (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
    ).sqrt()
    z = mu_diff / joint_rd
    return 1.0 / (1.0 + (-PHI_APPROX_COEF * z).exp())


@feature(
    name="glicko_truesk_pwin_sigma",
    description="Φ(zscore_sigma) — TrueSkill-style P(win) on joint sigma",
    mirror=False,
    impute=None,
)
def glicko_truesk_pwin_sigma() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    joint_s = (
        pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
    ).sqrt()
    z = mu_diff / joint_s
    return 1.0 / (1.0 + (-PHI_APPROX_COEF * z).exp())


@feature(
    name="glicko_truesk_pwin_total",
    description="Φ(zscore_total) — TrueSkill-style P(win) on combined uncertainty",
    mirror=False,
    impute=None,
)
def glicko_truesk_pwin_total() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    joint = (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
        + pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
    ).sqrt()
    z = mu_diff / joint
    return 1.0 / (1.0 + (-PHI_APPROX_COEF * z).exp())


# ============================================================================
# 6. ASYMMETRIC UNCERTAINTY INTERACTIONS — anchored to player vs opp specifically
# ============================================================================
# These are NOT mirror=True because mirroring would produce semantically wrong
# values (the mirrored version is not the same as the swapped-perspective
# version). Each player-anchored and opp-anchored variant is registered
# separately. Anti-symmetric under player/opp swap → match_level=False.

@feature(
    name="glicko_mu_diff_x_player_rd",
    description="mu_diff × player_rd — skill diff scaled by player's own uncertainty",
    mirror=False,
    impute=None,
)
def glicko_mu_diff_x_player_rd() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return mu_diff * pl.col("player_glicko_rd")


@feature(
    name="glicko_mu_diff_x_opp_rd",
    description="mu_diff × opp_rd — skill diff scaled by opponent's uncertainty",
    mirror=False,
    impute=None,
)
def glicko_mu_diff_x_opp_rd() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return mu_diff * pl.col("opp_glicko_rd")


@feature(
    name="glicko_mu_diff_x_player_sigma",
    description="mu_diff × player_sigma — skill diff scaled by player volatility",
    mirror=False,
    impute=None,
)
def glicko_mu_diff_x_player_sigma() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return mu_diff * pl.col("player_glicko_sigma")


@feature(
    name="glicko_mu_diff_x_opp_sigma",
    description="mu_diff × opp_sigma — skill diff scaled by opponent volatility",
    mirror=False,
    impute=None,
)
def glicko_mu_diff_x_opp_sigma() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return mu_diff * pl.col("opp_glicko_sigma")


@feature(
    name="glicko_mu_diff_x_rd_asymmetry",
    description="mu_diff × (rd_p - rd_o) — signed product of skill and RD asymmetries",
    mirror=False,
    match_level=True,  # (-a)(-b) = ab → invariant under swap
    impute=None,
)
def glicko_mu_diff_x_rd_asymmetry() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    rd_diff = pl.col("player_glicko_rd") - pl.col("opp_glicko_rd")
    return mu_diff * rd_diff


@feature(
    name="glicko_mu_diff_x_sigma_asymmetry",
    description="mu_diff × (sigma_p - sigma_o) — signed product of skill and sigma asymmetries",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_mu_diff_x_sigma_asymmetry() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    sigma_diff = pl.col("player_glicko_sigma") - pl.col("opp_glicko_sigma")
    return mu_diff * sigma_diff


# ============================================================================
# 7. SHRINKAGE FORMS — mu_diff discounted by uncertainty (anti-symmetric)
# ============================================================================

@feature(
    name="glicko_shrunk_diff_rd",
    description="mu_diff × 1/(1 + rd_sum) — Bayesian shrinkage form (linear in rd_sum)",
    mirror=False,
    impute=None,
)
def glicko_shrunk_diff_rd() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    rd_sum = pl.col("player_glicko_rd") + pl.col("opp_glicko_rd")
    return mu_diff / (1.0 + rd_sum)


@feature(
    name="glicko_shrunk_diff_rdsq",
    description="mu_diff × 1/(1 + rd_p^2 + rd_o^2) — quadratic shrinkage (sharper)",
    mirror=False,
    impute=None,
)
def glicko_shrunk_diff_rdsq() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    rd_sq = (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
    )
    return mu_diff / (1.0 + rd_sq)


@feature(
    name="glicko_shrunk_diff_sigma",
    description="mu_diff × 1/(1 + sigma_sum) — shrinkage by joint volatility",
    mirror=False,
    impute=None,
)
def glicko_shrunk_diff_sigma() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    sigma_sum = pl.col("player_glicko_sigma") + pl.col("opp_glicko_sigma")
    return mu_diff / (1.0 + sigma_sum)


# ============================================================================
# 8. LOGISTIC-SATURATED — Elo-style fixed-scale P(win), no uncertainty term
# ============================================================================

@feature(
    name="glicko_logistic_diff",
    description=(
        "1/(1 + exp(-mu_diff / 173.7178)) — Elo-style P(win) at Glicko-2 "
        "standard scale (base-e equivalent of Elo's base-10 `400`)"
    ),
    mirror=False,
    impute=None,
)
def glicko_logistic_diff() -> pl.Expr:
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return 1.0 / (1.0 + (-mu_diff / GLICKO_SCALE).exp())


# ============================================================================
# 9. DISTRIBUTION OVERLAP — Gaussian closed-forms
# ============================================================================

@feature(
    name="glicko_bhattacharyya_rd",
    description=(
        "Bhattacharyya coefficient of N(mu_p, rd_p^2) and N(mu_o, rd_o^2) — "
        "exact closed form for Gaussians"
    ),
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_bhattacharyya_rd() -> pl.Expr:
    mu_p = pl.col("player_glicko_mu")
    mu_o = pl.col("opp_glicko_mu")
    rd_p = pl.col("player_glicko_rd")
    rd_o = pl.col("opp_glicko_rd")
    rd_sq_sum = rd_p ** 2 + rd_o ** 2
    coef = (2.0 * rd_p * rd_o / rd_sq_sum).sqrt()
    exponent = -((mu_p - mu_o) ** 2) / (4.0 * rd_sq_sum)
    return coef * exponent.exp()


@feature(
    name="glicko_bhattacharyya_sigma",
    description=(
        "Bhattacharyya coefficient using sigma as the distribution width — "
        "exact closed form for Gaussians"
    ),
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_bhattacharyya_sigma() -> pl.Expr:
    mu_p = pl.col("player_glicko_mu")
    mu_o = pl.col("opp_glicko_mu")
    s_p = pl.col("player_glicko_sigma")
    s_o = pl.col("opp_glicko_sigma")
    s_sq_sum = s_p ** 2 + s_o ** 2
    coef = (2.0 * s_p * s_o / s_sq_sum).sqrt()
    exponent = -((mu_p - mu_o) ** 2) / (4.0 * s_sq_sum)
    return coef * exponent.exp()


@feature(
    name="glicko_overlap_coefficient_rd",
    description=(
        "Overlapping Coefficient (equal-variance approximation): "
        "2 * Φ(-|mu_diff| / (2 * sqrt((rd_p^2 + rd_o^2) / 2))). "
        "EXACT only when rd_p == rd_o; approximate otherwise. The Φ is computed "
        "via the sigmoid approximation `1/(1+exp(-1.702 z))`."
    ),
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_overlap_coefficient_rd() -> pl.Expr:
    abs_mu_diff = (
        pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    ).abs()
    half_rd_sq_sum = (
        pl.col("player_glicko_rd") ** 2 + pl.col("opp_glicko_rd") ** 2
    ) / 2.0
    arg = -abs_mu_diff / (2.0 * half_rd_sq_sum.sqrt())
    # OVL = 2 * Φ(arg), Φ via sigmoid approximation
    return 2.0 / (1.0 + (-PHI_APPROX_COEF * arg).exp())
