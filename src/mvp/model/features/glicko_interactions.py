"""Glicko-2 interaction features (H58 family).

Engineered interaction features over the (μ, RD, σ) triple, designed to
surface uncertainty-aware constructions that XGB cannot efficiently
reconstruct from the independent scalars already in `glicko.py`.

Ten mechanistic categories:
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
 10. Form-volatility interactions — LIVE replacements for the dead sigma forms in
     categories 6-8. Glicko σ is frozen at ~0.06 (single-game rating periods), so
     every σ-based member above is a constant rescaling of its non-σ part.
     `form_volatility` (result-dispersion vs the glicko win prob) is the live
     quantity those forms reach for; each here re-instantiates a dead σ form with it.

Categories 1-9 reference only inline raw columns — no `depends_on` declarations and
no references to other registered feature names (those columns don't exist on the
source DataFrame; the feature function receives raw columns only). Category 10 is the
deliberate exception: it depends on the registered `form_volatility` feature (and so
carries `depends_on` and a `days`-windowed column resolution, like `match_count_max`).

DEPRECATED — every σ-based feature below is commented out (deregistered). Glicko-2 σ
is pinned at ~0.06 under single-game rating periods, so each was a constant, an
identically-zero column, or a constant rescaling of its non-σ part — no signal. They
are left in place (commented) for reference and superseded by category 10; the raw
`player_glicko_sigma` column is untouched (the RD+σ `_total` forms still use it). Each
carries a one-line `# DEPRECATED (frozen sigma):` marker; this paragraph is the why.

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


# DEPRECATED (frozen sigma): rescaled mu.
# @feature(
#     name="glicko_mu_over_sigma",
#     description="Glicko mu / sigma — signal-to-volatility per player",
#     mirror=True,
#     impute=None,
# )
# def glicko_mu_over_sigma() -> pl.Expr:
#     return pl.col("player_glicko_mu") / pl.col("player_glicko_sigma")


@feature(
    name="glicko_mu_x_rd",
    description="Glicko mu × RD — joint magnitude per player",
    mirror=True,
    impute=None,
)
def glicko_mu_x_rd() -> pl.Expr:
    return pl.col("player_glicko_mu") * pl.col("player_glicko_rd")


# DEPRECATED (frozen sigma): rescaled mu.
# @feature(
#     name="glicko_mu_x_sigma",
#     description="Glicko mu × sigma — joint magnitude with volatility per player",
#     mirror=True,
#     impute=None,
# )
# def glicko_mu_x_sigma() -> pl.Expr:
#     return pl.col("player_glicko_mu") * pl.col("player_glicko_sigma")


# DEPRECATED (frozen sigma): rescaled rd.
# @feature(
#     name="glicko_rd_x_sigma",
#     description="Glicko RD × sigma — combined uncertainty score per player",
#     mirror=True,
#     impute=None,
# )
# def glicko_rd_x_sigma() -> pl.Expr:
#     return pl.col("player_glicko_rd") * pl.col("player_glicko_sigma")


# DEPRECATED (frozen sigma): rescaled rd.
# @feature(
#     name="glicko_rd_over_sigma",
#     description="Glicko RD / sigma — which uncertainty type dominates per player",
#     mirror=True,
#     impute=None,
# )
# def glicko_rd_over_sigma() -> pl.Expr:
#     return pl.col("player_glicko_rd") / pl.col("player_glicko_sigma")


@feature(
    name="glicko_log_rd",
    description="log(Glicko RD) — log-scaled estimation uncertainty (heavy tail)",
    mirror=True,
    impute=None,
)
def glicko_log_rd() -> pl.Expr:
    return pl.col("player_glicko_rd").log()


# DEPRECATED (frozen sigma): constant.
# @feature(
#     name="glicko_log_sigma",
#     description="log(Glicko sigma) — log-scaled volatility",
#     mirror=True,
#     impute=None,
# )
# def glicko_log_sigma() -> pl.Expr:
#     return pl.col("player_glicko_sigma").log()


@feature(
    name="glicko_precision",
    description="1 / Glicko RD — inverse estimation uncertainty per player",
    mirror=True,
    impute=None,
)
def glicko_precision() -> pl.Expr:
    return 1.0 / pl.col("player_glicko_rd")


# DEPRECATED (frozen sigma): constant.
# @feature(
#     name="glicko_precision_sigma",
#     description="1 / Glicko sigma — inverse volatility per player",
#     mirror=True,
#     impute=None,
# )
# def glicko_precision_sigma() -> pl.Expr:
#     return 1.0 / pl.col("player_glicko_sigma")


# ============================================================================
# 2. PAIR-LEVEL DIFFERENTIALS AND SUMS (symmetric → match_level=True)
# ============================================================================

# DEPRECATED (frozen sigma): constant.
# @feature(
#     name="glicko_sigma_sum",
#     description="Combined Glicko sigma (total volatility)",
#     mirror=False,
#     match_level=True,
#     impute=None,
# )
# def glicko_sigma_sum() -> pl.Expr:
#     return pl.col("player_glicko_sigma") + pl.col("opp_glicko_sigma")


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


# DEPRECATED (frozen sigma): constant.
# @feature(
#     name="glicko_sigma_max",
#     description="Max of player and opponent Glicko sigmas",
#     mirror=False,
#     match_level=True,
#     impute=None,
# )
# def glicko_sigma_max() -> pl.Expr:
#     return pl.max_horizontal(
#         pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
#     )


# DEPRECATED (frozen sigma): constant.
# @feature(
#     name="glicko_sigma_min",
#     description="Min of player and opponent Glicko sigmas",
#     mirror=False,
#     match_level=True,
#     impute=None,
# )
# def glicko_sigma_min() -> pl.Expr:
#     return pl.min_horizontal(
#         pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
#     )


# DEPRECATED (frozen sigma): constant (=1).
# @feature(
#     name="glicko_sigma_ratio",
#     description="max(sigma) / min(sigma) — volatility asymmetry magnitude",
#     mirror=False,
#     match_level=True,
#     impute=None,
# )
# def glicko_sigma_ratio() -> pl.Expr:
#     max_s = pl.max_horizontal(
#         pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
#     )
#     min_s = pl.min_horizontal(
#         pl.col("player_glicko_sigma"), pl.col("opp_glicko_sigma"),
#     )
#     return pl.when(min_s > 0).then(max_s / min_s).otherwise(1.0)


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


# DEPRECATED (frozen sigma): constant.
# @feature(
#     name="glicko_joint_sigma",
#     description="sqrt(sigma_p^2 + sigma_o^2) — joint volatility",
#     mirror=False,
#     match_level=True,
#     impute=None,
# )
# def glicko_joint_sigma() -> pl.Expr:
#     return (
#         pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
#     ).sqrt()


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


# DEPRECATED (frozen sigma): rescaled mu_diff.
# @feature(
#     name="glicko_zscore_sigma",
#     description="mu_diff / sqrt(sigma_p^2 + sigma_o^2) — separation in joint-sigma units",
#     mirror=False,
#     impute=None,
# )
# def glicko_zscore_sigma() -> pl.Expr:
#     mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
#     joint_s = (
#         pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
#     ).sqrt()
#     return mu_diff / joint_s


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


# DEPRECATED (frozen sigma): rescaled mu_diff.
# @feature(
#     name="glicko_diff_over_sigma_sum",
#     description="mu_diff / (sigma_p + sigma_o) — sigma-based simple inversion",
#     mirror=False,
#     impute=None,
# )
# def glicko_diff_over_sigma_sum() -> pl.Expr:
#     mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
#     sigma_sum = pl.col("player_glicko_sigma") + pl.col("opp_glicko_sigma")
#     return mu_diff / sigma_sum


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


# DEPRECATED (frozen sigma): monotone transform of mu_diff.
# @feature(
#     name="glicko_truesk_pwin_sigma",
#     description="Φ(zscore_sigma) — TrueSkill-style P(win) on joint sigma",
#     mirror=False,
#     impute=None,
# )
# def glicko_truesk_pwin_sigma() -> pl.Expr:
#     mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
#     joint_s = (
#         pl.col("player_glicko_sigma") ** 2 + pl.col("opp_glicko_sigma") ** 2
#     ).sqrt()
#     z = mu_diff / joint_s
#     return 1.0 / (1.0 + (-PHI_APPROX_COEF * z).exp())


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


# DEPRECATED (frozen sigma): rescaled mu_diff.
# @feature(
#     name="glicko_mu_diff_x_player_sigma",
#     description="mu_diff × player_sigma — skill diff scaled by player volatility",
#     mirror=False,
#     impute=None,
# )
# def glicko_mu_diff_x_player_sigma() -> pl.Expr:
#     mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
#     return mu_diff * pl.col("player_glicko_sigma")


# DEPRECATED (frozen sigma): rescaled mu_diff.
# @feature(
#     name="glicko_mu_diff_x_opp_sigma",
#     description="mu_diff × opp_sigma — skill diff scaled by opponent volatility",
#     mirror=False,
#     impute=None,
# )
# def glicko_mu_diff_x_opp_sigma() -> pl.Expr:
#     mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
#     return mu_diff * pl.col("opp_glicko_sigma")


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


# DEPRECATED (frozen sigma): identically zero.
# @feature(
#     name="glicko_mu_diff_x_sigma_asymmetry",
#     description="mu_diff × (sigma_p - sigma_o) — signed product of skill and sigma asymmetries",
#     mirror=False,
#     match_level=True,
#     impute=None,
# )
# def glicko_mu_diff_x_sigma_asymmetry() -> pl.Expr:
#     mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
#     sigma_diff = pl.col("player_glicko_sigma") - pl.col("opp_glicko_sigma")
#     return mu_diff * sigma_diff


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


# DEPRECATED (frozen sigma): rescaled mu_diff.
# @feature(
#     name="glicko_shrunk_diff_sigma",
#     description="mu_diff × 1/(1 + sigma_sum) — shrinkage by joint volatility",
#     mirror=False,
#     impute=None,
# )
# def glicko_shrunk_diff_sigma() -> pl.Expr:
#     mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
#     sigma_sum = pl.col("player_glicko_sigma") + pl.col("opp_glicko_sigma")
#     return mu_diff / (1.0 + sigma_sum)


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


# DEPRECATED (frozen sigma): function of mu_diff only.
# @feature(
#     name="glicko_bhattacharyya_sigma",
#     description=(
#         "Bhattacharyya coefficient using sigma as the distribution width — "
#         "exact closed form for Gaussians"
#     ),
#     mirror=False,
#     match_level=True,
#     impute=None,
# )
# def glicko_bhattacharyya_sigma() -> pl.Expr:
#     mu_p = pl.col("player_glicko_mu")
#     mu_o = pl.col("opp_glicko_mu")
#     s_p = pl.col("player_glicko_sigma")
#     s_o = pl.col("opp_glicko_sigma")
#     s_sq_sum = s_p ** 2 + s_o ** 2
#     coef = (2.0 * s_p * s_o / s_sq_sum).sqrt()
#     exponent = -((mu_p - mu_o) ** 2) / (4.0 * s_sq_sum)
#     return coef * exponent.exp()


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


# ============================================================================
# 10. FORM-VOLATILITY INTERACTIONS — live replacements for the frozen-sigma forms
# ============================================================================
# Unlike categories 1-9, these depend on the REGISTERED `form_volatility` feature
# (result-dispersion vs the glicko win prob) rather than raw glicko columns — so
# each carries `depends_on=["form_volatility"]` and resolves the window-specific
# column name (`match_count_max` in form.py is the precedent). The engine
# propagates the `days` param to the dependency, so requesting the interaction at
# a window computes form_volatility at that same window.
#
# FORMVOL_RIDGE — additive stabilizer for the two divide-by-volatility forms
# (zscore, diff_over_sum). form_volatility is a sample std that can sit near zero,
# and dividing by it blows the ratio up. Pinned from the EMPIRICAL denominator
# distribution (singles, 2024-25, via scripts/probe_formvol_ridge.py): the joint
# denominator's bulk sits at ~2.2-3.0 with only a thin sub-1.0 tail (near-zero
# needs BOTH players near-flat, which is rare). At 0.1 the ridge dominates the raw
# denominator on <0.1% of rows (so >99.9% keep full volatility dependence — no
# mu_diff/const degeneracy) while shifting the median denominator only ~2-5%, and
# it floors the literal-zero tail. Larger (>=0.25) starts distorting the sum-form
# bulk toward that degeneracy; smaller buys a marginally cleaner bulk for a looser
# tail floor. Not tuned — a numerical stabilizer sized to where form_volatility lives.
FORMVOL_RIDGE = 0.1


def _formvol_cols(days: int | None) -> tuple[str, str]:
    """(player, opp) form_volatility column names for the requested window."""
    suffix = "" if days is None else f"_{days}d"
    return f"player_form_volatility{suffix}", f"opp_form_volatility{suffix}"


@feature(
    name="glicko_shrunk_diff_formvol",
    params=["days"],
    depends_on=["form_volatility"],
    description=(
        "mu_diff / (1 + fv_p + fv_o) — skill gap shrunk by combined result "
        "volatility (live analog of glicko_shrunk_diff_sigma)"
    ),
    mirror=False,
    impute=None,
)
def glicko_shrunk_diff_formvol(days: int | None = None) -> pl.Expr:
    pfv, ofv = _formvol_cols(days)
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    # denominator >= 1 whenever non-null → no guard needed (unlike the divide forms)
    return mu_diff / (1.0 + pl.col(pfv) + pl.col(ofv))


@feature(
    name="glicko_zscore_formvol",
    params=["days"],
    depends_on=["form_volatility"],
    description=(
        "mu_diff / sqrt(fv_p^2 + fv_o^2 + RIDGE) — separation in result-volatility "
        "units (live analog of glicko_zscore_sigma)"
    ),
    mirror=False,
    impute=None,
)
def glicko_zscore_formvol(days: int | None = None) -> pl.Expr:
    pfv, ofv = _formvol_cols(days)
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    joint = (pl.col(pfv) ** 2 + pl.col(ofv) ** 2 + FORMVOL_RIDGE).sqrt()
    return mu_diff / joint


@feature(
    name="glicko_diff_over_formvol_sum",
    params=["days"],
    depends_on=["form_volatility"],
    description=(
        "mu_diff / (fv_p + fv_o + RIDGE) — L1 volatility-normalized gap "
        "(live analog of glicko_diff_over_sigma_sum)"
    ),
    mirror=False,
    impute=None,
)
def glicko_diff_over_formvol_sum(days: int | None = None) -> pl.Expr:
    pfv, ofv = _formvol_cols(days)
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return mu_diff / (pl.col(pfv) + pl.col(ofv) + FORMVOL_RIDGE)


@feature(
    name="glicko_mu_diff_x_player_formvol",
    params=["days"],
    depends_on=["form_volatility"],
    description=(
        "mu_diff × player form_volatility — skill diff scaled by the player's own "
        "result volatility (live analog of glicko_mu_diff_x_player_sigma)"
    ),
    mirror=False,
    impute=None,
)
def glicko_mu_diff_x_player_formvol(days: int | None = None) -> pl.Expr:
    pfv, _ = _formvol_cols(days)
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return mu_diff * pl.col(pfv)


@feature(
    name="glicko_mu_diff_x_opp_formvol",
    params=["days"],
    depends_on=["form_volatility"],
    description=(
        "mu_diff × opp form_volatility — skill diff scaled by the opponent's "
        "result volatility (live analog of glicko_mu_diff_x_opp_sigma)"
    ),
    mirror=False,
    impute=None,
)
def glicko_mu_diff_x_opp_formvol(days: int | None = None) -> pl.Expr:
    _, ofv = _formvol_cols(days)
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return mu_diff * pl.col(ofv)


@feature(
    name="glicko_mu_diff_x_formvol_asymmetry",
    params=["days"],
    depends_on=["form_volatility"],
    description=(
        "mu_diff × (fv_p - fv_o) — signed product of skill and result-volatility "
        "asymmetries (live analog of glicko_mu_diff_x_sigma_asymmetry)"
    ),
    mirror=False,
    match_level=True,  # (-a)(-b) = ab → invariant under swap
    impute=None,
)
def glicko_mu_diff_x_formvol_asymmetry(days: int | None = None) -> pl.Expr:
    pfv, ofv = _formvol_cols(days)
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    # inline subtraction (mirror the sigma analog) rather than depend on the
    # mirror=False form_volatility_diff — the resolver would request that feature's
    # nonexistent opp_ prefix. Identical result; no special null handling to drift.
    return mu_diff * (pl.col(pfv) - pl.col(ofv))
