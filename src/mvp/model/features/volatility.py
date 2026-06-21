"""Result-volatility features — empirical erraticism of recent outcomes.

A net-new feature family (issue #95), distinct from the Glicko-2 ``sigma``
features. Glicko-2 volatility (``player_glicko_sigma``) is frozen at its 0.06
initial value under single-game rating periods, so every sigma-based feature is
a constant rescaling of ``glicko_diff`` and carries no information. Rather than
repair that latent quantity, this measures volatility *directly* from data: the
dispersion of a player's recent results around their pre-match rating-implied
win probability.

For each match a standardized (Pearson) residual is formed against the glicko
win probability ``E``:

    E       = 1 / (1 + exp(-(player_glicko_mu - opp_glicko_mu) / GLICKO_SCALE))
    z       = (won - E) / sqrt(E * (1 - E))

``form_volatility`` is the rolling sample std of ``z`` over the player's recent
matches (strictly pre-match — the current result is excluded). Standardizing by
``sqrt(E(1-E))`` removes the mechanical heteroskedasticity of win/loss outcomes
(close matches have larger raw residuals regardless of erraticism), so the
measure is interpretable on a fixed scale:

    ~1.0  results as dispersed as the Bernoulli null predicts (well-explained)
    >1    more erratic than skill/uncertainty alone explain
    <1    more consistent than expected

``impute=None``: thin-history rows (fewer than 2 prior matches in the window)
are left null so "no volatility history" is distinguishable from "low
volatility" — never fabricated. Mirror generates ``opp_form_volatility`` via the
engine's match self-join (the opponent's own value), and ``register_diff`` adds
the player-minus-opponent contrast.

NOTE: a streakiness axis (lag-1 autocorrelation of signed residuals — runs
hot/cold, a genuinely different construct from dispersion) is intentionally NOT
included here; it does not express cleanly as a rolling primitive and should be
added as its own feature only if dispersion earns its place in selection first.
"""

import polars as pl

from mvp.model.primitives import cumulative_std, rolling_std
from mvp.model.registry import feature, register_diff

# Glicko-2 / Elo base-e scale (400 / ln(10)); ratings are stored in Elo-point
# units in this codebase, so the win prob uses this as the logistic scale.
# Matches GLICKO_SCALE in glicko_interactions.py — domain-grounded, not tuned.
GLICKO_SCALE = 173.7178

# Win-prob clamp for the standardizing denominator sqrt(E(1-E)); keeps it away
# from 0 for extreme mismatches without materially altering normal-range values.
_E_CLAMP = (0.02, 0.98)


def _standardized_residual() -> pl.Expr:
    """Pearson residual of the outcome vs the glicko-implied win probability."""
    mu_diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    e = (1.0 / (1.0 + (-mu_diff / GLICKO_SCALE).exp())).clip(*_E_CLAMP)
    won = pl.col("won").cast(pl.Float64)
    return (won - e) / (e * (1.0 - e)).sqrt()


@feature(
    name="form_volatility",
    params=["days"],
    description=(
        "Rolling std of standardized result residuals vs glicko win prob "
        "(recent erraticism; ~1 = Bernoulli-expected, >1 = erratic)"
    ),
    mirror=True,
    impute=None,
)
def form_volatility(days: int | None = None) -> pl.Expr:
    """Dispersion of recent results around the rating-implied win prob.

    Windowed (``days`` set) or all-time (``days=None``). Strictly pre-match:
    the current outcome is excluded from the window.
    """
    resid = _standardized_residual()
    if days is None:
        return cumulative_std(resid, group_by="player_id")
    return rolling_std(resid, days=days, group_by="player_id")


register_diff("form_volatility")
