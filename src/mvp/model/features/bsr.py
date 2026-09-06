"""Serve/return skill filter columns (mvp.atptour.bsr) as model features.

Twelve pre-match passthroughs per side, all null-preserving: a player with no
serve-count observation yet has no state, and that absence is information a
tree reads through the sd and count columns rather than a median.
"""

import polars as pl

from mvp.model.registry import feature, register_diff, register_sum


@feature(
    name="bsr_serve_mu",
    description="Serve skill posterior mean (logit scale)",
    mirror=True,
    impute=None,
)
def bsr_serve_mu() -> pl.Expr:
    return pl.col("player_bsr_serve_mu")


@feature(
    name="bsr_serve_sd",
    description="Serve skill posterior sd",
    mirror=True,
    impute=None,
)
def bsr_serve_sd() -> pl.Expr:
    return pl.col("player_bsr_serve_sd")


@feature(
    name="bsr_return_mu",
    description="Return skill posterior mean (logit scale)",
    mirror=True,
    impute=None,
)
def bsr_return_mu() -> pl.Expr:
    return pl.col("player_bsr_return_mu")


@feature(
    name="bsr_return_sd",
    description="Return skill posterior sd",
    mirror=True,
    impute=None,
)
def bsr_return_sd() -> pl.Expr:
    return pl.col("player_bsr_return_sd")


@feature(
    name="bsr_serve_surface_mu",
    description="Serve surface-residual posterior mean, this match's surface",
    mirror=True,
    impute=None,
)
def bsr_serve_surface_mu() -> pl.Expr:
    return pl.col("player_bsr_serve_surface_mu")


@feature(
    name="bsr_serve_surface_sd",
    description="Serve surface-residual posterior sd",
    mirror=True,
    impute=None,
)
def bsr_serve_surface_sd() -> pl.Expr:
    return pl.col("player_bsr_serve_surface_sd")


@feature(
    name="bsr_return_surface_mu",
    description="Return surface-residual posterior mean, this match's surface",
    mirror=True,
    impute=None,
)
def bsr_return_surface_mu() -> pl.Expr:
    return pl.col("player_bsr_return_surface_mu")


@feature(
    name="bsr_return_surface_sd",
    description="Return surface-residual posterior sd",
    mirror=True,
    impute=None,
)
def bsr_return_surface_sd() -> pl.Expr:
    return pl.col("player_bsr_return_surface_sd")


@feature(
    name="bsr_n_serve_obs",
    description="Serve-count observations the filter has seen for the player",
    mirror=True,
    impute=None,
)
def bsr_n_serve_obs() -> pl.Expr:
    return pl.col("player_bsr_n_serve_obs")


@feature(
    name="bsr_days_since_serve_obs",
    description="Days since the player's last serve-count observation",
    mirror=True,
    impute=None,
)
def bsr_days_since_serve_obs() -> pl.Expr:
    return pl.col("player_bsr_days_since_serve_obs")


@feature(
    name="bsr_pserve_logit",
    description="Matchup serve-point win logit vs this opponent, posterior mean",
    mirror=True,
    impute=None,
)
def bsr_pserve_logit() -> pl.Expr:
    return pl.col("player_bsr_pserve_logit")


@feature(
    name="bsr_pserve_logit_sd",
    description="Its posterior sd, including the per-match random effect",
    mirror=True,
    impute=None,
)
def bsr_pserve_logit_sd() -> pl.Expr:
    return pl.col("player_bsr_pserve_logit_sd")


for _base in (
    "bsr_serve_mu", "bsr_return_mu", "bsr_serve_surface_mu",
    "bsr_return_surface_mu", "bsr_pserve_logit",
):
    register_diff(_base)
for _base in ("bsr_serve_mu", "bsr_return_mu", "bsr_pserve_logit"):
    register_sum(_base)
