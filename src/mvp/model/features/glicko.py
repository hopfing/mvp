"""Glicko-2 derived features.

These features use the pre-computed Glicko-2 columns from the aggregator
(player_glicko_mu, opp_glicko_mu, etc.).
"""

import polars as pl

from mvp.model.registry import feature


@feature(
    name="glicko_mu",
    description="Glicko-2 mu rating",
    mirror=True,
)
def glicko_mu() -> pl.Expr:
    return pl.col("player_glicko_mu")


@feature(
    name="glicko_rd",
    description="Glicko-2 rating deviation (uncertainty)",
    mirror=True,
)
def glicko_rd() -> pl.Expr:
    return pl.col("player_glicko_rd")


@feature(
    name="glicko_sigma",
    description="Glicko-2 volatility",
    mirror=True,
)
def glicko_sigma() -> pl.Expr:
    return pl.col("player_glicko_sigma")


@feature(
    name="glicko_diff",
    description="Base Glicko-2 mu difference (player - opponent)",
    mirror=False,
    impute=None,
)
def glicko_diff() -> pl.Expr:
    return pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")


@feature(
    name="glicko_rd_sum",
    description="Combined Glicko-2 RD (total match uncertainty)",
    mirror=False,
    match_level=True,
)
def glicko_rd_sum() -> pl.Expr:
    return pl.col("player_glicko_rd") + pl.col("opp_glicko_rd")


@feature(
    name="glicko_rd_diff",
    description="Glicko-2 RD difference (asymmetric uncertainty)",
    mirror=False,
    impute=None,
)
def glicko_rd_diff() -> pl.Expr:
    return pl.col("player_glicko_rd") - pl.col("opp_glicko_rd")


@feature(
    name="glicko_sigma_diff",
    description="Glicko-2 volatility difference (erratic vs consistent)",
    mirror=False,
    impute=None,
)
def glicko_sigma_diff() -> pl.Expr:
    return pl.col("player_glicko_sigma") - pl.col("opp_glicko_sigma")


@feature(
    name="glicko_surface_rd_sum",
    description="Surface-specific Glicko-2 RD sum (surface uncertainty)",
    mirror=False,
    match_level=True,
)
def glicko_surface_rd_sum() -> pl.Expr:
    player_rd = (
        pl.when(pl.col("surface") == "Hard")
        .then(pl.col("player_glicko_hard_rd"))
        .when(pl.col("surface") == "Clay")
        .then(pl.col("player_glicko_clay_rd"))
        .when(pl.col("surface") == "Grass")
        .then(pl.col("player_glicko_grass_rd"))
        .otherwise(pl.col("player_glicko_rd"))
    )
    opp_rd = (
        pl.when(pl.col("surface") == "Hard")
        .then(pl.col("opp_glicko_hard_rd"))
        .when(pl.col("surface") == "Clay")
        .then(pl.col("opp_glicko_clay_rd"))
        .when(pl.col("surface") == "Grass")
        .then(pl.col("opp_glicko_grass_rd"))
        .otherwise(pl.col("opp_glicko_rd"))
    )
    return player_rd + opp_rd


@feature(
    name="glicko_diff_abs",
    description="Absolute Glicko-2 mu difference (match competitiveness)",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_diff_abs() -> pl.Expr:
    """Absolute Glicko gap — larger means more lopsided match."""
    return (pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")).abs()


@feature(
    name="glicko_diff_sq",
    description="Squared Glicko-2 mu difference (nonlinear competitiveness)",
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_diff_sq() -> pl.Expr:
    """Squared Glicko gap — captures diminishing marginal effect of skill gap."""
    diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    return diff ** 2


@feature(
    name="glicko_diff_x_rd_sum",
    description="Glicko diff weighted by combined uncertainty",
    mirror=False,
    impute=None,
)
def glicko_diff_x_rd_sum() -> pl.Expr:
    diff = pl.col("player_glicko_mu") - pl.col("opp_glicko_mu")
    rd_sum = pl.col("player_glicko_rd") + pl.col("opp_glicko_rd")
    return diff * rd_sum


@feature(
    name="glicko_rd_x_match_count",
    params=["days"],
    description="Combined RD multiplied by combined recent-match count",
    depends_on=["match_count"],
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_rd_x_match_count(days: int | None = None) -> pl.Expr:
    rd_sum = pl.col("player_glicko_rd") + pl.col("opp_glicko_rd")
    if days is None:
        mc_sum = pl.col("player_match_count") + pl.col("opp_match_count")
    else:
        mc_sum = (
            pl.col(f"player_match_count_{days}d")
            + pl.col(f"opp_match_count_{days}d")
        )
    return rd_sum * mc_sum


@feature(
    name="glicko_rd_x_days_since_last_match",
    description="Combined RD multiplied by combined days-since-last-match",
    depends_on=["days_since_last_match"],
    mirror=False,
    match_level=True,
    impute=None,
)
def glicko_rd_x_days_since_last_match() -> pl.Expr:
    rd_sum = pl.col("player_glicko_rd") + pl.col("opp_glicko_rd")
    dsl_sum = (
        pl.col("player_days_since_last_match")
        + pl.col("opp_days_since_last_match")
    )
    return rd_sum * dsl_sum
