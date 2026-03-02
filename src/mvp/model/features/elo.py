"""Elo-derived features.

These features use the pre-computed Elo columns from the aggregator
(player_elo, opp_elo, player_serve_elo, etc.).
"""

from __future__ import annotations

import polars as pl

from mvp.model.registry import feature


@feature(
    name="elo_diff",
    params=[],
    description="Overall Elo difference (player - opponent)",
    mirror=False,
)
def elo_diff() -> pl.Expr:
    """Overall Elo rating difference."""
    return pl.col("player_elo") - pl.col("opp_elo")


@feature(
    name="surface_elo_diff",
    params=[],
    description="Surface-adjusted Elo difference",
    mirror=False,
)
def surface_elo_diff() -> pl.Expr:
    """Surface-adjusted Elo difference.

    Uses the match surface to select the appropriate adjustment.
    """
    player_surface_elo = (
        pl.col("player_elo")
        + pl.when(pl.col("surface") == "Hard").then(pl.col("player_hard_adj"))
        .when(pl.col("surface") == "Clay").then(pl.col("player_clay_adj"))
        .when(pl.col("surface") == "Grass").then(pl.col("player_grass_adj"))
        .otherwise(0.0)
    )
    opp_surface_elo = (
        pl.col("opp_elo")
        + pl.when(pl.col("surface") == "Hard").then(pl.col("opp_hard_adj"))
        .when(pl.col("surface") == "Clay").then(pl.col("opp_clay_adj"))
        .when(pl.col("surface") == "Grass").then(pl.col("opp_grass_adj"))
        .otherwise(0.0)
    )
    return player_surface_elo - opp_surface_elo


@feature(
    name="serve_elo_diff",
    params=[],
    description="Serve Elo difference (player - opponent)",
    mirror=False,
)
def serve_elo_diff() -> pl.Expr:
    """Serve Elo rating difference."""
    return pl.col("player_serve_elo") - pl.col("opp_serve_elo")


@feature(
    name="return_elo_diff",
    params=[],
    description="Return Elo difference (player - opponent)",
    mirror=False,
)
def return_elo_diff() -> pl.Expr:
    """Return Elo rating difference."""
    return pl.col("player_return_elo") - pl.col("opp_return_elo")


@feature(
    name="serve_vs_return",
    params=[],
    description="Player serve Elo vs opponent return Elo (direct matchup)",
    mirror=False,
)
def serve_vs_return() -> pl.Expr:
    """Player's serving ability vs opponent's returning ability.

    High value = player has serve advantage in the matchup.
    """
    return pl.col("player_serve_elo") - pl.col("opp_return_elo")


@feature(
    name="return_vs_serve",
    params=[],
    description="Player return Elo vs opponent serve Elo (direct matchup)",
    mirror=False,
)
def return_vs_serve() -> pl.Expr:
    """Player's returning ability vs opponent's serving ability.

    High value = player has return advantage in the matchup.
    """
    return pl.col("player_return_elo") - pl.col("opp_serve_elo")


@feature(
    name="elo_rd_sum",
    params=[],
    description="Combined rating deviation (uncertainty indicator)",
    mirror=False,
)
def elo_rd_sum() -> pl.Expr:
    """Sum of player and opponent rating deviation.

    High value = more uncertainty in both ratings.
    Can be used for confidence scoring.
    """
    return pl.col("player_elo_rd") + pl.col("opp_elo_rd")


@feature(
    name="serve_vs_return_rd",
    params=[],
    description="Uncertainty in serve vs return matchup",
    mirror=False,
)
def serve_vs_return_rd() -> pl.Expr:
    """Uncertainty in the serve vs return matchup.

    High value = less confident in serve_vs_return prediction.
    """
    return pl.col("player_serve_elo_rd") + pl.col("opp_return_elo_rd")


@feature(
    name="return_vs_serve_rd",
    params=[],
    description="Uncertainty in return vs serve matchup",
    mirror=False,
)
def return_vs_serve_rd() -> pl.Expr:
    """Uncertainty in the return vs serve matchup.

    High value = less confident in return_vs_serve prediction.
    """
    return pl.col("player_return_elo_rd") + pl.col("opp_serve_elo_rd")


@feature(
    name="clay_specialist",
    params=[],
    description="Clay adjustment minus hard adjustment (clay preference)",
    mirror=True,
)
def clay_specialist() -> pl.Expr:
    """Clay court specialization indicator.

    Positive = player performs better on clay relative to hard.
    """
    return pl.col("player_clay_adj") - pl.col("player_hard_adj")


@feature(
    name="grass_specialist",
    params=[],
    description="Grass adjustment minus hard adjustment (grass preference)",
    mirror=True,
)
def grass_specialist() -> pl.Expr:
    """Grass court specialization indicator.

    Positive = player performs better on grass relative to hard.
    """
    return pl.col("player_grass_adj") - pl.col("player_hard_adj")


@feature(
    name="surface_consistency",
    params=[],
    description="Variance in surface adjustments (low = consistent across surfaces)",
    mirror=True,
)
def surface_consistency() -> pl.Expr:
    """Surface consistency indicator.

    Low value = player performs similarly across all surfaces.
    High value = player has strong surface preferences.
    """
    hard = pl.col("player_hard_adj")
    clay = pl.col("player_clay_adj")
    grass = pl.col("player_grass_adj")
    mean_adj = (hard + clay + grass) / 3
    variance = ((hard - mean_adj) ** 2 + (clay - mean_adj) ** 2 + (grass - mean_adj) ** 2) / 3
    return variance.sqrt()


# Style dimension features


@feature(
    name="first_serve_power_diff",
    params=[],
    description="First serve power difference (player - opponent)",
    mirror=False,
)
def first_serve_power_diff() -> pl.Expr:
    """First serve power difference."""
    return pl.col("player_first_serve_power") - pl.col("opp_first_serve_power")


@feature(
    name="second_serve_reliability_diff",
    params=[],
    description="Second serve reliability difference (player - opponent)",
    mirror=False,
)
def second_serve_reliability_diff() -> pl.Expr:
    """Second serve reliability difference."""
    return pl.col("player_second_serve_reliability") - pl.col("opp_second_serve_reliability")


@feature(
    name="ace_resistance_diff",
    params=[],
    description="Ace resistance difference (player - opponent)",
    mirror=False,
)
def ace_resistance_diff() -> pl.Expr:
    """Ace resistance difference."""
    return pl.col("player_ace_resistance") - pl.col("opp_ace_resistance")


@feature(
    name="serve_clutch_diff",
    params=[],
    description="Serve clutch difference (player - opponent)",
    mirror=False,
)
def serve_clutch_diff() -> pl.Expr:
    """Serve clutch (BP save rate) difference."""
    return pl.col("player_serve_clutch") - pl.col("opp_serve_clutch")


@feature(
    name="return_clutch_diff",
    params=[],
    description="Return clutch difference (player - opponent)",
    mirror=False,
)
def return_clutch_diff() -> pl.Expr:
    """Return clutch (BP conversion) difference."""
    return pl.col("player_return_clutch") - pl.col("opp_return_clutch")


@feature(
    name="tb_clutch_diff",
    params=[],
    description="Tiebreak clutch difference (player - opponent)",
    mirror=False,
)
def tb_clutch_diff() -> pl.Expr:
    """Tiebreak clutch difference."""
    return pl.col("player_tb_clutch") - pl.col("opp_tb_clutch")


@feature(
    name="overall_clutch_diff",
    params=[],
    description="Overall clutch difference (player - opponent)",
    mirror=False,
)
def overall_clutch_diff() -> pl.Expr:
    """Overall clutch difference."""
    return pl.col("player_overall_clutch") - pl.col("opp_overall_clutch")


@feature(
    name="indoor_adj_diff",
    params=[],
    description="Indoor adjustment difference (player - opponent)",
    mirror=False,
)
def indoor_adj_diff() -> pl.Expr:
    """Indoor venue adjustment difference."""
    return pl.col("player_indoor_adj") - pl.col("opp_indoor_adj")
