"""Elo-derived features.

These features use the pre-computed Elo columns from the aggregator
(player_elo, opp_elo, player_serve_elo, etc.).
"""


import polars as pl

from mvp.model.registry import feature


def surface_elo_expr(prefix: str) -> pl.Expr:
    """Surface-adjusted Elo for a player.

    Args:
        prefix: "player" or "opp"
    """
    return (
        pl.col(f"{prefix}_elo")
        + pl.when(pl.col("surface") == "Hard").then(pl.col(f"{prefix}_hard_adj"))
        .when(pl.col("surface") == "Clay").then(pl.col(f"{prefix}_clay_adj"))
        .when(pl.col("surface") == "Grass").then(pl.col(f"{prefix}_grass_adj"))
        .otherwise(0.0)
    )


# =============================================================================
# Raw Elo column passthroughs — expose individual ratings as model features
# =============================================================================


@feature(
    name="elo",
    description="Overall Elo rating",
    mirror=True,
)
def elo() -> pl.Expr:
    return pl.col("player_elo")


@feature(
    name="elo_surface",
    description="Surface-adjusted Elo rating",
    mirror=True,
)
def elo_surface() -> pl.Expr:
    return surface_elo_expr("player")


@feature(
    name="serve_elo",
    description="Serve Elo rating",
    mirror=True,
)
def serve_elo() -> pl.Expr:
    return pl.col("player_serve_elo")


@feature(
    name="return_elo",
    description="Return Elo rating",
    mirror=True,
)
def return_elo() -> pl.Expr:
    return pl.col("player_return_elo")


@feature(
    name="elo_rd",
    description="Elo rating deviation (uncertainty)",
    mirror=True,
)
def elo_rd() -> pl.Expr:
    return pl.col("player_elo_rd")


@feature(
    name="serve_elo_rd",
    description="Serve Elo rating deviation",
    mirror=True,
)
def serve_elo_rd() -> pl.Expr:
    return pl.col("player_serve_elo_rd")


@feature(
    name="return_elo_rd",
    description="Return Elo rating deviation",
    mirror=True,
)
def return_elo_rd() -> pl.Expr:
    return pl.col("player_return_elo_rd")


@feature(
    name="hard_adj",
    description="Hard court Elo adjustment",
    mirror=True,
)
def hard_adj() -> pl.Expr:
    return pl.col("player_hard_adj")


@feature(
    name="clay_adj",
    description="Clay court Elo adjustment",
    mirror=True,
)
def clay_adj() -> pl.Expr:
    return pl.col("player_clay_adj")


@feature(
    name="grass_adj",
    description="Grass court Elo adjustment",
    mirror=True,
)
def grass_adj() -> pl.Expr:
    return pl.col("player_grass_adj")


@feature(
    name="indoor_adj",
    description="Indoor venue Elo adjustment",
    mirror=True,
)
def indoor_adj() -> pl.Expr:
    return pl.col("player_indoor_adj")


@feature(
    name="first_serve_power",
    description="First serve power rating",
    mirror=True,
)
def first_serve_power() -> pl.Expr:
    return pl.col("player_first_serve_power")


@feature(
    name="second_serve_reliability",
    description="Second serve reliability rating",
    mirror=True,
)
def second_serve_reliability() -> pl.Expr:
    return pl.col("player_second_serve_reliability")


@feature(
    name="ace_resistance",
    description="Ace resistance rating",
    mirror=True,
)
def ace_resistance() -> pl.Expr:
    return pl.col("player_ace_resistance")


@feature(
    name="serve_clutch",
    description="Serve clutch (BP save) rating",
    mirror=True,
)
def serve_clutch() -> pl.Expr:
    return pl.col("player_serve_clutch")


@feature(
    name="return_clutch",
    description="Return clutch (BP conversion) rating",
    mirror=True,
)
def return_clutch() -> pl.Expr:
    return pl.col("player_return_clutch")


@feature(
    name="tb_clutch",
    description="Tiebreak clutch rating",
    mirror=True,
)
def tb_clutch() -> pl.Expr:
    return pl.col("player_tb_clutch")


@feature(
    name="overall_clutch",
    description="Overall clutch rating",
    mirror=True,
)
def overall_clutch() -> pl.Expr:
    return pl.col("player_overall_clutch")


# =============================================================================
# Match-level context features
# =============================================================================


@feature(
    name="elo_avg",
    params=[],
    description="Average Elo of both players (absolute level context)",
    mirror=False,
    match_level=True,
)
def elo_avg() -> pl.Expr:
    """Average Elo of both players.

    Gives the model context about the absolute level of the match,
    not just the difference between players.
    """
    return (pl.col("player_elo") + pl.col("opp_elo")) / 2


@feature(
    name="elo_avg_sq",
    params=[],
    description="Squared average Elo (nonlinear absolute level effect)",
    mirror=False,
    match_level=True,
)
def elo_avg_sq() -> pl.Expr:
    """Squared average Elo of both players.

    Lets logistic regression capture nonlinear calibration effects
    at Elo extremes that a linear elo_avg term can't model.
    """
    avg = (pl.col("player_elo") + pl.col("opp_elo")) / 2
    return avg ** 2


@feature(
    name="elo_min",
    params=[],
    description="Minimum Elo of both players (floor quality)",
    mirror=False,
    match_level=True,
)
def elo_min() -> pl.Expr:
    """Minimum Elo of the two players.

    Captures the floor quality — a match involving a 1300 player
    has different dynamics than one where both are 1700+.
    """
    return pl.min_horizontal("player_elo", "opp_elo")


@feature(
    name="elo_diff_x_elo_avg",
    params=[],
    description="Interaction: surface Elo diff × average Elo level",
    mirror=False,
    match_level=True,
    impute=0,
)
def elo_diff_x_elo_avg() -> pl.Expr:
    """Interaction between Elo difference and absolute level.

    Lets logistic regression learn that a 200-point diff means
    less at high absolute Elo than at low Elo.
    """
    diff = surface_elo_expr("player") - surface_elo_expr("opp")
    avg = (pl.col("player_elo") + pl.col("opp_elo")) / 2
    return diff * avg


@feature(
    name="elo_diff_x_rd_sum",
    params=[],
    description="Interaction: surface Elo diff × combined rating deviation",
    mirror=False,
    match_level=True,
    impute=0,
)
def elo_diff_x_rd_sum() -> pl.Expr:
    """Interaction between Elo difference and rating uncertainty.

    Lets logistic regression learn that Elo diffs mean less
    when rating uncertainty is high.
    """
    diff = surface_elo_expr("player") - surface_elo_expr("opp")
    rd_sum = pl.col("player_elo_rd") + pl.col("opp_elo_rd")
    return diff * rd_sum


@feature(
    name="elo_diff",
    params=[],
    description="Overall Elo difference (player - opponent)",
    mirror=False,
    impute=0,
)
def elo_diff() -> pl.Expr:
    """Overall Elo rating difference."""
    return pl.col("player_elo") - pl.col("opp_elo")


@feature(
    name="elo_surface_diff",
    params=[],
    description="Surface-adjusted Elo difference",
    mirror=False,
    impute=0,
)
def elo_surface_diff() -> pl.Expr:
    """Surface-adjusted Elo difference."""
    return surface_elo_expr("player") - surface_elo_expr("opp")


@feature(
    name="svc_elo_diff",
    params=[],
    description="Serve Elo difference (player - opponent)",
    mirror=False,
    impute=0,
)
def svc_elo_diff() -> pl.Expr:
    """Serve Elo rating difference."""
    return pl.col("player_serve_elo") - pl.col("opp_serve_elo")


@feature(
    name="ret_elo_diff",
    params=[],
    description="Return Elo difference (player - opponent)",
    mirror=False,
    impute=0,
)
def ret_elo_diff() -> pl.Expr:
    """Return Elo rating difference."""
    return pl.col("player_return_elo") - pl.col("opp_return_elo")


@feature(
    name="svc_elo_matchup",
    params=[],
    description="Player serve Elo vs opponent return Elo (direct matchup)",
    mirror=True,
    impute=0,
)
def svc_elo_matchup() -> pl.Expr:
    """Player's serving ability vs opponent's returning ability.

    High value = player has serve advantage in the matchup.
    """
    return pl.col("player_serve_elo") - pl.col("opp_return_elo")


@feature(
    name="ret_elo_matchup",
    params=[],
    description="Player return Elo vs opponent serve Elo (direct matchup)",
    mirror=True,
    impute=0,
)
def ret_elo_matchup() -> pl.Expr:
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
    name="svc_elo_matchup_rd",
    params=[],
    description="Uncertainty in serve vs return matchup",
    mirror=True,
    impute=0,
)
def svc_elo_matchup_rd() -> pl.Expr:
    """Uncertainty in the serve vs return matchup.

    High value = less confident in svc_elo_matchup prediction.
    """
    return pl.col("player_serve_elo_rd") + pl.col("opp_return_elo_rd")


@feature(
    name="ret_elo_matchup_rd",
    params=[],
    description="Uncertainty in return vs serve matchup",
    mirror=True,
    impute=0,
)
def ret_elo_matchup_rd() -> pl.Expr:
    """Uncertainty in the return vs serve matchup.

    High value = less confident in ret_elo_matchup prediction.
    """
    return pl.col("player_return_elo_rd") + pl.col("opp_serve_elo_rd")


@feature(
    name="elo_surface_diff_abs",
    params=[],
    description="Absolute surface-adjusted Elo difference (match competitiveness)",
    mirror=False,
    match_level=True,
    impute=0,
)
def elo_surface_diff_abs() -> pl.Expr:
    """Absolute Elo gap — larger means more lopsided match, fewer games."""
    return (surface_elo_expr("player") - surface_elo_expr("opp")).abs()


@feature(
    name="elo_surface_diff_sq",
    params=[],
    description="Squared surface-adjusted Elo difference (nonlinear competitiveness)",
    mirror=False,
    match_level=True,
    impute=0,
)
def elo_surface_diff_sq() -> pl.Expr:
    """Squared Elo gap — captures diminishing marginal effect of skill gap."""
    diff = surface_elo_expr("player") - surface_elo_expr("opp")
    return diff ** 2


@feature(
    name="elo_clay_specialist",
    params=[],
    description="Clay adjustment minus hard adjustment (clay preference)",
    mirror=True,
)
def elo_clay_specialist() -> pl.Expr:
    """Clay court specialization indicator.

    Positive = player performs better on clay relative to hard.
    """
    return pl.col("player_clay_adj") - pl.col("player_hard_adj")


@feature(
    name="elo_grass_specialist",
    params=[],
    description="Grass adjustment minus hard adjustment (grass preference)",
    mirror=True,
)
def elo_grass_specialist() -> pl.Expr:
    """Grass court specialization indicator.

    Positive = player performs better on grass relative to hard.
    """
    return pl.col("player_grass_adj") - pl.col("player_hard_adj")


@feature(
    name="elo_surface_consistency",
    params=[],
    description="Variance in surface adjustments (low = consistent across surfaces)",
    mirror=True,
)
def elo_surface_consistency() -> pl.Expr:
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
    name="svc_first_serve_power_diff",
    params=[],
    description="First serve power difference (player - opponent)",
    mirror=False,
    impute=0,
)
def svc_first_serve_power_diff() -> pl.Expr:
    """First serve power difference."""
    return pl.col("player_first_serve_power") - pl.col("opp_first_serve_power")


@feature(
    name="svc_second_serve_reliability_diff",
    params=[],
    description="Second serve reliability difference (player - opponent)",
    mirror=False,
    impute=0,
)
def svc_second_serve_reliability_diff() -> pl.Expr:
    """Second serve reliability difference."""
    return pl.col("player_second_serve_reliability") - pl.col("opp_second_serve_reliability")


@feature(
    name="ret_ace_resistance_diff",
    params=[],
    description="Ace resistance difference (player - opponent)",
    mirror=False,
    impute=0,
)
def ret_ace_resistance_diff() -> pl.Expr:
    """Ace resistance difference."""
    return pl.col("player_ace_resistance") - pl.col("opp_ace_resistance")


@feature(
    name="svc_clutch_diff",
    params=[],
    description="Serve clutch difference (player - opponent)",
    mirror=False,
    impute=0,
)
def svc_clutch_diff() -> pl.Expr:
    """Serve clutch (BP save rate) difference."""
    return pl.col("player_serve_clutch") - pl.col("opp_serve_clutch")


@feature(
    name="ret_clutch_diff",
    params=[],
    description="Return clutch difference (player - opponent)",
    mirror=False,
    impute=0,
)
def ret_clutch_diff() -> pl.Expr:
    """Return clutch (BP conversion) difference."""
    return pl.col("player_return_clutch") - pl.col("opp_return_clutch")


@feature(
    name="elo_tb_clutch_diff",
    params=[],
    description="Tiebreak clutch difference (player - opponent)",
    mirror=False,
    impute=0,
)
def elo_tb_clutch_diff() -> pl.Expr:
    """Tiebreak clutch difference."""
    return pl.col("player_tb_clutch") - pl.col("opp_tb_clutch")


@feature(
    name="elo_clutch_diff",
    params=[],
    description="Overall clutch difference (player - opponent)",
    mirror=False,
    impute=0,
)
def elo_clutch_diff() -> pl.Expr:
    """Overall clutch difference."""
    return pl.col("player_overall_clutch") - pl.col("opp_overall_clutch")


@feature(
    name="elo_indoor_adj_diff",
    params=[],
    description="Indoor adjustment difference (player - opponent)",
    mirror=False,
    impute=0,
)
def elo_indoor_adj_diff() -> pl.Expr:
    """Indoor venue adjustment difference."""
    return pl.col("player_indoor_adj") - pl.col("opp_indoor_adj")


# =============================================================================
# Matchup Features (player domain vs opponent opposite domain)
# =============================================================================


@feature(
    name="svc_first_serve_power_matchup",
    params=[],
    description="Player first serve power vs opponent ace resistance",
    mirror=True,
    impute=0,
)
def svc_first_serve_power_matchup() -> pl.Expr:
    """Player's first serve power vs opponent's ace resistance.

    High value = player's serve power exceeds opponent's ability to return.
    """
    return pl.col("player_first_serve_power") - pl.col("opp_ace_resistance")


@feature(
    name="svc_clutch_matchup",
    params=[],
    description="Player serve clutch vs opponent return clutch",
    mirror=True,
    impute=0,
)
def svc_clutch_matchup() -> pl.Expr:
    """Player's BP save ability vs opponent's BP conversion ability.

    High value = player wins BP battles when serving.
    """
    return pl.col("player_serve_clutch") - pl.col("opp_return_clutch")


@feature(
    name="ret_clutch_matchup",
    params=[],
    description="Player return clutch vs opponent serve clutch",
    mirror=True,
    impute=0,
)
def ret_clutch_matchup() -> pl.Expr:
    """Player's BP conversion ability vs opponent's BP save ability.

    High value = player wins BP battles when returning.
    """
    return pl.col("player_return_clutch") - pl.col("opp_serve_clutch")
