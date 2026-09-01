"""Elo-derived features.

These features use the pre-computed Elo columns from the aggregator
(player_elo, opp_elo, player_serve_elo, etc.).
"""


import polars as pl

from mvp.model.registry import feature


def surface_adj_expr(prefix: str) -> pl.Expr:
    """Surface-selected Elo adjustment for a player (no base Elo).

    Picks the hard/clay/grass adjustment matching the match surface; 0.0 for
    any other surface.

    Args:
        prefix: "player" or "opp"
    """
    return (
        pl.when(pl.col("surface") == "Hard").then(pl.col(f"{prefix}_hard_adj"))
        .when(pl.col("surface") == "Clay").then(pl.col(f"{prefix}_clay_adj"))
        .when(pl.col("surface") == "Grass").then(pl.col(f"{prefix}_grass_adj"))
        .otherwise(0.0)
    )


def indoor_adj_expr(prefix: str) -> pl.Expr:
    """Indoor Elo adjustment for a player, applied only on indoor matches.

    0.0 on outdoor matches — the indoor adjustment is a venue effect that only
    bears on indoor play.

    Args:
        prefix: "player" or "opp"
    """
    return (
        pl.when(pl.col("indoor")).then(pl.col(f"{prefix}_indoor_adj"))
        .otherwise(0.0)
    )


def surface_elo_expr(prefix: str) -> pl.Expr:
    """Surface-adjusted Elo for a player.

    Args:
        prefix: "player" or "opp"
    """
    return pl.col(f"{prefix}_elo") + surface_adj_expr(prefix)


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
    name="elo_indoor",
    description="Indoor-adjusted Elo (base Elo + indoor adj on indoor matches)",
    mirror=True,
)
def elo_indoor() -> pl.Expr:
    return pl.col("player_elo") + indoor_adj_expr("player")


@feature(
    name="elo_surface_indoor",
    description="Surface- and indoor-adjusted Elo (base + surface adj + indoor adj)",
    mirror=True,
)
def elo_surface_indoor() -> pl.Expr:
    return surface_elo_expr("player") + indoor_adj_expr("player")


@feature(
    name="melo",
    description="Margin-of-victory Elo (games-share outcome, K-rescaled; "
                "bare rating, no surface/indoor layers)",
    mirror=True,
)
def melo() -> pl.Expr:
    return pl.col("player_melo")


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
    name="surface_adj",
    description="Surface-selected Elo adjustment (hard/clay/grass by match surface)",
    mirror=True,
)
def surface_adj() -> pl.Expr:
    return surface_adj_expr("player")


@feature(
    name="venue_adj",
    description="Venue-selected Elo adjustment (indoor adj indoors, 0 outdoors)",
    mirror=True,
)
def venue_adj() -> pl.Expr:
    return indoor_adj_expr("player")


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
    impute=None,
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
    impute=None,
)
def serve_clutch() -> pl.Expr:
    return pl.col("player_serve_clutch")


@feature(
    name="return_clutch",
    description="Return clutch (BP conversion) rating",
    mirror=True,
    impute=None,
)
def return_clutch() -> pl.Expr:
    return pl.col("player_return_clutch")


@feature(
    name="tb_clutch",
    description="Tiebreak clutch rating",
    mirror=True,
    impute=None,
)
def tb_clutch() -> pl.Expr:
    return pl.col("player_tb_clutch")


@feature(
    name="overall_clutch",
    description="Overall clutch rating",
    mirror=True,
    impute=None,
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
    impute=None,
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
    impute=None,
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
    impute=None,
)
def elo_diff() -> pl.Expr:
    """Overall Elo rating difference."""
    return pl.col("player_elo") - pl.col("opp_elo")


@feature(
    name="melo_diff",
    params=[],
    description="Margin-of-victory Elo difference (player - opponent). "
                "Complementary to elo_diff, not a replacement: the gate read "
                "was joint -0.0054 vs the composite, standalone weaker.",
    mirror=False,
    impute=None,
)
def melo_diff() -> pl.Expr:
    return pl.col("player_melo") - pl.col("opp_melo")


@feature(
    name="elo_surface_diff",
    params=[],
    description="Surface-adjusted Elo difference",
    mirror=False,
    impute=None,
)
def elo_surface_diff() -> pl.Expr:
    """Surface-adjusted Elo difference."""
    return surface_elo_expr("player") - surface_elo_expr("opp")


@feature(
    name="surface_adj_diff",
    params=[],
    description="Surface-selected adjustment difference (player - opponent)",
    mirror=False,
    impute=None,
)
def surface_adj_diff() -> pl.Expr:
    """Surface-selected adjustment difference."""
    return surface_adj_expr("player") - surface_adj_expr("opp")


@feature(
    name="elo_indoor_diff",
    params=[],
    description="Indoor-adjusted Elo difference (player - opponent)",
    mirror=False,
    impute=None,
)
def elo_indoor_diff() -> pl.Expr:
    """Indoor-adjusted Elo difference."""
    return (
        (pl.col("player_elo") + indoor_adj_expr("player"))
        - (pl.col("opp_elo") + indoor_adj_expr("opp"))
    )


@feature(
    name="elo_surface_indoor_diff",
    params=[],
    description="Surface- and indoor-adjusted Elo difference (player - opponent)",
    mirror=False,
    impute=None,
)
def elo_surface_indoor_diff() -> pl.Expr:
    """Surface- and indoor-adjusted Elo difference."""
    return (
        (surface_elo_expr("player") + indoor_adj_expr("player"))
        - (surface_elo_expr("opp") + indoor_adj_expr("opp"))
    )


@feature(
    name="svc_elo_diff",
    params=[],
    description="Serve Elo difference (player - opponent)",
    mirror=False,
    impute=None,
)
def svc_elo_diff() -> pl.Expr:
    """Serve Elo rating difference."""
    return pl.col("player_serve_elo") - pl.col("opp_serve_elo")


@feature(
    name="ret_elo_diff",
    params=[],
    description="Return Elo difference (player - opponent)",
    mirror=False,
    impute=None,
)
def ret_elo_diff() -> pl.Expr:
    """Return Elo rating difference."""
    return pl.col("player_return_elo") - pl.col("opp_return_elo")


@feature(
    name="svc_elo_matchup",
    params=[],
    description="Player serve Elo vs opponent return Elo (direct matchup)",
    mirror=True,
    impute=None,
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
    impute=None,
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
    impute=None,
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
    impute=None,
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
    impute=None,
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
    impute=None,
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
    name="elo_hard_specialist",
    params=[],
    description="Hard adjustment minus clay adjustment (hard preference)",
    mirror=True,
)
def elo_hard_specialist() -> pl.Expr:
    """Hard court specialization indicator.

    Positive = player performs better on hard relative to clay.
    Inverse of elo_clay_specialist; registered separately so the is_hard_specialist
    label can apply a top-quartile threshold to it directly.
    """
    return pl.col("player_hard_adj") - pl.col("player_clay_adj")


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
    impute=None,
)
def svc_first_serve_power_diff() -> pl.Expr:
    """First serve power difference."""
    return pl.col("player_first_serve_power") - pl.col("opp_first_serve_power")


@feature(
    name="svc_second_serve_reliability_diff",
    params=[],
    description="Second serve reliability difference (player - opponent)",
    mirror=False,
    impute=None,
)
def svc_second_serve_reliability_diff() -> pl.Expr:
    """Second serve reliability difference."""
    return pl.col("player_second_serve_reliability") - pl.col("opp_second_serve_reliability")


@feature(
    name="ret_ace_resistance_diff",
    params=[],
    description="Ace resistance difference (player - opponent)",
    mirror=False,
    impute=None,
)
def ret_ace_resistance_diff() -> pl.Expr:
    """Ace resistance difference."""
    return pl.col("player_ace_resistance") - pl.col("opp_ace_resistance")


@feature(
    name="svc_clutch_diff",
    params=[],
    description="Serve clutch difference (player - opponent)",
    mirror=False,
    impute=None,
)
def svc_clutch_diff() -> pl.Expr:
    """Serve clutch (BP save rate) difference."""
    return pl.col("player_serve_clutch") - pl.col("opp_serve_clutch")


@feature(
    name="ret_clutch_diff",
    params=[],
    description="Return clutch difference (player - opponent)",
    mirror=False,
    impute=None,
)
def ret_clutch_diff() -> pl.Expr:
    """Return clutch (BP conversion) difference."""
    return pl.col("player_return_clutch") - pl.col("opp_return_clutch")


@feature(
    name="elo_tb_clutch_diff",
    params=[],
    description="Tiebreak clutch difference (player - opponent)",
    mirror=False,
    impute=None,
)
def elo_tb_clutch_diff() -> pl.Expr:
    """Tiebreak clutch difference."""
    return pl.col("player_tb_clutch") - pl.col("opp_tb_clutch")


@feature(
    name="elo_clutch_diff",
    params=[],
    description="Overall clutch difference (player - opponent)",
    mirror=False,
    impute=None,
)
def elo_clutch_diff() -> pl.Expr:
    """Overall clutch difference."""
    return pl.col("player_overall_clutch") - pl.col("opp_overall_clutch")


@feature(
    name="elo_indoor_adj_diff",
    params=[],
    description="Indoor adjustment difference (player - opponent)",
    mirror=False,
    impute=None,
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
    impute=None,
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
    impute=None,
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
    impute=None,
)
def ret_clutch_matchup() -> pl.Expr:
    """Player's BP conversion ability vs opponent's BP save ability.

    High value = player wins BP battles when returning.
    """
    return pl.col("player_return_clutch") - pl.col("opp_serve_clutch")


# =============================================================================
# Serve / return surface and venue adjustments
# =============================================================================
#
# Serve Elo is one number per player; overall Elo is not — it carries surface
# and indoor adjustments on top. That gap is why a player who serves very
# differently by surface had it averaged away.
#
# The raw adjustments are registered individually AS WELL AS inside composites,
# mirroring how hard_adj/clay_adj/grass_adj are. That is not symmetry for its
# own sake: forward selection here fits shallow trees, which cannot un-sum a
# precomputed composite to threshold on one component, so a component only
# competes if it is a candidate in its own right.


def svc_surface_adj_expr(prefix: str) -> pl.Expr:
    """Surface-selected serve adjustment (no base rating).

    Mirrors surface_adj_expr. 0.0 on any surface the ratings do not model.
    """
    return (
        pl.when(pl.col("surface") == "Hard").then(pl.col(f"{prefix}_svc_hard_adj"))
        .when(pl.col("surface") == "Clay").then(pl.col(f"{prefix}_svc_clay_adj"))
        .when(pl.col("surface") == "Grass").then(pl.col(f"{prefix}_svc_grass_adj"))
        .otherwise(0.0)
    )


def ret_surface_adj_expr(prefix: str) -> pl.Expr:
    """Surface-selected return adjustment (no base rating)."""
    return (
        pl.when(pl.col("surface") == "Hard").then(pl.col(f"{prefix}_ret_hard_adj"))
        .when(pl.col("surface") == "Clay").then(pl.col(f"{prefix}_ret_clay_adj"))
        .when(pl.col("surface") == "Grass").then(pl.col(f"{prefix}_ret_grass_adj"))
        .otherwise(0.0)
    )


def svc_indoor_adj_expr(prefix: str) -> pl.Expr:
    """Indoor serve adjustment, on indoor HARD matches only.

    Narrower than indoor_adj_expr, which applies on any indoor match. The serve
    indoor correction is measured for hard courts alone — indoor clay and indoor
    grass are too rare to support their own population baseline, so the rating
    never trains those and reading it here would be reading an untrained zero.
    """
    return (
        pl.when(pl.col("indoor") & (pl.col("surface") == "Hard"))
        .then(pl.col(f"{prefix}_svc_indoor_adj"))
        .otherwise(0.0)
    )


def ret_indoor_adj_expr(prefix: str) -> pl.Expr:
    """Indoor return adjustment, on indoor HARD matches only."""
    return (
        pl.when(pl.col("indoor") & (pl.col("surface") == "Hard"))
        .then(pl.col(f"{prefix}_ret_indoor_adj"))
        .otherwise(0.0)
    )


def svc_elo_surface_expr(prefix: str) -> pl.Expr:
    """Serve rating plus its surface adjustment."""
    return pl.col(f"{prefix}_serve_elo") + svc_surface_adj_expr(prefix)


def ret_elo_surface_expr(prefix: str) -> pl.Expr:
    """Return rating plus its surface adjustment."""
    return pl.col(f"{prefix}_return_elo") + ret_surface_adj_expr(prefix)


def svc_elo_surface_indoor_expr(prefix: str) -> pl.Expr:
    """Serve rating plus surface and venue adjustments — the full stack."""
    return svc_elo_surface_expr(prefix) + svc_indoor_adj_expr(prefix)


def ret_elo_surface_indoor_expr(prefix: str) -> pl.Expr:
    """Return rating plus surface and venue adjustments."""
    return ret_elo_surface_expr(prefix) + ret_indoor_adj_expr(prefix)


# --- registered features -----------------------------------------------

@feature(
    name="svc_hard_adj",
    params=[],
    description="Serve Elo hard adjustment",
    mirror=True,
)
def svc_hard_adj() -> pl.Expr:
    return pl.col("player_svc_hard_adj")


@feature(
    name="svc_hard_rd",
    params=[],
    description="Serve Elo hard rating deviation",
    mirror=True,
)
def svc_hard_rd() -> pl.Expr:
    return pl.col("player_svc_hard_rd")

@feature(
    name="svc_clay_adj",
    params=[],
    description="Serve Elo clay adjustment",
    mirror=True,
)
def svc_clay_adj() -> pl.Expr:
    return pl.col("player_svc_clay_adj")


@feature(
    name="svc_clay_rd",
    params=[],
    description="Serve Elo clay rating deviation",
    mirror=True,
)
def svc_clay_rd() -> pl.Expr:
    return pl.col("player_svc_clay_rd")

@feature(
    name="svc_grass_adj",
    params=[],
    description="Serve Elo grass adjustment",
    mirror=True,
)
def svc_grass_adj() -> pl.Expr:
    return pl.col("player_svc_grass_adj")


@feature(
    name="svc_grass_rd",
    params=[],
    description="Serve Elo grass rating deviation",
    mirror=True,
)
def svc_grass_rd() -> pl.Expr:
    return pl.col("player_svc_grass_rd")

@feature(
    name="svc_indoor_adj",
    params=[],
    description="Serve Elo indoor adjustment",
    mirror=True,
)
def svc_indoor_adj() -> pl.Expr:
    return pl.col("player_svc_indoor_adj")


@feature(
    name="svc_indoor_rd",
    params=[],
    description="Serve Elo indoor rating deviation",
    mirror=True,
)
def svc_indoor_rd() -> pl.Expr:
    return pl.col("player_svc_indoor_rd")

@feature(
    name="ret_hard_adj",
    params=[],
    description="Return Elo hard adjustment",
    mirror=True,
)
def ret_hard_adj() -> pl.Expr:
    return pl.col("player_ret_hard_adj")


@feature(
    name="ret_hard_rd",
    params=[],
    description="Return Elo hard rating deviation",
    mirror=True,
)
def ret_hard_rd() -> pl.Expr:
    return pl.col("player_ret_hard_rd")

@feature(
    name="ret_clay_adj",
    params=[],
    description="Return Elo clay adjustment",
    mirror=True,
)
def ret_clay_adj() -> pl.Expr:
    return pl.col("player_ret_clay_adj")


@feature(
    name="ret_clay_rd",
    params=[],
    description="Return Elo clay rating deviation",
    mirror=True,
)
def ret_clay_rd() -> pl.Expr:
    return pl.col("player_ret_clay_rd")

@feature(
    name="ret_grass_adj",
    params=[],
    description="Return Elo grass adjustment",
    mirror=True,
)
def ret_grass_adj() -> pl.Expr:
    return pl.col("player_ret_grass_adj")


@feature(
    name="ret_grass_rd",
    params=[],
    description="Return Elo grass rating deviation",
    mirror=True,
)
def ret_grass_rd() -> pl.Expr:
    return pl.col("player_ret_grass_rd")

@feature(
    name="ret_indoor_adj",
    params=[],
    description="Return Elo indoor adjustment",
    mirror=True,
)
def ret_indoor_adj() -> pl.Expr:
    return pl.col("player_ret_indoor_adj")


@feature(
    name="ret_indoor_rd",
    params=[],
    description="Return Elo indoor rating deviation",
    mirror=True,
)
def ret_indoor_rd() -> pl.Expr:
    return pl.col("player_ret_indoor_rd")

@feature(
    name="svc_elo_surface",
    params=[],
    description="Serve Elo adjusted for surface",
    mirror=True,
)
def svc_elo_surface() -> pl.Expr:
    return svc_elo_surface_expr("player")


@feature(
    name="svc_elo_surface_diff",
    params=[],
    description="Serve Elo (surface) difference",
    mirror=False,
)
def svc_elo_surface_diff() -> pl.Expr:
    return svc_elo_surface_expr("player") - svc_elo_surface_expr("opp")


@feature(
    name="svc_elo_surface_matchup",
    params=[],
    description="Player serve vs opponent return, both surface adjusted",
    mirror=True,
    impute=None,
)
def svc_elo_surface_matchup() -> pl.Expr:
    return svc_elo_surface_expr("player") - ret_elo_surface_expr("opp")

@feature(
    name="svc_elo_surface_indoor",
    params=[],
    description="Serve Elo adjusted for surface and indoor",
    mirror=True,
)
def svc_elo_surface_indoor() -> pl.Expr:
    return svc_elo_surface_indoor_expr("player")


@feature(
    name="svc_elo_surface_indoor_diff",
    params=[],
    description="Serve Elo (surface+indoor) difference",
    mirror=False,
)
def svc_elo_surface_indoor_diff() -> pl.Expr:
    return svc_elo_surface_indoor_expr("player") - svc_elo_surface_indoor_expr("opp")


@feature(
    name="svc_elo_surface_indoor_matchup",
    params=[],
    description="Player serve vs opponent return, both surface+indoor adjusted",
    mirror=True,
    impute=None,
)
def svc_elo_surface_indoor_matchup() -> pl.Expr:
    return svc_elo_surface_indoor_expr("player") - ret_elo_surface_indoor_expr("opp")

@feature(
    name="ret_elo_surface",
    params=[],
    description="Return Elo adjusted for surface",
    mirror=True,
)
def ret_elo_surface() -> pl.Expr:
    return ret_elo_surface_expr("player")


@feature(
    name="ret_elo_surface_diff",
    params=[],
    description="Return Elo (surface) difference",
    mirror=False,
)
def ret_elo_surface_diff() -> pl.Expr:
    return ret_elo_surface_expr("player") - ret_elo_surface_expr("opp")


@feature(
    name="ret_elo_surface_matchup",
    params=[],
    description="Player return vs opponent serve, both surface adjusted",
    mirror=True,
    impute=None,
)
def ret_elo_surface_matchup() -> pl.Expr:
    return ret_elo_surface_expr("player") - svc_elo_surface_expr("opp")

@feature(
    name="ret_elo_surface_indoor",
    params=[],
    description="Return Elo adjusted for surface and indoor",
    mirror=True,
)
def ret_elo_surface_indoor() -> pl.Expr:
    return ret_elo_surface_indoor_expr("player")


@feature(
    name="ret_elo_surface_indoor_diff",
    params=[],
    description="Return Elo (surface+indoor) difference",
    mirror=False,
)
def ret_elo_surface_indoor_diff() -> pl.Expr:
    return ret_elo_surface_indoor_expr("player") - ret_elo_surface_indoor_expr("opp")


@feature(
    name="ret_elo_surface_indoor_matchup",
    params=[],
    description="Player return vs opponent serve, both surface+indoor adjusted",
    mirror=True,
    impute=None,
)
def ret_elo_surface_indoor_matchup() -> pl.Expr:
    return ret_elo_surface_indoor_expr("player") - svc_elo_surface_indoor_expr("opp")

@feature(
    name="svc_clay_specialist",
    params=[],
    description="Serve clay adjustment minus hard adjustment (clay preference)",
    mirror=True,
)
def svc_clay_specialist() -> pl.Expr:
    """Both terms already have the population surface baseline removed, so the
    difference is a clean read on preference rather than a mix of the two."""
    return pl.col("player_svc_clay_adj") - pl.col("player_svc_hard_adj")

@feature(
    name="svc_grass_specialist",
    params=[],
    description="Serve grass adjustment minus hard adjustment (grass preference)",
    mirror=True,
)
def svc_grass_specialist() -> pl.Expr:
    """Both terms already have the population surface baseline removed, so the
    difference is a clean read on preference rather than a mix of the two."""
    return pl.col("player_svc_grass_adj") - pl.col("player_svc_hard_adj")

@feature(
    name="ret_clay_specialist",
    params=[],
    description="Return clay adjustment minus hard adjustment (clay preference)",
    mirror=True,
)
def ret_clay_specialist() -> pl.Expr:
    """Both terms already have the population surface baseline removed, so the
    difference is a clean read on preference rather than a mix of the two."""
    return pl.col("player_ret_clay_adj") - pl.col("player_ret_hard_adj")

@feature(
    name="ret_grass_specialist",
    params=[],
    description="Return grass adjustment minus hard adjustment (grass preference)",
    mirror=True,
)
def ret_grass_specialist() -> pl.Expr:
    """Both terms already have the population surface baseline removed, so the
    difference is a clean read on preference rather than a mix of the two."""
    return pl.col("player_ret_grass_adj") - pl.col("player_ret_hard_adj")
