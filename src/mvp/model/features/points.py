"""Points-related features."""


import polars as pl

from mvp.model.primitives import ratio_feature
from mvp.model.registry import feature


# =============================================================================
# Single Stats
# =============================================================================


@feature(
    name="pts_total_won_pct",
    params=["days"],
    description="Total points won percentage (windowed or all-time)",
    mirror=True,
)
def pts_total_won_pct(days: int | None = None) -> pl.Expr:
    """Total points won percentage (serve + return combined)."""
    return ratio_feature("pts_total_pts_won", "pts_total_pts_played", days)


@feature(
    name="pts_service_won_pct",
    params=["days"],
    description="Service points won percentage (windowed or all-time)",
    mirror=True,
)
def pts_service_won_pct(days: int | None = None) -> pl.Expr:
    """Service points won percentage."""
    return ratio_feature("pts_service_pts_won", "pts_service_pts_played", days)


@feature(
    name="pts_return_won_pct",
    params=["days"],
    description="Return points won percentage (windowed or all-time)",
    mirror=True,
)
def pts_return_won_pct(days: int | None = None) -> pl.Expr:
    """Return points won percentage."""
    return ratio_feature("pts_return_pts_won", "pts_return_pts_played", days)


# =============================================================================
# Diff Features (player - opponent, same stat)
# =============================================================================


@feature(
    name="pts_total_won_pct_diff",
    params=["days"],
    description="Total points won pct difference (player - opponent)",
    depends_on=["pts_total_won_pct"],
    mirror=False,
)
def pts_total_won_pct_diff(days: int | None = None) -> pl.Expr:
    """Total points won percentage difference."""
    if days is None:
        return pl.col("player_pts_total_won_pct") - pl.col("opp_pts_total_won_pct")
    return pl.col(f"player_pts_total_won_pct_{days}d") - pl.col(f"opp_pts_total_won_pct_{days}d")


@feature(
    name="pts_service_won_pct_diff",
    params=["days"],
    description="Service points won pct difference (player - opponent)",
    depends_on=["pts_service_won_pct"],
    mirror=False,
)
def pts_service_won_pct_diff(days: int | None = None) -> pl.Expr:
    """Service points won percentage difference."""
    if days is None:
        return pl.col("player_pts_service_won_pct") - pl.col("opp_pts_service_won_pct")
    return (
        pl.col(f"player_pts_service_won_pct_{days}d")
        - pl.col(f"opp_pts_service_won_pct_{days}d")
    )


@feature(
    name="pts_return_won_pct_diff",
    params=["days"],
    description="Return points won pct difference (player - opponent)",
    depends_on=["pts_return_won_pct"],
    mirror=False,
)
def pts_return_won_pct_diff(days: int | None = None) -> pl.Expr:
    """Return points won percentage difference."""
    if days is None:
        return pl.col("player_pts_return_won_pct") - pl.col("opp_pts_return_won_pct")
    return (
        pl.col(f"player_pts_return_won_pct_{days}d")
        - pl.col(f"opp_pts_return_won_pct_{days}d")
    )


# =============================================================================
# Matchup Features (player domain vs opponent opposite domain)
# =============================================================================


@feature(
    name="svc_pts_won_pct_matchup",
    params=["days"],
    description="Player service pts % minus opponent return pts % (serve advantage)",
    depends_on=["pts_service_won_pct", "pts_return_won_pct"],
    mirror=False,
)
def svc_pts_won_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's serve strength vs opponent's return strength.

    Positive means player's serve is stronger than opponent's return.
    """
    if days is None:
        return pl.col("player_pts_service_won_pct") - pl.col("opp_pts_return_won_pct")
    return (
        pl.col(f"player_pts_service_won_pct_{days}d")
        - pl.col(f"opp_pts_return_won_pct_{days}d")
    )


@feature(
    name="ret_pts_won_pct_matchup",
    params=["days"],
    description="Player return pts % minus opponent service pts % (return advantage)",
    depends_on=["pts_service_won_pct", "pts_return_won_pct"],
    mirror=False,
)
def ret_pts_won_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's return strength vs opponent's serve strength.

    Positive means player's return is stronger than opponent's serve.
    """
    if days is None:
        return pl.col("player_pts_return_won_pct") - pl.col("opp_pts_service_won_pct")
    return (
        pl.col(f"player_pts_return_won_pct_{days}d")
        - pl.col(f"opp_pts_service_won_pct_{days}d")
    )
