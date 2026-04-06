"""Points-related features."""


import polars as pl

from mvp.model.primitives import ratio_feature
from mvp.model.registry import feature, register_diff, register_matchup, register_sum

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

for _base in ["pts_total_won_pct", "pts_service_won_pct", "pts_return_won_pct"]:
    register_diff(_base)

for _base in ["pts_total_won_pct", "pts_service_won_pct", "pts_return_won_pct"]:
    register_sum(_base)


# =============================================================================
# Matchup Features (player domain vs opponent opposite domain)
# =============================================================================

register_matchup(
    "svc_pts_won_pct_matchup",
    "player_pts_service_won_pct", "opp_pts_return_won_pct",
    "pts_service_won_pct", "pts_return_won_pct",
    "Player service pts % minus opponent return pts % (serve advantage)",
)
register_matchup(
    "ret_pts_won_pct_matchup",
    "player_pts_return_won_pct", "opp_pts_service_won_pct",
    "pts_return_won_pct", "pts_service_won_pct",
    "Player return pts % minus opponent service pts % (return advantage)",
)
