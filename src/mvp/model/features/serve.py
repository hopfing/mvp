"""Serve-related features."""


import polars as pl

from mvp.model.primitives import cumulative_mean, ratio_feature, rolling_mean
from mvp.model.registry import feature


# =============================================================================
# Single Stats
# =============================================================================


@feature(
    name="svc_first_serve_win_pct",
    params=["days"],
    description="First serve points won percentage (windowed or all-time)",
    mirror=True,
)
def svc_first_serve_win_pct(days: int | None = None) -> pl.Expr:
    """First serve points won percentage."""
    return ratio_feature("svc_first_serve_pts_won", "svc_first_serve_pts_played", days)


@feature(
    name="svc_second_serve_win_pct",
    params=["days"],
    description="Second serve points won percentage (windowed or all-time)",
    mirror=True,
)
def svc_second_serve_win_pct(days: int | None = None) -> pl.Expr:
    """Second serve points won percentage."""
    return ratio_feature("svc_second_serve_pts_won", "svc_second_serve_pts_played", days)


@feature(
    name="svc_ace_pct",
    params=["days"],
    description="Ace percentage (aces / first serve attempts)",
    mirror=True,
)
def svc_ace_pct(days: int | None = None) -> pl.Expr:
    """Ace percentage - aces per first serve attempt."""
    return ratio_feature("svc_aces", "svc_first_serve_att", days)


@feature(
    name="svc_df_pct",
    params=["days"],
    description="Double fault percentage (double faults / first serve attempts)",
    mirror=True,
)
def svc_df_pct(days: int | None = None) -> pl.Expr:
    """Double fault percentage - double faults per first serve attempt."""
    return ratio_feature("svc_double_faults", "svc_first_serve_att", days)


@feature(
    name="svc_bp_save_pct",
    params=["days"],
    description="Break points saved percentage (windowed or all-time)",
    mirror=True,
)
def svc_bp_save_pct(days: int | None = None) -> pl.Expr:
    """Break points saved percentage."""
    return ratio_feature("svc_bp_saved", "svc_bp_faced", days)


@feature(
    name="svc_first_serve_in_pct",
    params=["days"],
    description="First serve in percentage (first serves in / attempts)",
    mirror=True,
)
def svc_first_serve_in_pct(days: int | None = None) -> pl.Expr:
    """First serve in percentage."""
    return ratio_feature("svc_first_serve_in", "svc_first_serve_att", days)


@feature(
    name="svc_rating",
    params=["days"],
    description="ATP serve rating average (windowed or all-time)",
    mirror=True,
)
def svc_rating(days: int | None = None) -> pl.Expr:
    """ATP composite serve rating (average over time)."""
    if days is None:
        return cumulative_mean("svc_serve_rating", group_by="player_id")
    return rolling_mean("svc_serve_rating", days=days, group_by="player_id")


# =============================================================================
# Diff Features (player - opponent, same stat)
# =============================================================================


@feature(
    name="svc_first_serve_win_pct_diff",
    params=["days"],
    description="First serve win pct difference (player - opponent)",
    depends_on=["svc_first_serve_win_pct"],
    mirror=False,
)
def svc_first_serve_win_pct_diff(days: int | None = None) -> pl.Expr:
    """First serve win percentage difference."""
    if days is None:
        return pl.col("player_svc_first_serve_win_pct") - pl.col("opp_svc_first_serve_win_pct")
    return (
        pl.col(f"player_svc_first_serve_win_pct_{days}d")
        - pl.col(f"opp_svc_first_serve_win_pct_{days}d")
    )


@feature(
    name="svc_second_serve_win_pct_diff",
    params=["days"],
    description="Second serve win pct difference (player - opponent)",
    depends_on=["svc_second_serve_win_pct"],
    mirror=False,
)
def svc_second_serve_win_pct_diff(days: int | None = None) -> pl.Expr:
    """Second serve win percentage difference."""
    if days is None:
        return pl.col("player_svc_second_serve_win_pct") - pl.col("opp_svc_second_serve_win_pct")
    return (
        pl.col(f"player_svc_second_serve_win_pct_{days}d")
        - pl.col(f"opp_svc_second_serve_win_pct_{days}d")
    )


@feature(
    name="svc_ace_pct_diff",
    params=["days"],
    description="Ace percentage difference (player - opponent)",
    depends_on=["svc_ace_pct"],
    mirror=False,
)
def svc_ace_pct_diff(days: int | None = None) -> pl.Expr:
    """Ace percentage difference."""
    if days is None:
        return pl.col("player_svc_ace_pct") - pl.col("opp_svc_ace_pct")
    return pl.col(f"player_svc_ace_pct_{days}d") - pl.col(f"opp_svc_ace_pct_{days}d")


@feature(
    name="svc_df_pct_diff",
    params=["days"],
    description="Double fault percentage difference (player - opponent)",
    depends_on=["svc_df_pct"],
    mirror=False,
)
def svc_df_pct_diff(days: int | None = None) -> pl.Expr:
    """Double fault percentage difference."""
    if days is None:
        return pl.col("player_svc_df_pct") - pl.col("opp_svc_df_pct")
    return pl.col(f"player_svc_df_pct_{days}d") - pl.col(f"opp_svc_df_pct_{days}d")


@feature(
    name="svc_bp_save_pct_diff",
    params=["days"],
    description="Break point save percentage difference (player - opponent)",
    depends_on=["svc_bp_save_pct"],
    mirror=False,
)
def svc_bp_save_pct_diff(days: int | None = None) -> pl.Expr:
    """Break point save percentage difference."""
    if days is None:
        return pl.col("player_svc_bp_save_pct") - pl.col("opp_svc_bp_save_pct")
    return pl.col(f"player_svc_bp_save_pct_{days}d") - pl.col(f"opp_svc_bp_save_pct_{days}d")


@feature(
    name="svc_first_serve_in_pct_diff",
    params=["days"],
    description="First serve in percentage difference (player - opponent)",
    depends_on=["svc_first_serve_in_pct"],
    mirror=False,
)
def svc_first_serve_in_pct_diff(days: int | None = None) -> pl.Expr:
    """First serve in percentage difference."""
    if days is None:
        return pl.col("player_svc_first_serve_in_pct") - pl.col("opp_svc_first_serve_in_pct")
    return (
        pl.col(f"player_svc_first_serve_in_pct_{days}d")
        - pl.col(f"opp_svc_first_serve_in_pct_{days}d")
    )


@feature(
    name="svc_rating_diff",
    params=["days"],
    description="ATP serve rating difference (player - opponent)",
    depends_on=["svc_rating"],
    mirror=False,
)
def svc_rating_diff(days: int | None = None) -> pl.Expr:
    """ATP serve rating difference."""
    if days is None:
        return pl.col("player_svc_rating") - pl.col("opp_svc_rating")
    return pl.col(f"player_svc_rating_{days}d") - pl.col(f"opp_svc_rating_{days}d")


# =============================================================================
# Matchup Features (player serve vs opponent return)
# =============================================================================


@feature(
    name="svc_first_serve_win_pct_matchup",
    params=["days"],
    description="Player first serve win % minus opponent first return win %",
    depends_on=["svc_first_serve_win_pct", "ret_first_serve_win_pct"],
    mirror=False,
)
def svc_first_serve_win_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's first serve vs opponent's first serve return."""
    if days is None:
        return pl.col("player_svc_first_serve_win_pct") - pl.col("opp_ret_first_serve_win_pct")
    return (
        pl.col(f"player_svc_first_serve_win_pct_{days}d")
        - pl.col(f"opp_ret_first_serve_win_pct_{days}d")
    )


@feature(
    name="svc_second_serve_win_pct_matchup",
    params=["days"],
    description="Player second serve win % minus opponent second return win %",
    depends_on=["svc_second_serve_win_pct", "ret_second_serve_win_pct"],
    mirror=False,
)
def svc_second_serve_win_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's second serve vs opponent's second serve return."""
    if days is None:
        return pl.col("player_svc_second_serve_win_pct") - pl.col("opp_ret_second_serve_win_pct")
    return (
        pl.col(f"player_svc_second_serve_win_pct_{days}d")
        - pl.col(f"opp_ret_second_serve_win_pct_{days}d")
    )


@feature(
    name="svc_bp_pct_matchup",
    params=["days"],
    description="Player BP save % minus opponent BP convert %",
    depends_on=["svc_bp_save_pct", "ret_bp_convert_pct"],
    mirror=False,
)
def svc_bp_pct_matchup(days: int | None = None) -> pl.Expr:
    """Player's clutch serving vs opponent's clutch returning."""
    if days is None:
        return pl.col("player_svc_bp_save_pct") - pl.col("opp_ret_bp_convert_pct")
    return pl.col(f"player_svc_bp_save_pct_{days}d") - pl.col(f"opp_ret_bp_convert_pct_{days}d")
