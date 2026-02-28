"""Cross-domain matchup features (serve vs return, etc.)."""

from __future__ import annotations

import polars as pl

from mvp.model.registry import feature


@feature(
    name="ace_vs_return",
    params=["days"],
    description="Player ace rate minus opponent first return win pct",
    depends_on=["ace_rate", "ret_first_win_pct"],
    mirror=False,
)
def ace_vs_return(days: int | None = None) -> pl.Expr:
    """Player's ace ability vs opponent's return ability."""
    if days is None:
        return pl.col("player_ace_rate") - pl.col("opp_ret_first_win_pct")
    return pl.col(f"player_ace_rate_{days}d") - pl.col(f"opp_ret_first_win_pct_{days}d")


@feature(
    name="bp_clutch_diff",
    params=["days"],
    description="Player BP save pct minus opponent BP convert pct",
    depends_on=["bp_save_pct", "bp_convert_pct"],
    mirror=False,
)
def bp_clutch_diff(days: int | None = None) -> pl.Expr:
    """Player's clutch serving vs opponent's clutch returning."""
    if days is None:
        return pl.col("player_bp_save_pct") - pl.col("opp_bp_convert_pct")
    return pl.col(f"player_bp_save_pct_{days}d") - pl.col(f"opp_bp_convert_pct_{days}d")


@feature(
    name="first_serve_matchup",
    params=["days"],
    description="Player first serve win pct minus opponent first return win pct",
    depends_on=["svc_first_win_pct", "ret_first_win_pct"],
    mirror=False,
)
def first_serve_matchup(days: int | None = None) -> pl.Expr:
    """Player's first serve vs opponent's first serve return."""
    if days is None:
        return pl.col("player_svc_first_win_pct") - pl.col("opp_ret_first_win_pct")
    return pl.col(f"player_svc_first_win_pct_{days}d") - pl.col(f"opp_ret_first_win_pct_{days}d")


@feature(
    name="second_serve_matchup",
    params=["days"],
    description="Player second serve win pct minus opponent second return win pct",
    depends_on=["svc_second_win_pct", "ret_second_win_pct"],
    mirror=False,
)
def second_serve_matchup(days: int | None = None) -> pl.Expr:
    """Player's second serve vs opponent's second serve return."""
    if days is None:
        return pl.col("player_svc_second_win_pct") - pl.col("opp_ret_second_win_pct")
    return pl.col(f"player_svc_second_win_pct_{days}d") - pl.col(f"opp_ret_second_win_pct_{days}d")
