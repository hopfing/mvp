"""Surface transition features: recency and frequency of surface changes."""


import polars as pl

from mvp.model.primitives import cumulative_count, rolling_count
from mvp.model.registry import feature

DATE_COL = "effective_match_date"


@feature(
    name="days_since_surface",
    params=[],
    description="Days since player last played on current surface",
    mirror=True,
    impute="median",
)
def days_since_surface() -> pl.Expr:
    """Days since this player last played on the current surface."""
    group_by = ["player_id", "surface"]
    prev_date = pl.col(DATE_COL).shift(1).over(group_by, order_by=DATE_COL)
    return (pl.col(DATE_COL) - prev_date).dt.total_days().cast(pl.Float64)


@feature(
    name="surface_switch",
    params=[],
    description="1 if player changed surfaces since last match, 0 if same",
    mirror=True,
    impute=0,
)
def surface_switch() -> pl.Expr:
    """Whether player switched surfaces since their previous match."""
    prev_surface = pl.col("surface").shift(1).over("player_id", order_by=DATE_COL)
    return (pl.col("surface") != prev_surface).cast(pl.Float64)


@feature(
    name="pct_matches_on_surface",
    params=["days"],
    description="Fraction of recent matches played on current surface",
    mirror=True,
    impute="median",
)
def pct_matches_on_surface(days: int | None = None) -> pl.Expr:
    """Fraction of a player's matches on the current surface in a rolling window."""
    if days is None:
        surf_count = cumulative_count(group_by=["player_id", "surface"])
        total_count = cumulative_count(group_by="player_id")
    else:
        surf_count = rolling_count(days=days, group_by=["player_id", "surface"])
        total_count = rolling_count(days=days, group_by="player_id")
    return pl.when(total_count > 0).then(surf_count / total_count).otherwise(None)


# --- Derived diff features ---


@feature(
    name="days_since_surface_diff",
    params=[],
    description="Player days_since_surface minus opponent",
    depends_on=["days_since_surface"],
    mirror=False,
    impute=0,
)
def days_since_surface_diff() -> pl.Expr:
    """Days-since-surface difference (player - opponent)."""
    return pl.col("player_days_since_surface") - pl.col("opp_days_since_surface")


@feature(
    name="surface_switch_diff",
    params=[],
    description="Player surface_switch minus opponent",
    depends_on=["surface_switch"],
    mirror=False,
    impute=0,
)
def surface_switch_diff() -> pl.Expr:
    """Surface switch difference (player - opponent)."""
    return pl.col("player_surface_switch") - pl.col("opp_surface_switch")


@feature(
    name="pct_matches_on_surface_diff",
    params=["days"],
    description="Player pct_matches_on_surface minus opponent",
    depends_on=["pct_matches_on_surface"],
    mirror=False,
    impute=0,
)
def pct_matches_on_surface_diff(days: int | None = None) -> pl.Expr:
    """Pct matches on surface difference (player - opponent)."""
    if days is None:
        return pl.col("player_pct_matches_on_surface") - pl.col("opp_pct_matches_on_surface")
    return pl.col(f"player_pct_matches_on_surface_{days}d") - pl.col(f"opp_pct_matches_on_surface_{days}d")
