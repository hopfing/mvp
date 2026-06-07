"""Surface transition features: recency and frequency of surface changes."""


import polars as pl

from mvp.model.primitives import cumulative_count, rolling_count
from mvp.model.registry import feature, register_diff

DATE_COL = "effective_match_date"


@feature(
    name="days_since_surface",
    params=[],
    description="Days since player last played on current surface",
    mirror=True,
    impute=None,
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
    impute=None,
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
    impute=None,
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

register_diff("days_since_surface")
register_diff("surface_switch")
register_diff("pct_matches_on_surface")
