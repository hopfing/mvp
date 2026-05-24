"""Fitness and durability features: retirement/walkover history."""


import polars as pl

from mvp.model.primitives import cumulative_count, rolling_count
from mvp.model.registry import feature, register_diff

DATE_COL = "effective_match_date"


def _player_retired() -> pl.Expr:
    """1 if the player themselves retired/walked over (lost + RET or W/O)."""
    return (
        pl.col("reason").fill_null("").is_in(["RET", "W/O"])
        & ~pl.col("won").cast(pl.Boolean)
    ).cast(pl.Int64)


@feature(
    name="retirement_rate",
    params=["days"],
    description="Fraction of recent same-draw-type matches ending in player's own retirement",
    mirror=True,
    impute=None,
)
def retirement_rate(days: int | None = None) -> pl.Expr:
    """Rolling rate of player's own retirements/walkovers (singles-only when filtered)."""
    # Group by draw_type so doubles retirements don't pollute singles rate
    group_by = ["player_id", "draw_type"]
    retired = _player_retired()
    if days is None:
        ret_count = (
            retired.cum_sum().shift(1).over(group_by, order_by=DATE_COL).fill_null(0)
        )
        total = cumulative_count(group_by=group_by)
    else:
        ret_count = (
            retired
            .rolling_sum_by(by=DATE_COL, window_size=f"{days}d", closed="left")
            .over(group_by)
            .fill_null(0)
        )
        total = rolling_count(days=days, group_by=group_by)
    return pl.when(total > 0).then(ret_count / total).otherwise(None)


@feature(
    name="last_match_retirement",
    params=[],
    description="1 if player's previous same-draw-type match ended in their own retirement",
    mirror=True,
    impute=None,
)
def last_match_retirement() -> pl.Expr:
    """Whether the player retired/walked over in their most recent same-draw-type match."""
    # Group by draw_type so a doubles retirement doesn't flag a singles match
    group_by = ["player_id", "draw_type"]
    return _player_retired().cast(pl.Float64).shift(1).over(group_by, order_by=DATE_COL)


# --- Derived diff features ---

register_diff("retirement_rate")
register_diff("last_match_retirement")
