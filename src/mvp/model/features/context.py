"""Match context features (indoor, circuit, event type, seeding)."""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import cumulative_mean, cumulative_sum, cumulative_count, rolling_mean, rolling_sum, rolling_count
from mvp.model.registry import feature


@feature(
    name="indoor_win_rate",
    params=["days"],
    description="Win rate on indoor courts",
    mirror=True,
)
def indoor_win_rate(days: int | None = None) -> pl.Expr:
    """Win rate on indoor courts."""
    group_by = ["player_id", "indoor"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)


@feature(
    name="indoor_win_rate_diff",
    params=["days"],
    description="Player indoor win rate minus opponent indoor win rate",
    depends_on=["indoor_win_rate"],
    mirror=False,
)
def indoor_win_rate_diff(days: int | None = None) -> pl.Expr:
    """Indoor win rate difference."""
    if days is None:
        return pl.col("player_indoor_win_rate") - pl.col("opp_indoor_win_rate")
    return pl.col(f"player_indoor_win_rate_{days}d") - pl.col(f"opp_indoor_win_rate_{days}d")


@feature(
    name="circuit_win_rate",
    params=["days"],
    description="Win rate on current circuit (tour vs challenger)",
    mirror=True,
)
def circuit_win_rate(days: int | None = None) -> pl.Expr:
    """Win rate on the current circuit."""
    group_by = ["player_id", "circuit"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)


@feature(
    name="circuit_win_rate_diff",
    params=["days"],
    description="Player circuit win rate minus opponent circuit win rate",
    depends_on=["circuit_win_rate"],
    mirror=False,
)
def circuit_win_rate_diff(days: int | None = None) -> pl.Expr:
    """Circuit win rate difference."""
    if days is None:
        return pl.col("player_circuit_win_rate") - pl.col("opp_circuit_win_rate")
    return pl.col(f"player_circuit_win_rate_{days}d") - pl.col(f"opp_circuit_win_rate_{days}d")


@feature(
    name="is_seeded",
    params=[],
    description="1 if player is seeded, 0 otherwise",
    mirror=True,
)
def is_seeded() -> pl.Expr:
    """Whether player is seeded in this tournament."""
    return pl.col("player_seed").is_not_null().cast(pl.Float64)


@feature(
    name="seed_diff",
    params=[],
    description="Player seed minus opponent seed (lower seed is better)",
    mirror=False,
)
def seed_diff() -> pl.Expr:
    """Seed difference (negative = player has better seed)."""
    return pl.col("player_seed") - pl.col("opp_seed")


@feature(
    name="both_seeded",
    params=[],
    description="1 if both players seeded, 0 otherwise",
    mirror=False,
)
def both_seeded() -> pl.Expr:
    """Whether both players are seeded."""
    return (
        pl.col("player_seed").is_not_null() & pl.col("opp_seed").is_not_null()
    ).cast(pl.Float64)


@feature(
    name="neither_seeded",
    params=[],
    description="1 if neither player seeded, 0 otherwise",
    mirror=False,
)
def neither_seeded() -> pl.Expr:
    """Whether neither player is seeded."""
    return (
        pl.col("player_seed").is_null() & pl.col("opp_seed").is_null()
    ).cast(pl.Float64)
