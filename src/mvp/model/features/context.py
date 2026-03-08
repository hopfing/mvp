"""Match context features (indoor, circuit, event type, seeding)."""


import polars as pl

from mvp.model.primitives import cumulative_mean, cumulative_sum, cumulative_count, rolling_mean, rolling_sum, rolling_count
from mvp.model.registry import feature


@feature(
    name="venue_win_pct",
    params=["days"],
    description="Win percentage on current venue type (indoor/outdoor)",
    mirror=True,
)
def venue_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage on current venue type (indoor/outdoor)."""
    group_by = ["player_id", "indoor"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)


@feature(
    name="venue_win_pct_diff",
    params=["days"],
    description="Player venue win pct minus opponent venue win pct",
    depends_on=["venue_win_pct"],
    mirror=False,
)
def venue_win_pct_diff(days: int | None = None) -> pl.Expr:
    """Venue win percentage difference (indoor/outdoor)."""
    if days is None:
        return pl.col("player_venue_win_pct") - pl.col("opp_venue_win_pct")
    return pl.col(f"player_venue_win_pct_{days}d") - pl.col(f"opp_venue_win_pct_{days}d")


@feature(
    name="circuit_win_pct",
    params=["days"],
    description="Win percentage on current circuit (tour vs challenger)",
    mirror=True,
)
def circuit_win_pct(days: int | None = None) -> pl.Expr:
    """Win percentage on the current circuit."""
    group_by = ["player_id", "circuit"]
    if days is None:
        wins = cumulative_sum("won", group_by=group_by)
        matches = cumulative_count(group_by=group_by)
    else:
        wins = rolling_sum("won", days=days, group_by=group_by)
        matches = rolling_count(days=days, group_by=group_by)
    return pl.when(matches > 0).then(wins / matches).otherwise(None)


@feature(
    name="circuit_win_pct_diff",
    params=["days"],
    description="Player circuit win pct minus opponent circuit win pct",
    depends_on=["circuit_win_pct"],
    mirror=False,
)
def circuit_win_pct_diff(days: int | None = None) -> pl.Expr:
    """Circuit win percentage difference."""
    if days is None:
        return pl.col("player_circuit_win_pct") - pl.col("opp_circuit_win_pct")
    return pl.col(f"player_circuit_win_pct_{days}d") - pl.col(f"opp_circuit_win_pct_{days}d")


@feature(
    name="tour_match_pct",
    params=["days"],
    description="Fraction of recent matches played on ATP Tour circuit",
    mirror=True,
)
def tour_match_pct(days: int | None = None) -> pl.Expr:
    """Fraction of a player's matches on the Tour circuit in a rolling window."""
    group_by = ["player_id"]
    tour_indicator = (pl.col("circuit") == "tour").cast(pl.Int64)
    if days is None:
        tour_count = (
            tour_indicator.cum_sum().shift(1).over(group_by, order_by="effective_match_date").fill_null(0)
        )
        total = cumulative_count(group_by=group_by)
    else:
        tour_count = (
            tour_indicator
            .rolling_sum_by(by="effective_match_date", window_size=f"{days}d", closed="left")
            .over(group_by)
            .fill_null(0)
        )
        total = rolling_count(days=days, group_by=group_by)
    return pl.when(total > 0).then(tour_count / total).otherwise(None)


@feature(
    name="tour_match_pct_diff",
    params=["days"],
    description="Player tour match pct minus opponent tour match pct",
    depends_on=["tour_match_pct"],
    mirror=False,
)
def tour_match_pct_diff(days: int | None = None) -> pl.Expr:
    """Tour match percentage difference between player and opponent."""
    if days is None:
        return pl.col("player_tour_match_pct") - pl.col("opp_tour_match_pct")
    return pl.col(f"player_tour_match_pct_{days}d") - pl.col(f"opp_tour_match_pct_{days}d")


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


# Match-level features (no player/opp prefix)


@feature(
    name="is_tour",
    params=[],
    description="1 if match is on ATP Tour, 0 otherwise",
    match_level=True,
)
def is_tour() -> pl.Expr:
    """Whether match is on ATP Tour circuit."""
    return (pl.col("circuit") == "tour").cast(pl.Float64)


@feature(
    name="is_chal",
    params=[],
    description="1 if match is on Challenger circuit, 0 otherwise",
    match_level=True,
)
def is_chal() -> pl.Expr:
    """Whether match is on Challenger circuit."""
    return (pl.col("circuit") == "chal").cast(pl.Float64)


@feature(
    name="is_itf",
    params=[],
    description="1 if match is on ITF circuit, 0 otherwise",
    match_level=True,
)
def is_itf() -> pl.Expr:
    """Whether match is on ITF circuit."""
    return (pl.col("circuit") == "itf").cast(pl.Float64)


@feature(
    name="is_hard",
    params=[],
    description="1 if match is on hard court, 0 otherwise",
    match_level=True,
)
def is_hard() -> pl.Expr:
    """Whether match is on hard court."""
    return (pl.col("surface") == "Hard").cast(pl.Float64)


@feature(
    name="is_clay",
    params=[],
    description="1 if match is on clay court, 0 otherwise",
    match_level=True,
)
def is_clay() -> pl.Expr:
    """Whether match is on clay court."""
    return (pl.col("surface") == "Clay").cast(pl.Float64)


@feature(
    name="is_grass",
    params=[],
    description="1 if match is on grass court, 0 otherwise",
    match_level=True,
)
def is_grass() -> pl.Expr:
    """Whether match is on grass court."""
    return (pl.col("surface") == "Grass").cast(pl.Float64)


@feature(
    name="is_indoor",
    params=[],
    description="1 if match is indoors, 0 otherwise",
    match_level=True,
)
def is_indoor() -> pl.Expr:
    """Whether match is played indoors."""
    return pl.col("indoor").cast(pl.Float64)


@feature(
    name="round_ordinal",
    params=[],
    description="Round as ordinal from round_order column (Q1=1 through F=12)",
    match_level=True,
)
def round_ordinal() -> pl.Expr:
    """Round encoded as ordinal progression. Uses existing round_order column."""
    return pl.col("round_order").cast(pl.Float64)


@feature(
    name="is_qualifying",
    params=[],
    description="1 if qualifying round (Q1/Q2/Q3), 0 otherwise",
    match_level=True,
)
def is_qualifying() -> pl.Expr:
    """Whether match is in qualifying rounds."""
    return pl.col("round").is_in(["Q1", "Q2", "Q3"]).cast(pl.Float64)
