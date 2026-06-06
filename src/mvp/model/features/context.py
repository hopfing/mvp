"""Match context features (indoor, circuit, event type, seeding)."""


import polars as pl

from mvp.model.primitives import (
    cumulative_count,
    cumulative_sum,
    rolling_count,
    rolling_sum,
)
from mvp.model.registry import feature, register_diff


@feature(
    name="venue_win_pct",
    params=["days"],
    description="Win percentage on current venue type (indoor/outdoor)",
    mirror=True,
    impute=None,
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


register_diff("venue_win_pct")


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


register_diff("circuit_win_pct")


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


register_diff("tour_match_pct")


@feature(
    name="chal_match_pct",
    params=["days"],
    description="Fraction of recent matches played on Challenger circuit",
    mirror=True,
)
def chal_match_pct(days: int | None = None) -> pl.Expr:
    """Fraction of a player's matches on the Challenger circuit in a rolling window."""
    group_by = ["player_id"]
    chal_indicator = (pl.col("circuit") == "chal").cast(pl.Int64)
    if days is None:
        chal_count = (
            chal_indicator.cum_sum().shift(1).over(group_by, order_by="effective_match_date").fill_null(0)
        )
        total = cumulative_count(group_by=group_by)
    else:
        chal_count = (
            chal_indicator
            .rolling_sum_by(by="effective_match_date", window_size=f"{days}d", closed="left")
            .over(group_by)
            .fill_null(0)
        )
        total = rolling_count(days=days, group_by=group_by)
    return pl.when(total > 0).then(chal_count / total).otherwise(None)


register_diff("chal_match_pct")


@feature(
    name="itf_match_pct",
    params=["days"],
    description="Fraction of recent matches played on ITF circuit",
    mirror=True,
)
def itf_match_pct(days: int | None = None) -> pl.Expr:
    """Fraction of a player's matches on the ITF circuit in a rolling window."""
    group_by = ["player_id"]
    itf_indicator = (pl.col("circuit") == "itf").cast(pl.Int64)
    if days is None:
        itf_count = (
            itf_indicator.cum_sum().shift(1).over(group_by, order_by="effective_match_date").fill_null(0)
        )
        total = cumulative_count(group_by=group_by)
    else:
        itf_count = (
            itf_indicator
            .rolling_sum_by(by="effective_match_date", window_size=f"{days}d", closed="left")
            .over(group_by)
            .fill_null(0)
        )
        total = rolling_count(days=days, group_by=group_by)
    return pl.when(total > 0).then(itf_count / total).otherwise(None)


register_diff("itf_match_pct")


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
    impute=None,
)
def seed_diff() -> pl.Expr:
    """Seed difference (negative = player has better seed)."""
    return pl.col("player_seed") - pl.col("opp_seed")


@feature(
    name="both_seeded",
    params=[],
    description="1 if both players seeded, 0 otherwise",
    mirror=False,
    impute=0,
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
    impute=0,
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
    name="best_of",
    params=[],
    description="Best-of format (3 or 5)",
    match_level=True,
)
def best_of() -> pl.Expr:
    """Best-of format for the match."""
    return pl.col("best_of").fill_null(3).cast(pl.Float64)


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
    impute=None,
)
def round_ordinal() -> pl.Expr:
    """Round encoded as ordinal progression. Uses existing round_order column."""
    return pl.col("round_order").cast(pl.Float64)


@feature(
    name="tournament_round_ordinal",
    params=[],
    description="Signed round position within the draw: opener=+1 upward, "
    "qualifying negative toward the main draw (R32 is +1 in a 32-draw, +2 in a 64-draw)",
    match_level=True,
)
def tournament_round_ordinal() -> pl.Expr:
    """Round position relative to the draw opener (precomputed in matches.parquet)."""
    return pl.col("tournament_round_ordinal").cast(pl.Float64)


@feature(
    name="is_draw_opener",
    params=[],
    description="1 if the match is the first main-draw round of its tournament",
    match_level=True,
)
def is_draw_opener() -> pl.Expr:
    """Whether this round is the draw opener (tournament_round_ordinal == 1)."""
    return (pl.col("tournament_round_ordinal") == 1).cast(pl.Float64)


@feature(
    name="is_qualifying",
    params=[],
    description="1 if qualifying round (Q1/Q2/Q3), 0 otherwise",
    match_level=True,
)
def is_qualifying() -> pl.Expr:
    """Whether match is in qualifying rounds."""
    return pl.col("round").is_in(["Q1", "Q2", "Q3"]).cast(pl.Float64)


@feature(
    name="match_period",
    params=[],
    description="Fractional year of match date (e.g. 2024.25 = April 2024)",
    match_level=True,
    impute=None,
)
def match_period() -> pl.Expr:
    """Match date as fractional year at monthly resolution."""
    dt = pl.col("effective_match_date")
    return dt.dt.year().cast(pl.Float64) + (dt.dt.month().cast(pl.Float64) - 1) / 12


@feature(
    name="match_season",
    params=[],
    description="Month of year as fraction (0.0 = Jan, 0.917 = Dec) — seasonal cycle",
    match_level=True,
    impute=None,
)
def match_season() -> pl.Expr:
    """Month of year as fraction, capturing seasonal patterns."""
    return (pl.col("effective_match_date").dt.month().cast(pl.Float64) - 1) / 12


@feature(
    name="match_period_qtr",
    params=[],
    description="Fractional year at quarterly resolution (e.g. 2024.0, 2024.25)",
    match_level=True,
    impute=None,
)
def match_period_qtr() -> pl.Expr:
    """Match date as fractional year at quarterly resolution."""
    dt = pl.col("effective_match_date")
    return dt.dt.year().cast(pl.Float64) + ((dt.dt.month() - 1) // 3).cast(pl.Float64) / 4


@feature(
    name="match_season_qtr",
    params=[],
    description="Quarter of year as fraction (0.0, 0.25, 0.5, 0.75) — seasonal cycle",
    match_level=True,
    impute=None,
)
def match_season_qtr() -> pl.Expr:
    """Quarter of year as fraction, capturing seasonal patterns."""
    return ((pl.col("effective_match_date").dt.month() - 1) // 3).cast(pl.Float64) / 4
