"""Tournament-tier features.

Match-level features expose the tournament-tier signal that's in
matches.parquet but unused by other feature modules: tournament_level
(GS / 1000 / 500 / 250 / CH175 / CH125 / CH100 / CH75 / CH50 / FU) and
prize_money. Player-rolling variants capture recent exposure to high-tier
play, which can proxy for level of competition and motivation.
"""


import polars as pl

from mvp.model.primitives import rolling_count, rolling_mean, rolling_sum
from mvp.model.registry import feature, register_diff

# Tier ordinal map: FU = 0, GS = 9. Monotone in stakes / level.
_TIER_ORDINAL = {
    "FU": 0,
    "CH50": 1,
    "CH75": 2,
    "CH100": 3,
    "CH125": 4,
    "CH175": 5,
    "250": 6,
    "500": 7,
    "1000": 8,
    "GS": 9,
}


# --------------------------------------------------------------------------
# Per-level boolean indicators (match-level)
# --------------------------------------------------------------------------


@feature(
    name="is_grand_slam",
    params=[],
    description="1 if match is at a Grand Slam, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_grand_slam() -> pl.Expr:
    return (pl.col("tournament_level") == "GS").cast(pl.Float64)


@feature(
    name="is_atp_1000",
    params=[],
    description="1 if match is at an ATP Masters 1000, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_atp_1000() -> pl.Expr:
    return (pl.col("tournament_level") == "1000").cast(pl.Float64)


@feature(
    name="is_atp_500",
    params=[],
    description="1 if match is at an ATP 500, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_atp_500() -> pl.Expr:
    return (pl.col("tournament_level") == "500").cast(pl.Float64)


@feature(
    name="is_atp_250",
    params=[],
    description="1 if match is at an ATP 250, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_atp_250() -> pl.Expr:
    return (pl.col("tournament_level") == "250").cast(pl.Float64)


@feature(
    name="is_challenger_175",
    params=[],
    description="1 if match is at a Challenger 175, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_175() -> pl.Expr:
    return (pl.col("tournament_level") == "CH175").cast(pl.Float64)


@feature(
    name="is_challenger_125",
    params=[],
    description="1 if match is at a Challenger 125, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_125() -> pl.Expr:
    return (pl.col("tournament_level") == "CH125").cast(pl.Float64)


@feature(
    name="is_challenger_100",
    params=[],
    description="1 if match is at a Challenger 100, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_100() -> pl.Expr:
    return (pl.col("tournament_level") == "CH100").cast(pl.Float64)


@feature(
    name="is_challenger_75",
    params=[],
    description="1 if match is at a Challenger 75, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_75() -> pl.Expr:
    return (pl.col("tournament_level") == "CH75").cast(pl.Float64)


@feature(
    name="is_challenger_50",
    params=[],
    description="1 if match is at a Challenger 50, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_50() -> pl.Expr:
    return (pl.col("tournament_level") == "CH50").cast(pl.Float64)


@feature(
    name="is_futures",
    params=[],
    description="1 if match is at an ITF Futures event, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_futures() -> pl.Expr:
    return (pl.col("tournament_level") == "FU").cast(pl.Float64)


# --------------------------------------------------------------------------
# Bundled tier indicators (match-level)
# --------------------------------------------------------------------------


@feature(
    name="is_challenger_high",
    params=[],
    description="1 if match is at a high-tier Challenger (CH175/125/100), 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_high() -> pl.Expr:
    return pl.col("tournament_level").is_in(["CH175", "CH125", "CH100"]).cast(pl.Float64)


@feature(
    name="is_challenger_low",
    params=[],
    description="1 if match is at a low-tier Challenger (CH75/50), 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_low() -> pl.Expr:
    return pl.col("tournament_level").is_in(["CH75", "CH50"]).cast(pl.Float64)


@feature(
    name="is_challenger_any",
    params=[],
    description="1 if match is at any Challenger level, 0 otherwise",
    match_level=True,
    impute=0,
)
def is_challenger_any() -> pl.Expr:
    return (
        pl.col("tournament_level")
        .is_in(["CH175", "CH125", "CH100", "CH75", "CH50"])
        .cast(pl.Float64)
    )


# --------------------------------------------------------------------------
# Continuous tier signals (match-level)
# --------------------------------------------------------------------------


def _tier_ordinal_expr() -> pl.Expr:
    """Map tournament_level to ordinal (FU=0..GS=9); null/unknown -> null."""
    expr = pl.when(pl.col("tournament_level").is_null()).then(None)
    for level, ordinal in _TIER_ORDINAL.items():
        expr = expr.when(pl.col("tournament_level") == level).then(ordinal)
    return expr.otherwise(None).cast(pl.Float64)


@feature(
    name="tournament_tier_ordinal",
    params=[],
    description=(
        "Ordinal tournament tier: FU=0, CH50=1, CH75=2, CH100=3, CH125=4, "
        "CH175=5, 250=6, 500=7, 1000=8, GS=9"
    ),
    match_level=True,
    impute=None,
)
def tournament_tier_ordinal() -> pl.Expr:
    return _tier_ordinal_expr()


@feature(
    name="prize_money_log",
    params=[],
    description="log1p of prize_money; null prize -> null",
    match_level=True,
    impute=None,
)
def prize_money_log() -> pl.Expr:
    return (
        pl.when(pl.col("prize_money").is_null())
        .then(None)
        .otherwise(pl.col("prize_money").cast(pl.Float64).log1p())
    )


# --------------------------------------------------------------------------
# Player-rolling tier features
# --------------------------------------------------------------------------


@feature(
    name="high_tier_match_pct",
    params=["days"],
    description=(
        "Fraction of player's matches in the past N days played at "
        "high-tier events (GS / 1000 / 500). Null when no priors in window."
    ),
    mirror=True,
)
def high_tier_match_pct(days: int | None = None) -> pl.Expr:
    """Fraction of recent matches at GS/1000/500."""
    # High-tier indicator: ordinal >= 7 (500/1000/GS)
    high_indicator = (_tier_ordinal_expr() >= 7).cast(pl.Int64)
    group_by = ["player_id"]
    if days is None:
        # Cumulative variant: count all priors
        total = (
            pl.col("effective_match_date")
            .is_not_null()
            .cast(pl.Int64)
            .cum_sum()
            .shift(1)
            .over(group_by, order_by="effective_match_date")
            .fill_null(0)
        )
        high_count = (
            high_indicator
            .cum_sum()
            .shift(1)
            .over(group_by, order_by="effective_match_date")
            .fill_null(0)
        )
    else:
        total = rolling_count(days=days, group_by=group_by)
        high_count = rolling_sum(high_indicator, days=days, group_by=group_by)
    return pl.when(total > 0).then(high_count / total).otherwise(None)


register_diff("high_tier_match_pct")


@feature(
    name="tier_ordinal_avg",
    params=["days"],
    description=(
        "Average tournament_tier_ordinal across player's matches in past N days. "
        "Captures the level of competition recently played."
    ),
    mirror=True,
)
def tier_ordinal_avg(days: int | None = None) -> pl.Expr:
    """Avg tier ordinal across recent matches."""
    group_by = ["player_id"]
    if days is None:
        # Cumulative mean: sum / count
        ordinal_sum = (
            _tier_ordinal_expr()
            .fill_null(0)
            .cum_sum()
            .shift(1)
            .over(group_by, order_by="effective_match_date")
            .fill_null(0)
        )
        total = (
            pl.col("effective_match_date")
            .is_not_null()
            .cast(pl.Int64)
            .cum_sum()
            .shift(1)
            .over(group_by, order_by="effective_match_date")
            .fill_null(0)
        )
        return pl.when(total > 0).then(ordinal_sum / total).otherwise(None)
    return rolling_mean(_tier_ordinal_expr(), days=days, group_by=group_by)


register_diff("tier_ordinal_avg")


@feature(
    name="prize_money_log_avg",
    params=["days"],
    description=(
        "Average log1p(prize_money) across player's matches in past N days. "
        "Continuous variant of recent tier exposure."
    ),
    mirror=True,
)
def prize_money_log_avg(days: int | None = None) -> pl.Expr:
    """Avg log prize money across recent matches."""
    prize_log = (
        pl.when(pl.col("prize_money").is_null())
        .then(None)
        .otherwise(pl.col("prize_money").cast(pl.Float64).log1p())
    )
    group_by = ["player_id"]
    if days is None:
        prize_sum = (
            prize_log
            .fill_null(0)
            .cum_sum()
            .shift(1)
            .over(group_by, order_by="effective_match_date")
            .fill_null(0)
        )
        total = (
            pl.col("effective_match_date")
            .is_not_null()
            .cast(pl.Int64)
            .cum_sum()
            .shift(1)
            .over(group_by, order_by="effective_match_date")
            .fill_null(0)
        )
        return pl.when(total > 0).then(prize_sum / total).otherwise(None)
    return rolling_mean(prize_log, days=days, group_by=group_by)


register_diff("prize_money_log_avg")
