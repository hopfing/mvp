"""Tournament-tier features.

Match-level features expose the tournament-tier signal that's in
matches.parquet but unused by other feature modules: tournament_level
(GS / 1000 / 500 / 250 / CH175 / CH125 / CH100 / CH75 / CH50 / FU) and
prize_money. Player-rolling variants capture recent exposure to high-tier
play, which can proxy for level of competition and motivation.
"""


import polars as pl

from mvp.model.features.elo import indoor_adj_expr, surface_elo_expr
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
    impute=None,
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
            .over(group_by, order_by=["effective_match_date", "tournament_start_date", "round_order", "match_uid"])
            .fill_null(0)
        )
        high_count = (
            high_indicator
            .cum_sum()
            .shift(1)
            .over(group_by, order_by=["effective_match_date", "tournament_start_date", "round_order", "match_uid"])
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
    impute=None,
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
            .over(group_by, order_by=["effective_match_date", "tournament_start_date", "round_order", "match_uid"])
            .fill_null(0)
        )
        total = (
            pl.col("effective_match_date")
            .is_not_null()
            .cast(pl.Int64)
            .cum_sum()
            .shift(1)
            .over(group_by, order_by=["effective_match_date", "tournament_start_date", "round_order", "match_uid"])
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
    impute=None,
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
            .over(group_by, order_by=["effective_match_date", "tournament_start_date", "round_order", "match_uid"])
            .fill_null(0)
        )
        total = (
            pl.col("effective_match_date")
            .is_not_null()
            .cast(pl.Int64)
            .cum_sum()
            .shift(1)
            .over(group_by, order_by=["effective_match_date", "tournament_start_date", "round_order", "match_uid"])
            .fill_null(0)
        )
        return pl.when(total > 0).then(prize_sum / total).otherwise(None)
    return rolling_mean(prize_log, days=days, group_by=group_by)


register_diff("prize_money_log_avg")


# --------------------------------------------------------------------------
# Level-gap features: current match tier vs the player's recent typical tier.
# Positive = playing up (bigger event than usual), negative = playing down.
#
# Kept as a mirror pair (player_/opp_), NOT a diff: the diff cancels the current
# tier — (T - A_player) - (T - A_opp) = A_opp - A_player — discarding the very
# context (this event's level relative to each player's norm) that is the point.
# --------------------------------------------------------------------------

# Winner ranking points per tier — the tiers' own definitional, non-uniform
# spacing (CH175 = 175 pts, 1000 = 1000, GS = 2000). A strength-of-level scale
# that needs no live field. FU (ITF) anchored just below CH50.
_TIER_POINTS = {
    "FU": 30,
    "CH50": 50,
    "CH75": 75,
    "CH100": 100,
    "CH125": 125,
    "CH175": 175,
    "250": 250,
    "500": 500,
    "1000": 1000,
    "GS": 2000,
}


def _tier_points_expr() -> pl.Expr:
    """Map tournament_level to winner ranking points; null/unknown -> null."""
    expr = pl.when(pl.col("tournament_level").is_null()).then(None)
    for level, pts in _TIER_POINTS.items():
        expr = expr.when(pl.col("tournament_level") == level).then(pts)
    return expr.otherwise(None).cast(pl.Float64)


def _trailing_mean(value_expr: pl.Expr, days: int | None) -> pl.Expr:
    """Per-player trailing mean of value_expr over the past N days (None = all
    priors), leakage-safe (shifted to exclude the current match)."""
    group_by = ["player_id"]
    order_by = [
        "effective_match_date", "tournament_start_date", "round_order", "match_uid",
    ]
    if days is None:
        value_sum = (
            value_expr.fill_null(0).cum_sum().shift(1)
            .over(group_by, order_by=order_by).fill_null(0)
        )
        total = (
            pl.col("effective_match_date").is_not_null().cast(pl.Int64)
            .cum_sum().shift(1).over(group_by, order_by=order_by).fill_null(0)
        )
        return pl.when(total > 0).then(value_sum / total).otherwise(None)
    return rolling_mean(value_expr, days=days, group_by=group_by)


@feature(
    name="level_gap",
    params=["days"],
    description=(
        "Ordinal tier gap: current match tier ordinal minus the player's "
        "trailing average tier ordinal over the past N days. Positive = playing "
        "up, negative = down. Null when no priors in window."
    ),
    mirror=True,
    impute=None,
)
def level_gap(days: int | None = None) -> pl.Expr:
    """Current tier ordinal minus the player's recent typical tier ordinal."""
    return _tier_ordinal_expr() - _trailing_mean(_tier_ordinal_expr(), days)


@feature(
    name="level_gap_pts",
    params=["days"],
    description=(
        "Points-scaled tier gap: current match winner-points minus the player's "
        "trailing average winner-points over the past N days. Non-uniform tier "
        "spacing (CH175=175 .. GS=2000). Null when no priors in window."
    ),
    mirror=True,
    impute=None,
)
def level_gap_pts(days: int | None = None) -> pl.Expr:
    """Current tier points minus the player's recent typical tier points."""
    return _tier_points_expr() - _trailing_mean(_tier_points_expr(), days)


# --------------------------------------------------------------------------
# Level-gap combinations: the joint (player_gap, opp_gap) configuration, built
# on the mirrored level_gap / level_gap_pts so they combine both sides. All
# windowed; FS decides which (scale, window, combo) carry.
# --------------------------------------------------------------------------


def _register_gap_combo(base, suffix, combine, match_level, mirror, description):
    """Register a windowed feature combining the player/opp sides of `base`."""

    @feature(
        name=f"{base}_{suffix}",
        params=["days"],
        depends_on=[base],
        mirror=mirror,
        match_level=match_level,
        impute=None,
        description=description,
    )
    def _combo(days: int | None = None, _b: str = base, _c=combine) -> pl.Expr:
        s = "" if days is None else f"_{days}d"
        return _c(pl.col(f"player_{_b}{s}"), pl.col(f"opp_{_b}{s}"))


# (suffix, combine(player, opp), match_level, mirror, description)
_GAP_COMBOS = [
    ("disp", lambda p, o: p.abs() + o.abs(), True, False,
     "Total tier displacement |player_gap| + |opp_gap| (how out-of-position the match is)"),
    ("asym", lambda p, o: p.abs() - o.abs(), False, False,
     "Displacement asymmetry |player_gap| - |opp_gap| (who is more out of position)"),
    ("prod", lambda p, o: p * o, True, False,
     "Gap product player_gap * opp_gap (>0 same direction, <0 opposite)"),
    ("minabs", lambda p, o: pl.min_horizontal(p.abs(), o.abs()), True, False,
     "Both-out intensity min(|player_gap|, |opp_gap|)"),
]

for _base in ("level_gap", "level_gap_pts"):
    for _sfx, _combine, _ml, _mir, _desc in _GAP_COMBOS:
        _register_gap_combo(_base, _sfx, _combine, _ml, _mir, _desc)


@feature(
    name="elo_surface_diff_x_level_disp",
    params=["days"],
    depends_on=["level_gap"],
    mirror=False,
    match_level=True,
    impute=None,
    description=(
        "elo_surface_diff modulated by total tier displacement — lets the model "
        "trust the rating diff less/more when players are out of position."
    ),
)
def elo_surface_diff_x_level_disp(days: int | None = None) -> pl.Expr:
    """Surface-Elo diff times total tier displacement."""
    s = "" if days is None else f"_{days}d"
    disp = pl.col(f"player_level_gap{s}").abs() + pl.col(f"opp_level_gap{s}").abs()
    elo_diff = surface_elo_expr("player") - surface_elo_expr("opp")
    return elo_diff * disp


@feature(
    name="elo_surface_indoor_diff_x_level_disp",
    params=["days"],
    depends_on=["level_gap"],
    mirror=False,
    match_level=True,
    impute=None,
    description=(
        "elo_surface_indoor_diff modulated by total tier displacement — the "
        "indoor-adjusted rating trusted less/more when players are out of position."
    ),
)
def elo_surface_indoor_diff_x_level_disp(days: int | None = None) -> pl.Expr:
    """Surface+indoor Elo diff times total tier displacement."""
    s = "" if days is None else f"_{days}d"
    disp = pl.col(f"player_level_gap{s}").abs() + pl.col(f"opp_level_gap{s}").abs()
    elo_diff = (
        (surface_elo_expr("player") + indoor_adj_expr("player"))
        - (surface_elo_expr("opp") + indoor_adj_expr("opp"))
    )
    return elo_diff * disp
