"""Playing style features.

Three layers of features derived from match_beats, stroke_analysis,
and rally_analysis data (2022+).

Layer 1: 365-day rolling raw style metrics (29 single + 29 diff + 15 matchup)
Layer 2: Bool style labels via population percentile thresholds (7)
Layer 3: Explicit matchup interaction terms (7)
"""


import polars as pl

from mvp.model.primitives import ratio_feature, rolling_max, rolling_mean
from mvp.model.registry import feature, register_diff, register_matchup

_DEFAULT_DAYS = 365
_GRP = "player_id"
_DATE = "effective_match_date"


def _rolling(expr: pl.Expr, days: int | None = None) -> pl.Expr:
    """Rolling mean of a per-match expression, partitioned by player."""
    d = days or _DEFAULT_DAYS
    return (
        expr.rolling_mean_by(by=_DATE, window_size=f"{d}d", closed="left")
        .over(_GRP)
    )


_THRESHOLD_WINDOW_DAYS = 730

# Type-axis tertile cut points. Each style axis is a single TYPE dimension split
# in thirds: top tertile = one archetype, bottom = the other, middle = neutral
# (no label fires). Mutually exclusive by construction. Labels stay null when the
# underlying rolling style metric is absent (no impute) so "unknown" != "neutral".
_LO_TERTILE = 1 / 3
_HI_TERTILE = 2 / 3


def _rolling_threshold(col: str, q: float) -> pl.Expr:
    """Time-aware threshold: rolling quantile of `col` over past 730d, excluding current row.

    Replaces population-wide `.quantile(q)` to avoid future-data leakage in label
    and matchup thresholds. Each row's threshold is computed from the prior 730d
    of the dataset's distribution of `col` (across all rows / all players).
    """
    return pl.col(col).rolling_quantile_by(
        by=_DATE,
        window_size=f"{_THRESHOLD_WINDOW_DAYS}d",
        closed="left",
        quantile=q,
    )


# =============================================================================
# Layer 1: Raw Style Metrics — Serve (match_beats)
# =============================================================================


@feature(
    name="style_avg_1st_serve_speed",
    params=["days"],
    description="Rolling mean of per-match avg 1st serve speed (km/h)",
    mirror=True,
)
def style_avg_1st_serve_speed(days: int | None = None) -> pl.Expr:
    return rolling_mean("mb_player_avg_1st_serve_speed", days=days or _DEFAULT_DAYS, group_by=_GRP)


@feature(
    name="style_max_1st_serve_speed",
    params=["days"],
    description="Rolling max of per-match max 1st serve speed (peak power)",
    mirror=True,
)
def style_max_1st_serve_speed(days: int | None = None) -> pl.Expr:
    return rolling_max("mb_player_max_1st_serve_speed", days=days or _DEFAULT_DAYS, group_by=_GRP)


@feature(
    name="style_avg_2nd_serve_speed",
    params=["days"],
    description="Rolling mean of per-match avg 2nd serve speed (km/h)",
    mirror=True,
)
def style_avg_2nd_serve_speed(days: int | None = None) -> pl.Expr:
    return rolling_mean("mb_player_avg_2nd_serve_speed", days=days or _DEFAULT_DAYS, group_by=_GRP)


@feature(
    name="style_max_2nd_serve_speed",
    params=["days"],
    description="Rolling max of per-match max 2nd serve speed",
    mirror=True,
)
def style_max_2nd_serve_speed(days: int | None = None) -> pl.Expr:
    return rolling_max("mb_player_max_2nd_serve_speed", days=days or _DEFAULT_DAYS, group_by=_GRP)


@feature(
    name="style_1st_serve_speed_variance",
    params=["days"],
    description="Rolling mean of within-match 1st serve speed std dev (tactical variety)",
    mirror=True,
)
def style_1st_serve_speed_variance(days: int | None = None) -> pl.Expr:
    return rolling_mean("mb_player_std_1st_serve_speed", days=days or _DEFAULT_DAYS, group_by=_GRP)


# =============================================================================
# Layer 1: Raw Style Metrics — Aggression & Errors (match_beats)
# =============================================================================


@feature(
    name="style_winner_rate",
    params=["days"],
    description="Rolling winners per point (offensive output)",
    mirror=True,
)
def style_winner_rate(days: int | None = None) -> pl.Expr:
    return _rolling(pl.col("mb_player_winners") / pl.col("total_points"), days)


@feature(
    name="style_ue_rate",
    params=["days"],
    description="Rolling unforced errors per point",
    mirror=True,
)
def style_ue_rate(days: int | None = None) -> pl.Expr:
    return _rolling(pl.col("mb_player_ues") / pl.col("total_points"), days)


@feature(
    name="style_winner_ue_ratio",
    params=["days"],
    description="Rolling winners/UEs ratio (aggression efficiency)",
    mirror=True,
)
def style_winner_ue_ratio(days: int | None = None) -> pl.Expr:
    per_match = (
        pl.when(pl.col("mb_player_ues") > 0)
        .then(pl.col("mb_player_winners") / pl.col("mb_player_ues"))
        .otherwise(None)
    )
    return _rolling(per_match, days)


@feature(
    name="style_forced_error_rate",
    params=["days"],
    description="Rolling opponent FEs per point (offensive pressure)",
    mirror=True,
)
def style_forced_error_rate(days: int | None = None) -> pl.Expr:
    return _rolling(pl.col("mb_opp_fes") / pl.col("total_points"), days)


# =============================================================================
# Layer 1: Raw Style Metrics — Service Game Quality (match_beats)
# =============================================================================


@feature(
    name="style_easy_hold_pct",
    params=["days"],
    description="Rolling easy holds / service games (serve dominance)",
    mirror=True,
    impute=None,
)
def style_easy_hold_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "mb_player_easy_holds", "mb_player_service_games", days=days or _DEFAULT_DAYS
    )


@feature(
    name="style_difficult_hold_pct",
    params=["days"],
    description="Rolling difficult holds / service games (under-pressure serving)",
    mirror=True,
)
def style_difficult_hold_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "mb_player_difficult_holds", "mb_player_service_games", days=days or _DEFAULT_DAYS
    )


@feature(
    name="style_crucial_pts_win_pct",
    params=["days"],
    description="Rolling crucial points won / played (big-point performance)",
    mirror=True,
)
def style_crucial_pts_win_pct(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "mb_player_crucial_points_won", "mb_player_crucial_points_played", days=days or _DEFAULT_DAYS
    )


# =============================================================================
# Layer 1: Raw Style Metrics — Rally Shape (match_beats)
# =============================================================================


@feature(
    name="style_rally_won_avg_length",
    params=["days"],
    description="Rolling avg rally length when winning (total shots in won rallies / count)",
    mirror=True,
)
def style_rally_won_avg_length(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "mb_player_rally_won_shots", "mb_player_rally_won_count", days=days or _DEFAULT_DAYS
    )


@feature(
    name="style_rally_lost_avg_length",
    params=["days"],
    description="Rolling avg rally length when losing",
    mirror=True,
)
def style_rally_lost_avg_length(days: int | None = None) -> pl.Expr:
    return ratio_feature(
        "mb_player_rally_lost_shots", "mb_player_rally_lost_count", days=days or _DEFAULT_DAYS
    )


# =============================================================================
# Layer 1: Raw Style Metrics — Wing Preference (stroke_analysis)
# =============================================================================


@feature(
    name="style_fh_winner_share",
    params=["days"],
    description="Rolling FH winners as share of FH+BH winners (offensive wing preference)",
    mirror=True,
)
def style_fh_winner_share(days: int | None = None) -> pl.Expr:
    total = pl.col("player_fh_winners") + pl.col("player_bh_winners")
    per_match = pl.when(total > 0).then(pl.col("player_fh_winners") / total).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_fh_ue_share",
    params=["days"],
    description="Rolling FH UEs as share of FH+BH UEs (error wing tendency)",
    mirror=True,
)
def style_fh_ue_share(days: int | None = None) -> pl.Expr:
    total = pl.col("player_fh_unforced_errors") + pl.col("player_bh_unforced_errors")
    per_match = pl.when(total > 0).then(pl.col("player_fh_unforced_errors") / total).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_fh_winner_rate",
    params=["days"],
    description="Rolling FH productivity (FH winners / total FH outcomes)",
    mirror=True,
)
def style_fh_winner_rate(days: int | None = None) -> pl.Expr:
    total = (
        pl.col("player_fh_winners")
        + pl.col("player_fh_forced_errors")
        + pl.col("player_fh_unforced_errors")
    )
    per_match = pl.when(total > 0).then(pl.col("player_fh_winners") / total).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_bh_winner_rate",
    params=["days"],
    description="Rolling BH productivity (BH winners / total BH outcomes)",
    mirror=True,
)
def style_bh_winner_rate(days: int | None = None) -> pl.Expr:
    total = (
        pl.col("player_bh_winners")
        + pl.col("player_bh_forced_errors")
        + pl.col("player_bh_unforced_errors")
    )
    per_match = pl.when(total > 0).then(pl.col("player_bh_winners") / total).otherwise(None)
    return _rolling(per_match, days)


# =============================================================================
# Layer 1: Raw Style Metrics — Rally Ball-Striking (stroke_analysis)
# =============================================================================


def _shot_type_total(prefix: str) -> pl.Expr:
    """Total outcomes for a shot type: winners + FE + UE + others."""
    return (
        pl.col(f"player_{prefix}_winners")
        + pl.col(f"player_{prefix}_forced_errors")
        + pl.col(f"player_{prefix}_unforced_errors")
        + pl.col(f"player_{prefix}_others")
    )


@feature(
    name="style_ground_stroke_winner_rate",
    params=["days"],
    description="Rolling ground stroke winners / total ground strokes (rally offense)",
    mirror=True,
)
def style_ground_stroke_winner_rate(days: int | None = None) -> pl.Expr:
    total = _shot_type_total("ground_stroke")
    per_match = pl.when(total > 0).then(pl.col("player_ground_stroke_winners") / total).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_ground_stroke_ue_rate",
    params=["days"],
    description="Rolling ground stroke UEs / total ground strokes (rally error tendency)",
    mirror=True,
)
def style_ground_stroke_ue_rate(days: int | None = None) -> pl.Expr:
    total = _shot_type_total("ground_stroke")
    per_match = pl.when(total > 0).then(pl.col("player_ground_stroke_unforced_errors") / total).otherwise(None)
    return _rolling(per_match, days)


# =============================================================================
# Layer 1: Raw Style Metrics — Shot Variety (stroke_analysis)
# =============================================================================


@feature(
    name="style_net_approach_frequency",
    params=["days"],
    description="Rolling net play shots (volley+approach+overhead) per point",
    mirror=True,
)
def style_net_approach_frequency(days: int | None = None) -> pl.Expr:
    net_total = _shot_type_total("volley") + _shot_type_total("approach") + _shot_type_total("overhead")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(net_total / pts).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_drop_shot_frequency",
    params=["days"],
    description="Rolling drop shots per point (craft/variety)",
    mirror=True,
)
def style_drop_shot_frequency(days: int | None = None) -> pl.Expr:
    total = _shot_type_total("drop_shot")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(total / pts).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_drop_shot_effectiveness",
    params=["days"],
    description="Rolling drop shot winners / total drop shots",
    mirror=True,
)
def style_drop_shot_effectiveness(days: int | None = None) -> pl.Expr:
    total = _shot_type_total("drop_shot")
    per_match = pl.when(total > 0).then(pl.col("player_drop_shot_winners") / total).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_passing_frequency",
    params=["days"],
    description="Rolling passing shots per point",
    mirror=True,
)
def style_passing_frequency(days: int | None = None) -> pl.Expr:
    total = _shot_type_total("passing")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(total / pts).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_lob_frequency",
    params=["days"],
    description="Rolling lob shots per point (defensive variety)",
    mirror=True,
)
def style_lob_frequency(days: int | None = None) -> pl.Expr:
    total = _shot_type_total("lob")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(total / pts).otherwise(None)
    return _rolling(per_match, days)


# =============================================================================
# Layer 1: Raw Style Metrics — Rally Length (rally_analysis)
# =============================================================================


@feature(
    name="style_short_rally_pct",
    params=["days"],
    description="Rolling short rallies as share of total rallies (serve-dominated play)",
    mirror=True,
)
def style_short_rally_pct(days: int | None = None) -> pl.Expr:
    rpwd = pl.col("rally_points_with_data")
    per_match = pl.when(rpwd > 0).then(pl.col("rally_short_count") / rpwd).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_long_rally_pct",
    params=["days"],
    description="Rolling long rallies as share of total rallies (grinder tendency)",
    mirror=True,
)
def style_long_rally_pct(days: int | None = None) -> pl.Expr:
    rpwd = pl.col("rally_points_with_data")
    per_match = pl.when(rpwd > 0).then(pl.col("rally_long_count") / rpwd).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_short_rally_win_pct",
    params=["days"],
    description="Rolling short rally points won / total short rallies (quick-point efficiency)",
    mirror=True,
)
def style_short_rally_win_pct(days: int | None = None) -> pl.Expr:
    total = pl.col("player_short_won") + pl.col("player_short_err")
    per_match = pl.when(total > 0).then(pl.col("player_short_won") / total).otherwise(None)
    return _rolling(per_match, days)


@feature(
    name="style_long_rally_win_pct",
    params=["days"],
    description="Rolling long rally points won / total long rallies (endurance/consistency)",
    mirror=True,
)
def style_long_rally_win_pct(days: int | None = None) -> pl.Expr:
    total = pl.col("player_long_won") + pl.col("player_long_err")
    per_match = pl.when(total > 0).then(pl.col("player_long_won") / total).otherwise(None)
    return _rolling(per_match, days)


# =============================================================================
# Layer 1: Diff Features (player - opponent, same stat)
# =============================================================================

_STYLE_SINGLE_FEATURES = [
    "style_avg_1st_serve_speed",
    "style_max_1st_serve_speed",
    "style_avg_2nd_serve_speed",
    "style_max_2nd_serve_speed",
    "style_1st_serve_speed_variance",
    "style_winner_rate",
    "style_ue_rate",
    "style_winner_ue_ratio",
    "style_forced_error_rate",
    "style_easy_hold_pct",
    "style_difficult_hold_pct",
    "style_crucial_pts_win_pct",
    "style_rally_won_avg_length",
    "style_rally_lost_avg_length",
    "style_fh_winner_share",
    "style_fh_ue_share",
    "style_fh_winner_rate",
    "style_bh_winner_rate",
    "style_ground_stroke_winner_rate",
    "style_ground_stroke_ue_rate",
    "style_net_approach_frequency",
    "style_drop_shot_frequency",
    "style_drop_shot_effectiveness",
    "style_passing_frequency",
    "style_lob_frequency",
    "style_short_rally_pct",
    "style_long_rally_pct",
    "style_short_rally_win_pct",
    "style_long_rally_win_pct",
]


for _base in _STYLE_SINGLE_FEATURES:
    register_diff(_base)


# =============================================================================
# Layer 1: Matchup Features (cross-domain player X vs opponent Y)
# =============================================================================

# (matchup_name, player_col, opp_col, dep1, dep2, description)
_STYLE_MATCHUP_PAIRS = [
    ("style_winner_rate_matchup",
     "player_style_winner_rate", "opp_style_forced_error_rate",
     "style_winner_rate", "style_forced_error_rate",
     "Player offensive output vs opponent offensive pressure"),
    ("style_fh_winner_rate_matchup",
     "player_style_fh_winner_rate", "opp_style_bh_winner_rate",
     "style_fh_winner_rate", "style_bh_winner_rate",
     "Player FH strength vs opponent BH quality"),
    ("style_bh_winner_rate_matchup",
     "player_style_bh_winner_rate", "opp_style_fh_winner_rate",
     "style_bh_winner_rate", "style_fh_winner_rate",
     "Player BH strength vs opponent FH quality"),
    ("style_net_approach_frequency_matchup",
     "player_style_net_approach_frequency", "opp_style_passing_frequency",
     "style_net_approach_frequency", "style_passing_frequency",
     "Net rusher vs passer"),
    ("style_lob_frequency_matchup",
     "player_style_lob_frequency", "opp_style_net_approach_frequency",
     "style_lob_frequency", "style_net_approach_frequency",
     "Lobber vs net rusher"),
    ("style_short_rally_pct_matchup",
     "player_style_short_rally_pct", "opp_style_long_rally_pct",
     "style_short_rally_pct", "style_long_rally_pct",
     "Quick play preference vs grind preference"),
    ("style_long_rally_pct_matchup",
     "player_style_long_rally_pct", "opp_style_short_rally_pct",
     "style_long_rally_pct", "style_short_rally_pct",
     "Grind preference vs quick play preference"),
    ("style_short_rally_win_pct_matchup",
     "player_style_short_rally_win_pct", "opp_style_long_rally_win_pct",
     "style_short_rally_win_pct", "style_long_rally_win_pct",
     "Quick-point efficiency vs endurance"),
    ("style_long_rally_win_pct_matchup",
     "player_style_long_rally_win_pct", "opp_style_short_rally_win_pct",
     "style_long_rally_win_pct", "style_short_rally_win_pct",
     "Endurance vs quick-point efficiency"),
    ("style_ground_stroke_winner_rate_matchup",
     "player_style_ground_stroke_winner_rate", "opp_style_ground_stroke_ue_rate",
     "style_ground_stroke_winner_rate", "style_ground_stroke_ue_rate",
     "Rally offense vs opponent rally errors"),
    ("style_easy_hold_pct_matchup",
     "player_style_easy_hold_pct", "opp_ret_bp_convert_pct",
     "style_easy_hold_pct", "ret_bp_convert_pct",
     "Serve dominance vs opponent break point conversion"),
    ("style_difficult_hold_pct_matchup",
     "player_style_difficult_hold_pct", "opp_style_forced_error_rate",
     "style_difficult_hold_pct", "style_forced_error_rate",
     "Under-pressure serving vs opponent offensive pressure"),
    ("style_rally_won_avg_length_matchup",
     "player_style_rally_won_avg_length", "opp_style_rally_lost_avg_length",
     "style_rally_won_avg_length", "style_rally_lost_avg_length",
     "Player winning rally length vs opponent losing rally length"),
    ("style_ue_rate_matchup",
     "player_style_ue_rate", "opp_style_winner_rate",
     "style_ue_rate", "style_winner_rate",
     "Player error tendency vs opponent attack output"),
    ("style_drop_shot_effectiveness_matchup",
     "player_style_drop_shot_effectiveness", "opp_style_net_approach_frequency",
     "style_drop_shot_effectiveness", "style_net_approach_frequency",
     "Drop shot craft vs opponent net rushing"),
]


for _m in _STYLE_MATCHUP_PAIRS:
    register_matchup(*_m)


# =============================================================================
# Layer 2: Bool Style Labels (population percentile thresholds)
# =============================================================================


@feature(
    name="is_power_server",
    params=[],
    impute=None,
    description="Top-tertile rolling 1st-serve speed (power serve type)",
    depends_on=["style_avg_1st_serve_speed"],
    mirror=True,
)
def is_power_server() -> pl.Expr:
    hi = _rolling_threshold("player_style_avg_1st_serve_speed", _HI_TERTILE)
    return (pl.col("player_style_avg_1st_serve_speed") >= hi).cast(pl.Int8)


@feature(
    name="is_placement_server",
    params=[],
    impute=None,
    description="Bottom-tertile rolling 1st-serve speed (placement serve type)",
    depends_on=["style_avg_1st_serve_speed"],
    mirror=True,
)
def is_placement_server() -> pl.Expr:
    lo = _rolling_threshold("player_style_avg_1st_serve_speed", _LO_TERTILE)
    return (pl.col("player_style_avg_1st_serve_speed") <= lo).cast(pl.Int8)


@feature(
    name="is_counterpuncher",
    params=[],
    impute=None,
    description="Bottom-tertile rolling winner rate (defensive rally type)",
    depends_on=["style_winner_rate"],
    mirror=True,
)
def is_counterpuncher() -> pl.Expr:
    lo = _rolling_threshold("player_style_winner_rate", _LO_TERTILE)
    return (pl.col("player_style_winner_rate") <= lo).cast(pl.Int8)


@feature(
    name="is_aggressive_baseliner",
    params=[],
    impute=None,
    description="Top-tertile rolling winner rate (aggressive rally type)",
    depends_on=["style_winner_rate"],
    mirror=True,
)
def is_aggressive_baseliner() -> pl.Expr:
    hi = _rolling_threshold("player_style_winner_rate", _HI_TERTILE)
    return (pl.col("player_style_winner_rate") >= hi).cast(pl.Int8)


@feature(
    name="is_net_rusher",
    params=[],
    impute=None,
    description="Comes forward frequently (above 75th percentile)",
    depends_on=["style_net_approach_frequency"],
    mirror=True,
)
def is_net_rusher() -> pl.Expr:
    p75 = _rolling_threshold("player_style_net_approach_frequency", 0.75)
    return (pl.col("player_style_net_approach_frequency") > p75).cast(pl.Int8)


@feature(
    name="is_clay_specialist",
    params=[],
    impute=None,
    description="Strong clay Elo adjustment (existing elo_clay_specialist > threshold)",
    depends_on=["elo_clay_specialist"],
    mirror=True,
)
def is_clay_specialist() -> pl.Expr:
    p75 = _rolling_threshold("player_elo_clay_specialist", 0.75)
    return (pl.col("player_elo_clay_specialist") > p75).cast(pl.Int8)


@feature(
    name="is_hard_specialist",
    params=[],
    impute=None,
    description="Strong hard Elo adjustment (existing elo_hard_specialist > threshold)",
    depends_on=["elo_hard_specialist"],
    mirror=True,
)
def is_hard_specialist() -> pl.Expr:
    p75 = _rolling_threshold("player_elo_hard_specialist", 0.75)
    return (pl.col("player_elo_hard_specialist") > p75).cast(pl.Int8)


@feature(
    name="is_clutch_player",
    params=[],
    impute=None,
    description="High BP save and BP convert rates (above median both)",
    depends_on=["svc_bp_save_pct", "ret_bp_convert_pct"],
    mirror=True,
)
def is_clutch_player() -> pl.Expr:
    bp_save_p50 = _rolling_threshold("player_svc_bp_save_pct", 0.50)
    bp_conv_p50 = _rolling_threshold("player_ret_bp_convert_pct", 0.50)
    return (
        (pl.col("player_svc_bp_save_pct") > bp_save_p50)
        & (pl.col("player_ret_bp_convert_pct") > bp_conv_p50)
    ).cast(pl.Int8)


# =============================================================================
# Layer 3: Matchup Interactions (bool combinations)
# =============================================================================


@feature(
    name="matchup_power_serve_vs_strong_return",
    params=[],
    impute=None,
    description="Player is power server AND opponent has strong return",
    depends_on=["is_power_server", "ret_first_serve_win_pct"],
    mirror=True,
)
def matchup_power_serve_vs_strong_return() -> pl.Expr:
    ret_p75 = _rolling_threshold("opp_ret_first_serve_win_pct", 0.75)
    return (
        (pl.col("player_is_power_server") == 1)
        & (pl.col("opp_ret_first_serve_win_pct") > ret_p75)
    ).cast(pl.Int8)


@feature(
    name="matchup_placement_serve_vs_strong_return",
    params=[],
    impute=None,
    description="Player is placement server AND opponent has strong return",
    depends_on=["is_placement_server", "ret_first_serve_win_pct"],
    mirror=True,
)
def matchup_placement_serve_vs_strong_return() -> pl.Expr:
    ret_p75 = _rolling_threshold("opp_ret_first_serve_win_pct", 0.75)
    return (
        (pl.col("player_is_placement_server") == 1)
        & (pl.col("opp_ret_first_serve_win_pct") > ret_p75)
    ).cast(pl.Int8)


@feature(
    name="matchup_aggressor_vs_counterpuncher",
    params=[],
    impute=None,
    description="Player is aggressive baseliner AND opponent is counterpuncher",
    depends_on=["is_aggressive_baseliner", "is_counterpuncher"],
    mirror=True,
)
def matchup_aggressor_vs_counterpuncher() -> pl.Expr:
    return (
        (pl.col("player_is_aggressive_baseliner") == 1)
        & (pl.col("opp_is_counterpuncher") == 1)
    ).cast(pl.Int8)


@feature(
    name="matchup_counterpuncher_vs_aggressor",
    params=[],
    impute=None,
    description="Player is counterpuncher AND opponent is aggressive baseliner",
    depends_on=["is_counterpuncher", "is_aggressive_baseliner"],
    mirror=True,
)
def matchup_counterpuncher_vs_aggressor() -> pl.Expr:
    return (
        (pl.col("player_is_counterpuncher") == 1)
        & (pl.col("opp_is_aggressive_baseliner") == 1)
    ).cast(pl.Int8)


@feature(
    name="matchup_both_power_servers",
    params=[],
    impute=None,
    description="Both players are power servers",
    depends_on=["is_power_server"],
    mirror=False,
    match_level=True,
)
def matchup_both_power_servers() -> pl.Expr:
    return (
        (pl.col("player_is_power_server") == 1)
        & (pl.col("opp_is_power_server") == 1)
    ).cast(pl.Int8)


@feature(
    name="matchup_both_counterpunchers",
    params=[],
    impute=None,
    description="Both players are counterpunchers",
    depends_on=["is_counterpuncher"],
    mirror=False,
    match_level=True,
)
def matchup_both_counterpunchers() -> pl.Expr:
    return (
        (pl.col("player_is_counterpuncher") == 1)
        & (pl.col("opp_is_counterpuncher") == 1)
    ).cast(pl.Int8)


@feature(
    name="matchup_net_rusher_vs_passer",
    params=[],
    impute=None,
    description="Player is net rusher AND opponent has high passing frequency",
    depends_on=["is_net_rusher", "style_passing_frequency"],
    mirror=True,
)
def matchup_net_rusher_vs_passer() -> pl.Expr:
    pass_p75 = _rolling_threshold("opp_style_passing_frequency", 0.75)
    return (
        (pl.col("player_is_net_rusher") == 1)
        & (pl.col("opp_style_passing_frequency") > pass_p75)
    ).cast(pl.Int8)
