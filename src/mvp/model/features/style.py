"""Playing style features.

Three layers of features derived from match_beats, stroke_analysis,
and rally_analysis data (2022+).

Layer 1: 365-day rolling raw style metrics (29 single + 29 diff + ~15 matchup)
Layer 2: Bool style labels via population percentile thresholds (7)
Layer 3: Explicit matchup interaction terms (7)
"""

from __future__ import annotations

import polars as pl

from mvp.model.primitives import ratio_feature, rolling_max, rolling_mean
from mvp.model.registry import feature

_DAYS = 365
_GRP = "player_id"
_DATE = "effective_match_date"


def _rolling_365(expr: pl.Expr) -> pl.Expr:
    """365-day rolling mean of a per-match expression, partitioned by player."""
    return (
        expr.rolling_mean_by(by=_DATE, window_size=f"{_DAYS}d", closed="left")
        .over(_GRP)
    )


# =============================================================================
# Layer 1: Raw Style Metrics — Serve (match_beats)
# =============================================================================


@feature(
    name="style_avg_1st_serve_speed",
    params=[],
    description="365d rolling mean of per-match avg 1st serve speed (km/h)",
    mirror=True,
)
def style_avg_1st_serve_speed() -> pl.Expr:
    return rolling_mean("mb_player_avg_1st_serve_speed", days=_DAYS, group_by=_GRP)


@feature(
    name="style_max_1st_serve_speed",
    params=[],
    description="365d rolling max of per-match max 1st serve speed (peak power)",
    mirror=True,
)
def style_max_1st_serve_speed() -> pl.Expr:
    return rolling_max("mb_player_max_1st_serve_speed", days=_DAYS, group_by=_GRP)


@feature(
    name="style_avg_2nd_serve_speed",
    params=[],
    description="365d rolling mean of per-match avg 2nd serve speed (km/h)",
    mirror=True,
)
def style_avg_2nd_serve_speed() -> pl.Expr:
    return rolling_mean("mb_player_avg_2nd_serve_speed", days=_DAYS, group_by=_GRP)


@feature(
    name="style_max_2nd_serve_speed",
    params=[],
    description="365d rolling max of per-match max 2nd serve speed",
    mirror=True,
)
def style_max_2nd_serve_speed() -> pl.Expr:
    return rolling_max("mb_player_max_2nd_serve_speed", days=_DAYS, group_by=_GRP)


@feature(
    name="style_1st_serve_speed_variance",
    params=[],
    description="365d rolling mean of within-match 1st serve speed std dev (tactical variety)",
    mirror=True,
)
def style_1st_serve_speed_variance() -> pl.Expr:
    return rolling_mean("mb_player_std_1st_serve_speed", days=_DAYS, group_by=_GRP)


# =============================================================================
# Layer 1: Raw Style Metrics — Aggression & Errors (match_beats)
# =============================================================================


@feature(
    name="style_winner_rate",
    params=[],
    description="365d rolling winners per point (offensive output)",
    mirror=True,
)
def style_winner_rate() -> pl.Expr:
    return _rolling_365(pl.col("mb_player_winners") / pl.col("total_points"))


@feature(
    name="style_ue_rate",
    params=[],
    description="365d rolling unforced errors per point",
    mirror=True,
)
def style_ue_rate() -> pl.Expr:
    return _rolling_365(pl.col("mb_player_ues") / pl.col("total_points"))


@feature(
    name="style_winner_ue_ratio",
    params=[],
    description="365d rolling winners/UEs ratio (aggression efficiency)",
    mirror=True,
)
def style_winner_ue_ratio() -> pl.Expr:
    per_match = (
        pl.when(pl.col("mb_player_ues") > 0)
        .then(pl.col("mb_player_winners") / pl.col("mb_player_ues"))
        .otherwise(None)
    )
    return _rolling_365(per_match)


@feature(
    name="style_forced_error_rate",
    params=[],
    description="365d rolling opponent FEs per point (offensive pressure)",
    mirror=True,
)
def style_forced_error_rate() -> pl.Expr:
    return _rolling_365(pl.col("mb_opp_fes") / pl.col("total_points"))


# =============================================================================
# Layer 1: Raw Style Metrics — Service Game Quality (match_beats)
# =============================================================================


@feature(
    name="style_easy_hold_pct",
    params=[],
    description="365d easy holds / service games (serve dominance)",
    mirror=True,
)
def style_easy_hold_pct() -> pl.Expr:
    return ratio_feature(
        "mb_player_easy_holds", "mb_player_service_games", days=_DAYS
    )


@feature(
    name="style_difficult_hold_pct",
    params=[],
    description="365d difficult holds / service games (under-pressure serving)",
    mirror=True,
)
def style_difficult_hold_pct() -> pl.Expr:
    return ratio_feature(
        "mb_player_difficult_holds", "mb_player_service_games", days=_DAYS
    )


# =============================================================================
# Layer 1: Raw Style Metrics — Pressure (match_beats)
# =============================================================================


@feature(
    name="style_crucial_pts_win_pct",
    params=[],
    description="365d crucial points won percentage (big-point performance)",
    mirror=True,
)
def style_crucial_pts_win_pct() -> pl.Expr:
    return ratio_feature(
        "mb_player_crucial_points_won",
        "mb_player_crucial_points_played",
        days=_DAYS,
    )


# =============================================================================
# Layer 1: Raw Style Metrics — Rally Shape (match_beats)
# =============================================================================


@feature(
    name="style_rally_won_avg_length",
    params=[],
    description="365d avg rally length when winning (total shots in won rallies / count)",
    mirror=True,
)
def style_rally_won_avg_length() -> pl.Expr:
    return ratio_feature(
        "mb_player_rally_won_shots", "mb_player_rally_won_count", days=_DAYS
    )


@feature(
    name="style_rally_lost_avg_length",
    params=[],
    description="365d avg rally length when losing",
    mirror=True,
)
def style_rally_lost_avg_length() -> pl.Expr:
    return ratio_feature(
        "mb_player_rally_lost_shots", "mb_player_rally_lost_count", days=_DAYS
    )


# =============================================================================
# Layer 1: Raw Style Metrics — Wing Preference (stroke_analysis)
# =============================================================================


@feature(
    name="style_fh_winner_share",
    params=[],
    description="365d FH winners as share of FH+BH winners (offensive wing preference)",
    mirror=True,
)
def style_fh_winner_share() -> pl.Expr:
    total = pl.col("player_fh_winners") + pl.col("player_bh_winners")
    per_match = pl.when(total > 0).then(pl.col("player_fh_winners") / total).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_fh_ue_share",
    params=[],
    description="365d FH UEs as share of FH+BH UEs (error wing tendency)",
    mirror=True,
)
def style_fh_ue_share() -> pl.Expr:
    total = pl.col("player_fh_unforced_errors") + pl.col("player_bh_unforced_errors")
    per_match = pl.when(total > 0).then(pl.col("player_fh_unforced_errors") / total).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_fh_winner_rate",
    params=[],
    description="365d FH productivity (FH winners / total FH outcomes)",
    mirror=True,
)
def style_fh_winner_rate() -> pl.Expr:
    total = (
        pl.col("player_fh_winners")
        + pl.col("player_fh_forced_errors")
        + pl.col("player_fh_unforced_errors")
    )
    per_match = pl.when(total > 0).then(pl.col("player_fh_winners") / total).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_bh_winner_rate",
    params=[],
    description="365d BH productivity (BH winners / total BH outcomes)",
    mirror=True,
)
def style_bh_winner_rate() -> pl.Expr:
    total = (
        pl.col("player_bh_winners")
        + pl.col("player_bh_forced_errors")
        + pl.col("player_bh_unforced_errors")
    )
    per_match = pl.when(total > 0).then(pl.col("player_bh_winners") / total).otherwise(None)
    return _rolling_365(per_match)


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
    params=[],
    description="365d ground stroke winners / total ground strokes (rally offense)",
    mirror=True,
)
def style_ground_stroke_winner_rate() -> pl.Expr:
    total = _shot_type_total("ground_stroke")
    per_match = pl.when(total > 0).then(pl.col("player_ground_stroke_winners") / total).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_ground_stroke_ue_rate",
    params=[],
    description="365d ground stroke UEs / total ground strokes (rally error tendency)",
    mirror=True,
)
def style_ground_stroke_ue_rate() -> pl.Expr:
    total = _shot_type_total("ground_stroke")
    per_match = pl.when(total > 0).then(pl.col("player_ground_stroke_unforced_errors") / total).otherwise(None)
    return _rolling_365(per_match)


# =============================================================================
# Layer 1: Raw Style Metrics — Shot Variety (stroke_analysis)
# =============================================================================


@feature(
    name="style_net_approach_frequency",
    params=[],
    description="365d net play shots (volley+approach+overhead) per point",
    mirror=True,
)
def style_net_approach_frequency() -> pl.Expr:
    net_total = _shot_type_total("volley") + _shot_type_total("approach") + _shot_type_total("overhead")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(net_total / pts).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_drop_shot_frequency",
    params=[],
    description="365d drop shots per point (craft/variety)",
    mirror=True,
)
def style_drop_shot_frequency() -> pl.Expr:
    total = _shot_type_total("drop_shot")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(total / pts).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_drop_shot_effectiveness",
    params=[],
    description="365d drop shot winners / total drop shots",
    mirror=True,
)
def style_drop_shot_effectiveness() -> pl.Expr:
    total = _shot_type_total("drop_shot")
    per_match = pl.when(total > 0).then(pl.col("player_drop_shot_winners") / total).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_passing_frequency",
    params=[],
    description="365d passing shots per point",
    mirror=True,
)
def style_passing_frequency() -> pl.Expr:
    total = _shot_type_total("passing")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(total / pts).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_lob_frequency",
    params=[],
    description="365d lob shots per point (defensive variety)",
    mirror=True,
)
def style_lob_frequency() -> pl.Expr:
    total = _shot_type_total("lob")
    pts = pl.col("pts_total_pts_played")
    per_match = pl.when(pts > 0).then(total / pts).otherwise(None)
    return _rolling_365(per_match)


# =============================================================================
# Layer 1: Raw Style Metrics — Rally Length (rally_analysis)
# =============================================================================


@feature(
    name="style_short_rally_pct",
    params=[],
    description="365d short rallies as share of total rallies (serve-dominated play)",
    mirror=True,
)
def style_short_rally_pct() -> pl.Expr:
    rpwd = pl.col("rally_points_with_data")
    per_match = pl.when(rpwd > 0).then(pl.col("rally_short_count") / rpwd).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_long_rally_pct",
    params=[],
    description="365d long rallies as share of total rallies (grinder tendency)",
    mirror=True,
)
def style_long_rally_pct() -> pl.Expr:
    rpwd = pl.col("rally_points_with_data")
    per_match = pl.when(rpwd > 0).then(pl.col("rally_long_count") / rpwd).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_short_rally_win_pct",
    params=[],
    description="365d short rally points won / total short rallies (quick-point efficiency)",
    mirror=True,
)
def style_short_rally_win_pct() -> pl.Expr:
    total = pl.col("player_short_won") + pl.col("player_short_err")
    per_match = pl.when(total > 0).then(pl.col("player_short_won") / total).otherwise(None)
    return _rolling_365(per_match)


@feature(
    name="style_long_rally_win_pct",
    params=[],
    description="365d long rally points won / total long rallies (endurance/consistency)",
    mirror=True,
)
def style_long_rally_win_pct() -> pl.Expr:
    total = pl.col("player_long_won") + pl.col("player_long_err")
    per_match = pl.when(total > 0).then(pl.col("player_long_won") / total).otherwise(None)
    return _rolling_365(per_match)


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


def _register_diff(base_name: str) -> None:
    """Register a diff feature for a single stat."""
    diff_name = f"{base_name}_diff"

    @feature(
        name=diff_name,
        params=[],
        description=f"{base_name} difference (player - opponent)",
        depends_on=[base_name],
        mirror=False,
    )
    def _diff() -> pl.Expr:
        return pl.col(f"player_{base_name}") - pl.col(f"opp_{base_name}")

    globals()[diff_name] = _diff


for _base in _STYLE_SINGLE_FEATURES:
    _register_diff(_base)
