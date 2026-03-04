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
