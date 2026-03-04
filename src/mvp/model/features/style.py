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
