"""Tier features: the all-time trailing means skip null values, as the windowed
(rolling_mean) variants do, instead of counting a null as a zero."""

from __future__ import annotations

import math
from datetime import date

import polars as pl
import pytest

from mvp.model.features.tier import (
    level_gap,
    level_gap_pts,
    prize_money_log_avg,
    tier_ordinal_avg,
)


def frame(levels, prizes):
    """One player's matches on consecutive days, then the row under test."""
    n = len(levels)
    return pl.DataFrame(
        {
            "match_uid": [f"m{i}" for i in range(n)],
            "player_id": ["a"] * n,
            "effective_match_date": [date(2024, 1, 1 + i) for i in range(n)],
            "tournament_start_date": [date(2024, 1, 1 + i) for i in range(n)],
            "round_order": [1] * n,
            "tournament_level": levels,
            "prize_money": prizes,
        },
        schema={
            "match_uid": pl.String,
            "player_id": pl.String,
            "effective_match_date": pl.Date,
            "tournament_start_date": pl.Date,
            "round_order": pl.Int64,
            "tournament_level": pl.String,
            "prize_money": pl.Int64,
        },
    )


# Prior matches: a 250 with a prize, a doubles-like row with neither, a GS with a
# prize; then the row under test (a CH100 with a prize).
LEVELS = ["250", None, "GS", "CH100"]
PRIZES = [1000, None, 3000, 500]


def last(df, expr):
    return df.with_columns(expr.alias("x"))["x"][-1]


@pytest.mark.parametrize("days", [None, 365])
def test_prize_money_log_avg_skips_null_prizes(days):
    expected = (math.log1p(1000) + math.log1p(3000)) / 2
    got = last(frame(LEVELS, PRIZES), prize_money_log_avg(days=days))
    assert got == pytest.approx(expected, abs=1e-12)


@pytest.mark.parametrize("days", [None, 365])
def test_tier_ordinal_avg_skips_null_tiers(days):
    got = last(frame(LEVELS, PRIZES), tier_ordinal_avg(days=days))
    assert got == pytest.approx((6 + 9) / 2, abs=1e-12)


@pytest.mark.parametrize("days", [None, 365])
def test_level_gaps_skip_null_tiers(days):
    df = frame(LEVELS, PRIZES)
    assert last(df, level_gap(days=days)) == pytest.approx(3 - (6 + 9) / 2, abs=1e-12)
    assert last(df, level_gap_pts(days=days)) == pytest.approx(
        100 - (250 + 2000) / 2, abs=1e-12
    )


def test_all_time_mean_is_null_with_only_null_priors():
    df = frame([None, None, "CH100"], [None, None, 500])
    assert last(df, prize_money_log_avg()) is None
    assert last(df, tier_ordinal_avg()) is None
    assert last(df, level_gap()) is None
