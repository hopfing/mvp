"""Tests for the result-volatility feature family (issue #95)."""

import datetime as dt
import math

import polars as pl
import pytest

from mvp.model.features.volatility import form_volatility
from mvp.model.registry import get_registry


def _frame() -> pl.DataFrame:
    """Two players, mu_diff=0 so E=0.5 and the standardized residual is +/-1.

    Player P results -> z = [+1, -1, +1, -1]; Player Q -> z = [+1, +1, +1]
    (Q interleaves with P on the calendar to exercise per-player grouping).
    """
    rows = [
        ("m1", "P", dt.date(2024, 1, 1), True),
        ("m2", "P", dt.date(2024, 1, 8), False),
        ("m3", "P", dt.date(2024, 1, 15), True),
        ("m4", "P", dt.date(2024, 1, 22), False),
        ("q1", "Q", dt.date(2024, 1, 3), True),
        ("q2", "Q", dt.date(2024, 1, 10), True),
        ("q3", "Q", dt.date(2024, 1, 17), True),
    ]
    return pl.DataFrame(
        {
            "match_uid": [r[0] for r in rows],
            "player_id": [r[1] for r in rows],
            "effective_match_date": [r[2] for r in rows],
            "won": [r[3] for r in rows],
            "round_order": [12] * len(rows),
            "tournament_start_date": dt.date(2020, 1, 1),
            "player_glicko_mu": [1500.0] * len(rows),
            "opp_glicko_mu": [1500.0] * len(rows),
        }
    ).sort("effective_match_date")


def _p_values(df: pl.DataFrame, col: str) -> list:
    return (
        df.filter(pl.col("player_id") == "P")
        .sort("effective_match_date")[col]
        .to_list()
    )


def test_windowed_values_and_temporal_safety():
    """Std uses only STRICTLY PRIOR matches; the current result is excluded.

    If the current row leaked in, P's 3rd value would be std({+1,-1,+1})=1.1547,
    not std({+1,-1})=sqrt(2). The exact sqrt(2) is the temporal-safety check.
    """
    df = _frame().with_columns(form_volatility(days=3650).alias("v"))
    p = _p_values(df, "v")
    assert p[0] is None  # no prior matches
    assert p[1] is None  # only 1 prior -> std needs >= 2
    assert p[2] == pytest.approx(math.sqrt(2.0), abs=1e-9)  # std({+1,-1})
    assert p[3] == pytest.approx(1.15470054, abs=1e-6)      # std({+1,-1,+1})


def test_per_player_grouping_isolated():
    """Q's window must not pull in P's matches (and vice versa)."""
    df = _frame().with_columns(form_volatility(days=3650).alias("v"))
    q = (
        df.filter(pl.col("player_id") == "Q")
        .sort("effective_match_date")["v"]
        .to_list()
    )
    assert q[0] is None
    assert q[1] is None
    assert q[2] == pytest.approx(0.0, abs=1e-12)  # std({+1,+1}) = 0, not affected by P


def test_alltime_matches_windowed_on_full_history():
    """days=None (cumulative_std) equals the windowed path when the window
    spans the whole history."""
    df = _frame().with_columns(
        form_volatility(days=3650).alias("w"),
        form_volatility(days=None).alias("a"),
    )
    for w, a in zip(_p_values(df, "w"), _p_values(df, "a")):
        if w is None:
            assert a is None
        else:
            assert a == pytest.approx(w, abs=1e-9)


def test_null_won_does_not_corrupt_std():
    """A null-`won` row contributes nothing to the moments and isn't counted.

    Prior residuals at the 4th match are {+1, null, -1} -> non-null {+1, -1},
    so std = sqrt(2) on BOTH the windowed and all-time paths. Guards the
    cumulative_std moment accumulation against null pollution.
    """
    rows = [
        ("m1", "P", dt.date(2024, 1, 1), True),    # z = +1
        ("m2", "P", dt.date(2024, 1, 8), None),    # null won -> z null
        ("m3", "P", dt.date(2024, 1, 15), False),  # z = -1
        ("m4", "P", dt.date(2024, 1, 22), True),   # current row (excluded)
    ]
    df = pl.DataFrame(
        {
            "match_uid": [r[0] for r in rows],
            "player_id": [r[1] for r in rows],
            "effective_match_date": [r[2] for r in rows],
            "won": [r[3] for r in rows],
            "round_order": [12] * len(rows),
            "tournament_start_date": dt.date(2020, 1, 1),
            "player_glicko_mu": [1500.0] * len(rows),
            "opp_glicko_mu": [1500.0] * len(rows),
        }
    ).sort("effective_match_date").with_columns(
        form_volatility(days=3650).alias("w"),
        form_volatility(days=None).alias("a"),
    )
    w = df.sort("effective_match_date")["w"].to_list()
    a = df.sort("effective_match_date")["a"].to_list()
    assert w[0] is None and w[1] is None and w[2] is None  # <2 non-null prior
    assert a[0] is None and a[1] is None and a[2] is None
    assert w[3] == pytest.approx(math.sqrt(2.0), abs=1e-9)  # std({+1,-1})
    assert a[3] == pytest.approx(math.sqrt(2.0), abs=1e-9)


def test_registered_metadata():
    reg = get_registry()
    fdef = reg.get("form_volatility")
    assert fdef.params == ["days"]
    assert fdef.impute is None
    assert fdef.mirror is True
    diff = reg.get("form_volatility_diff")
    assert diff.impute is None  # inherits base's no-fabricate impute


# --- Surface-conditioned variant (A1) ---


def _surface_frame() -> pl.DataFrame:
    """Player P on two surfaces, mu_diff=0 so E=0.5 and residuals are +/-1.

    Hard: won [T, F, T] -> z=[+1,-1,+1]; Clay: won [T, T, T] -> z=[+1,+1,+1],
    interleaved on the calendar to exercise the (player, surface) grouping.
    """
    rows = [
        ("h1", "Hard", dt.date(2024, 1, 1), True),
        ("c1", "Clay", dt.date(2024, 1, 3), True),
        ("h2", "Hard", dt.date(2024, 1, 8), False),
        ("c2", "Clay", dt.date(2024, 1, 10), True),
        ("h3", "Hard", dt.date(2024, 1, 15), True),
        ("c3", "Clay", dt.date(2024, 1, 17), True),
    ]
    return pl.DataFrame(
        {
            "match_uid": [r[0] for r in rows],
            "player_id": ["P"] * len(rows),
            "surface": [r[1] for r in rows],
            "effective_match_date": [r[2] for r in rows],
            "won": [r[3] for r in rows],
            "round_order": [12] * len(rows),
            "tournament_start_date": dt.date(2020, 1, 1),
            "player_glicko_mu": [1500.0] * len(rows),
            "opp_glicko_mu": [1500.0] * len(rows),
        }
    ).sort("effective_match_date")


def test_surface_form_volatility_isolated_by_surface():
    """The Hard window must use only prior Hard residuals, not the interleaved Clay.

    If Clay leaked into h3's window, prior would be {+1,+1,-1,+1} -> std=1.0.
    The exact sqrt(2)=std({+1,-1}) is the surface-isolation check.
    """
    from mvp.model.features.volatility import surface_form_volatility

    df = _surface_frame().with_columns(
        surface_form_volatility(days=3650).alias("v")
    )
    by_uid = dict(zip(df["match_uid"].to_list(), df["v"].to_list()))
    assert by_uid["h1"] is None   # no prior Hard
    assert by_uid["h2"] is None   # 1 prior Hard -> std needs >= 2
    assert by_uid["h3"] == pytest.approx(math.sqrt(2.0), abs=1e-9)  # std({+1,-1}), Clay excluded
    assert by_uid["c3"] == pytest.approx(0.0, abs=1e-12)            # std({+1,+1}) = 0


def test_surface_form_volatility_registered():
    reg = get_registry()
    fdef = reg.get("surface_form_volatility")
    assert fdef.params == ["days"]
    assert fdef.impute is None
    assert fdef.mirror is True
    assert reg.get("surface_form_volatility_diff").impute is None
