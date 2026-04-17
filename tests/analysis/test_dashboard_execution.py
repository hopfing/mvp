# tests/analysis/test_dashboard_execution.py
"""Tests for execution page data functions."""

from datetime import datetime, timezone

import polars as pl


def _make_bet_ds():
    """Analysis dataset with bet and CLV columns."""
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3", "m4", "m5"],
        "status": ["resolved"] * 5,
        "model_correct": [True, True, False, True, False],
        "bet_side": ["P1", "P2", "P1", "P1", "P2"],
        "bet_odds": ["2.10", "1.75", "1.90", "2.30", "1.65"],
        "stake": ["10", "15", "10", "10", "15"],
        "net": ["11.00", "-15.00", "-10.00", "13.00", "-15.00"],
        "book": ["DraftKings", "Bet365", "DraftKings", "MGM", "Bet365"],
        "consensus": [1.0, 0.8, 1.0, 0.6, 0.8],
        "clv_vs_avg": [0.03, 0.01, -0.02, 0.05, -0.01],
        "clv_vs_best": [0.02, 0.005, -0.03, 0.04, -0.02],
        "bet_closing_best": [2.05, 1.74, 1.96, 2.21, 1.68],
    })


def test_clv_by_book_wld_counts():
    from mvp.analysis.dashboard.execution import clv_by_group

    ds = _make_bet_ds()
    result = clv_by_group(ds, group_col="book")

    expected_cols = {"group", "n", "positive", "negative", "even", "pos_pct"}
    assert expected_cols.issubset(set(result.columns))
    books = result["group"].to_list()
    assert "DraftKings" in books and "Bet365" in books

    # m1 DK 2.10>2.05 pos; m3 DK 1.90<1.96 neg -> DK 1-1-0
    dk = result.filter(pl.col("group") == "DraftKings")
    assert dk["positive"][0] == 1
    assert dk["negative"][0] == 1
    assert dk["even"][0] == 0
    assert dk["n"][0] == 2


def test_clv_by_group_filters_to_bets():
    from mvp.analysis.dashboard.execution import clv_by_group

    ds = _make_bet_ds()
    extra = pl.DataFrame({
        "match_uid": ["m6"],
        "status": ["resolved"],
        "model_correct": [True],
        "bet_side": [""],
        "bet_odds": [None],
        "stake": [None],
        "net": [None],
        "book": [None],
        "consensus": [1.0],
        "clv_vs_avg": [None],
        "clv_vs_best": [None],
        "bet_closing_best": [None],
    })
    ds_ext = pl.concat([ds, extra], how="diagonal_relaxed")
    result = clv_by_group(ds_ext, group_col="book")

    total_n = int(result["n"].sum())
    assert total_n == 5


def test_execution_summary():
    from mvp.analysis.dashboard.execution import execution_summary

    ds = _make_bet_ds()
    result = execution_summary(ds)

    assert result["n_bets"] == 5
    assert result["avg_bet_odds"] is not None
    assert result["avg_closing_odds"] is not None
    # Fixture bet_odds vs bet_closing_best (all rounded to 2dp):
    # m1: 2.10 > 2.05 pos, m2: 1.75 > 1.74 pos, m3: 1.90 < 1.96 neg,
    # m4: 2.30 > 2.21 pos, m5: 1.65 < 1.68 neg
    assert result["n_settled"] == 5
    assert result["n_positive"] == 3
    assert result["n_negative"] == 2
    assert result["n_even"] == 0
    assert result["pos_pct"] == 3 / 5


def test_execution_summary_even_exact_match():
    """Even requires exact float match, no rounding."""
    from mvp.analysis.dashboard.execution import execution_summary

    ds = pl.DataFrame({
        "match_uid": ["m1", "m2"],
        "status": ["resolved", "resolved"],
        "bet_side": ["P1", "P1"],
        "bet_odds": ["2.004", "2.10"],
        "bet_closing_best": [2.001, 2.10],
    })
    result = execution_summary(ds)
    assert result["n_settled"] == 2
    assert result["n_positive"] == 1
    assert result["n_even"] == 1


def test_execution_summary_no_bets():
    from mvp.analysis.dashboard.execution import execution_summary

    ds = pl.DataFrame({
        "match_uid": ["m1"],
        "status": ["resolved"],
        "model_correct": [True],
    })
    result = execution_summary(ds)
    assert result["n_bets"] == 0
    assert result["n_settled"] == 0
    assert result["pos_pct"] is None


def _make_bet_ds_with_timing():
    """Analysis dataset with bet_placed_at and first_live_fetched_at."""
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3", "m4"],
        "status": ["resolved"] * 4,
        "model_correct": [True, True, False, True],
        "bet_side": ["P1", "P2", "P1", "P1"],
        "bet_odds": ["2.10", "1.75", "1.90", "2.30"],
        "stake": ["10", "15", "10", "10"],
        "net": ["11.00", "-15.00", "-10.00", "13.00"],
        "book": ["DraftKings", "Bet365", "DraftKings", "MGM"],
        "clv_vs_best": [0.03, 0.01, -0.02, 0.05],
        "bet_closing_best": [2.05, 1.74, 1.96, 2.21],
        "bet_placed_at": [
            "2026-03-25 08:00",
            "2026-03-25 10:00",
            "2026-03-25 11:30",
            "2026-03-25 14:00",
        ],
        "first_live_fetched_at": [
            datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 25, 15, 0, tzinfo=timezone.utc),
        ],
    })


def test_clv_by_timing():
    from mvp.analysis.dashboard.execution import clv_by_timing

    ds = _make_bet_ds_with_timing()
    result = clv_by_timing(ds)

    expected_cols = {"bucket", "n", "positive", "negative", "even", "pos_pct"}
    assert expected_cols.issubset(set(result.columns))
    assert len(result) > 0
    # Total bets covered should be 4 (all post-reliable-after cutoff)
    assert int(result["n"].sum()) == 4


def test_clv_by_timing_no_timestamps():
    from mvp.analysis.dashboard.execution import clv_by_timing

    ds = _make_bet_ds()  # existing fixture - no bet_placed_at column
    result = clv_by_timing(ds)
    assert len(result) == 0
