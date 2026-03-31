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


def test_clv_by_consensus():
    from mvp.analysis.dashboard.execution import clv_by_group

    ds = _make_bet_ds()
    result = clv_by_group(ds, group_col="consensus", clv_col="clv_vs_avg")

    assert "group" in result.columns
    assert "n" in result.columns
    assert "mean_clv" in result.columns
    assert "median_clv" in result.columns
    assert len(result) > 0


def test_clv_by_book():
    from mvp.analysis.dashboard.execution import clv_by_group

    ds = _make_bet_ds()
    result = clv_by_group(ds, group_col="book", clv_col="clv_vs_avg")

    books = result["group"].to_list()
    assert "DraftKings" in books
    assert "Bet365" in books


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
    result = clv_by_group(ds_ext, group_col="consensus", clv_col="clv_vs_avg")

    total_n = result["n"].sum()
    assert total_n == 5


def test_execution_summary():
    from mvp.analysis.dashboard.execution import execution_summary

    ds = _make_bet_ds()
    result = execution_summary(ds)

    assert result["n_bets"] == 5
    assert result["avg_bet_odds"] is not None
    assert result["avg_closing_odds"] is not None


def test_execution_summary_no_bets():
    from mvp.analysis.dashboard.execution import execution_summary

    ds = pl.DataFrame({
        "match_uid": ["m1"],
        "status": ["resolved"],
        "model_correct": [True],
    })
    result = execution_summary(ds)
    assert result["n_bets"] == 0


def _make_bet_ds_with_timing():
    """Analysis dataset with bet_placed_at and closing timestamps."""
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3", "m4"],
        "status": ["resolved"] * 4,
        "model_correct": [True, True, False, True],
        "bet_side": ["P1", "P2", "P1", "P1"],
        "bet_odds": ["2.10", "1.75", "1.90", "2.30"],
        "stake": ["10", "15", "10", "10"],
        "net": ["11.00", "-15.00", "-10.00", "13.00"],
        "book": ["DraftKings", "Bet365", "DraftKings", "MGM"],
        "clv_vs_avg": [0.03, 0.01, -0.02, 0.05],
        "bet_placed_at": [
            "2026-03-25 08:00",
            "2026-03-25 10:00",
            "2026-03-25 11:30",
            "2026-03-25 14:00",
        ],
        "dk_closing_fetched_at": [
            datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 25, 15, 0, tzinfo=timezone.utc),
        ],
    })


def test_clv_by_timing():
    from mvp.analysis.dashboard.execution import clv_by_timing

    ds = _make_bet_ds_with_timing()
    result = clv_by_timing(ds, clv_col="clv_vs_avg")

    assert "bucket" in result.columns
    assert "n" in result.columns
    assert "mean_clv" in result.columns
    assert len(result) > 0


def test_clv_by_timing_no_timestamps():
    from mvp.analysis.dashboard.execution import clv_by_timing

    ds = _make_bet_ds()  # existing fixture - no bet_placed_at column
    result = clv_by_timing(ds, clv_col="clv_vs_avg")
    assert len(result) == 0
