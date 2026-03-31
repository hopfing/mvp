"""Tests for overview page data extraction."""

import polars as pl


def _make_ds():
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3", "m4", "m5"],
        "status": ["resolved", "resolved", "resolved", "resolved", "pending"],
        "model_correct": [True, True, False, True, None],
        "pred_odds_best_close": [2.0, 1.8, 1.5, 2.2, 1.6],
        "stake": ["10", "15", None, "10", None],
        "net": ["11.00", "-15.00", None, "12.00", None],
        "clv_vs_avg": [0.03, 0.01, None, 0.05, None],
        "bet_odds": [2.1, 1.75, None, 2.3, None],
    })


def test_compute_headlines():
    from mvp.analysis.dashboard.overview import compute_headlines

    ds = _make_ds()
    h = compute_headlines(ds)

    assert h["n_predictions"] == 5
    assert h["n_resolved"] == 4
    assert h["accuracy"] == 0.75  # 3/4
    assert h["n_bets"] == 3


def test_compute_headlines_no_bets():
    from mvp.analysis.dashboard.overview import compute_headlines

    ds = pl.DataFrame({
        "match_uid": ["m1"],
        "status": ["resolved"],
        "model_correct": [True],
    })
    h = compute_headlines(ds)
    assert h["n_predictions"] == 1
    assert h["n_bets"] == 0
    assert h["pnl"] is None
