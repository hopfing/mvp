"""Tests for overview page data extraction."""

import polars as pl


def _make_ds():
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3", "m4", "m5"],
        "status": ["resolved", "resolved", "resolved", "resolved", "pending"],
        "model_correct": [True, True, False, True, None],
        "pred_odds_best_close": [2.0, 1.8, 1.5, 2.2, 1.6],
        "bet_side": ["P1", "P2", "", "P1", ""],
        "bet_result": ["W", "L", "", "W", ""],
        "bet_odds": ["2.10", "1.75", None, "2.30", None],
        "stake": ["10", "15", None, "10", None],
        "net": ["11.00", "-15.00", None, "12.00", None],
    })


def test_model_performance():
    from mvp.analysis.dashboard.overview import compute_model_performance

    ds = _make_ds()
    m = compute_model_performance(ds)

    assert m["n"] == 4  # 4 resolved
    assert m["wins"] == 3
    assert m["losses"] == 1
    assert m["accuracy"] == 0.75
    assert m["stake"] == 4  # flat $1 per prediction


def test_model_performance_pnl():
    from mvp.analysis.dashboard.overview import compute_model_performance

    ds = _make_ds()
    m = compute_model_performance(ds)

    # Won 3 at odds 2.0, 1.8, 2.2 = returned 6.0. Staked 4. P&L = +2.0
    assert m["pnl"] == 2.0
    assert m["roi"] == 0.5


def test_bet_performance():
    from mvp.analysis.dashboard.overview import compute_bet_performance

    ds = _make_ds()
    b = compute_bet_performance(ds)

    assert b["n"] == 3  # 3 bets (m1, m2, m4)
    assert b["wins"] == 2  # W, W
    assert b["losses"] == 1  # L
    assert b["void"] == 0


def test_bet_performance_with_void():
    from mvp.analysis.dashboard.overview import compute_bet_performance

    ds = _make_ds()
    extra = pl.DataFrame({
        "match_uid": ["m6"],
        "status": ["resolved"],
        "model_correct": [True],
        "pred_odds_best_close": [1.9],
        "bet_side": ["P1"],
        "bet_result": ["V"],
        "bet_odds": ["1.90"],
        "stake": ["10"],
        "net": ["0"],
    })
    ds_ext = pl.concat([ds, extra], how="diagonal_relaxed")
    b = compute_bet_performance(ds_ext)

    assert b["void"] == 1
    # Accuracy excludes voids: 2W / (2W + 1L) = 66.7%
    assert b["accuracy"] == 2 / 3


def test_bet_performance_no_bets():
    from mvp.analysis.dashboard.overview import compute_bet_performance

    ds = pl.DataFrame({
        "match_uid": ["m1"],
        "status": ["resolved"],
        "model_correct": [True],
    })
    b = compute_bet_performance(ds)
    assert b["n"] == 0
    assert b["pnl"] is None


def test_odds_coverage():
    from mvp.analysis.dashboard.overview import compute_odds_coverage

    ds = _make_ds()
    c = compute_odds_coverage(ds)

    assert c["n_predictions"] == 5
    assert c["n_resolved"] == 4
    assert c["n_pending"] == 1
