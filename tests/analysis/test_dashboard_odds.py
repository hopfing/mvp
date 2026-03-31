"""Tests for odds page data prep."""

import polars as pl


def _make_ds():
    return pl.DataFrame({
        "match_uid": [f"m{i}" for i in range(10)],
        "status": ["resolved"] * 10,
        "model_correct": [True, True, False, True, False,
                          True, True, False, True, False],
        "pred_odds_best_close": [1.15, 1.35, 1.60, 1.85, 2.10,
                                  2.30, 2.60, 1.45, 1.70, 1.95],
        "model_edge_best_close": [0.10, 0.05, 0.02, -0.01, 0.08,
                                   -0.03, 0.12, 0.06, -0.05, 0.01],
        "pred_prob": [0.75, 0.68, 0.60, 0.52, 0.55,
                      0.40, 0.50, 0.65, 0.54, 0.50],
    })


def test_bucket_by_odds_range():
    from mvp.analysis.dashboard.odds import bucket_by_odds

    ds = _make_ds()
    bucketed = bucket_by_odds(ds, odds_col="pred_odds_best_close")

    assert "odds_bucket" in bucketed.columns
    # 1.15 -> 1.00-1.25, 1.35 -> 1.25-1.50
    row0 = bucketed.filter(pl.col("match_uid") == "m0")
    assert row0["odds_bucket"][0] == "1.00-1.25"
    row1 = bucketed.filter(pl.col("match_uid") == "m1")
    assert row1["odds_bucket"][0] == "1.25-1.50"


def test_odds_range_summary():
    from mvp.analysis.dashboard.odds import odds_range_summary

    ds = _make_ds()
    summary = odds_range_summary(ds, odds_col="pred_odds_best_close")

    assert "odds_bucket" in summary.columns
    assert "n" in summary.columns
    assert "accuracy" in summary.columns
    assert "roi" in summary.columns
    assert len(summary) > 0


def test_2_50_plus_bucket():
    from mvp.analysis.dashboard.odds import bucket_by_odds

    ds = _make_ds()
    bucketed = bucket_by_odds(ds, odds_col="pred_odds_best_close")
    # 2.60 -> 2.50+
    row = bucketed.filter(pl.col("match_uid") == "m6")
    assert row["odds_bucket"][0] == "2.50+"


def test_odds_range_summary_includes_edge():
    from mvp.analysis.dashboard.odds import odds_range_summary

    ds = _make_ds()
    summary = odds_range_summary(ds, odds_col="pred_odds_best_close", edge_col="model_edge_best_close")
    assert "mean_edge" in summary.columns
