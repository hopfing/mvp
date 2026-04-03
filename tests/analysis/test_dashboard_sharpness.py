"""Tests for book sharpness page data functions."""

import numpy as np
import polars as pl


def _make_sims_with_books():
    """Simulations with per-book edge band scenarios."""
    rows = []
    for book in ["dk", "br", "b365"]:
        for band in ["edge_10pct", "edge_5pct", "edge_0pct", "neg_5pct", "neg_10pct"]:
            for cut in ["open", "close", "best_intra", "worst_intra"]:
                rows.append({
                    "model_version": "all",
                    "scenario": f"{band}_{book}_{cut}",
                    "segment": "overall",
                    "segment_value": "overall",
                    "n_bets": 15, "n_wins": 10, "n_losses": 5,
                    "accuracy": 0.667, "total_staked": 15.0,
                    "total_returned": 18.0, "net_pnl": 3.0,
                    "roi": 0.20, "yield_pct": 0.20,
                    "filter_desc": f"{band}_{book}_{cut}",
                })
        for cut in ["open", "close", "best_intra", "worst_intra"]:
            rows.append({
                "model_version": "all",
                "scenario": f"flat_{book}_{cut}",
                "segment": "overall", "segment_value": "overall",
                "n_bets": 100, "n_wins": 68, "n_losses": 32,
                "accuracy": 0.68, "total_staked": 100.0,
                "total_returned": 105.0, "net_pnl": 5.0,
                "roi": 0.05, "yield_pct": 0.05,
                "filter_desc": f"flat_{book}_{cut}",
            })
    return pl.DataFrame(rows)


def test_detect_books():
    from mvp.analysis.dashboard.sharpness import detect_books
    sims = _make_sims_with_books()
    books = detect_books(sims)
    assert "dk" in books
    assert "br" in books
    assert "b365" in books


def test_book_edge_table():
    from mvp.analysis.dashboard.sharpness import book_edge_table
    sims = _make_sims_with_books()
    result = book_edge_table(sims, book="dk", cut="close")
    assert len(result) > 0
    assert "scenario" in result.columns
    assert "n_bets" in result.columns


def test_book_comparison():
    from mvp.analysis.dashboard.sharpness import book_comparison
    sims = _make_sims_with_books()
    result = book_comparison(sims, edge_band="edge_10pct", cut="close")
    assert "book" in result.columns
    assert len(result) == 3


def _make_ds_with_books(n=200, seed=42):
    """Synthetic analysis dataset with per-book odds and edge columns."""
    rng = np.random.default_rng(seed)
    books = ["dk", "br"]
    cuts = ["open", "close"]
    data = {
        "status": ["resolved"] * n,
        "model_correct": rng.choice([True, False], size=n, p=[0.6, 0.4]),
        "pred_prob": rng.uniform(0.55, 0.75, size=n),
    }
    for book in books:
        for cut in cuts:
            odds = rng.uniform(1.2, 4.0, size=n)
            data[f"pred_odds_{book}_{cut}"] = odds
            data[f"model_edge_{book}_{cut}"] = (
                data["pred_prob"] - 1.0 / odds
            )
    return pl.DataFrame(data)


def test_compute_book_edge_table():
    from mvp.analysis.dashboard.sharpness import compute_book_edge_table
    ds = _make_ds_with_books()
    result = compute_book_edge_table(ds, book="dk", cut="close")
    assert not result.is_empty()
    assert "scenario" in result.columns
    assert "n_bets" in result.columns
    assert "accuracy" in result.columns
    assert "roi" in result.columns
    assert "net_pnl" in result.columns
    # Every scenario should reference dk_close
    for s in result["scenario"].to_list():
        assert s.endswith("_dk_close")


def test_compute_book_edge_table_missing_columns():
    from mvp.analysis.dashboard.sharpness import compute_book_edge_table
    ds = pl.DataFrame({"status": ["resolved"], "model_correct": [True]})
    result = compute_book_edge_table(ds, book="dk", cut="close")
    assert result.is_empty()


def test_compute_book_comparison():
    from mvp.analysis.dashboard.sharpness import compute_book_comparison
    ds = _make_ds_with_books()
    # Use edge_10pct (>=10%) which captures a wide range of the synthetic data
    result = compute_book_comparison(ds, edge_band="edge_10pct", cut="close")
    assert "book" in result.columns
    books = result["book"].to_list()
    assert "dk" in books
    assert "br" in books


def test_compute_book_comparison_with_odds_range():
    from mvp.analysis.dashboard.sharpness import compute_book_comparison
    ds = _make_ds_with_books()
    full = compute_book_comparison(ds, edge_band="edge_5pct", cut="close")
    filtered = compute_book_comparison(
        ds, edge_band="edge_5pct", cut="close", odds_range=(1.5, 2.5),
    )
    # Filtered should have fewer or equal bets per book
    if not full.is_empty() and not filtered.is_empty():
        full_n = dict(zip(
            full["book"].to_list(), full["n_bets"].to_list(),
        ))
        for row in filtered.iter_rows(named=True):
            assert row["n_bets"] <= full_n.get(row["book"], float("inf"))
