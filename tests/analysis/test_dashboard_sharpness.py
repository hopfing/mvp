"""Tests for book sharpness page data functions."""

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
