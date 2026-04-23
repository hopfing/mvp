"""Tests for shared dashboard components."""

import polars as pl


def test_metric_card_returns_dict():
    from mvp.analysis.dashboard.components import metric_card_data

    result = metric_card_data("Accuracy", 0.682, fmt=".1%")
    assert result["label"] == "Accuracy"
    assert result["value"] == "68.2%"


def test_metric_card_with_delta():
    from mvp.analysis.dashboard.components import metric_card_data

    result = metric_card_data("ROI", 0.05, fmt=".1%", delta=0.02, delta_fmt=".1%")
    assert result["value"] == "5.0%"
    assert result["delta"] == "2.0%"


def test_metric_card_none_value():
    from mvp.analysis.dashboard.components import metric_card_data

    result = metric_card_data("P&L", None, fmt=".2f")
    assert result["value"] == "—"


def test_style_roi_column():
    from mvp.analysis.dashboard.components import style_roi

    assert "green" in style_roi(0.15).lower() or "4caf50" in style_roi(0.15).lower()
    assert "red" in style_roi(-0.10).lower() or "ef5350" in style_roi(-0.10).lower()
    assert style_roi(0.0) is not None


def test_format_edge_table():
    from mvp.analysis.dashboard.components import format_sim_table

    sims = pl.DataFrame({
        "scenario": ["edge_10pct", "edge_5pct"],
        "segment": ["overall", "overall"],
        "segment_value": ["overall", "overall"],
        "n_bets": [20, 30],
        "accuracy": [0.70, 0.55],
        "roi": [0.15, -0.05],
        "net_pnl": [3.0, -1.5],
        "model_version": ["all", "all"],
    })

    result = format_sim_table(sims, scenarios=["edge_10pct", "edge_5pct"])
    assert len(result) == 2
    assert "N" in result.columns


def test_expand_by_book_splits_two_book_rows():
    from mvp.analysis.dashboard.components import expand_by_book

    bets = pl.DataFrame({
        "match_uid": ["M1", "M2"],
        "book": ["BR", "DK"],
        "book2": ["DK", ""],
        "stake": ["100", "50"],
        "net": ["10", "-50"],
        "bet_result": ["W", "L"],
    })
    out = expand_by_book(bets)
    assert "book2" not in out.columns
    assert out.height == 3

    m1 = out.filter(pl.col("match_uid") == "M1").sort("book")
    assert m1["book"].to_list() == ["BR", "DK"]
    assert m1["stake"].to_list() == [50.0, 50.0]
    assert m1["net"].to_list() == [5.0, 5.0]
    assert m1["bet_result"].to_list() == ["W", "W"]

    m2 = out.filter(pl.col("match_uid") == "M2")
    assert m2.height == 1
    assert m2["book"][0] == "DK"
    assert m2["stake"][0] == 50.0
    assert m2["net"][0] == -50.0


def test_expand_by_book_drops_rows_without_book():
    from mvp.analysis.dashboard.components import expand_by_book

    bets = pl.DataFrame({
        "match_uid": ["M1", "M2"],
        "book": ["BR", ""],
        "book2": ["", ""],
        "stake": ["100", "50"],
        "net": ["10", "-50"],
    })
    out = expand_by_book(bets)
    assert out.height == 1
    assert out["match_uid"][0] == "M1"


def test_expand_by_book_no_book2_column():
    from mvp.analysis.dashboard.components import expand_by_book

    bets = pl.DataFrame({
        "match_uid": ["M1"],
        "book": ["BR"],
        "stake": ["100"],
        "net": ["10"],
    })
    out = expand_by_book(bets)
    assert out.height == 1
    assert out["stake"][0] == 100.0
    assert out["net"][0] == 10.0
