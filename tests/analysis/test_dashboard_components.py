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
