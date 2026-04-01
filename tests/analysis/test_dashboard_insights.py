# tests/analysis/test_dashboard_insights.py
"""Tests for insights dashboard page."""

import polars as pl


def _make_insights():
    """Sample insights DataFrame."""
    return pl.DataFrame({
        "depth": [0, 1, 1, 1, 2, 2],
        "dimensions": ["", "circuit", "circuit", "surface", "circuit|surface", "circuit|surface"],
        "filters": ["overall", "chal", "tour", "Hard", "chal | Hard", "tour | Clay"],
        "n": [100, 60, 40, 55, 30, 20],
        "accuracy": [0.68, 0.72, 0.62, 0.70, 0.80, 0.55],
        "roi": [0.03, 0.10, -0.08, 0.05, 0.20, -0.15],
        "pnl": [3.0, 6.0, -3.2, 2.75, 6.0, -3.0],
        "parent_dimensions": [None, "", "", "", "circuit", "circuit"],
        "parent_filters": [None, "overall", "overall", "overall", "chal", "tour"],
        "parent_roi": [None, 0.03, 0.03, 0.03, 0.10, -0.08],
        "roi_delta": [None, 0.07, -0.11, 0.02, 0.10, -0.07],
        "direction": [None, "outperformer", "danger_zone", "outperformer", "outperformer", "danger_zone"],
        "surprise": [None, 0.542, 0.696, 0.148, 0.548, 0.313],
    })


def test_filter_insights_by_depth():
    from mvp.analysis.dashboard.insights import filter_insights

    insights = _make_insights()

    depth_1 = filter_insights(insights, depth=1)
    assert all(d == 1 for d in depth_1["depth"].to_list())
    assert len(depth_1) == 3

    depth_2 = filter_insights(insights, depth=2)
    assert all(d == 2 for d in depth_2["depth"].to_list())
    assert len(depth_2) == 2


def test_filter_insights_by_direction():
    from mvp.analysis.dashboard.insights import filter_insights

    insights = _make_insights()

    dangers = filter_insights(insights, depth=1, direction="danger_zone")
    assert len(dangers) == 1
    assert dangers["filters"][0] == "tour"


def test_filter_insights_sorted_by_surprise():
    from mvp.analysis.dashboard.insights import filter_insights

    insights = _make_insights()
    result = filter_insights(insights, depth=1)

    surprises = result["surprise"].to_list()
    assert surprises == sorted(surprises, reverse=True)
