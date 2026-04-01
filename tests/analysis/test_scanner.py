# tests/analysis/test_scanner.py
"""Tests for insight scanner."""

import polars as pl
import pytest


def _make_resolved_ds():
    """Minimal resolved analysis dataset with all dimension columns."""
    import random
    random.seed(99)
    n = 60
    return pl.DataFrame({
        "match_uid": [f"m{i}" for i in range(n)],
        "status": ["resolved"] * n,
        "model_correct": [random.choice([True, False]) for _ in range(n)],
        "pred_odds_best_close": [random.uniform(1.1, 4.0) for _ in range(n)],
        "model_edge_best_close": [random.uniform(-0.12, 0.15) for _ in range(n)],
        "consensus": [random.choice([1.0, 0.8, 0.6]) for _ in range(n)],
        "circuit": [random.choice(["chal", "tour"]) for _ in range(n)],
        "surface": [random.choice(["Hard", "Clay"]) for _ in range(n)],
    })


def test_bucket_dimensions():
    from mvp.analysis.scanner import bucket_dimensions

    ds = _make_resolved_ds()
    result = bucket_dimensions(ds)

    assert "odds_bucket" in result.columns
    assert "edge_bucket" in result.columns
    assert "consensus" in result.columns
    assert "circuit" in result.columns
    assert "surface" in result.columns


def test_bucket_dimensions_odds_labels():
    from mvp.analysis.scanner import bucket_dimensions, ODDS_LABELS

    ds = _make_resolved_ds()
    result = bucket_dimensions(ds)
    buckets = result["odds_bucket"].unique().to_list()
    for b in buckets:
        assert b in ODDS_LABELS


def test_bucket_dimensions_edge_labels():
    from mvp.analysis.scanner import bucket_dimensions, EDGE_LABELS

    ds = _make_resolved_ds()
    result = bucket_dimensions(ds)
    buckets = result["edge_bucket"].unique().to_list()
    for b in buckets:
        assert b in EDGE_LABELS


def test_compute_slices_depth_0():
    from mvp.analysis.scanner import bucket_dimensions, compute_slices

    ds = _make_resolved_ds()
    bucketed = bucket_dimensions(ds)
    slices = compute_slices(bucketed, max_depth=0)

    assert len(slices) == 1
    row = slices.row(0, named=True)
    assert row["depth"] == 0
    assert row["dimensions"] == ""
    assert row["filters"] == "overall"
    assert row["n"] == len(bucketed)


def test_compute_slices_depth_1():
    from mvp.analysis.scanner import bucket_dimensions, compute_slices

    ds = _make_resolved_ds()
    bucketed = bucket_dimensions(ds)
    slices = compute_slices(bucketed, max_depth=1)

    depth_1 = slices.filter(pl.col("depth") == 1)
    assert len(depth_1) > 0

    circuit_slices = depth_1.filter(pl.col("dimensions") == "circuit")
    assert len(circuit_slices) == 2  # chal, tour


def test_compute_slices_depth_2():
    from mvp.analysis.scanner import bucket_dimensions, compute_slices

    ds = _make_resolved_ds()
    bucketed = bucket_dimensions(ds)
    slices = compute_slices(bucketed, max_depth=2)

    depth_2 = slices.filter(pl.col("depth") == 2)
    assert len(depth_2) > 0

    dims = depth_2["dimensions"].to_list()
    assert all("|" in d for d in dims)


def test_compute_slices_respects_min_n():
    from mvp.analysis.scanner import bucket_dimensions, compute_slices

    ds = _make_resolved_ds()
    bucketed = bucket_dimensions(ds)
    slices = compute_slices(bucketed, max_depth=1, min_n=100)

    # With min_n=100 and only 60 rows, only depth-0 should survive
    assert all(d == 0 for d in slices["depth"].to_list())


def test_compute_slices_has_roi():
    from mvp.analysis.scanner import bucket_dimensions, compute_slices

    ds = _make_resolved_ds()
    bucketed = bucket_dimensions(ds)
    slices = compute_slices(bucketed, max_depth=1)

    assert "roi" in slices.columns
    assert "accuracy" in slices.columns
    assert "pnl" in slices.columns
