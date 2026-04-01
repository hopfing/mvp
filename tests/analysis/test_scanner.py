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
