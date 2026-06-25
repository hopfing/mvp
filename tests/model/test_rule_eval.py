"""Tests for the rules decision-rule evaluator's countable logic."""

import polars as pl

from mvp.model.rule_eval import _config_order, _crosstab_cells, summarize


def _votes_df():
    # m1: two rows -> A net+1 (2-1), B net-1; A won        -> pick A, correct
    # m2: two rows -> A net+2 (2-0), B net-2; A lost        -> pick A, wrong
    # m3: single row net-1 (opp favored, no opp row)        -> no pick
    # m4: two rows net 0 (1-1 tie)                          -> no pick
    return pl.DataFrame(
        {
            "match_uid":     ["m1", "m1", "m2", "m2", "m3", "m4", "m4"],
            "won":           [1,    0,    0,    1,    0,    0,    1],
            "net":           [1,   -1,    2,   -2,   -1,    0,    0],
            "for_count":     [2,    1,    2,    0,    0,    1,    1],
            "against_count": [1,    2,    0,    2,    1,    1,    1],
            "circuit":       ["tour"] * 7,
            "year":          [2024] * 7,
        }
    )


def _picks_with_config():
    return _votes_df().filter(pl.col("net") > 0).with_columns(
        (pl.col("for_count").cast(pl.Utf8) + "-"
         + pl.col("against_count").cast(pl.Utf8)).alias("config")
    )


def test_one_pick_per_match():
    s = summarize(_votes_df())
    assert s["unique_matches"] == 4
    assert s["n_picks"] == 2          # m1 and m2, the higher-net side only
    assert s["single_no_pick"] == 1   # m3


def test_coverage_and_accuracy():
    s = summarize(_votes_df())
    assert abs(s["coverage"] - 0.5) < 1e-9    # 2 picks / 4 matches
    assert abs(s["accuracy"] - 0.5) < 1e-9    # m1 correct, m2 wrong


def test_config_order_by_net_then_for():
    # picks here are 2-1 and 2-0; 2-0 (net 2) must come before 2-1 (net 1).
    assert _config_order(_picks_with_config()) == ["2-0", "2-1"]


def test_crosstab_cells():
    picks = _picks_with_config()
    cells, allc = _crosstab_cells(picks, "circuit")
    assert cells[("tour", "2-1")] == (1, 1.0)   # m1: pick A, won
    assert cells[("tour", "2-0")] == (1, 0.0)   # m2: pick A, lost
    assert allc["tour"] == (2, 0.5)
