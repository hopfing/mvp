# tests/analysis/test_dashboard_bets.py
"""Tests for Bet Performance page data functions."""

import polars as pl


def _make_bets() -> pl.DataFrame:
    """Bets with as-bet (bet_pred_side) and live (pred_side) picks.

    m1/m2: no flip. m3: model flipped away after the bet (bet on P1, model
    now picks P2). m4: bet_pred_side null (no flip detectable).
    """
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3", "m4"],
        "bet_side": ["P1", "P2", "P1", "P1"],
        "bet_pred_side": ["P1", "P2", "P1", None],
        "pred_side": ["P1", "P2", "P2", "P1"],
        "p1_name": ["A1", "B1", "C1", "D1"],
        "p2_name": ["A2", "B2", "C2", "D2"],
        "bet_result": ["W", "L", "W", "W"],
        "net": [11.0, -15.0, 30.0, 10.0],
    })


def test_flipped_bets_selects_only_flips():
    from mvp.analysis.dashboard.bets import _flipped_bets

    flipped = _flipped_bets(_make_bets())
    assert flipped["match_uid"].to_list() == ["m3"]
    assert flipped["bet_pred_side"][0] == "P1"
    assert flipped["pred_side"][0] == "P2"


def test_flipped_bets_empty_when_columns_absent():
    from mvp.analysis.dashboard.bets import _flipped_bets

    df = pl.DataFrame({"match_uid": ["m1"], "bet_side": ["P1"]})  # no pick cols
    assert len(_flipped_bets(df)) == 0


def test_flipped_bets_ignores_non_bet_and_null_sides():
    from mvp.analysis.dashboard.bets import _flipped_bets

    df = pl.DataFrame({
        "match_uid": ["m1", "m2"],
        "bet_side": ["P1", "P1"],
        "bet_pred_side": [None, "P1"],
        "pred_side": ["P2", None],
    })
    # m1: as-bet null; m2: live null -> neither is a detectable flip
    assert len(_flipped_bets(df)) == 0
