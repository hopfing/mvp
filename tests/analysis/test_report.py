"""Tests for CLI analysis report."""

import polars as pl
import pytest


class TestAnalysisReport:
    def test_formats_summary(self):
        from mvp.analysis.report import format_summary

        ds = pl.DataFrame({
            "match_uid": ["m1", "m2", "m3"],
            "status": ["resolved", "resolved", "pending"],
            "model_correct": [True, False, None],
            "bet_side": ["P1", "", ""],
            "net": ["11.00", "", ""],
            "dk_closing_implied_p1": [0.50, 0.55, None],
            "p1_win_prob": [0.65, 0.55, 0.70],
        })

        output = format_summary(ds)
        assert "Predictions:" in output
        assert "3" in output  # total
        assert "2 resolved" in output
        assert "Model accuracy:" in output

    def test_empty_dataset(self):
        from mvp.analysis.report import format_summary

        ds = pl.DataFrame(schema={
            "match_uid": pl.Utf8,
            "status": pl.Utf8,
        })

        output = format_summary(ds)
        assert "No predictions" in output or "0" in output

    def test_pnl_included_when_bets_exist(self):
        from mvp.analysis.report import format_summary

        ds = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "status": ["resolved", "resolved"],
            "model_correct": [True, False],
            "bet_side": ["P1", "P2"],
            "net": ["11.00", "-15.00"],
        })

        output = format_summary(ds)
        assert "P&L" in output
