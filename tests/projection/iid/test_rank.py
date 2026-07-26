"""iid-rank table assembly from fingerprint-dir artifacts."""

from __future__ import annotations

import json

import polars as pl
import pytest

from mvp.projection.iid.rank import collect_rows, format_rank_table


@pytest.fixture
def eval_root(tmp_path, monkeypatch):
    root = tmp_path / "dataroot"
    (root / "projection_evaluations").mkdir(parents=True)
    monkeypatch.setenv("MVP_DATA_ROOT", str(root))
    return root / "projection_evaluations"


def _make_run(
    eval_root, fp, *, crps=2.90, source="parent", run_id=None,
    with_backtest=False, with_clv=False, folds=(2.85, 2.95),
):
    d = eval_root / fp
    d.mkdir(parents=True, exist_ok=True)
    (d / "projection.json").write_text(json.dumps({
        "metrics": {
            "iid_crps_total_games": crps,
            "iid_crps_spread": crps + 0.5,
            "iid_total_cal": 0.021,
            "iid_total_cal_max": 0.044,
            "signed_total_bias": -0.12,
        },
        "fold_metrics": [{"iid_crps_total_games": v} for v in folds],
        "n_folds": len(folds),
    }))
    (d / "source.txt").write_text(
        f"{source}\t{run_id or fp}\t2026-07-26T12:00:00\n", encoding="utf-8",
    )
    if with_backtest:
        # One row per (match, market, line, side) — best-across-books happens at
        # pricing time. m1 offers two total lines; only 21.5 is the main line.
        # A spread row is included so the market split is exercised.
        pl.DataFrame({
            "match_uid": ["m1", "m1", "m2", "m1"],
            "market": ["total_games"] * 3 + ["game_spread"],
            "line": [21.5, 24.5, 22.5, -3.5],
            "side": ["over", "over", "under", "p1"],
            "is_main_line": [1, 0, 1, 1],
            "open_odds": [2.00, 3.50, 1.85, 1.95],
            "open_edge_novig": [0.03, 0.09, 0.02, 0.04],
            "pnl_open": [1.00, -1.0, -1.0, 0.95],
            "clv_open": [0.004, -0.02, 0.001, 0.010],
            "won": [1, 0, 0, 1],
        }).write_csv(d / "backtest.csv")
    if with_clv:
        (d / "clv.json").write_text(json.dumps({
            "n": 3800, "avg_clvpin": 0.0031, "positive_rate": 0.54,
        }))
    return d


class TestCollectRows:
    def test_skips_dirs_without_projection_json(self, eval_root):
        (eval_root / "empty").mkdir()
        assert collect_rows() == []

    def test_reads_metrics_and_sources(self, eval_root):
        _make_run(eval_root, "aaa111", crps=2.88, source="totals", run_id="totals__d01_t5")
        rows = collect_rows()
        assert len(rows) == 1
        assert rows[0].metrics["iid_crps_total_games"] == 2.88
        assert rows[0].sources == ["totals"]
        assert rows[0].label == "totals__d01_t5"

    def test_source_filter(self, eval_root):
        _make_run(eval_root, "aaa111", source="totals")
        _make_run(eval_root, "bbb222", source="other")
        assert len(collect_rows(source="totals")) == 1

    def test_fold_metrics_are_retained(self, eval_root):
        _make_run(eval_root, "aaa111", folds=(2.80, 2.99))
        assert len(collect_rows()[0].fold_metrics) == 2

    def test_fold_spread_rendered_per_market(self, eval_root):
        _make_run(eval_root, "aaa111", folds=(2.80, 2.99), run_id="run_a")
        out = "\n".join(format_rank_table())
        assert "0.190" in out  # 2.99 - 2.80 on the totals row


class TestBettingSummary:
    def test_absent_without_backtest_csv(self, eval_root):
        _make_run(eval_root, "aaa111")
        assert collect_rows()[0].betting is None

    def test_splits_by_market(self, eval_root):
        """Totals and spreads are priced off different projections against
        different books — a pooled ROI describes no runnable strategy."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        b = collect_rows()[0].betting
        assert set(b) == {"total_games", "game_spread"}

    def test_reports_the_main_line_subset_per_market(self, eval_root):
        """Alternate lines are where the fake edge lives, so the headline is the
        main-line cut and n_all keeps the gap visible."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        totals = collect_rows()[0].betting["total_games"]
        assert totals["n_all"] == 3
        assert totals["n_bets"] == 2       # the 24.5 alternate line drops out
        assert totals["pl_units"] == pytest.approx(0.0)

    def test_spread_numbers_exclude_totals_rows(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True)
        spread = collect_rows()[0].betting["game_spread"]
        assert spread["n_bets"] == 1
        assert spread["pl_units"] == pytest.approx(0.95)
        assert spread["avg_clv"] == pytest.approx(0.010)

    def test_settles_at_the_entry_price(self, eval_root):
        """Entry is at open, so pnl_open is the settlement column."""
        totals_only = collect_rows
        _make_run(eval_root, "aaa111", with_backtest=True)
        totals = totals_only()[0].betting["total_games"]
        assert totals["roi"] == pytest.approx(0.0)
        assert totals["avg_clv"] == pytest.approx((0.004 + 0.001) / 2)


class TestFormatting:
    def test_empty_state_explains_how_to_populate(self, eval_root):
        out = "\n".join(format_rank_table())
        assert "No evaluated IID projection configs found" in out
        assert "iid-sweep" in out

    def test_sorted_by_crps_ascending(self, eval_root):
        _make_run(eval_root, "worse", crps=2.95, run_id="worse")
        _make_run(eval_root, "better", crps=2.85, run_id="better")
        lines = format_rank_table()
        body = [ln for ln in lines if ln.startswith(("better", "worse"))]
        assert body[0].startswith("better")

    def test_top_n_limits_rows(self, eval_root):
        for i in range(4):
            _make_run(eval_root, f"fp{i}", crps=2.9 + i / 100, run_id=f"r{i}")
        lines = format_rank_table(top_n=2)
        # "run" is the header; count only the r0/r1/... data rows.
        data_rows = [ln for ln in lines if ln[:2] in {"r0", "r1", "r2", "r3"}]
        assert len(data_rows) == 2

    def test_shows_all_three_instruments(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        out = "\n".join(format_rank_table())
        for band in ("distributional", "soft-book", "sharp CLV"):
            assert band in out
        assert "CRPS" in out

    def test_one_row_per_run_and_market(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        body = [ln for ln in format_rank_table() if ln.startswith("run_a")]
        assert len(body) == 2
        assert any(" totals " in ln for ln in body)
        assert any(" spread " in ln for ln in body)

    def test_clv_only_on_the_totals_row(self, eval_root):
        """The oddspapi scorer prices total games; there is no spread equivalent,
        so the spread row must not borrow the totals CLV."""
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        body = [ln for ln in format_rank_table() if ln.startswith("run_a")]
        totals_row = next(ln for ln in body if " totals " in ln)
        spread_row = next(ln for ln in body if " spread " in ln)
        assert "3800" in totals_row
        assert "3800" not in spread_row

    def test_states_the_betting_gate(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        out = "\n".join(format_rank_table())
        assert "MAIN LINE, open no-vig edge>0" in out
        assert "never pooled" in out

    def test_reports_what_is_missing(self, eval_root):
        _make_run(eval_root, "aaa111", run_id="run_a")
        out = "\n".join(format_rank_table())
        assert "1/1 runs have no backtest" in out
        assert "1/1 have no CLV" in out

    def test_no_composite_score_column(self, eval_root):
        """Instruments are shown side by side, never collapsed into one number."""
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True)
        out = "\n".join(format_rank_table()).lower()
        assert "composite" not in out
        assert "overall score" not in out
