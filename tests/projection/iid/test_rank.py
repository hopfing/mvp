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
        pl.DataFrame({
            "match_uid": ["m1", "m1", "m2"],
            "market": ["total_games"] * 3,
            "line": [21.5, 21.5, 22.5],
            "side": ["over", "over", "under"],
            "book": ["dk", "br", "dk"],
            "odds": [1.90, 2.00, 1.85],
            "edge_novig": [0.03, 0.05, 0.02],
            "won": [1, 1, 0],
            "profit": [0.90, 1.00, -1.0],
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

    def test_fold_spread_is_computed(self, eval_root):
        _make_run(eval_root, "aaa111", folds=(2.80, 2.99))
        assert collect_rows()[0].fold_spread == (2.80, 2.99)

    def test_single_fold_has_no_spread(self, eval_root):
        _make_run(eval_root, "aaa111", folds=(2.80,))
        assert collect_rows()[0].fold_spread is None


class TestBettingSummary:
    def test_absent_without_backtest_csv(self, eval_root):
        _make_run(eval_root, "aaa111")
        assert collect_rows()[0].betting is None

    def test_dedupes_to_best_price_across_books(self, eval_root):
        """The same bet offered at two books is one bet, taken at the better price."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        b = collect_rows()[0].betting
        assert b["n_bets"] == 2          # 3 rows, two of which are the same bet
        assert b["pl_units"] == pytest.approx(0.0)   # took 2.00, not 1.90


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
        assert "CRPS_tot" in out

    def test_one_line_per_run(self, eval_root):
        """Matches model-rank: a run is a row, not a multi-line block."""
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        lines = format_rank_table()
        assert len([ln for ln in lines if ln.startswith("run_a")]) == 1

    def test_betting_uses_the_main_line_novig_cut(self, eval_root):
        """The CSV is a broad ledger; the summary must not report it raw."""
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        rows = collect_rows()
        assert rows[0].betting["n_all"] >= rows[0].betting["n_bets"]
        out = "\n".join(format_rank_table())
        assert "MAIN LINE, no-vig" in out
        assert "Ledger vs bettable" in out

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
