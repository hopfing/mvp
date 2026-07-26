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
            "bet_type": ["over", "over", "under", "favorite"],
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

    def test_one_table_per_instrument_and_market(self, eval_root):
        """Cramming instruments into one row is what capped how much each could
        show; pooling markets invited a comparison that makes no sense."""
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        out = "\n".join(format_rank_table())
        for title in (
            "Table 1: TOTAL GAMES — distributional",
            "Table 2: GAME SPREAD — distributional",
            "Table 3: TOTAL GAMES — betting",
            "Table 4: GAME SPREAD — betting",
            "Table 5: TOTAL GAMES — sharp CLV",
        ):
            assert title in out

    def test_sorted_by_crps_ascending(self, eval_root):
        _make_run(eval_root, "worse", crps=2.95, run_id="worse")
        _make_run(eval_root, "better", crps=2.85, run_id="better")
        body = [ln for ln in format_rank_table() if " better" in ln or " worse" in ln]
        assert " better" in body[0]

    def test_rows_are_ranked(self, eval_root):
        _make_run(eval_root, "aaa111", crps=2.85, run_id="first")
        _make_run(eval_root, "bbb222", crps=2.95, run_id="second")
        body = [ln for ln in format_rank_table() if " first" in ln or " second" in ln]
        assert body[0].strip().startswith("1 ")
        assert body[1].strip().startswith("2 ")

    def test_top_n_limits_rows(self, eval_root):
        for i in range(4):
            _make_run(eval_root, f"fp{i}", crps=2.9 + i / 100, run_id=f"r{i}")
        out = "\n".join(format_rank_table(top_n=2))
        assert "r0" in out and "r1" in out
        assert "r2" not in out and "r3" not in out

    def test_variant_tag_is_parenthesised(self, eval_root):
        """Sweep trials share a config name; only the variant should differ."""
        _make_run(eval_root, "aaa111", run_id="totals_cfg__d01_t8")
        out = "\n".join(format_rank_table())
        assert "totals_cfg (d01_t8)" in out

    def test_plain_run_id_has_no_parens(self, eval_root):
        _make_run(eval_root, "aaa111", run_id="totals_cfg")
        out = "\n".join(format_rank_table())
        assert "totals_cfg (" not in out

    def test_run_appears_once_per_table(self, eval_root):
        """4 market tables + the CLV table = 5 rows for one run."""
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        body = [ln for ln in format_rank_table() if ln.strip().startswith("1 run_a")]
        assert len(body) == 5

    def test_spread_table_omits_a_run_with_no_spread_bets(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        lines = format_rank_table()
        start = next(i for i, ln in enumerate(lines) if "Table 4" in ln)
        end = next(i for i, ln in enumerate(lines) if "Table 5" in ln)
        assert any("run_a" in ln for ln in lines[start:end])

    def test_clv_table_excludes_spreads(self, eval_root):
        """The oddspapi scorer prices total games; there is no spread equivalent."""
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        lines = format_rank_table()
        start = next(i for i, ln in enumerate(lines) if "Table 3" in ln)
        clv_block = "\n".join(lines[start:])
        assert "3800" in clv_block
        assert " spread " not in clv_block

    def test_sides_are_columns_per_market(self, eval_root):
        """A tuning change often floods one side rather than moving ROI, so the
        split gets real columns rather than a crammed string."""
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        lines = format_rank_table()
        totals_band = next(ln for ln in lines if "over" in ln and "under" in ln)
        spread_band = next(ln for ln in lines if "fav" in ln and "dog" in ln)
        assert totals_band and spread_band
        assert "over:" not in "\n".join(lines)   # not the old inline form

    def test_states_the_betting_gate(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        out = "\n".join(format_rank_table())
        assert "Main line, open entry, no-vig edge>0" in out
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
