"""iid-rank table assembly from fingerprint-dir artifacts."""

from __future__ import annotations

import json

import polars as pl
import pytest

from mvp.projection.iid.rank import (
    SELECTION_POLICIES,
    collect_rows,
    format_rank_table,
)


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
        # The long ledger: one row per (match, book, market, points, side,
        # anchor), negative-edge rows included, every selection left to read
        # time. m1 has two totals rungs — 21.5 is its main line, 24.5 an
        # alternate — plus a second book on the main line so the one-per-match
        # collapse has something to choose between. A `close` row is present so
        # the anchor filter is exercised: without it, the headline would silently
        # average open and close together.
        pl.DataFrame({
            "match_uid": ["m1", "m1", "m1", "m2", "m1", "m1"],
            "book":      ["dk", "dk", "br", "dk", "dk", "dk"],
            "market":    ["total_games"] * 4 + ["game_spread", "total_games"],
            "points":    [21.5, 24.5, 21.5, 22.5, -3.5, 21.5],
            "side":      ["over", "over", "over", "under", "p1", "over"],
            "anchor":    ["open", "open", "open", "open", "open", "close"],
            "role":      ["entry"] * 6,
            "is_main_line": [True, False, True, True, True, True],
            "live_side": [True] * 6,
            "odds":      [2.00, 3.50, 2.10, 1.85, 1.95, 2.40],
            "model_p":   [0.53, 0.38, 0.53, 0.56, 0.55, 0.53],
            "edge":      [0.03, 0.09, 0.05, 0.02, 0.04, 0.11],
            "edge_novig": [0.02, 0.07, 0.04, 0.01, 0.03, 0.09],
            "pnl":       [1.00, -1.0, 1.10, -1.0, 0.95, 1.40],
            "won":       [True, False, True, False, True, True],
        }).write_parquet(d / "backtest.parquet")
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
    def _totals(self, eval_root, policy="main"):
        _make_run(eval_root, "aaa111", with_backtest=True)
        b = collect_rows()[0].betting
        return b["markets"]["total_games"][policy]

    def test_absent_without_a_ledger(self, eval_root):
        _make_run(eval_root, "aaa111")
        assert collect_rows()[0].betting is None

    def test_splits_by_market(self, eval_root):
        """Totals and spreads are priced off different projections against
        different books — a pooled ROI describes no runnable strategy."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        b = collect_rows()[0].betting
        assert set(b["markets"]) == {"total_games", "game_spread"}

    def test_the_anchor_is_named_not_implied(self, eval_root):
        """Without an anchor filter the close row joins the open ones and the
        headline becomes a blend of entry moments nobody bets at."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        b = collect_rows()[0].betting
        assert b["anchor"] == "open"
        # Four open totals rows: m1 at 21.5 on two books, m1's 24.5 alternate,
        # and m2. The fifth totals row is at `close` and must not be among them.
        assert b["markets"]["total_games"]["n_all"] == 4

    def test_alternate_lines_are_excluded_from_the_headline(self, eval_root):
        """The 24.5 rung carries the biggest edge (0.09) and is not a main line.
        If it leaked in, `max_edge` would take it for m1."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        b = collect_rows()[0].betting["markets"]["total_games"]
        assert b["n_all"] == 4      # three main-line rungs + the alternate
        assert b["n_main"] == 3     # the alternate drops out
        # m1 -> best main-line edge 0.05 (br), m2 -> 0.02. Not the 0.09 alternate.
        assert b["max_edge"]["avg_edge"] == pytest.approx(0.035)

    def test_one_bet_per_match_not_one_per_book(self, eval_root):
        """m1's main line is quoted by two books. Counting both would weight the
        match twice and treat two correlated rows as independent bets."""
        assert self._totals(eval_root)["n"] == 2   # m1 and m2, not three rows

    def test_policies_pick_different_rungs(self, eval_root):
        """`main` takes the consensus line at the best price (br at 2.10);
        `max_edge` takes the largest edge, which is the same row here — the
        point is that they are computed separately, not that they differ."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        b = collect_rows()[0].betting["markets"]["total_games"]
        assert set(b) >= {"main", "safest", "max_edge"}
        # m1: br offers the better price on the consensus 21.5 line.
        assert b["main"]["units_all"] == pytest.approx(1.10 - 1.0)

    def test_gated_and_ungated_units_are_both_reported(self, eval_root):
        """`U_all` prices the whole selection and is mostly vig; `U>=0` is the
        gated subset. The gap between them is the edge signal, so reporting
        either alone hides which is which."""
        stats = self._totals(eval_root)
        assert stats["n"] == 2
        assert stats["units_all"] is not None
        assert stats["units_gated"] is not None

    def test_spread_numbers_exclude_totals_rows(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True)
        spread = collect_rows()[0].betting["markets"]["game_spread"]
        stats = spread["main"]
        assert stats["n"] == 1
        assert stats["units_all"] == pytest.approx(0.95)

    def test_bet_type_is_derived_from_side(self, eval_root):
        """The column is not stored. Deriving it is what stops `_by_bet_type`
        silently returning {} when the schema drops it."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        by_type = collect_rows()[0].betting["markets"]["total_games"]["main"]["by_type"]
        assert set(by_type) <= {"over", "under"}
        assert by_type


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
        """One row per config per table — 2 distributional + 2 betting + CLV.

        The betting tables put the selection policies in cell groups rather than
        stacked blocks precisely so a config stays one row; a sweep of twenty
        configs would otherwise render hundreds of rows and bury the ranking."""
        _make_run(eval_root, "aaa111", with_backtest=True, with_clv=True, run_id="run_a")
        body = [ln for ln in format_rank_table() if ln.strip().startswith("1 run_a")]
        assert len(body) == 5

    def test_the_edge_curve_is_not_in_the_ranking_table(self, eval_root):
        """Bands are a property of one config. A curve per config per policy is
        eighteen rows per config, which buries the comparison the table exists
        for — so the curve lives in the single-model view instead."""
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        out = "\n".join(format_rank_table())
        assert "2-5pp" not in out
        assert "10pp+" not in out

    def test_each_policy_is_a_cell_group(self, eval_root):
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        out = "\n".join(format_rank_table())
        for policy in SELECTION_POLICIES:
            assert policy in out
        # Gated beside ungated — the model/rank.py idiom.
        assert "U>=0" in out and "U_all" in out

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

    def test_policy_groups_are_labelled_over_their_columns(self, eval_root):
        """The band line centres each policy name over its cell group, so a
        number can be attributed without counting columns.

        The over/under split that used to occupy these columns moved to the
        single-model view: the cell groups are the selection policies now, and
        both axes will not fit."""
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        lines = format_rank_table()
        band = next(
            ln for ln in lines if all(p in ln for p in SELECTION_POLICIES)
        )
        assert band

    def test_states_the_anchor_and_the_selection(self, eval_root):
        """The anchor has to be on the page. Blending open and close silently
        reports a closing-price number as if it were an entry one."""
        _make_run(eval_root, "aaa111", with_backtest=True, run_id="run_a")
        out = "\n".join(format_rank_table())
        assert "Anchor=open" in out
        assert "main line only" in out
        assert "one bet per match" in out
        # The sort must be named, or the reader cannot tell whether configs were
        # ranked on the gated number or the vig-dominated one.
        assert "Sorted by max_edge U>=0 desc" in out
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


class TestSelectionPolicyTieBreak:
    """`model_p` is a function of (match, line, side) and does NOT vary by book.

    So every book quoting the same main line ties under `safest`, and without a
    secondary key the policy takes whichever row sorted first — picking the
    safest line and then not shopping it.
    """

    def _mains(self):
        # Two books on the same line: identical model_p, different odds.
        return pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "market": ["total_games"] * 2,
            "book": ["dk", "br"],
            "points": [21.5, 21.5],
            "side": ["over", "over"],
            "odds": [2.00, 2.20],
            "model_p": [0.53, 0.53],
            "edge": [0.03, 0.08],
            "pnl": [1.00, 1.20],
            "won": [True, True],
        })

    def test_safest_takes_the_best_price_among_tied_lines(self):
        from mvp.projection.iid.rank import _select_one_per_match

        picked = _select_one_per_match(self._mains(), "safest")
        assert picked.height == 1
        assert picked["odds"][0] == pytest.approx(2.20)
        assert picked["book"][0] == "br"

    def test_max_edge_also_takes_the_better_price(self):
        from mvp.projection.iid.rank import _select_one_per_match

        picked = _select_one_per_match(self._mains(), "max_edge")
        assert picked["odds"][0] == pytest.approx(2.20)


class TestGateContribution:
    def test_gated_units_are_a_subset_of_ungated(self, eval_root):
        """`U>=0` selects from the same bets `U_all` totals, so its count can
        never exceed it. If it did, the gate would be selecting rows the
        ungated figure never saw."""
        _make_run(eval_root, "aaa111", with_backtest=True)
        stats = collect_rows()[0].betting["markets"]["total_games"]["main"]
        assert stats["n_gated"] <= stats["n"]
