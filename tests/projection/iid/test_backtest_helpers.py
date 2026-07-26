"""Main-line selection and view gating for the IID backtest.

Context: the CSV emits every offered line, including negative-edge rows. It used
to emit only rows whose RAW edge cleared zero, which made the offered-line
universe unrecoverable — "main line" could only be the median of the lines the
model happened to like, and every summary view inherited a raw-edge gate no
matter which edge column it claimed to use.
"""

from __future__ import annotations

import polars as pl
import pytest

from mvp.projection.iid.backtest import (
    _offered_context,
    _print_view,
    _select_main_line,
)


def _rows(lines: list[float], market: str = "total_games", **extra) -> pl.DataFrame:
    n = len(lines)
    base = {
        "match_uid": ["m1"] * n,
        "market": [market] * n,
        "line": lines,
        "side": ["over"] * n,
        "book": [f"b{i}" for i in range(n)],
        "won": [1] * n,
        "profit": [0.9] * n,
        "edge": [0.05] * n,
        "edge_novig": [0.03] * n,
    }
    base.update(extra)
    return pl.DataFrame(base)


class TestOfferedContext:
    def test_median_count_and_main_over_every_line(self):
        joined = pl.DataFrame({
            "match_uid": ["m1"] * 5,
            "points": [19.5, 20.5, 21.5, 22.5, 23.5],
            "book": ["a", "a", "a", "a", "a"],
        })
        assert _offered_context(joined, "points")["m1"] == (21.5, 5, 21.5)

    def test_uses_magnitude_so_paired_spread_sides_collapse(self):
        joined = pl.DataFrame({
            "match_uid": ["m1"] * 4,
            "p1_points": [-3.5, 3.5, -4.5, 4.5],
            "book": ["a", "a", "a", "a"],
        })
        med, n_lines, main = _offered_context(joined, "p1_points")["m1"]
        assert (n_lines, med) == (2, 4.0)

    def test_ties_broken_by_book_count(self):
        """19.5 and 23.5 are equidistant from the median; 23.5 is at two books."""
        joined = pl.DataFrame({
            "match_uid": ["m1"] * 4,
            "points": [19.5, 21.5, 23.5, 23.5],
            "book": ["a", "a", "a", "b"],
        })
        joined = joined.filter(pl.col("points") != 21.5)
        _med, _n, main = _offered_context(joined, "points")["m1"]
        assert main == 23.5


class TestSelectMainLine:
    def test_reads_the_stamped_flag(self):
        df = _rows([19.5, 21.5, 25.5], is_main_line=[0, 1, 0])
        assert _select_main_line(df)["line"].to_list() == [21.5]

    def test_flag_survives_a_pre_filtered_frame(self):
        """The flag was computed over every offered line, so filtering the frame
        first can't move the main line — the failure mode of the fallback."""
        df = _rows([19.5, 21.5, 25.5], is_main_line=[0, 1, 0])
        pre_filtered = df.filter(pl.col("line") != 19.5)
        assert _select_main_line(pre_filtered)["line"].to_list() == [21.5]

    def test_flag_can_select_nothing_when_the_main_line_was_filtered_out(self):
        df = _rows([19.5, 25.5], is_main_line=[0, 0])
        assert len(_select_main_line(df)) == 0

    def test_falls_back_when_the_column_is_absent(self):
        df = _rows([19.5, 21.5, 23.5])
        out = _select_main_line(df)
        assert out["line"].to_list() == [21.5]

    def test_fallback_median_is_over_surviving_rows_only(self):
        """Documents the fallback's weakness: with the offer set truncated, the
        median is of what remains. This is why the stamped column exists."""
        df = _rows([25.5, 26.5, 27.5])
        assert _select_main_line(df)["line"].to_list() == [26.5]

    def test_empty_frame_passes_through(self):
        assert len(_select_main_line(pl.DataFrame())) == 0


def _view_rows(open_novig: list[float], open_raw: list[float]) -> pl.DataFrame:
    n = len(open_novig)
    return pl.DataFrame({
        "market": ["total_games"] * n,
        "bet_type": ["over"] * n,
        "open_edge": open_raw,
        "open_edge_novig": open_novig,
        "close_edge_novig": open_novig,
        "won": [1] + [0] * (n - 1),
        "pnl_open": [0.9] + [-1.0] * (n - 1),
        "pnl_close": [0.8] + [-1.0] * (n - 1),
        "clv_open": [0.01] * n,
    })


class TestSparsePriceColumns:
    """`formed` prices are null on most lines — two books must quote in the same
    15-minute bucket. A schema inferred from an all-null head then fails on the
    first real price further down."""

    def test_frame_builds_when_early_rows_are_all_null(self):
        rows = [
            {"match_uid": f"m{i}", "formed_odds": None, "open_odds": 1.9}
            for i in range(150)
        ]
        rows.append({"match_uid": "m999", "formed_odds": 1.81, "open_odds": 1.9})
        with pytest.raises(Exception):
            pl.DataFrame(rows)  # default infer_schema_length=100
        out = pl.DataFrame(rows, infer_schema_length=None)
        assert out["formed_odds"].drop_nulls().to_list() == [1.81]

    def test_price_cols_emits_none_for_a_missing_point(self):
        from mvp.projection.iid.backtest import _price_cols

        cols = _price_cols(
            0.55, own={"open": 2.0, "formed": None, "close": 1.9},
            other={"open": 2.0, "formed": None, "close": 2.1}, won=1,
        )
        assert cols["formed_odds"] is None
        assert cols["formed_edge"] is None
        assert cols["pnl_formed"] is None
        assert cols["clv_formed"] is None
        assert cols["open_odds"] == 2.0
        assert cols["clv_open"] == pytest.approx(1 / 1.9 - 1 / 2.0)


class TestPrintViewGating:
    def test_gates_on_the_named_edge_column(self, capsys):
        _print_view("T", _view_rows([0.02, -0.01, -0.03], [0.05] * 3), "open_edge_novig")
        assert "Bets: 1 of 3 considered" in capsys.readouterr().out

    def test_raw_and_novig_gates_select_different_sets(self, capsys):
        """The old emission gated on raw edge, so the NO-VIG views were a
        raw-edge bet set with a different label."""
        _print_view("T", _view_rows([0.02, -0.01, -0.03], [0.05] * 3), "open_edge")
        assert "Bets: 3 of 3 considered" in capsys.readouterr().out

    def test_reports_zero_without_dividing_by_zero(self, capsys):
        _print_view("T", _view_rows([-0.02], [0.05]), "open_edge_novig")
        assert "Bets: 0 of 1 rows considered" in capsys.readouterr().out

    def test_pnl_follows_the_price_point_of_the_edge_column(self, capsys):
        """A close-edge view must settle at close prices, not open."""
        from mvp.projection.iid.backtest import _pnl_col

        assert _pnl_col("open_edge_novig") == "pnl_open"
        assert _pnl_col("close_edge_novig") == "pnl_close"
        assert _pnl_col("formed_edge") == "pnl_formed"
