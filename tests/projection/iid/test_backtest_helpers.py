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


class TestPrintViewGating:
    def test_gates_on_the_named_edge_column(self, capsys):
        df = pl.DataFrame({
            "market": ["total_games"] * 3,
            "bet_type": ["over"] * 3,
            "edge": [0.05, 0.05, 0.05],
            "edge_novig": [0.02, -0.01, -0.03],
            "won": [1, 0, 0],
            "profit": [0.9, -1.0, -1.0],
        })
        _print_view("T", df, "edge_novig")
        out = capsys.readouterr().out
        assert "Bets: 1 of 3 considered" in out

    def test_raw_and_novig_gates_select_different_sets(self, capsys):
        df = pl.DataFrame({
            "market": ["total_games"] * 3,
            "bet_type": ["over"] * 3,
            "edge": [0.05, 0.05, 0.05],
            "edge_novig": [0.02, -0.01, -0.03],
            "won": [1, 0, 0],
            "profit": [0.9, -1.0, -1.0],
        })
        _print_view("T", df, "edge")
        raw_out = capsys.readouterr().out
        assert "Bets: 3 of 3 considered" in raw_out

    def test_reports_zero_without_dividing_by_zero(self, capsys):
        df = pl.DataFrame({
            "market": ["total_games"],
            "bet_type": ["over"],
            "edge": [0.05],
            "edge_novig": [-0.02],
            "won": [0],
            "profit": [-1.0],
        })
        _print_view("T", df, "edge_novig")
        assert "Bets: 0 of 1 rows considered" in capsys.readouterr().out
