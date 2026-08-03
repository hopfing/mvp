"""Board reconstruction from the oddspapi tick stream.

Context: the stage layer is one row per side of one rung at one instant, with ticks
emitted only where a price moved. Answering "what was on offer at T" therefore means
carrying each rung's most recent quote forward, then pivoting the two sides onto a row
so balance and de-vig — which are properties of the pair — can be computed at all.

The load-bearing distinctions under test: a one-sided rung is kept but cannot be main
line; main line is read off DE-VIGGED probabilities rather than raw odds, so it
measures the book's opinion and not its margin; and `n_two_sided` is what separates a
main line that was selected from one that merely names the only rung priced.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from mvp.oddspapi import paths
from mvp.oddspapi.board import (
    BOARD_SCHEMA,
    _flag_main_line,
    _pivot_two_sided,
    board_at,
    coverage_and_agreement,
)

T0 = datetime(2026, 6, 1, 12, 0, 0)


def _sides(rows: list[tuple], t: datetime = T0) -> pl.DataFrame:
    """rows of (points, side, odds, quoted_at_offset_s, active)."""
    return pl.DataFrame(
        {
            "match_uid": ["m1"] * len(rows),
            "points": [r[0] for r in rows],
            "side": [r[1] for r in rows],
            "odds": [r[2] for r in rows],
            "quoted_at": [t - timedelta(seconds=r[3]) for r in rows],
            "active": [r[4] for r in rows],
            "t": [t] * len(rows),
        }
    )


def _board(rows: list[tuple], book: str = "bk") -> pl.DataFrame:
    return _flag_main_line(
        _pivot_two_sided(_sides(rows), "Over", "Under").with_columns(
            pl.lit(book).alias("book")
        )
    )


class TestDevig:
    def test_proportional_devig_of_a_balanced_pair(self):
        out = _pivot_two_sided(
            _sides([(22.5, "Over", 2.0, 0, True), (22.5, "Under", 2.0, 0, True)]),
            "Over",
            "Under",
        )
        assert out["p_over"][0] == pytest.approx(0.5)
        assert out["overround"][0] == pytest.approx(1.0)

    def test_overround_above_one_is_the_book_margin(self):
        out = _pivot_two_sided(
            _sides([(22.5, "Over", 1.91, 0, True), (22.5, "Under", 1.91, 0, True)]),
            "Over",
            "Under",
        )
        assert out["overround"][0] > 1.0
        # margin is symmetric, so the de-vigged pair is still balanced
        assert out["p_over"][0] == pytest.approx(0.5)

    def test_imbalance_is_distance_from_even_not_from_the_raw_price(self):
        out = _pivot_two_sided(
            _sides([(22.5, "Over", 1.5, 0, True), (22.5, "Under", 2.5, 0, True)]),
            "Over",
            "Under",
        )
        assert out["p_over"][0] == pytest.approx(0.625)
        assert out["imbalance"][0] == pytest.approx(0.125)

    def test_quote_age_comes_from_the_stalest_side(self):
        """A de-vigged pair is only as current as its older leg, so that is what a
        staleness gate must see. `last_change_s` carries the freshest leg separately."""
        out = _pivot_two_sided(
            _sides([(22.5, "Over", 2.0, 600, True), (22.5, "Under", 2.0, 120, True)]),
            "Over",
            "Under",
        )
        assert out["quote_age_s"][0] == 600
        assert out["last_change_s"][0] == 120

    def test_participant_sided_market_is_refused_not_guessed(self):
        """Sides "1"/"2" are positional and need side_player_id to orient. Producing a
        board keyed to the wrong player is worse than refusing."""
        with pytest.raises(ValueError, match="side_player_id"):
            _pivot_two_sided(
                _sides([(-2.5, "1", 1.9, 0, True), (-2.5, "2", 1.9, 0, True)]),
                "Over",
                "Under",
            )


class TestOneSidedRungs:
    def test_one_sided_rung_is_kept(self):
        out = _board([(22.5, "Over", 1.9, 0, True)])
        assert out.height == 1

    def test_one_sided_rung_has_no_devig_and_no_imbalance(self):
        out = _board([(22.5, "Over", 1.9, 0, True)])
        assert out["p_over"][0] is None
        assert out["imbalance"][0] is None

    def test_one_sided_rung_cannot_be_main_line(self):
        out = _board(
            [
                (22.5, "Over", 1.9, 0, True),
                (23.5, "Over", 1.95, 0, True),
                (23.5, "Under", 1.95, 0, True),
            ]
        )
        main = out.filter(pl.col("is_main_line"))
        assert main.height == 1
        assert main["points"][0] == 23.5

    def test_a_board_of_only_one_sided_rungs_has_no_main_line(self):
        out = _board([(22.5, "Over", 1.9, 0, True), (23.5, "Under", 1.9, 0, True)])
        assert out.filter(pl.col("is_main_line")).height == 0
        assert out["n_two_sided"].max() == 0


class TestMainLine:
    def test_picks_the_rung_closest_to_balanced(self):
        out = _board(
            [
                (21.5, "Over", 1.40, 0, True),
                (21.5, "Under", 2.90, 0, True),
                (22.5, "Over", 1.91, 0, True),
                (22.5, "Under", 1.95, 0, True),
                (23.5, "Over", 2.80, 0, True),
                (23.5, "Under", 1.42, 0, True),
            ]
        )
        assert out.filter(pl.col("is_main_line"))["points"][0] == 22.5

    def test_exactly_one_main_line_per_book(self):
        out = _board(
            [
                (22.5, "Over", 1.91, 0, True),
                (22.5, "Under", 1.95, 0, True),
                (23.5, "Over", 1.80, 0, True),
                (23.5, "Under", 2.05, 0, True),
            ]
        )
        assert out.filter(pl.col("is_main_line")).height == 1

    def test_a_lone_two_sided_rung_is_its_own_main_line(self):
        out = _board([(22.5, "Over", 1.4, 0, True), (22.5, "Under", 2.9, 0, True)])
        main = out.filter(pl.col("is_main_line"))
        assert main.height == 1
        assert main["n_two_sided"][0] == 1
        # nothing to beat, so no separation is claimed
        assert main["separation"][0] is None

    def test_separation_is_the_runner_up_margin(self):
        out = _board(
            [
                (22.5, "Over", 2.00, 0, True),
                (22.5, "Under", 2.00, 0, True),
                (23.5, "Over", 1.50, 0, True),
                (23.5, "Under", 3.00, 0, True),
            ]
        )
        main = out.filter(pl.col("is_main_line"))
        assert main["points"][0] == 22.5
        # pick imbalance 0.0, runner-up 0.1667
        assert main["separation"][0] == pytest.approx(1 / 6, abs=1e-4)

    def test_devig_ranking_differs_from_the_raw_implied_gap(self):
        """De-vig measures opinion; the raw gap conflates it with margin.

        22.5: implied 0.6250/0.6024, gap 0.02259, overround 1.2274 -> imbalance 0.00920
        23.5: implied 0.5155/0.4951, gap 0.02041, overround 1.0105 -> imbalance 0.01010

        The raw gap ranks 23.5 tighter; normalising by each rung's own overround
        reverses it. This is the case the choice of de-vigging actually decides.
        """
        out = _board(
            [
                (22.5, "Over", 1.60, 0, True),
                (22.5, "Under", 1.66, 0, True),
                (23.5, "Over", 1.94, 0, True),
                (23.5, "Under", 2.02, 0, True),
            ]
        )
        assert out.filter(pl.col("is_main_line"))["points"][0] == 22.5

    def test_inactive_rung_is_not_eligible(self):
        out = _board(
            [
                (22.5, "Over", 2.00, 0, False),
                (22.5, "Under", 2.00, 0, False),
                (23.5, "Over", 1.80, 0, True),
                (23.5, "Under", 2.05, 0, True),
            ]
        )
        main = out.filter(pl.col("is_main_line"))
        assert main.height == 1
        assert main["points"][0] == 23.5
        # the dead rung is not merely unpicked, it is not a candidate
        assert main["n_two_sided"][0] == 1

    def test_n_two_sided_separates_a_selection_from_a_tautology(self):
        selected = _board(
            [
                (22.5, "Over", 1.91, 0, True),
                (22.5, "Under", 1.95, 0, True),
                (23.5, "Over", 1.50, 0, True),
                (23.5, "Under", 2.60, 0, True),
            ]
        )
        tautology = _board(
            [(22.5, "Over", 1.91, 0, True), (22.5, "Under", 1.95, 0, True)]
        )
        assert selected.filter(pl.col("is_main_line"))["n_two_sided"][0] == 2
        assert tautology.filter(pl.col("is_main_line"))["n_two_sided"][0] == 1


class TestCoverageAndAgreement:
    def _multi_book(self) -> pl.DataFrame:
        frames = []
        for book, line in [("a", 22.5), ("b", 22.5), ("c", 22.5), ("d", 23.5)]:
            frames.append(
                _board([(line, "Over", 1.91, 0, True), (line, "Under", 1.95, 0, True)],
                       book=book)
            )
        return pl.concat(frames)

    def test_support_counts_books_per_line(self):
        ca = coverage_and_agreement(self._multi_book())
        assert ca.filter(pl.col("points") == 22.5)["n_books_on_line"][0] == 3
        assert ca.filter(pl.col("points") == 23.5)["n_books_on_line"][0] == 1

    def test_coverage_is_books_not_lines(self):
        ca = coverage_and_agreement(self._multi_book())
        assert set(ca["n_books_live"].to_list()) == {4}

    def test_line_spread_is_the_range_across_books(self):
        ca = coverage_and_agreement(self._multi_book())
        assert set(ca["line_spread"].to_list()) == {1.0}

    def test_unanimous_books_have_zero_spread(self):
        frames = [
            _board([(22.5, "Over", 1.91, 0, True), (22.5, "Under", 1.95, 0, True)],
                   book=b)
            for b in ("a", "b")
        ]
        ca = coverage_and_agreement(pl.concat(frames))
        assert ca["line_spread"][0] == 0.0
        assert ca["n_books_on_line"][0] == 2

    def test_no_main_line_anywhere_yields_an_empty_frame(self):
        one_sided = _board([(22.5, "Over", 1.9, 0, True)])
        assert coverage_and_agreement(one_sided).is_empty()

    def test_an_empty_board_does_not_raise(self):
        """board_at returns a typed empty frame for a match nobody quoted; the next
        function in the module has to survive being handed one."""
        assert coverage_and_agreement(pl.DataFrame(schema=BOARD_SCHEMA)).is_empty()


class TestAsOfCarry:
    """The module's central claim: a rung's price at T is its last quote at or before
    T, carried forward, because ticks exist only where a price moved."""

    def _stage(self, tmp_path, rows: list[tuple], book: str = "betrivers"):
        d = tmp_path / book
        d.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "match_uid": [r[0] for r in rows],
                "points": [r[1] for r in rows],
                "side": [r[2] for r in rows],
                "odds": [r[3] for r in rows],
                "fetched_at": [T0 - timedelta(seconds=r[4]) for r in rows],
                "active": [r[5] for r in rows],
                "event_status": ["NOT_STARTED"] * len(rows),
            }
        ).write_parquet(d / "total_games.parquet")
        return d.parent

    def _times(self, uids: list[str], offset_s: int = 0) -> pl.DataFrame:
        return pl.DataFrame(
            {"match_uid": uids, "t": [T0 - timedelta(seconds=offset_s)] * len(uids)}
        )

    def test_carries_the_last_quote_before_t(self, tmp_path, monkeypatch):
        root = self._stage(
            tmp_path,
            [
                ("m1", 22.5, "Over", 1.80, 900, True),
                ("m1", 22.5, "Under", 2.00, 900, True),
                ("m1", 22.5, "Over", 1.91, 300, True),  # moved later; this one wins
                ("m1", 22.5, "Under", 1.95, 300, True),
            ],
        )
        monkeypatch.setattr(paths, "stage_root", lambda: root)
        out = board_at(self._times(["m1"]), "total_games", books=["betrivers"])
        assert out["over_odds"][0] == 1.91
        assert out["quote_age_s"][0] == 300

    def test_ignores_quotes_after_t(self, tmp_path, monkeypatch):
        root = self._stage(
            tmp_path,
            [
                ("m1", 22.5, "Over", 1.80, 900, True),
                ("m1", 22.5, "Under", 2.00, 900, True),
                ("m1", 22.5, "Over", 1.91, 60, True),
                ("m1", 22.5, "Under", 1.95, 60, True),
            ],
        )
        monkeypatch.setattr(paths, "stage_root", lambda: root)
        # anchor 10 min back: only the 15-min-old quote is visible
        out = board_at(self._times(["m1"], offset_s=600), "total_games",
                       books=["betrivers"])
        assert out["over_odds"][0] == 1.80

    def test_each_match_gets_its_own_anchor(self, tmp_path, monkeypatch):
        """The window key includes match_uid; dropping it would let one match's quotes
        satisfy another's anchor."""
        root = self._stage(
            tmp_path,
            [
                ("m1", 22.5, "Over", 1.80, 900, True),
                ("m1", 22.5, "Under", 2.00, 900, True),
                ("m2", 30.5, "Over", 1.50, 60, True),
                ("m2", 30.5, "Under", 2.60, 60, True),
            ],
        )
        monkeypatch.setattr(paths, "stage_root", lambda: root)
        times = pl.DataFrame(
            {"match_uid": ["m1", "m2"],
             "t": [T0, T0 - timedelta(seconds=600)]}
        )
        out = board_at(times, "total_games", books=["betrivers"])
        assert out.filter(pl.col("match_uid") == "m1").height == 1
        # m2's only quote is newer than its anchor, so it has no board
        assert out.filter(pl.col("match_uid") == "m2").height == 0

    def test_duplicate_match_uid_is_refused(self, tmp_path, monkeypatch):
        """Two anchors for one match would fan out the join and then collapse into a
        single board, silently losing the earlier one."""
        root = self._stage(
            tmp_path,
            [("m1", 22.5, "Over", 1.80, 900, True),
             ("m1", 22.5, "Under", 2.00, 900, True)],
        )
        monkeypatch.setattr(paths, "stage_root", lambda: root)
        times = pl.DataFrame(
            {"match_uid": ["m1", "m1"], "t": [T0, T0 - timedelta(seconds=400)]}
        )
        with pytest.raises(ValueError, match="duplicate match_uid"):
            board_at(times, "total_games", books=["betrivers"])

    def test_empty_result_keeps_the_schema(self, tmp_path, monkeypatch):
        root = self._stage(
            tmp_path, [("m1", 22.5, "Over", 1.80, 900, True)]
        )
        monkeypatch.setattr(paths, "stage_root", lambda: root)
        out = board_at(self._times(["absent"]), "total_games", books=["betrivers"])
        assert out.is_empty()
        assert list(out.columns) == list(BOARD_SCHEMA)


class TestDeterminism:
    def test_tied_imbalance_resolves_the_same_way_every_call(self, tmp_path,
                                                             monkeypatch):
        """Two rungs priced identically have equal imbalance. rank("ordinal") breaks
        the tie on row order, so board_at must sort before ranking or identical calls
        disagree with each other."""
        d = tmp_path / "betrivers"
        d.mkdir(parents=True)
        rows = []
        for line in (22.5, 23.5):
            for side, odds in (("Over", 2.0), ("Under", 2.0)):
                rows.append(("m1", line, side, odds))
        pl.DataFrame(
            {
                "match_uid": [r[0] for r in rows],
                "points": [r[1] for r in rows],
                "side": [r[2] for r in rows],
                "odds": [r[3] for r in rows],
                "fetched_at": [T0 - timedelta(seconds=300)] * len(rows),
                "active": [True] * len(rows),
                "event_status": ["NOT_STARTED"] * len(rows),
            }
        ).write_parquet(d / "total_games.parquet")
        monkeypatch.setattr(paths, "stage_root", lambda: tmp_path)

        times = pl.DataFrame({"match_uid": ["m1"], "t": [T0]})
        picks = {
            board_at(times, "total_games", books=["betrivers"])
            .filter(pl.col("is_main_line"))["points"][0]
            for _ in range(8)
        }
        assert picks == {22.5}, f"tie broke inconsistently: {picks}"
