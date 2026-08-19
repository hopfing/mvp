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

from mvp.oddspapi import board, paths
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

    def test_unexpected_side_values_are_refused_not_guessed(self):
        """The guard now covers a named-outcome market whose rows carry a side it
        was not given, rather than refusing participant markets outright —
        `board_at` re-sides those to a/b before they reach here. Producing a board
        keyed to the wrong player is still worse than refusing."""
        with pytest.raises(ValueError, match="are not Over/Under"):
            _pivot_two_sided(
                _sides([(-2.5, "1", 1.9, 0, True), (-2.5, "2", 1.9, 0, True)]),
                "Over",
                "Under",
            )

    def test_the_oriented_pair_is_output_not_input(self):
        """`board_at` refuses an oriented pair for a participant market. Passing
        one sends every row down `_orient_participant_sides`' otherwise-branch,
        splitting each rung into two one-sided ones with no error — so this is a
        raise rather than a degraded board."""
        from mvp.oddspapi.board import board_at

        times = pl.DataFrame({"match_uid": ["m1"], "t": [T0]})
        with pytest.raises(ValueError, match="not the oriented pair"):
            board_at(times, "game_spread", over_side="a", under_side="b")


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


class TestFeedSides:
    """Which side literals a market's STAGE FILES carry.

    Load-bearing because `anchors` decides a rung is two-sided by matching the
    `side` column against a pair defaulting to `Over`/`Under`. A participant-sided
    market matched nothing there and produced zero anchor times, silently.
    """

    def test_totals_is_named_outcome(self):
        assert board.feed_sides("total_games") == ("Over", "Under")

    def test_participant_markets_are_positional(self):
        assert board.feed_sides("game_spread") == ("1", "2")
        assert board.feed_sides("moneyline") == ("1", "2")

    def test_unknown_market_falls_back_to_totals(self):
        """Named-outcome is the safe default: a market nobody has classified is
        far more likely to be a totals variant than participant-sided, and the
        wrong answer here fails loudly at `_pivot_two_sided`'s guard rather than
        silently attributing prices to the wrong player."""
        assert board.feed_sides("first_set_total_games") == ("Over", "Under")

    def test_the_two_vocabularies_are_disjoint(self):
        """Nothing may appear in both. If they ever overlap, a market matched by
        the wrong pair would half-work rather than return nothing, which is the
        harder failure to spot."""
        assert not set(board.TOTALS_SIDES) & set(board.PARTICIPANT_SIDES)


class TestParticipantOrientation:
    """Re-siding a participant market to the projection's a/b frame.

    Two independent corrections — which player a price belongs to, and which
    direction the line points. Each alone leaves a board that looks right: the
    identity fix alone gives correct attribution at a backwards line, and the
    sign fix alone gives the right line on the wrong player.
    """

    @staticmethod
    def _per_side(points=2.5, p1="AA01", uid="2026_540_SGL_R32_AA01_ZZ99"):
        """One rung, both feed sides. `p1` is whoever the feed listed first."""
        p2 = "ZZ99" if p1 == "AA01" else "AA01"
        return pl.DataFrame({
            "match_uid": [uid, uid],
            "points": [points, points],
            "side": ["1", "2"],
            "side_player_id": [p1, p2],
            "odds": [2.0, 1.8],
        })

    def test_the_lower_id_becomes_a_whichever_side_the_feed_listed_it(self):
        from mvp.oddspapi.board import _orient_participant_sides

        for p1 in ("AA01", "ZZ99"):
            out = _orient_participant_sides(self._per_side(p1=p1), "1", "2")
            by_side = dict(zip(out["side"].to_list(), out["side_player_id"].to_list()))
            assert by_side["a"] == "AA01", f"feed listed {p1} first"
            assert by_side["b"] == "ZZ99"

    def test_points_becomes_a_threshold_on_margin_a(self):
        """Feed `points=+2.5` on participant 1 is a head start, so participant 1
        covers iff their margin > -2.5. With p1 == a that is `margin_a > -2.5`,
        so the ledger line is -2.5."""
        from mvp.oddspapi.board import _orient_participant_sides

        out = _orient_participant_sides(self._per_side(points=2.5, p1="AA01"), "1", "2")
        assert out["points"].unique().to_list() == [-2.5]

    def test_the_sign_flips_with_the_feed_ordering(self):
        """Same feed handicap, opposite listing: now p1 is b, so the a-side
        threshold is +2.5 rather than -2.5. This is the ~49% the orientation
        touches, and getting it wrong inverts the line on every rung."""
        from mvp.oddspapi.board import _orient_participant_sides

        out = _orient_participant_sides(self._per_side(points=2.5, p1="ZZ99"), "1", "2")
        assert out["points"].unique().to_list() == [2.5]

    def test_both_rows_of_a_rung_agree_on_the_line(self):
        """The transform is per row with no window, so the two sides landing on
        one value is structural rather than something to remember."""
        from mvp.oddspapi.board import _orient_participant_sides

        for p1 in ("AA01", "ZZ99"):
            for pts in (2.5, -2.5, 0.0, 6.5):
                out = _orient_participant_sides(
                    self._per_side(points=pts, p1=p1), "1", "2"
                )
                assert out["points"].n_unique() == 1, (p1, pts)


class TestSpreadBoardEndToEnd:
    """`board_at` for a participant market, through a real stage tree.

    The orientation is unit-tested on `_orient_participant_sides`, but the path
    around it is not: `side_player_id` has to survive `_latest_per_side`'s
    projection, the pivot's `values` list, the materialise loop, and finally
    `.select(BOARD_SCHEMA.keys())`. Each of those drops columns silently, so this
    exercises the seam rather than the arithmetic.
    """

    UID = "2026_540_SGL_R32_AA01_ZZ99"

    def _stage(self, tmp_path, rows, market="game_spread"):
        d = tmp_path / "pinnacle"
        d.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "match_uid": [self.UID] * len(rows),
            "points": [r[0] for r in rows],
            "side": [r[1] for r in rows],
            "side_player_id": [r[2] for r in rows],
            "odds": [r[3] for r in rows],
            "fetched_at": [T0 - timedelta(seconds=300)] * len(rows),
            "active": [True] * len(rows),
            "event_status": ["NOT_STARTED"] * len(rows),
        }).write_parquet(d / f"{market}.parquet")
        return d.parent

    def _board(self, tmp_path, monkeypatch, rows):
        root = self._stage(tmp_path, rows)
        monkeypatch.setattr(paths, "stage_root", lambda: root)
        times = pl.DataFrame({"match_uid": [self.UID], "t": [T0]})
        return board_at(times, "game_spread", books=["pinnacle"])

    def test_player_ids_survive_to_the_board(self, tmp_path, monkeypatch):
        """The pivot drops any column outside `index`/`values`, and BOARD_SCHEMA
        drops any column it does not list. Both had to change."""
        out = self._board(tmp_path, monkeypatch, [
            (2.5, "1", "AA01", 1.9), (2.5, "2", "ZZ99", 1.9),
        ])
        assert out.height == 1
        assert out["player_id_a"][0] == "AA01"
        assert out["player_id_b"][0] == "ZZ99"

    def test_a_is_the_lower_id_even_when_the_feed_listed_it_second(
        self, tmp_path, monkeypatch
    ):
        out = self._board(tmp_path, monkeypatch, [
            (2.5, "1", "ZZ99", 1.9), (2.5, "2", "AA01", 1.9),
        ])
        assert out["player_id_a"][0] == "AA01"
        assert out["player_id_b"][0] == "ZZ99"

    def test_the_line_is_a_threshold_on_margin_a(self, tmp_path, monkeypatch):
        """Feed +2.5 on its first participant. With that participant being a,
        a covers iff margin_a > -2.5, so the board emits -2.5."""
        out = self._board(tmp_path, monkeypatch, [
            (2.5, "1", "AA01", 1.9), (2.5, "2", "ZZ99", 1.9),
        ])
        assert out["points"][0] == -2.5

    def test_the_line_flips_with_the_feed_ordering(self, tmp_path, monkeypatch):
        out = self._board(tmp_path, monkeypatch, [
            (2.5, "1", "ZZ99", 1.9), (2.5, "2", "AA01", 1.9),
        ])
        assert out["points"][0] == 2.5

    def test_a_rung_stays_one_row_with_both_sides(self, tmp_path, monkeypatch):
        """Both feed rows must land on ONE ledger line. If the sign transform
        disagreed between them the rung would split into two one-sided ones —
        which is what passing the oriented pair as input used to do."""
        out = self._board(tmp_path, monkeypatch, [
            (2.5, "1", "AA01", 1.9), (2.5, "2", "ZZ99", 2.1),
        ])
        assert out.height == 1
        assert out["over_odds"][0] == 1.9 and out["under_odds"][0] == 2.1
        assert out["p_over"][0] is not None

    def test_totals_boards_carry_null_player_ids(self, tmp_path, monkeypatch):
        """Named-outcome markets have no player, and the columns are required by
        BOARD_SCHEMA — so they must be present and null, not absent."""
        d = tmp_path / "pinnacle"
        d.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "match_uid": [self.UID] * 2, "points": [22.5, 22.5],
            "side": ["Over", "Under"], "odds": [1.9, 1.9],
            "fetched_at": [T0 - timedelta(seconds=300)] * 2,
            "active": [True] * 2, "event_status": ["NOT_STARTED"] * 2,
        }).write_parquet(d / "total_games.parquet")
        monkeypatch.setattr(paths, "stage_root", lambda: tmp_path)
        out = board_at(
            pl.DataFrame({"match_uid": [self.UID], "t": [T0]}),
            "total_games", books=["pinnacle"],
        )
        assert out.height == 1
        assert out["player_id_a"].null_count() == 1
        assert out["player_id_b"].null_count() == 1
