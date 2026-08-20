"""Pricing a board against a projection's pmf, and settling it.

Two things here are easy to get silently wrong and are what these tests pin.

**Whole-number lines push.** Books quote them (pinnacle sits on 23.0 routinely) and a
match landing exactly on the line returns the stake. A half-integer-only fixture passes
with push handling entirely absent, so every test that matters uses a whole line.

**`p_over` means two different things.** The board carries the BOOK's de-vigged
probability under that name; the model's P(total > line) is a different quantity.
Naming both `p_over` destroys one of them and takes `edge_novig` -- the
model-against-the-book's-opinion measure -- with it, without raising.
"""

from __future__ import annotations

import polars as pl
import pytest

from mvp.projection.iid import evaluation
from mvp.projection.iid.evaluation import (
    add_clv,
    add_mean_covers,
    bets,
    cumulative,
    price,
    settle,
    unpivot_sides,
)


def _pmf(match_uid: str = "m1", weights: dict[int, float] | None = None,
         actual: float = 22.0) -> pl.DataFrame:
    """A pmf over game counts. `weights` maps game count -> probability."""
    weights = weights or {21: 0.25, 22: 0.30, 23: 0.25, 24: 0.20}
    arr = [0.0] * 40
    for games, p in weights.items():
        arr[games] = p
    return pl.DataFrame({
        "match_uid": [match_uid],
        "actual_total": [actual],
        "total_games_pmf": [arr],
    })


def _board(points: float, over: float | None = 2.0, under: float | None = 2.0,
           book_p_over: float = 0.5, match_uid: str = "m1") -> pl.DataFrame:
    return pl.DataFrame({
        "match_uid": [match_uid],
        "book": ["bk"],
        "points": [points],
        "over_odds": [over],
        "under_odds": [under],
        "p_over": [book_p_over],
    })


class TestCumulative:
    def test_index_is_the_game_count(self):
        cum = cumulative(_pmf()).filter(pl.col("p") > 0)
        assert cum["games"].to_list() == [21, 22, 23, 24]

    def test_cdf_reaches_one(self):
        assert cumulative(_pmf())["p_at_or_below"].max() == pytest.approx(1.0)

    def test_a_pmf_without_the_list_column_is_refused(self):
        with pytest.raises(ValueError, match="total_games_pmf"):
            cumulative(pl.DataFrame({"match_uid": ["m1"]}))


class TestWholeNumberLine:
    """The case a half-integer fixture cannot catch."""

    def test_the_three_probabilities_sum_to_one(self):
        p = price(_board(22.0), _pmf())
        row = p.row(0, named=True)
        total = row["model_p_over"] + row["p_push"] + row["model_p_under"]
        assert total == pytest.approx(1.0)

    def test_push_mass_is_the_probability_of_landing_on_the_line(self):
        p = price(_board(22.0), _pmf())
        assert p["p_push"][0] == pytest.approx(0.30)

    def test_over_excludes_the_line_itself(self):
        # 23 and 24 are over 22.0; 22 is a push, not an over
        assert price(_board(22.0), _pmf())["model_p_over"][0] == pytest.approx(0.45)

    def test_under_excludes_the_push(self):
        assert price(_board(22.0), _pmf())["model_p_under"][0] == pytest.approx(0.25)


class TestHalfIntegerLine:
    def test_no_push_mass(self):
        assert price(_board(22.5), _pmf())["p_push"][0] == pytest.approx(0.0)

    def test_over_and_under_partition_the_distribution(self):
        row = price(_board(22.5), _pmf()).row(0, named=True)
        assert row["model_p_over"] == pytest.approx(0.45)
        assert row["model_p_under"] == pytest.approx(0.55)


class TestBookProbabilitySurvives:
    def test_the_boards_p_over_is_not_overwritten(self):
        """The collision this module was fixed for: aliasing the model's probability
        to `p_over` silently replaced the book's and removed edge_novig's input."""
        p = price(_board(22.5, book_p_over=0.62), _pmf())
        assert p["p_over"][0] == pytest.approx(0.62)
        assert p["model_p_over"][0] == pytest.approx(0.45)

    def test_edge_is_against_the_price_and_edge_novig_against_the_book(self):
        p = price(_board(22.5, over=2.0, book_p_over=0.62), _pmf()).row(0, named=True)
        assert p["edge_over"] == pytest.approx(0.45 - 0.5)
        assert p["edge_novig_over"] == pytest.approx(0.45 - 0.62)


class TestSettle:
    def test_a_push_returns_the_stake_on_both_sides(self):
        s = settle(price(_board(22.0), _pmf()), _pmf(actual=22.0))
        assert s["is_push"][0]
        assert s["pnl_over"][0] == 0.0
        assert s["pnl_under"][0] == 0.0
        assert s["over_won"][0] is None

    def test_over_wins_pays_the_over_and_loses_the_under(self):
        s = settle(price(_board(22.0), _pmf()), _pmf(actual=24.0))
        assert s["over_won"][0]
        assert s["pnl_over"][0] == pytest.approx(1.0)
        assert s["pnl_under"][0] == pytest.approx(-1.0)

    def test_a_match_with_no_outcome_is_dropped_not_scored(self):
        outcomes = pl.DataFrame(
            {"match_uid": ["other"], "actual_total": [22.0]}
        )
        assert settle(price(_board(22.0), _pmf()), outcomes).is_empty()

    def test_outcomes_without_the_column_are_refused(self):
        with pytest.raises(ValueError, match="actual_total"):
            settle(price(_board(22.0), _pmf()), pl.DataFrame({"match_uid": ["m1"]}))


class TestBetsOnOneSidedRungs:
    """`a >= null` is null, and polars routes a null predicate to `otherwise`."""

    def _settled(self, over, under):
        return settle(price(_board(22.5, over=over, under=under), _pmf()),
                      _pmf(actual=24.0))

    def test_an_over_only_rung_is_taken_as_an_over(self):
        b = bets(self._settled(over=2.0, under=None), min_edge=-1.0)
        assert b.height == 1
        assert b["side_pos"][0] == "a"
        assert b["odds"][0] == pytest.approx(2.0)

    def test_an_under_only_rung_is_taken_as_an_under(self):
        b = bets(self._settled(over=None, under=2.0), min_edge=-1.0)
        assert b.height == 1
        assert b["side_pos"][0] == "b"
        assert b["odds"][0] == pytest.approx(2.0)

    def test_the_edge_belongs_to_the_side_actually_chosen(self):
        """Before the fix an over-only rung emitted side="under" carrying the OVER's
        edge — a bet on a price no book quoted, landing in the high-edge tail."""
        b = bets(self._settled(over=2.0, under=None), min_edge=-1.0)
        assert b["edge"][0] == pytest.approx(0.45 - 0.5)
        assert b["model_p"][0] == pytest.approx(0.45)

    def test_a_rung_with_neither_side_quoted_is_not_a_bet(self):
        assert bets(self._settled(over=None, under=None), min_edge=-1.0).is_empty()


class TestBetsSelection:
    def test_the_better_side_is_taken_when_both_are_quoted(self):
        s = settle(price(_board(22.5, over=1.5, under=5.0), _pmf()),
                   _pmf(actual=24.0))
        b = bets(s, min_edge=-1.0)
        assert b["side_pos"][0] == "b"

    def test_min_edge_is_a_floor_not_a_constant(self):
        s = settle(price(_board(22.5, over=2.0, under=2.0), _pmf()),
                   _pmf(actual=24.0))
        assert bets(s, min_edge=-1.0).height == 1
        assert bets(s, min_edge=0.5).is_empty()


class TestBetsExcludesDeadRungs:
    """A carried-forward price on a rung the book pulled is not an offer.

    `board_at` carries the last known price forward, so a rung the book took down
    still has odds on its row -- `live` is what says whether it was on the board.
    Measured at the close anchor, 36.8% of two-sided rows are not live.
    """

    def _settled(self, live: bool):
        b = _board(22.5).with_columns(pl.lit(live).alias("live"))
        return settle(price(b, _pmf()), _pmf(actual=24.0))

    def test_a_live_rung_is_a_bet(self):
        assert bets(self._settled(live=True), min_edge=-1.0).height == 1

    def test_a_dead_rung_is_not(self):
        assert bets(self._settled(live=False), min_edge=-1.0).is_empty()

    def test_a_board_without_the_column_is_not_silently_filtered(self):
        """`live` is optional so a hand-built frame still works; its absence must not
        mean 'drop everything'."""
        s = settle(price(_board(22.5), _pmf()), _pmf(actual=24.0))
        assert "live" not in s.columns
        assert bets(s, min_edge=-1.0).height == 1


class TestLivenessIsPerSide:
    """A rung the book pulled one leg of is still bettable on the other.

    `board_at`'s `live` is an AND over both legs because main-line selection needs a
    live PAIR to compare balance on. Filtering a bet set on that AND removed every
    one-sided rung before the side-selection logic ran — measured end to end, 111
    one-sided rows reached `settle` and none survived `bets`.
    """

    def _settled(self, **live):
        b = _board(22.5).with_columns(
            pl.lit(live.get("over", True)).alias("live_over"),
            pl.lit(live.get("under", True)).alias("live_under"),
        )
        return settle(price(b, _pmf()), _pmf(actual=24.0))

    def test_a_rung_live_on_one_side_is_still_a_bet(self):
        b = bets(self._settled(over=True, under=False), min_edge=-1.0)
        assert b.height == 1
        assert b["side_pos"][0] == "a"

    def test_the_dead_side_is_not_selectable_even_with_the_better_edge(self):
        """Odds carry forward from the last tick, so a pulled leg keeps a price."""
        b = bets(self._settled(over=False, under=True), min_edge=-1.0)
        assert b["side_pos"][0] == "b"

    def test_a_rung_dead_on_both_sides_is_not_a_bet(self):
        assert bets(self._settled(over=False, under=False), min_edge=-1.0).is_empty()


class TestReferenceBooksAreNotBets:
    def _settled(self, book: str):
        b = _board(22.5).with_columns(pl.lit(book).alias("book"))
        return settle(price(b, _pmf()), _pmf(actual=24.0))

    def test_a_reference_book_never_enters_the_bet_set(self):
        """Pinnacle belongs on the board — it is the CLV reference — but it is not
        reachable. It was 39% of bets at the open anchor before this filter."""
        assert bets(self._settled("pinnacle"), min_edge=-1.0).is_empty()

    def test_an_entry_book_does(self):
        assert bets(self._settled("betrivers"), min_edge=-1.0).height == 1


class TestUnpivotSides:
    """The ledger's grain. `settle()` is per RUNG; the ledger is per SIDE.

    A side transposition here is the failure mode §9 of the cutover plan singles
    out as invisible to every internal invariant — row counts, probability sums
    and P&L identities all survive it. So these tests check identity, not shape.
    """

    def _settled(self, points=22.0, actual=24.0, over=2.0, under=1.9):
        b = _board(points, over=over, under=under)
        return settle(price(b, _pmf()), _pmf(actual=actual))

    def test_one_rung_becomes_two_rows(self):
        out = unpivot_sides(self._settled())
        assert out.height == 2
        assert sorted(out["side"].to_list()) == ["over", "under"]

    def test_each_side_carries_its_own_price_and_probability(self):
        """The transposition check: the over row must hold the OVER's odds, model
        probability and edge — not the under's."""
        rung = self._settled().row(0, named=True)
        out = unpivot_sides(self._settled())
        over = out.filter(pl.col("side") == "over").row(0, named=True)
        under = out.filter(pl.col("side") == "under").row(0, named=True)

        assert over["odds"] == rung["over_odds"]
        assert under["odds"] == rung["under_odds"]
        assert over["model_p"] == pytest.approx(rung["model_p_over"])
        assert under["model_p"] == pytest.approx(rung["model_p_under"])
        assert over["edge"] == pytest.approx(rung["edge_over"])
        assert under["edge"] == pytest.approx(rung["edge_under"])
        assert over["pnl"] == rung["pnl_over"]
        assert under["pnl"] == rung["pnl_under"]

    def test_won_is_per_side_and_opposite(self):
        out = unpivot_sides(self._settled(points=22.0, actual=24.0))
        over = out.filter(pl.col("side") == "over").row(0, named=True)
        under = out.filter(pl.col("side") == "under").row(0, named=True)
        assert over["won"] is True
        assert under["won"] is False

    def test_a_push_wins_for_neither_side(self):
        """A push returns the stake. Scored as a loss on either side it would
        depress that side's hit rate with a bet that never lost."""
        out = unpivot_sides(self._settled(points=22.0, actual=22.0))
        assert out["won"].to_list() == [None, None]
        assert out["pnl"].to_list() == [0.0, 0.0]
        assert out["is_push"].to_list() == [True, True]

    def test_negative_edge_rows_survive(self):
        """The ledger is the OFFER set. `bets()` would drop these; the whole
        point of writing settle() output is that read time decides."""
        out = unpivot_sides(self._settled(over=1.01, under=1.01))
        assert (out["edge"] < 0).any()

    def test_rung_context_is_repeated_on_both_sides(self):
        """Properties of the RUNG or the pair are duplicated; properties of one
        side are not. `p_over` used to sit in the first group and describes one
        side, so it left the under row holding the over's number."""
        out = unpivot_sides(self._settled())
        for col in ("match_uid", "book", "points", "actual_total"):
            assert out[col].n_unique() == 1
        assert "p_over" not in out.columns

    def test_book_p_is_per_side_and_complements(self):
        """The book's de-vigged probability for THIS row's side, mirroring
        `model_p`. Together they make `edge_novig = model_p - book_p` hold on
        every row rather than only on the a side."""
        settled = self._settled(over=2.5, under=1.6)
        out = unpivot_sides(settled)
        by_pos = dict(zip(out["side_pos"].to_list(), out["book_p"].to_list()))
        assert by_pos["a"] == pytest.approx(settled["p_over"][0])
        assert by_pos["a"] + by_pos["b"] == pytest.approx(1.0)

    def test_side_pos_is_positional_and_side_is_the_label(self):
        """`side_pos` is what the two-way complements branch on; `side` is what a
        reader sees. For totals they line up; for spread they will not."""
        out = unpivot_sides(self._settled())
        assert sorted(out["side_pos"].to_list()) == ["a", "b"]
        assert sorted(out["side"].to_list()) == ["over", "under"]
        pairs = dict(zip(out["side_pos"].to_list(), out["side"].to_list()))
        assert pairs == {"a": "over", "b": "under"}

    def test_empty_in_empty_out(self):
        assert unpivot_sides(pl.DataFrame()).is_empty()

    def test_a_frame_that_is_not_settle_output_is_refused(self):
        with pytest.raises(ValueError, match="unpivot_sides expects"):
            unpivot_sides(pl.DataFrame({"match_uid": ["m1"], "over_odds": [2.0]}))


class TestMeanCovers:
    """A per-SIDE model-agreement gate: does the chain's centre agree with the
    side, or is the bet coming from a pmf tail alone?"""

    @staticmethod
    def _pmf_with_mean(actual: float = 22.0) -> pl.DataFrame:
        """The real pmf frame carries `expected_total_games`; the shared fixture
        deliberately does not, so the refusal test below still bites."""
        # 21*.25 + 22*.30 + 23*.25 + 24*.20 = 22.4
        return _pmf(actual=actual).with_columns(
            pl.lit(22.4).alias("expected_total_games")
        )

    def _ledger(self, points):
        b = _board(points)
        settled = settle(price(b, _pmf()), _pmf(actual=24.0))
        return add_mean_covers(unpivot_sides(settled), self._pmf_with_mean())

    def test_it_is_directional_not_a_property_of_the_rung(self):
        """E[total] is 22.4 for this pmf. At a line of 21.5 the mean lands on the
        over, so the over agrees and the under does not — same rung, opposite
        answers. A non-directional implementation gives both sides the same flag."""
        out = self._ledger(21.5)
        over = out.filter(pl.col("side") == "over")["mean_covers"][0]
        under = out.filter(pl.col("side") == "under")["mean_covers"][0]
        assert over is True
        assert under is False

    def test_it_flips_when_the_line_moves_past_the_mean(self):
        out = self._ledger(23.5)
        over = out.filter(pl.col("side") == "over")["mean_covers"][0]
        under = out.filter(pl.col("side") == "under")["mean_covers"][0]
        assert over is False
        assert under is True

    def test_a_pmf_without_the_mean_is_refused(self):
        b = _board(22.5)
        settled = settle(price(b, _pmf()), _pmf(actual=24.0))
        with pytest.raises(ValueError, match="expected_total_games"):
            add_mean_covers(unpivot_sides(settled), _pmf())


class TestLineOffset:
    """Distance to the main line of the book quoting the rung.

    The retired code measured against a cross-book pooled median, which is only
    defined where books happen to agree on the line — and is the source of the
    35.5-games artifact. A book's line is part of its opinion.
    """

    def _board(self, books, points, mains):
        return pl.DataFrame({
            "match_uid": ["m1"] * len(points),
            "book": books,
            "points": points,
            "is_main_line": mains,
        })

    def test_offset_is_signed_distance_from_the_books_own_main_line(self):
        from mvp.projection.iid.evaluation import add_line_offset

        out = add_line_offset(
            self._board(["dk"] * 3, [20.5, 21.5, 22.5], [False, True, False])
        )
        assert out["line_offset"].to_list() == [-1.0, 0.0, 1.0]

    def test_each_book_is_measured_against_its_own_line_not_a_pooled_one(self):
        """dk's main is 21.5 and br's is 22.5. A pooled median would give br's
        22.5 rung a non-zero offset; it is br's own main line, so it is 0."""
        from mvp.projection.iid.evaluation import add_line_offset

        out = add_line_offset(self._board(
            ["dk", "dk", "br", "br"], [20.5, 21.5, 22.5, 23.5],
            [False, True, True, False],
        ))
        rows = dict(zip(zip(out["book"], out["points"]), out["line_offset"]))
        assert rows[("dk", 21.5)] == 0.0
        assert rows[("br", 22.5)] == 0.0
        assert rows[("br", 23.5)] == 1.0

    def test_null_when_the_book_has_no_main_line(self):
        """Every live rung one-sided means no de-vig, so no balance to compare
        and nothing for the distance to be measured from."""
        from mvp.projection.iid.evaluation import add_line_offset

        out = add_line_offset(
            self._board(["dk"] * 2, [21.5, 22.5], [False, False])
        )
        assert out["line_offset"].to_list() == [None, None]

    def test_a_board_without_the_flag_passes_through(self):
        from mvp.projection.iid.evaluation import add_line_offset

        b = self._board(["dk"], [21.5], [True]).drop("is_main_line")
        assert "line_offset" not in add_line_offset(b).columns


class TestBuildLedger:
    """Assembly of the offer set: anchors -> board -> price -> settle -> sides.

    The board and anchor modules are stubbed so this exercises the driver's own
    wiring — anchor loop, per-anchor tagging, role, schema_version — without
    needing the stage tree on disk.
    """

    def _pmf(self):
        return _pmf(actual=24.0).with_columns(
            pl.lit(22.4).alias("expected_total_games")
        )

    def _install(self, monkeypatch, *, books=("dk", "br"), anchors_seen=None):
        from mvp.oddspapi import anchors as anchors_mod
        from mvp.oddspapi import board as board_mod

        def fake_board_at(times, market, *, books=None, **kw):
            rows = []
            for b in books:
                rows.append({
                    "match_uid": "m1", "book": b, "points": 22.0,
                    "over_odds": 2.0, "under_odds": 1.9, "p_over": 0.5,
                    "is_main_line": True, "live_over": True, "live_under": True,
                })
            return pl.DataFrame(rows)

        def fake_formed(market, n=2, **kw):
            if anchors_seen is not None:
                anchors_seen.append(("formed", n))
            return pl.DataFrame({"match_uid": ["m1"], "t": [0]})

        def fake_close(market=None, **kw):
            if anchors_seen is not None:
                anchors_seen.append(("close", None))
            return pl.DataFrame({"match_uid": ["m1"], "t": [0]})

        monkeypatch.setattr(board_mod, "board_at", fake_board_at)
        monkeypatch.setattr(board_mod, "entry_books", lambda m: list(books))
        monkeypatch.setattr(anchors_mod, "formed", fake_formed)
        monkeypatch.setattr(anchors_mod, "open_", lambda m, **kw: fake_formed(m, 1, **kw))
        monkeypatch.setattr(anchors_mod, "close", fake_close)

    def test_every_anchor_is_walked_and_tagged(self, monkeypatch):
        from mvp.projection.iid.evaluation import build_ledger

        seen: list = []
        self._install(monkeypatch, anchors_seen=seen)
        led = build_ledger(self._pmf())
        assert set(led["anchor"].unique()) == {"open", "formed2", "close"}
        assert ("formed", 1) in seen and ("formed", 2) in seen

    def test_both_sides_of_every_book_rung_are_present(self, monkeypatch):
        from mvp.projection.iid.evaluation import build_ledger

        self._install(monkeypatch)
        led = build_ledger(self._pmf())
        # 3 anchors x 2 books x 2 sides
        assert led.height == 12
        assert set(led["side"].unique()) == {"over", "under"}

    def test_reference_books_are_labelled_not_silently_mixed(self, monkeypatch):
        """A pinnacle row is indistinguishable from a takeable one without a
        role column, and pinnacle's thin vig makes it the most favourable half."""
        from mvp.projection.iid.evaluation import build_ledger

        self._install(monkeypatch, books=("dk", "pinnacle"))
        led = build_ledger(self._pmf())
        roles = dict(zip(led["book"], led["role"]))
        assert roles["pinnacle"] == "reference"
        assert roles["dk"] == "entry"

    def test_schema_version_is_carried(self, monkeypatch):
        from mvp.projection.iid.evaluation import (
            LEDGER_SCHEMA_VERSION,
            build_ledger,
        )

        self._install(monkeypatch)
        led = build_ledger(self._pmf())
        assert led["schema_version"].unique().to_list() == [LEDGER_SCHEMA_VERSION]

    def test_negative_edge_rows_reach_the_ledger(self, monkeypatch):
        """The ledger is the offer set. If this ever filters, `iid-rank`'s bands
        silently lose their denominator."""
        from mvp.projection.iid.evaluation import build_ledger

        self._install(monkeypatch)
        led = build_ledger(self._pmf())
        assert (led["edge"] < 0).any()

    def test_rungs_outside_the_pmf_support_are_reported_not_silent(
        self, monkeypatch, caplog
    ):
        """`price` inner-joins on the floored line, so a rung outside the pmf's
        support vanishes with nothing raised. A silently shorter ledger reads as
        a thinner market rather than a bug."""
        import logging

        from mvp.oddspapi import board as board_mod
        from mvp.projection.iid.evaluation import build_ledger

        self._install(monkeypatch, books=("dk",))

        def board_with_unsupported_line(times, market, *, books=None, **kw):
            return pl.DataFrame({
                "match_uid": ["m1", "m1"],
                "book": ["dk", "dk"],
                "points": [22.0, 900.0],   # 900 is far outside the pmf support
                "over_odds": [2.0, 2.0],
                "under_odds": [1.9, 1.9],
                "p_over": [0.5, 0.5],
                "is_main_line": [True, False],
                "live_over": [True, True],
                "live_under": [True, True],
            })

        monkeypatch.setattr(board_mod, "board_at", board_with_unsupported_line)
        with caplog.at_level(logging.WARNING):
            led = build_ledger(self._pmf(), anchor_names=("open",))
        assert led.height == 2      # one surviving rung, two sides
        assert any("no pmf support" in r.message for r in caplog.records)


class TestCLV:
    """Closing-line value against the reference book's de-vigged close.

    The metric that separates a bias the market has already priced from one it
    has not — residuals cannot tell those apart, so this column carries the
    whole question and its sign convention has to be right.
    """

    def _ledger(self, side="over", odds=2.0, points=22.0):
        # `side_pos` is what add_clv branches on -- the complement is against the
        # reference board's A-SIDE probability, which is positional. `side` rides
        # along as the label a reader sees.
        return pl.DataFrame({
            "match_uid": ["m1"], "book": ["dk"], "points": [points],
            "side": [side], "side_pos": ["a" if side == "over" else "b"],
            "odds": [odds], "anchor": ["open"],
        })

    def _ref(self, monkeypatch, p_over=0.60, points=22.0, books=("pinnacle",)):
        from mvp.oddspapi import anchors as anchors_mod
        from mvp.oddspapi import board as board_mod

        monkeypatch.setattr(board_mod, "available_books", lambda m: list(books))
        monkeypatch.setattr(
            anchors_mod, "close",
            lambda m=None, **kw: pl.DataFrame({"match_uid": ["m1"], "t": [0]}),
        )
        monkeypatch.setattr(
            board_mod, "board_at",
            lambda times, market, **kw: pl.DataFrame({
                "match_uid": ["m1"], "book": ["pinnacle"],
                "points": [points], "p_over": [p_over],
            }),
        )

    def test_over_side_uses_the_reference_probability_directly(self, monkeypatch):
        from mvp.projection.iid.evaluation import add_clv

        self._ref(monkeypatch, p_over=0.60)
        out = add_clv(self._ledger(side="over", odds=2.0))
        # 0.60 fair against a price implying 0.50 -> +0.10 of value
        assert out["clv"][0] == pytest.approx(0.60 - 0.5)

    def test_under_side_uses_the_complement(self, monkeypatch):
        """Handing the under side the OVER's fair is the transposition that
        makes every under look valuable and every over look terrible."""
        from mvp.projection.iid.evaluation import add_clv

        self._ref(monkeypatch, p_over=0.60)
        out = add_clv(self._ledger(side="under", odds=2.0))
        assert out["clv"][0] == pytest.approx((1 - 0.60) - 0.5)

    def test_a_better_price_yields_more_clv(self, monkeypatch):
        from mvp.projection.iid.evaluation import add_clv

        self._ref(monkeypatch, p_over=0.60)
        cheap = add_clv(self._ledger(odds=1.8))["clv"][0]
        rich = add_clv(self._ledger(odds=2.4))["clv"][0]
        assert rich > cheap

    def test_the_reference_must_be_the_same_rung(self, monkeypatch):
        """Scoring a 22.0 bet against the reference's fair for 25.0 is not
        closing-line value, it is two different bets."""
        from mvp.projection.iid.evaluation import add_clv

        self._ref(monkeypatch, p_over=0.60, points=25.0)
        out = add_clv(self._ledger(points=22.0))
        assert out["clv"][0] is None

    def test_no_reference_book_leaves_the_column_null_not_absent(self, monkeypatch):
        """A missing benchmark must be visible as null rather than the column
        vanishing, or a downstream mean silently reads as 'no CLV signal'."""
        from mvp.projection.iid.evaluation import add_clv

        self._ref(monkeypatch, books=("draftkings",))
        out = add_clv(self._ledger())
        assert "clv" in out.columns
        assert out["clv"][0] is None


class TestCumulativeSpread:
    """The spread pmf is signed and offset-stored. Reading it 0-based does not
    raise -- it shifts every lookup, which is why the offset travels with the
    data and is asserted rather than assumed."""

    @staticmethod
    def _spread_pmf(offset: int = 3, mass=None) -> pl.DataFrame:
        mass = mass if mass is not None else [0.0, 0.0, 0.25, 0.5, 0.25, 0.0, 0.0]
        return pl.DataFrame({
            "match_uid": ["m1"],
            "spread_offset": [offset],
            "spread_pmf": [mass],
        })

    def test_index_is_signed_around_zero(self):
        cum = cumulative(self._spread_pmf(), market="game_spread")
        assert cum["games"].to_list() == [-3, -2, -1, 0, 1, 2, 3]

    def test_cumulative_reads_at_the_signed_margin(self):
        cum = cumulative(self._spread_pmf(), market="game_spread")
        at = dict(zip(cum["games"].to_list(), cum["p_at_or_below"].to_list()))
        assert at[-1] == pytest.approx(0.25)
        assert at[0] == pytest.approx(0.75)
        assert at[3] == pytest.approx(1.0)

    def test_totals_is_unchanged_and_still_zero_based(self):
        """The offset path must not leak into the market that has no offset."""
        cum = cumulative(pl.DataFrame({
            "match_uid": ["m1"], "total_games_pmf": [[0.2, 0.3, 0.5]],
        }))
        assert cum["games"].to_list() == [0, 1, 2]

    def test_missing_offset_column_raises(self):
        """Without it the signed support cannot be recovered, and a 0-based read
        would put every margin `offset` places out."""
        pmf = self._spread_pmf().drop("spread_offset")
        with pytest.raises(ValueError, match="spread_offset"):
            cumulative(pmf, market="game_spread")

    def test_non_constant_offset_raises(self):
        """Two offsets in one frame means two incompatible supports were
        concatenated; picking either silently mis-indexes the other."""
        pmf = pl.concat([self._spread_pmf(3), self._spread_pmf(4).with_columns(
            pl.lit("m2").alias("match_uid"),
            pl.lit([0.0] * 9).alias("spread_pmf"),
        )], how="vertical_relaxed")
        with pytest.raises(ValueError, match="not constant"):
            cumulative(pmf, market="game_spread")

    def test_unknown_market_raises(self):
        with pytest.raises(ValueError, match="no pmf schema"):
            cumulative(self._spread_pmf(), market="moneyline")


class TestComplementsAreLockedToPosition:
    """The two-way complements must branch on `side_pos`, never on `side`.

    For totals the two coincide, so a suite built only on totals fixtures stays
    green if either branch is reverted to `side == "over"`. These fixtures put a
    NON-positional label on the rows -- `fav`/`dog`, as spread will -- so the two
    disagree and a `side`-keyed branch produces the wrong complement.

    This is the failure the plan calls its most dangerous class: silent,
    self-consistent, and invisible to every ledger invariant.
    """

    def test_add_clv_complements_on_position_not_label(self, monkeypatch):
        from mvp.oddspapi import anchors as anchors_mod
        from mvp.oddspapi import board as board_mod

        monkeypatch.setattr(board_mod, "available_books", lambda m: ["pinnacle"])
        monkeypatch.setattr(
            anchors_mod, "close",
            lambda m=None, **kw: pl.DataFrame({"match_uid": ["m1"], "t": [0]}),
        )
        monkeypatch.setattr(
            board_mod, "board_at",
            lambda times, market, **kw: pl.DataFrame({
                "match_uid": ["m1"], "book": ["pinnacle"],
                "points": [2.5], "p_over": [0.70],
            }),
        )
        # The `a` row is labelled `dog` and the `b` row `fav`: the label is the
        # OPPOSITE of the position, which is what a real spread ledger produces
        # on any rung where the lower-id player is not the favourite.
        ledger = pl.DataFrame({
            "match_uid": ["m1", "m1"], "book": ["dk", "dk"],
            "points": [2.5, 2.5], "anchor": ["open", "open"],
            "side": ["dog", "fav"], "side_pos": ["a", "b"],
            "odds": [2.0, 2.0],
        })
        out = add_clv(ledger, market="game_spread").sort("side_pos")
        assert out["p_ref_close"].to_list() == pytest.approx([0.70, 0.30])

    def test_mean_covers_complements_on_position_not_label(self):
        """`expected_spread` is E[margin_a], so the a side covers when it exceeds
        the line regardless of which side carries the `fav` label."""
        ledger = pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "points": [2.5, 2.5],
            "side": ["dog", "fav"], "side_pos": ["a", "b"],
        })
        pmf = pl.DataFrame({"match_uid": ["m1"], "expected_spread": [4.0]})
        out = add_mean_covers(ledger, pmf, market="game_spread").sort("side_pos")
        # E[margin_a] = 4.0 > 2.5, so the A side covers and B does not, whatever
        # the labels say.
        assert out["mean_covers"].to_list() == [True, False]

    def test_mean_covers_requires_side_pos(self):
        """A ledger without it cannot be branched correctly, so refuse rather
        than fall back to `side`."""
        ledger = pl.DataFrame({
            "match_uid": ["m1"], "points": [2.5], "side": ["fav"],
        })
        pmf = pl.DataFrame({"match_uid": ["m1"], "expected_spread": [4.0]})
        with pytest.raises(ValueError, match="side_pos"):
            add_mean_covers(ledger, pmf, market="game_spread")


class TestFavouriteLabelling:
    """`fav`/`dog` is a property of the (match, book, anchor) block, taken from
    the sign of its main line — not of the rung, and not from the shorter price."""

    @staticmethod
    def _settled(main_line: float, extra_lines=(), book="dk"):
        lines = [main_line, *extra_lines]
        n = len(lines)
        return pl.DataFrame({
            "match_uid": ["m1"] * n, "book": [book] * n,
            "points": list(lines),
            "is_main_line": [True] + [False] * (n - 1),
            "over_odds": [2.0] * n, "under_odds": [1.8] * n,
            "model_p_over": [0.5] * n, "model_p_under": [0.5] * n,
            "edge_over": [0.0] * n, "edge_under": [0.0] * n,
            "pnl_over": [0.0] * n, "pnl_under": [0.0] * n,
            "over_won": [True] * n, "actual_spread": [1.0] * n,
        })

    def test_positive_main_line_makes_a_the_favourite(self):
        """Ledger frame: a positive line means a must WIN by more than X."""
        out = unpivot_sides(self._settled(3.5), market="game_spread")
        assert dict(zip(out["side_pos"], out["side"])) == {"a": "fav", "b": "dog"}

    def test_negative_main_line_makes_a_the_underdog(self):
        out = unpivot_sides(self._settled(-3.5), market="game_spread")
        assert dict(zip(out["side_pos"], out["side"])) == {"a": "dog", "b": "fav"}

    def test_zero_main_line_is_pickem(self):
        out = unpivot_sides(self._settled(0.0), market="game_spread")
        assert set(out["side"].to_list()) == {"pk"}

    def test_every_rung_inherits_the_blocks_label(self):
        """A bet on the favourite at a long line is still a bet on the favourite.
        Assigning per rung from the shorter price would flip along the ladder."""
        out = unpivot_sides(
            self._settled(3.5, extra_lines=(-6.5, -2.5, 8.5)), market="game_spread"
        )
        for pos, expected in (("a", "fav"), ("b", "dog")):
            labels = out.filter(pl.col("side_pos") == pos)["side"].unique().to_list()
            assert labels == [expected], f"{pos} drifted across rungs: {labels}"

    def test_a_block_with_no_main_line_is_null_not_guessed(self):
        """`_flag_main_line` needs a live two-sided rung; a block with none has no
        sign to read. Null rather than a default, and harmless because both
        readers of `side` filter to main-line rows first."""
        df = self._settled(3.5).with_columns(pl.lit(False).alias("is_main_line"))
        out = unpivot_sides(df, market="game_spread")
        assert out["side"].null_count() == out.height

    def test_totals_labels_are_unaffected(self):
        out = unpivot_sides(self._settled(22.5).rename({"actual_spread": "actual_total"}))
        assert dict(zip(out["side_pos"], out["side"])) == {"a": "over", "b": "under"}


class TestOrientationAssert:
    """The pmf's `a` and the board's `a` are defined independently and can
    disagree. Nothing downstream notices: relabelling and re-signing together
    leave §7's correlation criterion unchanged, so this guard is the only thing
    between a mismatch and a silently mirrored ledger."""

    @staticmethod
    def _pmf(a_is_uid_min: bool, uid: str = "m1"):
        return pl.DataFrame({
            "match_uid": [uid],
            "spread_offset": [3],
            "spread_pmf": [[0.0, 0.0, 0.25, 0.5, 0.25, 0.0, 0.0]],
            "a_is_uid_min": [a_is_uid_min],
        })

    @staticmethod
    def _board(uid: str = "m1"):
        return pl.DataFrame({
            "match_uid": [uid], "book": ["dk"], "points": [0.5],
            "over_odds": [2.0], "under_odds": [1.9], "p_over": [0.5],
        })

    def test_agreement_prices_normally(self):
        out = price(self._board(), self._pmf(True), market="game_spread")
        assert out.height == 1

    def test_disagreement_raises(self):
        with pytest.raises(ValueError, match="orientation mismatch"):
            price(self._board(), self._pmf(False), market="game_spread")

    def test_only_priced_matches_are_checked(self):
        """A mismatched match nobody quotes is not an error — asserting over the
        whole projection would fire on ~11.5% of the test set immediately."""
        pmf = pl.concat([self._pmf(True, "m1"), self._pmf(False, "m2")])
        out = price(self._board("m1"), pmf, market="game_spread")
        assert out.height == 1

    def test_totals_frames_without_the_column_are_unaffected(self):
        board = self._board().with_columns(pl.lit(21.5).alias("points"))
        pmf = pl.DataFrame({
            "match_uid": ["m1"], "total_games_pmf": [[0.0] * 21 + [0.5, 0.5]],
        })
        assert price(board, pmf).height == 1

    def test_a_spread_pmf_without_the_column_raises(self):
        """Absent is fine for a market with no orientation to check. For spread
        the column is mandatory, and skipping BECAUSE the operand is missing is
        the same fail-open the guard exists to prevent."""
        pmf = self._pmf(True).drop("a_is_uid_min")
        with pytest.raises(ValueError, match="missing `a_is_uid_min`"):
            price(self._board(), pmf, market="game_spread")

    def test_a_null_flag_is_treated_as_disagreement(self):
        """An unknown orientation is exactly the state this refuses to price
        through — reading null as agreement would fail open."""
        pmf = self._pmf(True).with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("a_is_uid_min")
        )
        with pytest.raises(ValueError, match="orientation mismatch"):
            price(self._board(), pmf, market="game_spread")


class TestSpreadSettlement:
    """Settling against the realised margin rather than the realised total.

    The comparison is identical in shape for both markets because both are
    already in the a-frame — `actual_spread` is `games_a - games_b` and `points`
    is a threshold on that same quantity — so what is under test is that the
    right COLUMN is read, not that the arithmetic differs.
    """

    @staticmethod
    def _priced(points):
        return pl.DataFrame({
            "match_uid": ["m1"], "book": ["dk"], "points": [points],
            "over_odds": [2.0], "under_odds": [1.9],
            "model_p_over": [0.5], "model_p_under": [0.5],
            "edge_over": [0.0], "edge_under": [0.0],
            "p_push": [0.0],
        })

    @staticmethod
    def _outcomes(margin):
        return pl.DataFrame({"match_uid": ["m1"], "actual_spread": [margin]})

    def test_the_a_side_wins_when_the_margin_beats_the_line(self):
        out = settle(self._priced(2.5), self._outcomes(4.0), market="game_spread")
        assert out["over_won"][0] is True
        assert out["pnl_over"][0] > 0 and out["pnl_under"][0] < 0

    def test_the_a_side_loses_when_it_does_not(self):
        out = settle(self._priced(2.5), self._outcomes(1.0), market="game_spread")
        assert out["over_won"][0] is False

    def test_a_negative_line_settles_correctly(self):
        """`margin_a = -3` against a -4.5 line: a lost by 3, which beats losing
        by more than 4.5, so the a side covers."""
        out = settle(self._priced(-4.5), self._outcomes(-3.0), market="game_spread")
        assert out["over_won"][0] is True

    def test_a_whole_number_line_pushes(self):
        """A whole-number spread pushes exactly as a whole-number total does —
        stake returned, pnl 0 on BOTH sides, not a loss on either."""
        out = settle(self._priced(3.0), self._outcomes(3.0), market="game_spread")
        assert out["is_push"][0] is True
        assert out["pnl_over"][0] == 0.0 and out["pnl_under"][0] == 0.0

    def test_the_totals_column_is_not_accepted_for_spread(self):
        """Passing a totals outcomes frame must raise rather than settle the
        spread against a total, which would score every rung nonsensically."""
        totals = pl.DataFrame({"match_uid": ["m1"], "actual_total": [21.0]})
        with pytest.raises(ValueError, match="actual_spread"):
            settle(self._priced(2.5), totals, market="game_spread")

    def test_totals_settlement_is_unchanged(self):
        out = settle(
            self._priced(21.5),
            pl.DataFrame({"match_uid": ["m1"], "actual_total": [24.0]}),
        )
        assert out["over_won"][0] is True



class TestMarketFailureIsolation:
    """A market the stage tree does not carry must not take the other one down.

    `build_ledger` raises when no entry book carries the market. Letting that
    propagate loses the ledger that already succeeded — and after the per-market
    split there IS another one to lose.
    """

    def test_one_missing_market_does_not_lose_the_other(self, monkeypatch, tmp_path):
        from mvp.projection.iid import evaluation as ev

        calls = []

        def fake_build(pmf, *, market, **kw):
            calls.append(market)
            if market == "game_spread":
                raise ev.MarketNotCarried("no entry books carry game_spread")
            return pl.DataFrame({"match_uid": ["m1"], "anchor": ["open"]})

        class _Run:
            fp_dir = tmp_path
            def pmf_for(self, market):
                return pl.DataFrame({"match_uid": ["m1"]})

        monkeypatch.setattr(ev, "build_ledger", fake_build)
        monkeypatch.setattr(
            "mvp.projection.iid.projection_run.run_projection",
            lambda *a, **k: _Run(),
        )
        out = ev.run_backtest(tmp_path / "cfg.yaml")

        assert calls == ["total_games", "game_spread"], "both attempted"
        assert (tmp_path / "backtest.parquet").exists(), "totals ledger survived"
        assert not (tmp_path / "backtest_game_spread.parquet").exists(), (
            "nothing written for the missing market, so the trial retries"
        )
        assert set(out) == {"total_games", "game_spread"}

    def test_the_raise_site_and_the_catch_agree(self, monkeypatch):
        """Driving the REAL condition rather than patching `build_ledger`: if the
        raise at the top of `build_ledger` ever changes type, this fails rather
        than silently unhooking the catch above it."""
        from mvp.oddspapi import board as board_mod
        from mvp.projection.iid import evaluation as ev

        monkeypatch.setattr(board_mod, "entry_books", lambda m: [])
        with pytest.raises(ev.MarketNotCarried, match="no entry books carry"):
            ev.build_ledger(
                pl.DataFrame({"match_uid": ["m1"], "actual_total": [22.0]}),
                market="total_games",
            )

    def test_it_is_a_runtimeerror_so_existing_callers_still_catch_it(self):
        from mvp.projection.iid import evaluation as ev

        assert issubclass(ev.MarketNotCarried, RuntimeError)


class TestAnchorDisplayOrder:
    """Anchors render in the order the board reaches them, not alphabetically.

    `sorted()` gives close/formed2/open, which reads the price path backwards
    in every table of the backtest view.
    """

    @staticmethod
    def _frame(anchors):
        return pl.DataFrame({"anchor": anchors, "rows": list(range(len(anchors)))})

    def test_board_order_not_alphabetical(self):
        got = evaluation._anchors_in_board_order(
            self._frame(["close", "open", "formed2"])
        )
        assert got == ["open", "formed2", "close"]
        assert got != sorted(got)

    def test_a_missing_anchor_is_skipped_not_padded(self):
        got = evaluation._anchors_in_board_order(self._frame(["close", "open"]))
        assert got == ["open", "close"]

    def test_an_unknown_anchor_is_appended_not_dropped(self):
        """Dropping it would silently hide rows the ledger actually carries."""
        got = evaluation._anchors_in_board_order(
            self._frame(["formed3", "close", "open"])
        )
        assert got == ["open", "close", "formed3"]

    def test_frame_sort_follows_the_same_order(self):
        out = evaluation._in_board_order(self._frame(["close", "open", "formed2"]))
        assert out["anchor"].to_list() == ["open", "formed2", "close"]
        # The payload must ride along with its key, not be re-sorted separately.
        assert out["rows"].to_list() == [1, 2, 0]

    def test_order_matches_the_anchor_constant(self):
        """If DEFAULT_ANCHORS is ever reordered, the view follows it."""
        got = evaluation._anchors_in_board_order(
            self._frame(list(reversed(evaluation.DEFAULT_ANCHORS)))
        )
        assert got == list(evaluation.DEFAULT_ANCHORS)


def _curve_row(uid, book, points, side, odds, model_p, edge, main, pnl, won,
               anchor="open"):
    return {
        "match_uid": uid, "market": "total_games", "book": book,
        "points": points, "side": side, "odds": odds, "model_p": model_p,
        "edge": edge, "is_main_line": main, "pnl": pnl, "won": won,
        "anchor": anchor, "role": "entry", "live_side": True,
    }




class TestPolicyTables:
    """The lower view is grouped by selection policy, with the edge band as the
    inner row of every table.

    `safest` and `max_edge` read the whole ladder, so a table computed on main
    lines alone would diagnose a row set two of the three policies never draw
    from. A standalone band table is the marginal of these and only repeats what
    they already carry.
    """

    @staticmethod
    def _ledger():
        # m1: main line 24.5 with a small edge, a live shallow alternate, a dead
        # shallower one, and a deep alternate carrying the biggest edge.
        # m2: main line only, positive. m3: main line only, negative.
        return pl.DataFrame([
            _curve_row("m1", "dk", 22.5, "over", 1.60, 0.68, 0.03, False, 0.60, True),
            _curve_row("m1", "dk", 24.5, "over", 1.91, 0.58, 0.01, True, 0.91, True),
            _curve_row("m1", "dk", 27.5, "over", 3.10, 0.36, 0.12, False, -1.0, False),
            _curve_row("m1", "dk", 21.5, "over", 1.45, 0.74, -0.02, False, -1.0, False),
            _curve_row("m2", "dk", 23.5, "over", 1.95, 0.55, 0.07, True, -1.0, False),
            _curve_row("m3", "dk", 25.5, "over", 1.80, 0.50, -0.10, True, 0.80, True),
        ])

    def _out(self, capsys):
        evaluation._print_policy_tables(self._ledger())
        return capsys.readouterr().out

    @staticmethod
    def _sections(out):
        """Section titles in the order printed."""
        return [
            ln.strip() for ln in out.splitlines()
            if " - by " in ln and "anchor" not in ln
        ]

    @staticmethod
    def _rows(out, title):
        """Data rows under one section title, headers excluded."""
        keep, seen = [], False
        for ln in out.splitlines():
            if " - by " in ln and "anchor" not in ln:
                seen = ln.strip() == title
                continue
            if seen and ln.strip() and "anchor" not in ln:
                keep.append(ln.split())
        return keep

    def test_every_policy_gets_tables(self, capsys):
        titles = self._sections(self._out(capsys))
        for policy in ("main", "safest", "max_edge"):
            assert any(t.startswith(f"{policy} - by") for t in titles), policy

    def test_a_policys_tables_are_grouped_together(self, capsys):
        """Reading one policy end to end should not mean scanning every table
        for its rows."""
        titles = self._sections(self._out(capsys))
        owners = [t.split(" - ")[0] for t in titles]
        assert owners == sorted(owners, key=owners.index)
        assert len(set(owners)) == len(
            [i for i, o in enumerate(owners) if i == 0 or owners[i - 1] != o]
        )

    def test_main_has_no_ladder_depth_table(self, capsys):
        """It sits on the consensus main line by definition, so every row would
        read `main`."""
        titles = self._sections(self._out(capsys))
        assert "main - by side" in titles
        assert not any(
            t.startswith("main - by ladder depth") for t in titles
        ), titles

    def test_bands_are_rows_in_every_table(self, capsys):
        out = self._out(capsys)
        for title in self._sections(out):
            rows = self._rows(out, title)
            assert rows, title
            bands = {b for _, _, b, *_ in rows}
            assert bands <= {n for n, _, _ in evaluation._CURVE_BANDS}, title

    def test_no_standalone_band_table(self, capsys):
        """Its rows are the marginal of the policy tables."""
        out = self._out(capsys)
        assert "by edge band" not in out

    def test_ladder_policies_have_no_negative_band(self, capsys):
        """They carry the edge restriction in their definitions, so a `<0` row
        cannot exist for them. `main` is ungated and keeps its."""
        out = self._out(capsys)
        for title in self._sections(out):
            bands = {b for _, _, b, *_ in self._rows(out, title)}
            if title.startswith("main "):
                assert "<0" in bands, title
            else:
                assert "<0" not in bands, title

    def test_the_deep_rung_reaches_max_edge_but_not_main(self, capsys):
        """m1's 12pp edge is on an alternate rung. `main` cannot see it."""
        out = self._out(capsys)
        assert "10pp+" in {b for _, _, b, *_ in self._rows(out, "max_edge - by side")}
        assert "10pp+" not in {b for _, _, b, *_ in self._rows(out, "main - by side")}

    def test_one_bet_per_match_per_policy(self, capsys):
        """Three matches in, so a policy's side table cannot total more than
        three bets -- otherwise correlated rungs count as separate bets."""
        out = self._out(capsys)
        for policy in ("main", "safest", "max_edge"):
            rows = self._rows(out, f"{policy} - by side")
            assert sum(int(r[3]) for r in rows) <= 3, (policy, rows)

    def test_header_carries_no_parenthetical_commentary(self, capsys):
        out = self._out(capsys)
        assert "entry books" not in out
        assert "OFFER set" not in out
        assert "one bet per match" not in out

    def test_selection_is_not_reimplemented_here(self):
        """One definition of each policy. Two is how they drift apart -- the
        main-line filter this view used to apply was a stale copy of one already
        removed from `rank.py`."""
        import inspect

        body = inspect.getsource(evaluation._print_policy_tables).split('"""')[2]
        assert "_select_one_per_match" in body
        # Required as a column so `main` can be selected; never filtered on here.
        assert 'filter(pl.col("is_main_line")' not in body


class TestLadderDepthClassification:
    """A pick is classed against the consensus main line on its own side.

    `model_p` is the probability the bet wins, so above the baseline is the more
    forgiving number and below it the harder one -- which is the question the
    band curve cannot answer, since both land in whatever band their edge falls
    in regardless of where the market's own number sat.
    """

    @staticmethod
    def _at_anchor():
        return pl.DataFrame([
            _curve_row("m1", "dk", 21.5, "under", 1.45, 0.30, -0.02, False, -1.0, False),
            _curve_row("m1", "dk", 22.5, "under", 1.91, 0.52, 0.01, True, 0.91, True),
            _curve_row("m1", "dk", 23.5, "under", 1.80, 0.58, 0.04, False, 0.80, True),
        ])

    def _classify(self, points):
        at = self._at_anchor()
        picked = at.filter(pl.col("points") == points)
        return evaluation._classify_depth(picked, at)["depth"][0]

    def test_the_baseline_itself_is_main(self):
        assert self._classify(22.5) == "main"

    def test_a_higher_win_probability_is_easier(self):
        """under 23.5 wins more often than the 22.5 main line."""
        assert self._classify(23.5) == "easier"

    def test_a_lower_win_probability_is_harder(self):
        assert self._classify(21.5) == "harder"

    def test_the_baseline_is_the_main_policys_own_pick(self):
        """Books disagree on the main line, so `the` main line needs a tiebreak.
        Re-deriving one here is a second definition waiting to drift."""
        import inspect

        src = inspect.getsource(evaluation._classify_depth)
        assert '_select_one_per_match(at_anchor, "main")' in src
