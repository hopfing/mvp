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

from mvp.projection.iid.evaluation import (
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
        assert b["side"][0] == "over"
        assert b["odds"][0] == pytest.approx(2.0)

    def test_an_under_only_rung_is_taken_as_an_under(self):
        b = bets(self._settled(over=None, under=2.0), min_edge=-1.0)
        assert b.height == 1
        assert b["side"][0] == "under"
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
        assert b["side"][0] == "under"

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
        assert b["side"][0] == "over"

    def test_the_dead_side_is_not_selectable_even_with_the_better_edge(self):
        """Odds carry forward from the last tick, so a pulled leg keeps a price."""
        b = bets(self._settled(over=False, under=True), min_edge=-1.0)
        assert b["side"][0] == "under"

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
        out = unpivot_sides(self._settled())
        for col in ("match_uid", "book", "points", "p_over", "actual_total"):
            assert out[col].n_unique() == 1

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
        return pl.DataFrame({
            "match_uid": ["m1"], "book": ["dk"], "points": [points],
            "side": [side], "odds": [odds], "anchor": ["open"],
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
