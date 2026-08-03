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

from mvp.projection.iid.pricing import bets, cumulative, price, settle


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
