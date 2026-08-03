"""Entry anchors over the oddspapi stage tree.

The distinction these tests exist to pin: a book that becomes two-sided can stop being
two-sided by pulling a side, so "the n-th book to open" is NOT the moment n books are
available together. Readiness has to be swept as transitions, not taken as an order
statistic over each book's first quote.
`test_formed_requires_simultaneity_not_arrival_order` is the case that separates the
two implementations.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from mvp.oddspapi import anchors, paths

T0 = datetime(2026, 6, 1, 12, 0, 0)


def _write(tmp_path, book: str, rows: list[tuple]) -> None:
    """rows of (match_uid, points, side, offset_s, active). offset_s is BEFORE T0."""
    d = tmp_path / book
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "match_uid": [r[0] for r in rows],
            "points": [r[1] for r in rows],
            "side": [r[2] for r in rows],
            "odds": [1.9] * len(rows),
            "fetched_at": [T0 - timedelta(seconds=r[3]) for r in rows],
            "active": [r[4] for r in rows],
            "event_status": ["NOT_STARTED"] * len(rows),
        }
    ).write_parquet(d / "total_games.parquet")


def _fixture_map(tmp_path, uids: list[str], start: datetime | None = T0) -> None:
    """Minimal `_fixture_map.parquet`. `close` needs it: a book that never pulls was
    takeable until the match began, so the start time is the close."""
    pl.DataFrame(
        {
            "match_uid": uids,
            "true_start_time": [start] * len(uids),
            "start_time": [start] * len(uids),
        },
        # typed even when every value is null -- that is the real shape on disk, and
        # an untyped Null column would fail differently from real data
        schema={
            "match_uid": pl.String,
            "true_start_time": pl.Datetime("us"),
            "start_time": pl.Datetime("us"),
        },
    ).write_parquet(tmp_path / "_fixture_map.parquet")


def _at(secs: int) -> datetime:
    return T0 - timedelta(seconds=secs)


@pytest.fixture
def stage(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "stage_root", lambda: tmp_path)
    _fixture_map(tmp_path, ["m1", "m2"])
    return tmp_path


class TestOpen:
    def test_open_is_the_first_two_sided_moment(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True),    # one side only
            ("m1", 22.5, "Under", 600, True),   # pair completes here
        ])
        out = anchors.open_("total_games", books=["betrivers"])
        assert out["t"][0] == _at(600)

    def test_a_one_sided_book_never_opens(self, stage):
        _write(stage, "betrivers", [("m1", 22.5, "Over", 900, True)])
        assert anchors.open_("total_games", books=["betrivers"]).is_empty()

    def test_an_inactive_pair_does_not_open(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True),
            ("m1", 22.5, "Under", 900, False),
            ("m1", 22.5, "Under", 300, True),   # only now is it takeable
        ])
        assert anchors.open_("total_games", books=["betrivers"])["t"][0] == _at(300)

    def test_open_takes_the_earliest_book(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True)])
        _write(stage, "draftkings", [
            ("m1", 22.5, "Over", 300, True), ("m1", 22.5, "Under", 300, True)])
        out = anchors.open_("total_games", books=["betrivers", "draftkings"])
        assert out["t"][0] == _at(900)

    def test_open_is_formed_of_one(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True)])
        a = anchors.open_("total_games", books=["betrivers"])
        b = anchors.formed("total_games", 1, books=["betrivers"])
        assert a.equals(b)


class TestFormed:
    def test_formed_two_is_the_second_book(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True)])
        _write(stage, "draftkings", [
            ("m1", 22.5, "Over", 300, True), ("m1", 22.5, "Under", 300, True)])
        out = anchors.formed("total_games", 2, books=["betrivers", "draftkings"])
        assert out["t"][0] == _at(300)

    def test_formed_requires_simultaneity_not_arrival_order(self, stage):
        """betrivers opens at 900s then pulls a side at 600s; draftkings opens at 300s.

        Two books have opened by 300s, but only one is live then. An order statistic
        over per-book first quotes would return 300s. The correct answer is that the
        match never forms.
        """
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True),
            ("m1", 22.5, "Under", 900, True),
            ("m1", 22.5, "Under", 600, False),   # pulled
        ])
        _write(stage, "draftkings", [
            ("m1", 22.5, "Over", 300, True), ("m1", 22.5, "Under", 300, True)])
        out = anchors.formed("total_games", 2, books=["betrivers", "draftkings"])
        assert out.is_empty()

    def test_formed_fires_when_the_pulled_book_returns(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True),
            ("m1", 22.5, "Under", 900, True),
            ("m1", 22.5, "Under", 600, False),
            ("m1", 22.5, "Under", 200, True),    # back
        ])
        _write(stage, "draftkings", [
            ("m1", 22.5, "Over", 300, True), ("m1", 22.5, "Under", 300, True)])
        out = anchors.formed("total_games", 2, books=["betrivers", "draftkings"])
        assert out["t"][0] == _at(200)

    def test_a_book_swapping_rungs_in_one_instant_stays_ready(self, stage):
        """Dropping 22.5 and raising 23.5 at the same timestamp is a line move, not a
        gap in availability.

        This pins the observable behaviour, not the mechanism: the per-instant delta
        sum in `_ready_book_count` already cancels the pair, so the matching guard in
        `_book_ready_transitions` is belt-and-braces and removing it does not fail
        this test.
        """
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True),
            ("m1", 22.5, "Over", 600, False), ("m1", 22.5, "Under", 600, False),
            ("m1", 23.5, "Over", 600, True), ("m1", 23.5, "Under", 600, True),
        ])
        _write(stage, "draftkings", [
            ("m1", 22.5, "Over", 600, True), ("m1", 22.5, "Under", 600, True)])
        out = anchors.formed("total_games", 2, books=["betrivers", "draftkings"])
        assert out["t"][0] == _at(600)

    def test_unreachable_threshold_yields_no_row(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True)])
        assert anchors.formed("total_games", 3, books=["betrivers"]).is_empty()

    def test_n_books_below_one_is_refused(self, stage):
        with pytest.raises(ValueError, match="n_books"):
            anchors.formed("total_games", 0, books=["betrivers"])

    def test_matches_are_resolved_independently(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True),
            ("m2", 22.5, "Over", 100, True), ("m2", 22.5, "Under", 100, True),
        ])
        out = anchors.open_("total_games", books=["betrivers"]).sort("match_uid")
        assert out["t"].to_list() == [_at(900), _at(100)]


class TestClose:
    """`close` is a fixed lead before the start, not the last instant a price existed.

    Nobody transacts at the moment the market shuts. The earlier definition resolved
    to the last two-sided interval and fell back to the match start -- which landed on
    the start for 96.8% of matches and read the board mid-teardown, where 36.8% of
    two-sided rows are already dead against 17.0% fifteen minutes earlier.
    """

    def test_close_is_a_fixed_lead_before_the_start(self, stage):
        out = anchors.close("total_games")
        assert out["t"][0] == T0 - timedelta(minutes=anchors.CLOSE_LEAD_MINUTES)

    def test_it_does_not_land_on_the_start(self, stage):
        """The whole point: the start is when books tear the board down."""
        assert anchors.close("total_games")["t"][0] != T0

    def test_the_lead_is_a_parameter(self, stage):
        assert anchors.close("total_games", lead_minutes=60)["t"][0] == (
            T0 - timedelta(hours=1)
        )

    def test_it_does_not_depend_on_what_books_did(self, stage):
        """Exogenous, like offset. A book pulling early must not move the anchor --
        it is simply not live at it, which the board's `live` flag expresses."""
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True),
            ("m1", 22.5, "Under", 300, False),
        ])
        pulled = anchors.close("total_games")["t"][0]
        _write(stage, "draftkings", [
            ("m2", 22.5, "Over", 900, True), ("m2", 22.5, "Under", 900, True)])
        held = anchors.close("total_games").filter(
            pl.col("match_uid") == "m2")["t"][0]
        assert pulled == held == T0 - timedelta(minutes=anchors.CLOSE_LEAD_MINUTES)

    def test_a_match_without_a_start_time_is_dropped(self, stage):
        _fixture_map(stage, ["m1"], start=None)
        assert anchors.close("total_games").is_empty()


class TestOffset:
    def test_offset_subtracts_from_the_start(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True)])
        assert anchors.offset(1.0)["t"][0] == T0 - timedelta(hours=1)

    def test_offset_zero_is_the_off(self, stage):
        assert anchors.offset(0)["t"][0] == T0

    def test_matches_without_a_start_time_are_dropped_not_defaulted(self, stage):
        _fixture_map(stage, ["m1"], start=None)
        assert anchors.offset(1.0).is_empty()

    def test_a_rescheduled_fixture_resolves_deterministically(self, stage, tmp_path):
        """Two rows for one match with different start times. Keeping an arbitrary one
        would make the anchor irreproducible, which is the property it exists for."""
        pl.DataFrame(
            {
                "match_uid": ["m1", "m1"],
                "true_start_time": [T0, T0 + timedelta(hours=3)],
                "start_time": [T0, T0 + timedelta(hours=3)],
            }
        ).write_parquet(tmp_path / "_fixture_map.parquet")
        seen = {anchors.offset(0)["t"][0] for _ in range(5)}
        assert seen == {T0 + timedelta(hours=3)}


class TestFeedsBoardAt:
    def test_anchor_output_is_the_shape_board_at_consumes(self, stage):
        _write(stage, "betrivers", [
            ("m1", 22.5, "Over", 900, True), ("m1", 22.5, "Under", 900, True)])
        from mvp.oddspapi.board import board_at

        times = anchors.open_("total_games", books=["betrivers"])
        b = board_at(times, "total_games", books=["betrivers"])
        assert b.height == 1
        assert b["is_main_line"][0]
        assert b["quote_age_s"][0] == 0
