"""The (player, match) fetch ledger, and the trigger built on it.

The distinction these pin: only a SUCCESSFUL fetch settles a pair. An error or an
empty payload must leave it open, because one-fetch-per-pair-ever means a 403 would
otherwise burn a pair's only chance and bank a permanent hole in the data this exists
to keep current. That is the same class of silent loss the whole change targets.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from mvp.atptour import activity_ledger as L


@pytest.fixture
def root(tmp_path):
    return tmp_path


class TestLedger:
    def test_absent_ledger_settles_nothing(self, root):
        assert L.settled_pairs(root) == set()

    def test_a_successful_fetch_settles_every_pair_it_covered(self, root):
        L.record(root, "A1", ["m1", "m2"], L.OK)
        assert L.settled_pairs(root) == {("A1", "m1"), ("A1", "m2")}

    def test_an_error_leaves_the_pair_open(self, root):
        """A 403 must not consume a pair's only fetch."""
        L.record(root, "A1", ["m1"], L.ERROR, "403")
        assert L.settled_pairs(root) == set()

    def test_an_empty_payload_leaves_the_pair_open(self, root):
        """Empty previously read as success and would settle the pair forever."""
        L.record(root, "A1", ["m1"], L.EMPTY, "empty Activity payload")
        assert L.settled_pairs(root) == set()

    def test_a_retry_after_failure_settles_it(self, root):
        L.record(root, "A1", ["m1"], L.ERROR, "timeout")
        L.record(root, "A1", ["m1"], L.OK)
        assert ("A1", "m1") in L.settled_pairs(root)

    def test_attempts_accumulate_rather_than_overwrite(self, root):
        L.record(root, "A1", ["m1"], L.ERROR, "1")
        L.record(root, "A1", ["m1"], L.ERROR, "2")
        assert len(list(L.iter_records(root))) == 2

    def test_a_torn_line_does_not_break_the_read(self, root):
        """A crash mid-append leaves a partial line. Refusing to read the log
        because of it would turn a recoverable state into an outage."""
        L.record(root, "A1", ["m1"], L.OK)
        with L.ledger_path(root).open("a", encoding="utf-8") as fh:
            fh.write('{"player_id": "A2", "match_ui')
        assert L.settled_pairs(root) == {("A1", "m1")}

    def test_appending_never_rewrites_earlier_lines(self, root):
        L.record(root, "A1", ["m1"], L.OK)
        first = L.ledger_path(root).read_text(encoding="utf-8")
        L.record(root, "A2", ["m2"], L.OK)
        assert L.ledger_path(root).read_text(encoding="utf-8").startswith(first)

    def test_recording_no_pairs_is_a_no_op(self, root):
        L.record(root, "A1", [], L.OK)
        assert list(L.iter_records(root)) == []

    def test_the_ledger_is_written_immediately_not_batched(self, root):
        """A mid-tick crash must not re-fetch everything on the next run."""
        L.record(root, "A1", ["m1"], L.OK)
        assert L.ledger_path(root).exists()
        assert L.settled_pairs(root) == {("A1", "m1")}


T0 = datetime(2026, 8, 3, 12, 0, 0)


def _pairs(rows: list[tuple[str, str, datetime]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "player_id": [r[0] for r in rows],
            "match_uid": [r[1] for r in rows],
            "scheduled_datetime": [r[2] for r in rows],
        },
        schema={"player_id": pl.String, "match_uid": pl.String,
                "scheduled_datetime": pl.Datetime("us")},
    )


class _Extractor:
    """Drives the real selection logic with the HTTP call replaced."""

    def __init__(self, root, outcome=L.OK):
        from mvp.atptour.extractors.player_activity import PlayerActivityExtractor
        self.ex = PlayerActivityExtractor(data_root=root)
        self.fetched: list[str] = []
        self.outcome = outcome
        self.ex._fetch_player = self._fake

    def _fake(self, pid):
        self.fetched.append(pid)
        return self.outcome, None if self.outcome == L.OK else "boom"


class TestTrigger:
    def test_a_new_pair_triggers_a_fetch(self, root):
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs([("A1", "m1", T0)]))
        assert e.fetched == ["A1"]

    def test_the_same_pair_does_not_trigger_twice(self, root):
        p = _pairs([("A1", "m1", T0)])
        e = _Extractor(root)
        e.ex.run_for_pairs(p)
        e2 = _Extractor(root)
        e2.ex.run_for_pairs(p)
        assert e2.fetched == []

    def test_a_second_match_for_the_same_player_triggers_again(self, root):
        """The point of the design: a new match means re-ask, however recently
        we last did."""
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs([("A1", "m1", T0)]))
        e2 = _Extractor(root)
        e2.ex.run_for_pairs(_pairs([("A1", "m1", T0),
                                    ("A1", "m2", T0 + timedelta(hours=2))]))
        assert e2.fetched == ["A1"]

    def test_one_request_serves_every_open_pair_for_a_player(self, root):
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs([("A1", "m1", T0),
                                   ("A1", "m2", T0 + timedelta(hours=2))]))
        assert e.fetched == ["A1"]
        assert L.settled_pairs(root) == {("A1", "m1"), ("A1", "m2")}

    def test_a_failed_fetch_retries_on_the_next_run(self, root):
        p = _pairs([("A1", "m1", T0)])
        _Extractor(root, outcome=L.ERROR).ex.run_for_pairs(p)
        e2 = _Extractor(root)
        e2.ex.run_for_pairs(p)
        assert e2.fetched == ["A1"]

    def test_the_cap_bounds_a_run(self, root):
        rows = [(f"P{i}", f"m{i}", T0 + timedelta(hours=i)) for i in range(10)]
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs(rows), max_fetches=3)
        assert len(e.fetched) == 3

    def test_the_cap_serves_the_soonest_match_first(self, root):
        """A capped run must not defer the match closest to needing a prediction."""
        now = datetime.now()
        rows = [("LATE", "m3", now + timedelta(days=2)),
                ("SOON", "m1", now + timedelta(minutes=30)),
                ("MID", "m2", now + timedelta(hours=6))]
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs(rows), max_fetches=2)
        assert e.fetched == ["SOON", "MID"]

    def test_an_upcoming_match_outranks_a_played_one(self, root):
        """The bug this pins: ranking on a player's EARLIEST open pair put beaten
        players -- who keep their pairs forever -- ahead of players due tonight.

        PAST holds an older pair than TONIGHT's match, so an earliest-pair sort picks
        PAST first. It has no upcoming match at all and cannot need a prediction.
        """
        now = datetime.now()
        rows = [("PAST", "m_old", now - timedelta(days=6)),
                ("TONIGHT", "m_now", now + timedelta(hours=3))]
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs(rows), max_fetches=1)
        assert e.fetched == ["TONIGHT"]

    def test_a_player_with_only_past_pairs_still_drains(self, root):
        """Deprioritised, not dropped -- otherwise they never settle and are retried
        on every run forever."""
        now = datetime.now()
        rows = [("PAST", "m_old", now - timedelta(days=6)),
                ("TONIGHT", "m_now", now + timedelta(hours=3))]
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs(rows), max_fetches=5)
        assert sorted(e.fetched) == ["PAST", "TONIGHT"]

    def test_a_players_played_pair_does_not_drag_them_forward(self, root):
        """Rank is the soonest UPCOMING match, not the earliest open pair, so an old
        unsettled pair must not pull a player ahead of someone playing sooner."""
        now = datetime.now()
        rows = [("DRAGGED", "m_old", now - timedelta(days=9)),
                ("DRAGGED", "m_late", now + timedelta(days=2)),
                ("SOONER", "m_soon", now + timedelta(hours=1))]
        e = _Extractor(root)
        e.ex.run_for_pairs(_pairs(rows), max_fetches=1)
        assert e.fetched == ["SOONER"]

    def test_deferred_players_are_picked_up_next_run(self, root):
        rows = [(f"P{i}", f"m{i}", T0 + timedelta(hours=i)) for i in range(4)]
        p = _pairs(rows)
        e = _Extractor(root)
        e.ex.run_for_pairs(p, max_fetches=2)
        e2 = _Extractor(root)
        e2.ex.run_for_pairs(p, max_fetches=2)
        assert sorted(e.fetched + e2.fetched) == ["P0", "P1", "P2", "P3"]

    def test_the_time_budget_stops_a_degraded_run(self, root):
        """The cap counts requests, but a request is not a fixed cost. A degraded ATP
        can take ~2 min per player, and nothing serialises ticks."""
        import time as _t
        rows = [(f"P{i}", f"m{i}", T0 + timedelta(hours=i)) for i in range(6)]
        e = _Extractor(root)
        real = e._fake

        def slow(pid):
            _t.sleep(0.05)
            return real(pid)

        e.ex._fetch_player = slow
        e.ex.run_for_pairs(_pairs(rows), max_fetches=6, max_seconds=0.12)
        assert 0 < len(e.fetched) < 6

    def test_work_deferred_by_the_budget_is_not_lost(self, root):
        import time as _t
        rows = [(f"P{i}", f"m{i}", T0 + timedelta(hours=i)) for i in range(4)]
        p = _pairs(rows)
        e = _Extractor(root)
        real = e._fake
        e.ex._fetch_player = lambda pid: (_t.sleep(0.05), real(pid))[1]
        e.ex.run_for_pairs(p, max_fetches=4, max_seconds=0.12)
        e2 = _Extractor(root)
        e2.ex.run_for_pairs(p, max_fetches=4)
        assert sorted(e.fetched + e2.fetched) == ["P0", "P1", "P2", "P3"]

    def test_an_empty_schedule_fetches_nothing(self, root):
        e = _Extractor(root)
        assert e.ex.run_for_pairs(_pairs([])) == ([], set())
        assert e.fetched == []

    def test_it_reports_which_players_were_fetched(self, root):
        """The caller stages exactly this set. Staging the whole scheduled
        population instead would trigger a full activity rebuild every tick."""
        e = _Extractor(root)
        failed, ok = e.ex.run_for_pairs(_pairs([("A1", "m1", T0),
                                                ("A2", "m2", T0)]))
        assert failed == []
        assert ok == {"A1", "A2"}

    def test_a_failed_player_is_not_reported_as_fetched(self, root):
        e = _Extractor(root, outcome=L.ERROR)
        failed, ok = e.ex.run_for_pairs(_pairs([("A1", "m1", T0)]))
        assert ok == set()
        assert failed == [("A1", "boom")]

    def test_an_unsettleable_pair_stops_blocking_the_queue(self, root):
        """A player whose payload is always empty never settles. Without an attempt
        cap it holds the head of the queue on every run, re-requesting a host that has
        IP-flagged this machine and starving everyone behind it.
        """
        p = _pairs([("A1", "m1", T0)])
        for _ in range(L.MAX_ATTEMPTS):
            _Extractor(root, outcome=L.EMPTY).ex.run_for_pairs(p)
        e = _Extractor(root)
        e.ex.run_for_pairs(p)
        assert e.fetched == []

    def test_exhaustion_is_recorded_as_attempts_not_as_success(self, root):
        """It must stay visible that the pair gave up rather than succeeded."""
        p = _pairs([("A1", "m1", T0)])
        for _ in range(L.MAX_ATTEMPTS):
            _Extractor(root, outcome=L.EMPTY).ex.run_for_pairs(p)
        assert ("A1", "m1") in L.closed_pairs(root)
        assert ("A1", "m1") not in L.settled_pairs(root)

    def test_a_retry_within_the_cap_still_happens(self, root):
        p = _pairs([("A1", "m1", T0)])
        _Extractor(root, outcome=L.ERROR).ex.run_for_pairs(p)
        e = _Extractor(root)
        e.ex.run_for_pairs(p)
        assert e.fetched == ["A1"]

    def test_nothing_to_do_reports_no_fetches(self, root):
        """Most ticks land here, and the caller must not stage or consolidate."""
        p = _pairs([("A1", "m1", T0)])
        _Extractor(root).ex.run_for_pairs(p)
        assert _Extractor(root).ex.run_for_pairs(p) == ([], set())
