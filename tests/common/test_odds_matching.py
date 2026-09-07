"""Tests for BaseOddsMatcher in mvp.common.odds_matching (event-map-based)."""

import logging
from datetime import UTC, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import polars as pl

from mvp.analysis.event_map import EVENT_MAP_SCHEMA
from mvp.common.odds_matching import BaseOddsMatcher, OddsMatchResult


class _TestMatcher(BaseOddsMatcher):
    """Concrete subclass for testing the base class."""

    event_id_column = "test_event_id"
    book_label = "TEST"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="testbook", data_root=data_root)


def _make_odds(tmp_path, events):
    """Write a moneyline.parquet with test data."""
    rows = []
    for eid, pname, odds in events:
        rows.append({
            "test_event_id": eid,
            "player_name": pname,
            "odds": odds,
            "fetched_at": datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
            "event_status": "NOT_STARTED",
        })
    odds_dir = tmp_path / "stage" / "testbook"
    odds_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(odds_dir / "moneyline.parquet")


def _make_event_map(events):
    if not events:
        return pl.DataFrame(schema=EVENT_MAP_SCHEMA)
    return pl.DataFrame([
        {
            "match_uid": uid,
            "book": "test",
            "event_id": eid,
            "p1_book_name": p1,
            "p2_book_name": p2,
            "matched_at": datetime(2026, 3, 15, tzinfo=timezone.utc),
            "source": "auto",
        }
        for eid, uid, p1, p2 in events
    ])


def _make_predictions():
    return pl.DataFrame({
        "match_uid": ["m1", "m2"],
        "p1_id": ["PLAYER_A", "PLAYER_C"],
        "p2_id": ["PLAYER_B", "PLAYER_D"],
    })


class TestGetLatestOdds:
    def test_deduplicates(self, tmp_path):
        odds_dir = tmp_path / "stage" / "testbook"
        odds_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "test_event_id": ["e1", "e1", "e1", "e1"],
            "player_name": ["Alice Smith", "Bob Jones", "Alice Smith", "Bob Jones"],
            "odds": [2.0, 1.8, 2.1, 1.75],
            "fetched_at": [
                datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 15, 11, tzinfo=timezone.utc),
                datetime(2026, 3, 15, 11, tzinfo=timezone.utc),
            ],
            "event_status": ["NOT_STARTED"] * 4,
        })
        df.write_parquet(odds_dir / "moneyline.parquet")

        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.get_latest_odds()
        assert len(result) == 2
        alice = result.filter(pl.col("player_name") == "Alice Smith")
        assert alice["odds"][0] == 2.1

    def test_filters_started_events(self, tmp_path):
        odds_dir = tmp_path / "stage" / "testbook"
        odds_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "test_event_id": ["e1", "e1"],
            "player_name": ["Alice Smith", "Bob Jones"],
            "odds": [1.5, 2.5],
            "fetched_at": [
                datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
            ],
            "event_status": ["STARTED", "STARTED"],
        })
        df.write_parquet(odds_dir / "moneyline.parquet")

        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.get_latest_odds()
        assert len(result) == 0

    def test_missing_file(self, tmp_path):
        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.get_latest_odds()
        assert len(result) == 0


class TestMatch:
    def test_basic_lookup(self, tmp_path):
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", 1.5),
            ("e1", "Bob Jones", 2.5),
        ])
        event_map = _make_event_map([("e1", "m1", "Alice Smith", "Bob Jones")])

        matcher = _TestMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5
        assert result.odds["m1"]["PLAYER_B"] == 2.5

    def test_empty_odds(self, tmp_path):
        matcher = _TestMatcher(data_root=tmp_path)
        event_map = _make_event_map([])
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_empty_predictions(self, tmp_path):
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", 1.5),
            ("e1", "Bob Jones", 2.5),
        ])
        event_map = _make_event_map([("e1", "m1", "Alice Smith", "Bob Jones")])
        matcher = _TestMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(pl.DataFrame())
        assert result.odds == {}

    def test_unmapped_event_skipped(self, tmp_path):
        _make_odds(tmp_path, [
            ("e99", "Unknown Player", 1.5),
            ("e99", "Another Unknown", 2.5),
        ])
        event_map = _make_event_map([])
        matcher = _TestMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_log_output(self, tmp_path, caplog):
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", 1.5),
            ("e1", "Bob Jones", 2.5),
        ])
        event_map = _make_event_map([("e1", "m1", "Alice Smith", "Bob Jones")])
        matcher = _TestMatcher(data_root=tmp_path)
        with caplog.at_level(logging.INFO, logger="mvp.testbook.matcher"):
            with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
                matcher.match(_make_predictions())
        assert "TEST events" in caplog.text


def _make_run_odds(tmp_path, rows, tz=None):
    """Write a moneyline.parquet with explicit run_at stamps.

    rows: (event_id, player_name, odds, run_at) tuples.
    """
    odds_dir = tmp_path / "stage" / "testbook"
    odds_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([
        {
            "test_event_id": eid,
            "player_name": pname,
            "odds": odds,
            "fetched_at": run_at.replace(tzinfo=tz),
            "run_at": run_at.replace(tzinfo=tz),
            "event_status": "NOT_STARTED",
        }
        for eid, pname, odds, run_at in rows
    ]).write_parquet(odds_dir / "moneyline.parquet")


_TICK_1 = datetime(2026, 9, 7, 10, 45, 4)
_TICK_2 = datetime(2026, 9, 7, 11, 0, 5)


class TestRunAnchor:
    """The live pipeline reads every book at one shared run stamp so a book
    whose fetch failed this tick drops out instead of presenting its last
    good run as current."""

    def test_anchor_keeps_only_that_run(self, tmp_path):
        _make_run_odds(tmp_path, [
            ("e1", "Alice Smith", 2.0, _TICK_1), ("e1", "Bob Jones", 1.8, _TICK_1),
            ("e1", "Alice Smith", 2.1, _TICK_2), ("e1", "Bob Jones", 1.75, _TICK_2),
        ])
        result = _TestMatcher(data_root=tmp_path).get_latest_odds(anchor=_TICK_1)
        assert sorted(result["odds"].to_list()) == [1.8, 2.0]

    def test_anchor_newer_than_book_yields_nothing(self, tmp_path):
        """The book's newest run is one tick older than the anchor: stale."""
        _make_run_odds(tmp_path, [
            ("e1", "Alice Smith", 2.0, _TICK_1), ("e1", "Bob Jones", 1.8, _TICK_1),
        ])
        result = _TestMatcher(data_root=tmp_path).get_latest_odds(anchor=_TICK_2)
        assert len(result) == 0

    def test_no_anchor_uses_books_own_latest_run(self, tmp_path):
        _make_run_odds(tmp_path, [
            ("e1", "Alice Smith", 2.0, _TICK_1), ("e1", "Bob Jones", 1.8, _TICK_1),
            ("e1", "Alice Smith", 2.1, _TICK_2), ("e1", "Bob Jones", 1.75, _TICK_2),
        ])
        result = _TestMatcher(data_root=tmp_path).get_latest_odds()
        assert sorted(result["odds"].to_list()) == [1.75, 2.1]

    def test_anchor_compares_wall_clock_across_zone_aware_stage(self, tmp_path):
        _make_run_odds(tmp_path, [
            ("e1", "Alice Smith", 2.0, _TICK_2), ("e1", "Bob Jones", 1.8, _TICK_2),
        ], tz=UTC)
        matcher = _TestMatcher(data_root=tmp_path)
        assert matcher.latest_run_at() == _TICK_2
        assert len(matcher.get_latest_odds(anchor=_TICK_2)) == 2

    def test_latest_run_at(self, tmp_path):
        _make_run_odds(tmp_path, [
            ("e1", "Alice Smith", 2.0, _TICK_1), ("e1", "Alice Smith", 2.1, _TICK_2),
        ])
        assert _TestMatcher(data_root=tmp_path).latest_run_at() == _TICK_2

    def test_latest_run_at_missing_file(self, tmp_path):
        assert _TestMatcher(data_root=tmp_path).latest_run_at() is None

    def test_latest_run_anchor_is_max_across_books(self, tmp_path):
        from mvp.common.odds_matching import latest_run_anchor

        fresh, stale = tmp_path / "fresh", tmp_path / "stale"
        _make_run_odds(fresh, [("e1", "Alice Smith", 2.0, _TICK_2)])
        _make_run_odds(stale, [("e1", "Alice Smith", 2.0, _TICK_1)])
        matchers = [
            _TestMatcher(data_root=fresh),
            _TestMatcher(data_root=stale),
            _TestMatcher(data_root=tmp_path / "empty"),
        ]
        assert latest_run_anchor(matchers) == _TICK_2
        assert latest_run_anchor([_TestMatcher(data_root=tmp_path / "empty")]) is None

    def test_match_with_stale_anchor_returns_no_odds(self, tmp_path):
        _make_run_odds(tmp_path, [
            ("e1", "Alice Smith", 2.0, _TICK_1), ("e1", "Bob Jones", 1.8, _TICK_1),
        ])
        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions(), anchor=_TICK_2)
        assert result.odds == {}
