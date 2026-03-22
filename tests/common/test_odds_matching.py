"""Tests for BaseOddsMatcher in mvp.common.odds_matching (event-map-based)."""

import logging
from datetime import datetime, timezone
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
