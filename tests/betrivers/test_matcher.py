"""Tests for BetRivers odds matcher (event-map-based lookup)."""

from datetime import UTC, datetime
from unittest.mock import patch

import polars as pl

from mvp.betrivers.matcher import BetRiversOddsMatcher


def _make_odds(tmp_path, events):
    """Write a moneyline.parquet with test data."""
    rows = []
    for i, (eid, pname, oname, odds) in enumerate(events):
        rows.append({
            "br_event_id": eid,
            "player_name": pname,
            "opponent_name": oname,
            "odds": odds,
            "fetched_at": datetime(2026, 3, 9, 10, tzinfo=UTC),
            "br_tournament_id": "t1",
            "tournament": "Indian Wells",
            "br_selection_id": f"s{i}",
            "book": "br",
            "market": "moneyline",
            "circuit": "atp",
            "side": "OT_ONE" if i % 2 == 0 else "OT_TWO",
            "points": None,
            "event_status": "NOT_STARTED",
        })
    odds_dir = tmp_path / "stage" / "betrivers"
    odds_dir.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(odds_dir / "moneyline.parquet")


def _make_event_map(events):
    """Build an event map DataFrame."""
    if not events:
        from mvp.analysis.event_map import EVENT_MAP_SCHEMA
        return pl.DataFrame(schema=EVENT_MAP_SCHEMA)
    return pl.DataFrame([
        {
            "match_uid": uid,
            "book": "br",
            "event_id": eid,
            "p1_book_name": p1,
            "p2_book_name": p2,
            "matched_at": datetime(2026, 3, 9, tzinfo=UTC),
            "source": "auto",
        }
        for eid, uid, p1, p2 in events
    ])


def _make_predictions():
    return pl.DataFrame({
        "match_uid": ["m1"],
        "p1_id": ["PLAYER_A"],
        "p2_id": ["PLAYER_B"],
    })


class TestBetRiversOddsMatcher:
    def test_looks_up_odds_from_event_map(self, tmp_path):
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        event_map = _make_event_map([("e1", "m1", "Alice Smith", "Bob Jones")])

        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5
        assert result.odds["m1"]["PLAYER_B"] == 2.5

    def test_deduplicates_to_latest_odds(self, tmp_path):
        odds_dir = tmp_path / "stage" / "betrivers"
        odds_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "br_event_id": ["e1", "e1", "e1", "e1"],
            "player_name": ["Alice Smith", "Bob Jones", "Alice Smith", "Bob Jones"],
            "opponent_name": ["Bob Jones", "Alice Smith", "Bob Jones", "Alice Smith"],
            "odds": [2.0, 1.8, 2.1, 1.75],
            "fetched_at": [
                datetime(2026, 3, 9, 10, tzinfo=UTC),
                datetime(2026, 3, 9, 10, tzinfo=UTC),
                datetime(2026, 3, 9, 11, tzinfo=UTC),
                datetime(2026, 3, 9, 11, tzinfo=UTC),
            ],
            "event_status": ["NOT_STARTED"] * 4,
            "br_tournament_id": ["t1"] * 4,
            "tournament": ["Test"] * 4,
            "br_selection_id": ["s1", "s2", "s3", "s4"],
            "book": ["br"] * 4,
            "market": ["moneyline"] * 4,
            "circuit": ["atp"] * 4,
            "side": ["OT_ONE", "OT_TWO", "OT_ONE", "OT_TWO"],
            "points": [None] * 4,
        })
        df.write_parquet(odds_dir / "moneyline.parquet")
        event_map = _make_event_map([("e1", "m1", "Alice Smith", "Bob Jones")])

        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds["m1"]["PLAYER_A"] == 2.1
        assert result.odds["m1"]["PLAYER_B"] == 1.75

    def test_unmapped_event_skipped(self, tmp_path):
        _make_odds(tmp_path, [
            ("e99", "Unknown", "Another", 1.5),
            ("e99", "Another", "Unknown", 2.5),
        ])
        event_map = _make_event_map([])

        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_empty_odds(self, tmp_path):
        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        event_map = _make_event_map([])
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds == {}
