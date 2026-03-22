"""Tests for DK odds matcher (event-map-based lookup)."""

from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl
import pytest

from mvp.common.odds_matching import normalize_name
from mvp.draftkings.matcher import DraftKingsOddsMatcher


class TestNormalizeName:
    def test_strips_accents(self):
        assert normalize_name("José María") == "jose maria"

    def test_removes_hyphens(self):
        assert normalize_name("Mpetshi-Perricard") == "mpetshi perricard"

    def test_lowercases(self):
        assert normalize_name("Roger FEDERER") == "roger federer"

    def test_collapses_whitespace(self):
        assert normalize_name("  Roger   Federer  ") == "roger federer"

    def test_combined(self):
        assert normalize_name("Nicolás Álvarez-Varona") == "nicolas alvarez varona"

    def test_empty(self):
        assert normalize_name("") == ""


def _make_odds(tmp_path, events):
    """Write a moneyline.parquet with test data."""
    rows = []
    for i, (eid, pname, oname, odds) in enumerate(events):
        rows.append({
            "dk_event_id": eid,
            "player_name": pname,
            "opponent_name": oname,
            "odds": odds,
            "fetched_at": datetime(2026, 3, 5, 10, tzinfo=timezone.utc),
            "dk_tournament_id": "t1",
            "tournament": "ATP - Test",
            "dk_selection_id": f"s{i}",
            "book": "dk",
            "market": "moneyline",
            "country_code": "US",
            "side": "home" if i % 2 == 0 else "away",
            "points": None,
            "event_status": "NOT_STARTED",
        })
    odds_dir = tmp_path / "stage" / "draftkings"
    odds_dir.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(odds_dir / "moneyline.parquet")


def _make_event_map(events):
    """Build an event map DataFrame.

    events: list of (event_id, match_uid, p1_book_name, p2_book_name) tuples.
    """
    if not events:
        from mvp.analysis.event_map import EVENT_MAP_SCHEMA
        return pl.DataFrame(schema=EVENT_MAP_SCHEMA)
    return pl.DataFrame([
        {
            "match_uid": uid,
            "book": "dk",
            "event_id": eid,
            "p1_book_name": p1,
            "p2_book_name": p2,
            "matched_at": datetime(2026, 3, 5, tzinfo=timezone.utc),
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


class TestDraftKingsOddsMatcher:
    def test_looks_up_odds_from_event_map(self, tmp_path):
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        event_map = _make_event_map([
            ("e1", "m1", "Alice Smith", "Bob Jones"),
        ])

        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5
        assert result.odds["m1"]["PLAYER_B"] == 2.5

    def test_p1_p2_assignment_from_event_map(self, tmp_path):
        """p1_book_name/p2_book_name determine which odds go to which player."""
        _make_odds(tmp_path, [
            ("e1", "Bob Jones", "Alice Smith", 2.5),
            ("e1", "Alice Smith", "Bob Jones", 1.5),
        ])
        # p1_book_name = Alice (our p1 = PLAYER_A)
        event_map = _make_event_map([
            ("e1", "m1", "Alice Smith", "Bob Jones"),
        ])

        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())

        assert result.odds["m1"]["PLAYER_A"] == 1.5  # Alice's odds
        assert result.odds["m1"]["PLAYER_B"] == 2.5  # Bob's odds

    def test_deduplicates_to_latest_odds(self, tmp_path):
        odds_dir = tmp_path / "stage" / "draftkings"
        odds_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "dk_event_id": ["e1", "e1", "e1", "e1"],
            "player_name": ["Alice Smith", "Bob Jones", "Alice Smith", "Bob Jones"],
            "opponent_name": ["Bob Jones", "Alice Smith", "Bob Jones", "Alice Smith"],
            "odds": [2.0, 1.8, 2.1, 1.75],
            "fetched_at": [
                datetime(2026, 3, 5, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 5, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 5, 11, tzinfo=timezone.utc),
                datetime(2026, 3, 5, 11, tzinfo=timezone.utc),
            ],
            "event_status": ["NOT_STARTED"] * 4,
            "dk_tournament_id": ["t1"] * 4,
            "tournament": ["ATP - Test"] * 4,
            "dk_selection_id": ["s1", "s2", "s3", "s4"],
            "book": ["dk"] * 4,
            "market": ["moneyline"] * 4,
            "country_code": ["US"] * 4,
            "side": ["home", "away", "home", "away"],
            "points": [None] * 4,
        })
        df.write_parquet(odds_dir / "moneyline.parquet")
        event_map = _make_event_map([("e1", "m1", "Alice Smith", "Bob Jones")])

        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds["m1"]["PLAYER_A"] == 2.1
        assert result.odds["m1"]["PLAYER_B"] == 1.75

    def test_unmapped_event_skipped(self, tmp_path):
        """Events not in event_map are silently skipped."""
        _make_odds(tmp_path, [
            ("e99", "Unknown Player", "Another Unknown", 1.5),
            ("e99", "Another Unknown", "Unknown Player", 2.5),
        ])
        event_map = _make_event_map([])  # empty

        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_empty_odds(self, tmp_path):
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        event_map = _make_event_map([])
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_empty_predictions(self, tmp_path):
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        event_map = _make_event_map([("e1", "m1", "Alice Smith", "Bob Jones")])

        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(pl.DataFrame())
        assert result.odds == {}
