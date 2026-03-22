"""Tests for BetMGM odds matcher (event-map-based lookup)."""

from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl
import pytest

from mvp.analysis.event_map import EVENT_MAP_SCHEMA


def _make_staged_odds(tmp_path, entries):
    """Write staged odds parquet for testing."""
    df = pl.DataFrame(entries)
    path = tmp_path / "stage" / "betmgm" / "moneyline.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def _make_event_map(events):
    """Build an event map DataFrame."""
    if not events:
        return pl.DataFrame(schema=EVENT_MAP_SCHEMA)
    return pl.DataFrame([
        {
            "match_uid": uid,
            "book": "mgm",
            "event_id": eid,
            "p1_book_name": p1,
            "p2_book_name": p2,
            "matched_at": datetime(2026, 3, 11, tzinfo=timezone.utc),
            "source": "auto",
        }
        for eid, uid, p1, p2 in events
    ])


def _make_predictions(*matches):
    """Build a predictions DataFrame."""
    rows = []
    for m in matches:
        rows.append({
            "match_uid": m[0],
            "p1_id": m[1],
            "p2_id": m[2],
        })
    return pl.DataFrame(rows)


NOW = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)


class TestBetMGMOddsMatcher:
    def test_looks_up_odds_from_event_map(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1"],
            "player_name": ["Carlos Alcaraz", "Casper Ruud"],
            "odds": [1.08, 7.25],
            "fetched_at": [NOW, NOW],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
        })
        event_map = _make_event_map([
            ("e1", "M1", "Carlos Alcaraz", "Casper Ruud"),
        ])

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "ALCARAZ", "RUUD"))
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(preds)

        assert "M1" in result.odds
        assert result.odds["M1"]["ALCARAZ"] == pytest.approx(1.08)
        assert result.odds["M1"]["RUUD"] == pytest.approx(7.25)

    def test_unmapped_event_skipped(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1"],
            "player_name": ["Unknown", "Another"],
            "odds": [2.0, 1.8],
            "fetched_at": [NOW, NOW],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
        })
        event_map = _make_event_map([])

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "A", "B"))
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(preds)

        assert result.odds == {}

    def test_empty_odds_returns_empty(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "A", "B"))
        event_map = _make_event_map([])
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(preds)

        assert len(result.odds) == 0

    def test_deduplicates_to_latest(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        t1 = datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1", "e1", "e1"],
            "player_name": ["Carlos Alcaraz", "Casper Ruud", "Carlos Alcaraz", "Casper Ruud"],
            "odds": [1.5, 2.5, 1.6, 2.4],
            "fetched_at": [t1, t1, t2, t2],
            "event_status": ["NOT_STARTED", "NOT_STARTED", "NOT_STARTED", "NOT_STARTED"],
        })
        event_map = _make_event_map([
            ("e1", "M1", "Carlos Alcaraz", "Casper Ruud"),
        ])

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "ALCARAZ", "RUUD"))
        with patch("mvp.analysis.event_map.load_event_map_with_overrides", return_value=event_map):
            result = matcher.match(preds)

        assert result.odds["M1"]["ALCARAZ"] == pytest.approx(1.6)
        assert result.odds["M1"]["RUUD"] == pytest.approx(2.4)
