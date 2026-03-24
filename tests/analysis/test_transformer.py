"""Tests for per-book snapshot transformers."""

import polars as pl
import pytest
from datetime import datetime, timezone


class TestDKTransformer:
    def _make_event_map(self):
        return pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "book": ["dk", "dk"],
            "event_id": ["e1", "e2"],
            "p1_book_name": ["Player A1", "Player B1"],
            "p2_book_name": ["Player A2", "Player B2"],
            "source": ["auto", "auto"],
        })

    def _make_staged(self):
        t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
        return pl.DataFrame({
            "dk_event_id": ["e1", "e1", "e2", "e2"],
            "player_name": ["Player A1", "Player A2", "Player B1", "Player B2"],
            "odds": [2.20, 1.70, 1.85, 1.95],
            "event_status": ["NOT_STARTED", "NOT_STARTED", "NOT_STARTED", "NOT_STARTED"],
            "fetched_at": [t1, t1, t1, t1],
        })

    def test_resolves_match_uid_and_side(self):
        from mvp.draftkings.transformer import resolve_snapshots

        result = resolve_snapshots(self._make_staged(), self._make_event_map())

        assert set(result.columns) == {"match_uid", "book", "player_id", "odds", "fetched_at", "event_status"}
        assert len(result) == 4
        assert set(result["match_uid"].to_list()) == {"m1", "m2"}
        assert set(result["player_id"].to_list()) == {"p1", "p2"}
        assert result["book"].unique().to_list() == ["dk"]

    def test_unmatched_names_excluded(self):
        from mvp.draftkings.transformer import resolve_snapshots

        t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
        staged = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Wrong Name", "Player A2"],
            "odds": [2.20, 1.70],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
            "fetched_at": [t1, t1],
        })

        result = resolve_snapshots(staged, self._make_event_map())
        # Only the matched name passes through
        assert len(result) == 1
        assert result["player_id"][0] == "p2"

    def test_empty_event_map_returns_empty(self):
        from mvp.draftkings.transformer import resolve_snapshots

        empty_map = pl.DataFrame(schema={
            "match_uid": pl.Utf8, "book": pl.Utf8, "event_id": pl.Utf8,
            "p1_book_name": pl.Utf8, "p2_book_name": pl.Utf8, "source": pl.Utf8,
        })

        result = resolve_snapshots(self._make_staged(), empty_map)
        assert len(result) == 0


class TestBRTransformer:
    def test_resolves_snapshots(self):
        from mvp.betrivers.transformer import resolve_snapshots

        t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
        staged = pl.DataFrame({
            "br_event_id": ["e1", "e1"],
            "player_name": ["Player A1", "Player A2"],
            "odds": [2.00, 1.80],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
            "fetched_at": [t1, t1],
        })
        event_map = pl.DataFrame({
            "match_uid": ["m1"], "book": ["br"], "event_id": ["e1"],
            "p1_book_name": ["Player A1"], "p2_book_name": ["Player A2"],
            "source": ["auto"],
        })

        result = resolve_snapshots(staged, event_map)
        assert len(result) == 2
        assert result["book"].unique().to_list() == ["br"]


class TestMGMTransformer:
    def test_resolves_snapshots(self):
        from mvp.betmgm.transformer import resolve_snapshots

        t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
        staged = pl.DataFrame({
            "mgm_event_id": ["e1", "e1"],
            "player_name": ["Player A1", "Player A2"],
            "odds": [2.00, 1.80],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
            "fetched_at": [t1, t1],
        })
        event_map = pl.DataFrame({
            "match_uid": ["m1"], "book": ["mgm"], "event_id": ["e1"],
            "p1_book_name": ["Player A1"], "p2_book_name": ["Player A2"],
            "source": ["auto"],
        })

        result = resolve_snapshots(staged, event_map)
        assert len(result) == 2
        assert result["book"].unique().to_list() == ["mgm"]
