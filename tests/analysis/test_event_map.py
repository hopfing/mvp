"""Tests for event mapping persistence."""

import polars as pl
import pytest
import yaml
from datetime import datetime, timezone
from pathlib import Path

from mvp.common.odds_matching import EventMatch


class TestEventMap:
    def test_save_new_mappings(self, tmp_path):
        """New event matches should be saved to parquet."""
        from mvp.analysis.event_map import save_event_mappings

        matches = [
            EventMatch("m1", "dk_e1", "Player A", "Player B"),
            EventMatch("m2", "dk_e2", "Player C", "Player D"),
        ]

        path = tmp_path / "event_map.parquet"
        save_event_mappings(matches, book="dk", path=path)

        df = pl.read_parquet(path)
        assert len(df) == 2
        assert set(df.columns) >= {"match_uid", "book", "event_id", "p1_book_name", "p2_book_name", "source"}
        assert df["book"].unique().to_list() == ["dk"]
        assert df["source"].unique().to_list() == ["auto"]

    def test_append_without_duplicates(self, tmp_path):
        """Appending should skip existing (match_uid, book) pairs."""
        from mvp.analysis.event_map import save_event_mappings

        path = tmp_path / "event_map.parquet"

        batch1 = [EventMatch("m1", "dk_e1", "A", "B")]
        save_event_mappings(batch1, book="dk", path=path)

        batch2 = [
            EventMatch("m1", "dk_e1", "A", "B"),  # duplicate
            EventMatch("m2", "dk_e2", "C", "D"),  # new
        ]
        save_event_mappings(batch2, book="dk", path=path)

        df = pl.read_parquet(path)
        assert len(df) == 2

    def test_different_books_coexist(self, tmp_path):
        """DK and BR mappings for the same match should both be stored."""
        from mvp.analysis.event_map import save_event_mappings

        path = tmp_path / "event_map.parquet"
        save_event_mappings([EventMatch("m1", "dk_e1", "A", "B")], book="dk", path=path)
        save_event_mappings([EventMatch("m1", "br_e1", "A", "B")], book="br", path=path)

        df = pl.read_parquet(path)
        assert len(df) == 2
        assert set(df["book"].to_list()) == {"dk", "br"}

    def test_load_event_map(self, tmp_path):
        """Loading should return a DataFrame even if file doesn't exist."""
        from mvp.analysis.event_map import load_event_map

        path = tmp_path / "event_map.parquet"
        df = load_event_map(path)
        assert len(df) == 0
        assert "match_uid" in df.columns

    def test_manual_overrides_merged(self, tmp_path):
        """Manual override YAML entries should be merged into the map."""
        from mvp.analysis.event_map import load_event_map_with_overrides

        path = tmp_path / "event_map.parquet"
        override_path = tmp_path / "event_overrides.yaml"

        override_path.write_text(yaml.dump([
            {"match_uid": "m99", "book": "dk", "event_id": "dk_e99",
             "p1_book_name": "Override P1", "p2_book_name": "Override P2"},
        ]))

        df = load_event_map_with_overrides(path, override_path)
        assert len(df) == 1
        assert df["source"][0] == "manual"
