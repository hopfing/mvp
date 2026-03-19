"""Tests for BaseOddsMatcher in mvp.common.odds_matching."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from mvp.common.odds_matching import BaseOddsMatcher, EventMatch, OddsMatchResult


class _TestMatcher(BaseOddsMatcher):
    """Concrete subclass for testing the base class."""

    event_id_column = "test_event_id"
    book_label = "TEST"
    ALIASES_PATH = Path("/nonexistent/aliases.yaml")

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="testbook", data_root=data_root)


def _make_players(tmp_path, rows=None):
    """Write a players.parquet with test data."""
    if rows is None:
        rows = {
            "player_id": ["PLAYER_A", "PLAYER_B", "PLAYER_C", "PLAYER_D"],
            "first_name": ["Alice", "Bob", "Carlos", "David"],
            "last_name": ["Smith", "Jones", "López", "García"],
        }
    players_dir = tmp_path / "stage" / "atptour"
    players_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(players_dir / "players.parquet")


def _make_odds(tmp_path, events):
    """Write a moneyline.parquet with test data.

    events: list of (test_event_id, player_name, odds) tuples.
    """
    rows = []
    for eid, pname, odds in events:
        rows.append({
            "test_event_id": eid,
            "player_name": pname,
            "odds": odds,
            "fetched_at": datetime(2026, 3, 15, 10, tzinfo=timezone.utc),
        })
    odds_dir = tmp_path / "stage" / "testbook"
    odds_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(odds_dir / "moneyline.parquet")


def _make_predictions():
    return pl.DataFrame({
        "match_uid": ["m1", "m2"],
        "p1_id": ["PLAYER_A", "PLAYER_C"],
        "p2_id": ["PLAYER_B", "PLAYER_D"],
        "p1_name": ["Alice Smith", "Carlos López"],
        "p2_name": ["Bob Jones", "David García"],
    })


class TestLoadPlayers:
    def test_builds_name_map(self, tmp_path):
        _make_players(tmp_path)
        matcher = _TestMatcher(data_root=tmp_path)
        name_map = matcher._load_players()
        assert name_map["alice smith"] == "PLAYER_A"
        assert name_map["bob jones"] == "PLAYER_B"
        assert name_map["carlos lopez"] == "PLAYER_C"

    def test_caches_result(self, tmp_path):
        _make_players(tmp_path)
        matcher = _TestMatcher(data_root=tmp_path)
        first = matcher._load_players()
        second = matcher._load_players()
        assert first is second

    def test_warns_on_collision(self, tmp_path, caplog):
        _make_players(tmp_path, {
            "player_id": ["P_SR", "P_JR"],
            "first_name": ["Martin", "Martin"],
            "last_name": ["Damm", "Damm"],
        })
        matcher = _TestMatcher(data_root=tmp_path)
        with caplog.at_level(logging.WARNING, logger="mvp.testbook.matcher"):
            name_map = matcher._load_players()
        assert "Player name collision" in caplog.text
        assert "martin damm" in caplog.text
        # Last one wins
        assert name_map["martin damm"] == "P_JR"

    def test_missing_players_file(self, tmp_path):
        matcher = _TestMatcher(data_root=tmp_path)
        assert matcher._load_players() == {}


class TestLoadAliases:
    def test_loads_from_yaml(self, tmp_path):
        _make_players(tmp_path)
        alias_path = tmp_path / "aliases.yaml"
        alias_path.write_text('"A. Smithy": "PLAYER_A"\n"B. Jonesy": "PLAYER_B"\n')

        matcher = _TestMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = alias_path
        aliases = matcher._load_aliases()
        assert aliases["a. smithy"] == "PLAYER_A"
        assert aliases["b. jonesy"] == "PLAYER_B"

    def test_handles_missing_file(self, tmp_path):
        matcher = _TestMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = tmp_path / "nonexistent.yaml"
        assert matcher._load_aliases() == {}

    def test_caches_result(self, tmp_path):
        matcher = _TestMatcher(data_root=tmp_path)
        first = matcher._load_aliases()
        second = matcher._load_aliases()
        assert first is second


class TestResolveId:
    def test_alias_takes_precedence(self, tmp_path):
        _make_players(tmp_path)
        alias_path = tmp_path / "aliases.yaml"
        # Alias maps "Alice Smith" to a different ID
        alias_path.write_text('"Alice Smith": "ALIAS_A"\n')

        matcher = _TestMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = alias_path
        assert matcher._resolve_id("Alice Smith") == "ALIAS_A"

    def test_falls_back_to_players(self, tmp_path):
        _make_players(tmp_path)
        matcher = _TestMatcher(data_root=tmp_path)
        assert matcher._resolve_id("Alice Smith") == "PLAYER_A"

    def test_returns_none_for_unknown(self, tmp_path):
        _make_players(tmp_path)
        matcher = _TestMatcher(data_root=tmp_path)
        assert matcher._resolve_id("Unknown Player") is None


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
        })
        df.write_parquet(odds_dir / "moneyline.parquet")

        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.get_latest_odds()
        # Should have 2 rows (latest per event+player)
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
    def test_basic_pairing(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", 1.5),
            ("e1", "Bob Jones", 2.5),
        ])
        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5
        assert result.odds["m1"]["PLAYER_B"] == 2.5

    def test_empty_odds(self, tmp_path):
        _make_players(tmp_path)
        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_empty_predictions(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", 1.5),
            ("e1", "Bob Jones", 2.5),
        ])
        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.match(pl.DataFrame())
        assert result.odds == {}

    def test_unmatched_names(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e99", "Unknown Player", 1.5),
            ("e99", "Another Unknown", 2.5),
        ])
        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds == {}
        assert result.unmatched_names == {"Unknown Player", "Another Unknown"}

    def test_event_matches_populated(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", 1.5),
            ("e1", "Bob Jones", 2.5),
        ])
        matcher = _TestMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())

        assert len(result.event_matches) == 1
        em = result.event_matches[0]
        assert em.match_uid == "m1"
        assert em.event_id == "e1"
        assert em.p1_book_name == "Alice Smith"
        assert em.p2_book_name == "Bob Jones"

    def test_log_output(self, tmp_path, caplog):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", 1.5),
            ("e1", "Bob Jones", 2.5),
        ])
        matcher = _TestMatcher(data_root=tmp_path)
        with caplog.at_level(logging.INFO, logger="mvp.testbook.matcher"):
            matcher.match(_make_predictions())
        assert "TEST events" in caplog.text
