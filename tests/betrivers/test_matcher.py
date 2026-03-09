"""Tests for BetRivers odds matcher."""

from datetime import datetime, timezone

import polars as pl
import pytest

from mvp.betrivers.matcher import BetRiversOddsMatcher
from mvp.draftkings.matcher import normalize_name


def _make_players(tmp_path):
    """Write a players.parquet with test data."""
    players = pl.DataFrame({
        "player_id": ["PLAYER_A", "PLAYER_B", "PLAYER_C"],
        "first_name": ["Alice", "Bob", "Carlos"],
        "last_name": ["Smith", "Jones", "López"],
    })
    players_dir = tmp_path / "stage" / "atptour"
    players_dir.mkdir(parents=True)
    players.write_parquet(players_dir / "players.parquet")


def _make_odds(tmp_path, events):
    """Write a moneyline.parquet with test data.

    events: list of (br_event_id, player_name, opponent_name, odds) tuples.
    """
    rows = []
    for i, (eid, pname, oname, odds) in enumerate(events):
        rows.append({
            "br_event_id": eid,
            "player_name": pname,
            "opponent_name": oname,
            "odds": odds,
            "fetched_at": datetime(2026, 3, 9, 10, tzinfo=timezone.utc),
            "br_tournament_id": "t1",
            "tournament": "Indian Wells",
            "br_selection_id": f"s{i}",
            "book": "br",
            "market": "moneyline",
            "circuit": "atp",
            "side": "OT_ONE" if i % 2 == 0 else "OT_TWO",
            "points": None,
        })
    odds_dir = tmp_path / "stage" / "betrivers"
    odds_dir.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(odds_dir / "moneyline.parquet")


def _make_predictions():
    return pl.DataFrame({
        "match_uid": ["m1"],
        "p1_id": ["PLAYER_A"],
        "p2_id": ["PLAYER_B"],
        "p1_name": ["Alice Smith"],
        "p2_name": ["Bob Jones"],
        "tournament_id": ["t1"],
        "tournament_name": ["Indian Wells"],
    })


class TestBetRiversOddsMatcher:
    def test_matches_by_player_pair(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5
        assert result.odds["m1"]["PLAYER_B"] == 2.5

    def test_accent_matching(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Carlos Lopez", "Alice Smith", 1.8),
            ("e1", "Alice Smith", "Carlos Lopez", 2.0),
        ])
        preds = pl.DataFrame({
            "match_uid": ["m1"],
            "p1_id": ["PLAYER_C"],
            "p2_id": ["PLAYER_A"],
            "p1_name": ["Carlos López"],
            "p2_name": ["Alice Smith"],
            "tournament_id": ["t1"],
            "tournament_name": ["Test"],
        })
        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        result = matcher.match(preds)

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_C"] == 1.8

    def test_alias_override(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "A. Smithy", "B. Jonesy", 1.5),
            ("e1", "B. Jonesy", "A. Smithy", 2.5),
        ])
        aliases_path = tmp_path / "aliases.yaml"
        aliases_path.write_text('"A. Smithy": "PLAYER_A"\n"B. Jonesy": "PLAYER_B"\n')

        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = aliases_path
        result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5

    def test_deduplicates_to_latest_odds(self, tmp_path):
        _make_players(tmp_path)
        odds_dir = tmp_path / "stage" / "betrivers"
        odds_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "br_event_id": ["e1", "e1", "e1", "e1"],
            "player_name": ["Alice Smith", "Bob Jones", "Alice Smith", "Bob Jones"],
            "opponent_name": ["Bob Jones", "Alice Smith", "Bob Jones", "Alice Smith"],
            "odds": [2.0, 1.8, 2.1, 1.75],
            "fetched_at": [
                datetime(2026, 3, 9, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 9, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 9, 11, tzinfo=timezone.utc),
                datetime(2026, 3, 9, 11, tzinfo=timezone.utc),
            ],
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

        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds["m1"]["PLAYER_A"] == 2.1
        assert result.odds["m1"]["PLAYER_B"] == 1.75

    def test_empty_odds(self, tmp_path):
        _make_players(tmp_path)
        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_unmatched_names_reported(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Unknown Player", "Another Unknown", 1.5),
            ("e1", "Another Unknown", "Unknown Player", 2.5),
        ])
        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds == {}
        assert result.unmatched_names == {"Unknown Player", "Another Unknown"}

    def test_missing_aliases_file(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        matcher = BetRiversOddsMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = tmp_path / "nonexistent.yaml"
        result = matcher.match(_make_predictions())
        assert "m1" in result.odds
