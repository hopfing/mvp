"""Tests for DK odds matcher."""

from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl
import pytest

from mvp.common.odds_matching import EventMatch, OddsMatchResult, normalize_name
from mvp.draftkings.matcher import (
    DraftKingsOddsMatcher,
    normalize_tournament,
)


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


class TestNormalizeTournament:
    def test_strips_atp_prefix(self):
        assert normalize_tournament("ATP - Indian Wells") == "indian wells"

    def test_strips_challenger_prefix(self):
        assert normalize_tournament("Challenger - Santiago") == "santiago"

    def test_strips_challenger_quals_prefix(self):
        assert normalize_tournament("Challenger Quals. - Santiago") == "santiago"

    def test_no_prefix(self):
        assert normalize_tournament("Indian Wells") == "indian wells"

    def test_accents_in_tournament(self):
        assert normalize_tournament("ATP - São Paulo") == "sao paulo"


def _make_players(tmp_path):
    """Write a players.parquet with test data."""
    players = pl.DataFrame({
        "player_id": ["PLAYER_A", "PLAYER_B", "PLAYER_C", "PLAYER_D"],
        "first_name": ["Alice", "Bob", "Carlos", "David"],
        "last_name": ["Smith", "Jones", "López", "García"],
    })
    players_dir = tmp_path / "stage" / "atptour"
    players_dir.mkdir(parents=True)
    players.write_parquet(players_dir / "players.parquet")


def _make_odds(tmp_path, events):
    """Write a moneyline.parquet with test data.

    events: list of (dk_event_id, player_name, opponent_name, odds) tuples.
    """
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
        })
    odds_dir = tmp_path / "stage" / "draftkings"
    odds_dir.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(odds_dir / "moneyline.parquet")


def _make_predictions():
    return pl.DataFrame({
        "match_uid": ["m1", "m2"],
        "p1_id": ["PLAYER_A", "PLAYER_C"],
        "p2_id": ["PLAYER_B", "PLAYER_D"],
        "p1_name": ["Alice Smith", "Carlos López"],
        "p2_name": ["Bob Jones", "David García"],
        "tournament_id": ["t1", "t2"],
        "tournament_name": ["Test Open", "Test Challenger"],
    })


class TestDraftKingsOddsMatcher:
    def test_matches_by_player_pair(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5
        assert result.odds["m1"]["PLAYER_B"] == 2.5

    def test_accent_matching(self, tmp_path):
        """DK 'Carlos Lopez' matches our 'Carlos López' via normalization."""
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e2", "Carlos Lopez", "David Garcia", 1.8),
            ("e2", "David Garcia", "Carlos Lopez", 2.0),
        ])
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())

        assert "m2" in result.odds
        assert result.odds["m2"]["PLAYER_C"] == 1.8
        assert result.odds["m2"]["PLAYER_D"] == 2.0

    def test_alias_override(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "A. Smithy", "B. Jonesy", 1.5),
            ("e1", "B. Jonesy", "A. Smithy", 2.5),
        ])
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        aliases_yaml = {"A. Smithy": "PLAYER_A", "B. Jonesy": "PLAYER_B"}

        with patch.object(matcher, "_load_aliases", return_value={
            normalize_name(k): v for k, v in aliases_yaml.items()
        }):
            result = matcher.match(_make_predictions())

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5

    def test_deduplicates_to_latest_odds(self, tmp_path):
        _make_players(tmp_path)
        # Two snapshots — should use latest
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

        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds["m1"]["PLAYER_A"] == 2.1
        assert result.odds["m1"]["PLAYER_B"] == 1.75

    def test_empty_odds(self, tmp_path):
        _make_players(tmp_path)
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds == {}

    def test_empty_predictions(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        result = matcher.match(pl.DataFrame())
        assert result.odds == {}

    def test_unmatched_names_reported(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e99", "Unknown Player", "Another Unknown", 1.5),
            ("e99", "Another Unknown", "Unknown Player", 2.5),
        ])
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert result.odds == {}
        assert result.unmatched_names == {"Unknown Player", "Another Unknown"}

    def test_loads_aliases_from_yaml(self, tmp_path):
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "A. Smithy", "B. Jonesy", 1.5),
            ("e1", "B. Jonesy", "A. Smithy", 2.5),
        ])
        # Write a real aliases file
        aliases_path = tmp_path / "aliases.yaml"
        aliases_path.write_text('"A. Smithy": "PLAYER_A"\n"B. Jonesy": "PLAYER_B"\n')

        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = aliases_path
        result = matcher.match(_make_predictions())
        assert "m1" in result.odds

    def test_missing_aliases_file(self, tmp_path):
        """Missing aliases file doesn't crash."""
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = tmp_path / "nonexistent.yaml"
        result = matcher.match(_make_predictions())
        assert "m1" in result.odds

    def test_event_matches_populated(self, tmp_path):
        """Successful matches should populate event_matches."""
        _make_players(tmp_path)
        _make_odds(tmp_path, [
            ("e1", "Alice Smith", "Bob Jones", 1.5),
            ("e1", "Bob Jones", "Alice Smith", 2.5),
        ])
        matcher = DraftKingsOddsMatcher(data_root=tmp_path)
        result = matcher.match(_make_predictions())
        assert len(result.event_matches) == 1
        em = result.event_matches[0]
        assert em.match_uid == "m1"
        assert em.event_id == "e1"
        assert em.p1_book_name == "Alice Smith"
        assert em.p2_book_name == "Bob Jones"
