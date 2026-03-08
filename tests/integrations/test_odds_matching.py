"""Tests for DK odds matching."""

import polars as pl
import pytest

from mvp.integrations.odds_matching import (
    OddsMatchResult,
    get_latest_odds,
    load_aliases,
    match_odds_to_predictions,
    normalize_name,
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


class TestLoadAliases:
    def test_loads_yaml(self, tmp_path):
        alias_file = tmp_path / "aliases.yaml"
        alias_file.write_text('{"J. Sinner": "SINNER_J"}\n')
        result = load_aliases(alias_file)
        assert result == {"J. Sinner": "SINNER_J"}

    def test_missing_file(self, tmp_path):
        result = load_aliases(tmp_path / "nonexistent.yaml")
        assert result == {}

    def test_empty_file(self, tmp_path):
        alias_file = tmp_path / "aliases.yaml"
        alias_file.write_text("")
        result = load_aliases(alias_file)
        assert result == {}


class TestGetLatestOdds:
    def test_deduplicates_to_latest(self, tmp_path):
        from datetime import datetime, timezone

        df = pl.DataFrame({
            "dk_event_id": ["e1", "e1", "e1", "e1"],
            "player_name": ["Alice", "Bob", "Alice", "Bob"],
            "opponent_name": ["Bob", "Alice", "Bob", "Alice"],
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
        path = tmp_path / "odds.parquet"
        df.write_parquet(path)

        result = get_latest_odds(path)
        assert len(result) == 2
        alice_row = result.filter(pl.col("player_name") == "Alice")
        assert alice_row["odds"][0] == 2.1
        bob_row = result.filter(pl.col("player_name") == "Bob")
        assert bob_row["odds"][0] == 1.75

    def test_missing_file(self, tmp_path):
        result = get_latest_odds(tmp_path / "missing.parquet")
        assert len(result) == 0


class TestMatchOddsToPredictions:
    def _make_predictions(self):
        return pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "p1_id": ["PLAYER_A", "PLAYER_C"],
            "p2_id": ["PLAYER_B", "PLAYER_D"],
            "p1_name": ["Alice Smith", "Carlos López"],
            "p2_name": ["Bob Jones", "David García"],
            "tournament_id": ["t1", "t2"],
            "tournament_name": ["Test Open", "Test Challenger"],
        })

    def _make_odds(self):
        from datetime import datetime, timezone

        return pl.DataFrame({
            "dk_event_id": ["e1", "e1", "e2", "e2"],
            "player_name": ["Alice Smith", "Bob Jones", "Carlos Lopez", "David Garcia"],
            "opponent_name": ["Bob Jones", "Alice Smith", "David Garcia", "Carlos Lopez"],
            "odds": [1.5, 2.5, 1.8, 2.0],
            "fetched_at": [datetime(2026, 3, 5, tzinfo=timezone.utc)] * 4,
            "dk_tournament_id": ["t1", "t1", "t2", "t2"],
            "tournament": ["ATP - Test", "ATP - Test", "Challenger - Test", "Challenger - Test"],
            "dk_selection_id": ["s1", "s2", "s3", "s4"],
            "book": ["dk"] * 4,
            "market": ["moneyline"] * 4,
            "country_code": ["US"] * 4,
            "side": ["home", "away", "home", "away"],
            "points": [None] * 4,
        })

    def test_matches_by_player_pair(self):
        predictions = self._make_predictions()
        odds = self._make_odds()
        result = match_odds_to_predictions(odds, predictions, {})

        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5
        assert result.odds["m1"]["PLAYER_B"] == 2.5

    def test_accent_matching(self):
        """DK 'Carlos Lopez' matches our 'Carlos López' via normalization."""
        predictions = self._make_predictions()
        odds = self._make_odds()
        result = match_odds_to_predictions(odds, predictions, {})

        assert "m2" in result.odds
        assert result.odds["m2"]["PLAYER_C"] == 1.8
        assert result.odds["m2"]["PLAYER_D"] == 2.0

    def test_alias_override(self):
        predictions = pl.DataFrame({
            "match_uid": ["m1"],
            "p1_id": ["PLAYER_A"],
            "p2_id": ["PLAYER_B"],
            "p1_name": ["Alice Smith"],
            "p2_name": ["Bob Jones"],
            "tournament_id": ["t1"],
            "tournament_name": ["Test Open"],
        })
        from datetime import datetime, timezone

        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["A. Smithy", "B. Jonesy"],
            "opponent_name": ["B. Jonesy", "A. Smithy"],
            "odds": [1.5, 2.5],
            "fetched_at": [datetime(2026, 3, 5, tzinfo=timezone.utc)] * 2,
            "dk_tournament_id": ["t1"] * 2,
            "tournament": ["ATP - Test"] * 2,
            "dk_selection_id": ["s1", "s2"],
            "book": ["dk"] * 2,
            "market": ["moneyline"] * 2,
            "country_code": ["US"] * 2,
            "side": ["home", "away"],
            "points": [None] * 2,
        })
        aliases = {
            "A. Smithy": "PLAYER_A",
            "B. Jonesy": "PLAYER_B",
        }
        result = match_odds_to_predictions(odds, predictions, aliases)
        assert "m1" in result.odds
        assert result.odds["m1"]["PLAYER_A"] == 1.5

    def test_empty_odds(self):
        predictions = self._make_predictions()
        result = match_odds_to_predictions(pl.DataFrame(), predictions, {})
        assert result.odds == {}

    def test_empty_predictions(self):
        odds = self._make_odds()
        result = match_odds_to_predictions(odds, pl.DataFrame(), {})
        assert result.odds == {}

    def test_unmatched_names_reported(self):
        """DK names with no prediction match are returned in unmatched_names."""
        from datetime import datetime, timezone

        predictions = self._make_predictions()
        odds = pl.DataFrame({
            "dk_event_id": ["e99", "e99"],
            "player_name": ["Unknown Player", "Another Unknown"],
            "opponent_name": ["Another Unknown", "Unknown Player"],
            "odds": [1.5, 2.5],
            "fetched_at": [datetime(2026, 3, 5, tzinfo=timezone.utc)] * 2,
            "dk_tournament_id": ["t99"] * 2,
            "tournament": ["ATP - Mystery"] * 2,
            "dk_selection_id": ["s1", "s2"],
            "book": ["dk"] * 2,
            "market": ["moneyline"] * 2,
            "country_code": ["US"] * 2,
            "side": ["home", "away"],
            "points": [None] * 2,
        })
        result = match_odds_to_predictions(odds, predictions, {})
        assert result.odds == {}
        assert result.unmatched_names == {"Unknown Player", "Another Unknown"}
