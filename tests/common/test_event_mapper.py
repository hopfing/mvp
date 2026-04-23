"""Tests for the decoupled event mapper."""

import polars as pl
import pytest
from pathlib import Path
from unittest.mock import patch

from mvp.common.event_mapper import (
    MappingResult,
    build_match_catalog,
    build_player_lookup,
    map_book_events,
    _strip_circuit_prefix,
    _match_tournament,
    _parse_book_round,
)


# ---------------------------------------------------------------------------
# Player name lookup
# ---------------------------------------------------------------------------

class TestBuildPlayerLookup:
    def test_loads_bio_names(self, tmp_path):
        """Bio first_name + last_name are the baseline lookup."""
        bio = pl.DataFrame({
            "player_id": ["A001", "B002"],
            "first_name": ["Roger", "Rafael"],
            "last_name": ["Federer", "Nadal"],
        })
        bio_path = tmp_path / "stage" / "atptour" / "players.parquet"
        bio_path.parent.mkdir(parents=True)
        bio.write_parquet(bio_path)

        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup()

        assert lookup["roger federer"] == "A001"
        assert lookup["rafael nadal"] == "B002"

    def test_aliases_override_bio(self, tmp_path):
        """Per-book aliases take priority over bio names."""
        bio = pl.DataFrame({
            "player_id": ["A001"],
            "first_name": ["Roger"],
            "last_name": ["Federer"],
        })
        bio_path = tmp_path / "stage" / "atptour" / "players.parquet"
        bio_path.parent.mkdir(parents=True)
        bio.write_parquet(bio_path)

        aliases_path = tmp_path / "aliases.yaml"
        aliases_path.write_text('\"R. Federer\": \"A001\"\n')

        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup(aliases_path=aliases_path)

        assert lookup["r. federer"] == "A001"
        assert lookup["roger federer"] == "A001"

    def test_accent_normalization(self, tmp_path):
        """Accented names are normalized for matching."""
        bio = pl.DataFrame({
            "player_id": ["C003"],
            "first_name": ["Jiří"],
            "last_name": ["Lehečka"],
        })
        bio_path = tmp_path / "stage" / "atptour" / "players.parquet"
        bio_path.parent.mkdir(parents=True)
        bio.write_parquet(bio_path)

        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup()

        assert lookup["jiri lehecka"] == "C003"

    def test_empty_bio_graceful(self, tmp_path):
        """No bio file produces empty lookup without error."""
        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup()

        assert lookup == {}


# ---------------------------------------------------------------------------
# Match catalog
# ---------------------------------------------------------------------------

class TestBuildMatchCatalog:
    def _matches_df(self):
        return pl.DataFrame({
            "match_uid": ["m1", "m2", "m3"],
            "player_id": ["A001", "A001", "B002"],
            "opp_id": ["B002", "C003", "C003"],
            "tournament_id": ["403", "403", "580"],
            "year": [2026, 2026, 2026],
            "tournament_name": ["Miami Open", "Miami Open", "Australian Open"],
        })

    def test_indexes_by_player_pair(self):
        catalog = build_match_catalog(self._matches_df())
        pair_ab = frozenset({"A001", "B002"})
        assert pair_ab in catalog
        assert len(catalog[pair_ab]) == 1
        assert catalog[pair_ab][0]["match_uid"] == "m1"

    def test_deduplicates_by_match_uid(self):
        """Same match_uid from both perspectives should produce one entry."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "player_id": ["A001", "B002"],
            "opp_id": ["B002", "A001"],
            "tournament_id": ["403", "403"],
            "year": [2026, 2026],
        })
        catalog = build_match_catalog(df)
        pair = frozenset({"A001", "B002"})
        assert len(catalog[pair]) == 1

    def test_multiple_matches_same_pair(self):
        """Same pair in different tournaments produces multiple entries."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "player_id": ["A001", "A001"],
            "opp_id": ["B002", "B002"],
            "tournament_id": ["403", "580"],
            "year": [2026, 2026],
            "tournament_name": ["Miami", "Indian Wells"],
        })
        catalog = build_match_catalog(df)
        pair = frozenset({"A001", "B002"})
        assert len(catalog[pair]) == 2

    def test_missing_columns_raises(self):
        df = pl.DataFrame({"match_uid": ["m1"], "player_id": ["A001"]})
        with pytest.raises(ValueError, match="missing required columns"):
            build_match_catalog(df)

    def test_collision_warning_round_robin(self, caplog):
        """Same pair, same tournament+year logs a warning."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "player_id": ["A001", "A001"],
            "opp_id": ["B002", "B002"],
            "tournament_id": ["605", "605"],
            "year": [2026, 2026],
        })
        with caplog.at_level("WARNING"):
            build_match_catalog(df)
        assert "collision" in caplog.text.lower()

    def test_excludes_completed_matches(self):
        """Matches with a non-null result_type are excluded from the catalog."""
        df = pl.DataFrame({
            "match_uid": ["m_done", "m_ret", "m_wo", "m_open"],
            "player_id": ["A001", "A001", "A001", "A001"],
            "opp_id": ["B002", "B002", "B002", "B002"],
            "tournament_id": ["403", "580", "605", "1536"],
            "year": [2026, 2026, 2026, 2026],
            "result_type": ["completed", "retirement", "walkover", None],
        })
        catalog = build_match_catalog(df)
        pair = frozenset({"A001", "B002"})
        assert len(catalog[pair]) == 1
        assert catalog[pair][0]["match_uid"] == "m_open"

    def test_carries_round(self):
        """Round is included in catalog entries when available."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "player_id": ["A001", "A001"],
            "opp_id": ["B002", "B002"],
            "tournament_id": ["1536", "1536"],
            "year": [2026, 2026],
            "round": ["Q2", "R128"],
        })
        catalog = build_match_catalog(df)
        rounds = {e["round"] for e in catalog[frozenset({"A001", "B002"})]}
        assert rounds == {"Q2", "R128"}


# ---------------------------------------------------------------------------
# Tournament name matching
# ---------------------------------------------------------------------------

class TestStripCircuitPrefix:
    def test_atp_prefix(self):
        assert _strip_circuit_prefix("ATP - Miami") == "Miami"

    def test_challenger_prefix(self):
        assert _strip_circuit_prefix("Challenger - Phoenix") == "Phoenix"

    def test_challenger_quals(self):
        assert _strip_circuit_prefix("Challenger Quals. - Murcia") == "Murcia"

    def test_no_prefix(self):
        assert _strip_circuit_prefix("Miami Open") == "Miami Open"


class TestParseBookRound:
    def test_main_draw_numbered_round(self):
        assert _parse_book_round("ATP Madrid - Round 1") == "main"
        assert _parse_book_round("ATP Madrid - Round 2") == "main"

    def test_main_draw_final_variants(self):
        assert _parse_book_round("ATP Houston - Final") == "main"
        assert _parse_book_round("ATP Houston - Semifinales") == "main"
        assert _parse_book_round("Challenger Mexico City - 1/4 Final") == "main"

    def test_qualifier_forms(self):
        assert _parse_book_round("ATP Challenger Busan - Qualification - Hard") == "qual"
        assert _parse_book_round("Challenger Quals. - Madrid") == "qual"
        assert _parse_book_round("ATP Madrid - Qualifying") == "qual"

    def test_no_round_signal(self):
        assert _parse_book_round("ATP - Madrid") is None
        assert _parse_book_round("ATP Madrid 2026") is None
        assert _parse_book_round("Madrid") is None
        assert _parse_book_round("") is None


class TestMatchTournament:
    def test_narrows_by_name(self):
        candidates = [
            {"match_uid": "m1", "tournament_id": "403", "tournament_name": "Miami Open"},
            {"match_uid": "m2", "tournament_id": "580", "tournament_name": "Indian Wells"},
        ]
        result = _match_tournament("ATP - Miami", candidates)
        assert len(result) == 1
        assert result[0]["match_uid"] == "m1"

    def test_no_match_returns_all(self):
        candidates = [
            {"match_uid": "m1", "tournament_id": "403", "tournament_name": "Miami Open"},
        ]
        result = _match_tournament("ATP - Paris", candidates)
        assert len(result) == 1  # falls back to all candidates

    def test_empty_tournament_returns_all(self):
        candidates = [{"match_uid": "m1", "tournament_name": "Miami"}]
        result = _match_tournament("", candidates)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Core mapping logic
# ---------------------------------------------------------------------------

class TestMapBookEvents:
    def _staged_odds(self):
        return pl.DataFrame({
            "dk_event_id": ["e1", "e1", "e2", "e2", "e3", "e3"],
            "player_name": [
                "Roger Federer", "Rafael Nadal",
                "Novak Djokovic", "Andy Murray",
                "Unknown Player", "Roger Federer",
            ],
            "tournament": [
                "ATP - Miami", "ATP - Miami",
                "ATP - Miami", "ATP - Miami",
                "ATP - Miami", "ATP - Miami",
            ],
        })

    def _player_lookup(self):
        return {
            "roger federer": "A001",
            "rafael nadal": "B002",
            "novak djokovic": "C003",
            "andy murray": "D004",
        }

    def _match_catalog(self):
        return {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
            ],
            frozenset({"C003", "D004"}): [
                {"match_uid": "m2", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
            ],
        }

    def test_maps_known_events(self):
        result = map_book_events(
            self._staged_odds(), "dk_event_id", "dk",
            self._player_lookup(), self._match_catalog(),
        )
        assert len(result.event_matches) == 2
        uids = {em.match_uid for em in result.event_matches}
        assert uids == {"m1", "m2"}

    def test_unresolved_names_tracked(self):
        result = map_book_events(
            self._staged_odds(), "dk_event_id", "dk",
            self._player_lookup(), self._match_catalog(),
        )
        assert "Unknown Player" in result.unresolved_names

    def test_skips_existing_events(self):
        result = map_book_events(
            self._staged_odds(), "dk_event_id", "dk",
            self._player_lookup(), self._match_catalog(),
            existing_event_ids={"e1"},
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "m2"

    def test_no_match_tracked(self):
        """Both names resolve but no match in catalog."""
        odds = pl.DataFrame({
            "dk_event_id": ["e99", "e99"],
            "player_name": ["Roger Federer", "Novak Djokovic"],
            "tournament": ["ATP - Miami", "ATP - Miami"],
        })
        catalog = {}  # empty catalog
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.no_match_found) == 1
        assert result.no_match_found[0][0] == "e99"

    def test_ambiguous_match_collision(self):
        """Multiple candidates that can't be disambiguated."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Unknown", "ATP - Unknown"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
                {"match_uid": "m2", "tournament_id": "580", "year": 2026,
                 "tournament_name": "Indian Wells"},
            ],
        }
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 0
        assert len(result.collisions) == 1

    def test_disambiguation_by_tournament(self):
        """Multiple candidates narrowed to one by tournament name."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Miami", "ATP - Miami"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
                {"match_uid": "m2", "tournament_id": "580", "year": 2026,
                 "tournament_name": "Indian Wells Masters"},
            ],
        }
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "m1"

    def test_round_gate_rejects_single_qualifier_when_book_main(self):
        """Single candidate must be rejected if book says main draw but catalog has Q*."""
        odds = pl.DataFrame({
            "b365_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP Madrid - Round 1", "ATP Madrid - Round 1"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "q_match", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "Q2"},
            ],
        }
        result = map_book_events(
            odds, "b365_event_id", "b365",
            self._player_lookup(), catalog,
        )
        assert result.event_matches == []
        assert len(result.no_match_found) == 1

    def test_round_gate_resolves_quals_vs_main_ambiguity(self):
        """Same-tournament Q2 + R128 pair — book 'Round 1' picks R128."""
        odds = pl.DataFrame({
            "b365_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP Madrid - Round 1", "ATP Madrid - Round 1"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "q_match", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "Q2"},
                {"match_uid": "r128_match", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "R128"},
            ],
        }
        result = map_book_events(
            odds, "b365_event_id", "b365",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "r128_match"

    def test_round_gate_is_noop_when_book_has_no_round_signal(self):
        """DK-style tournament text without round info leaves all candidates in play."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Madrid", "ATP - Madrid"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "R128"},
            ],
        }
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "m1"
