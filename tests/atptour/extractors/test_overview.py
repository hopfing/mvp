"""Tests for OverviewExtractor."""

import json
from unittest.mock import patch

import pytest

from mvp.atptour.extractors.overview import OverviewExtractor
from mvp.common.enums import Circuit


class TestOverviewExtractorCachedRead:
    def test_reads_cached_overview_tour(self, tmp_path):
        """For archive tournaments with existing overview.json, read from disk."""
        data = {
            "EventType": "GS",
            "Location": "Melbourne, Australia",
            "Surface": "Hard",
            "InOutdoor": "O",
        }
        # Set up cached file in tour path
        path = (
            tmp_path
            / "raw"
            / "atptour"
            / "tournaments"
            / "tour"
            / "580"
            / "2023"
            / "overview.json"
        )
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(data), encoding="utf-8")

        extractor = OverviewExtractor(data_root=tmp_path)
        tournament = extractor.run(tournament_id="580", year=2023, is_archive=True)
        assert tournament.tournament_id == "580"
        assert tournament.circuit == Circuit.tour

    def test_reads_cached_overview_chal(self, tmp_path):
        """Checks chal/ path when tour/ doesn't exist."""
        data = {
            "EventType": "CH",
            "Location": "Lima, Peru",
            "Surface": "Clay",
            "InOutdoor": "O",
        }
        path = (
            tmp_path
            / "raw"
            / "atptour"
            / "tournaments"
            / "chal"
            / "1200"
            / "2023"
            / "overview.json"
        )
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(data), encoding="utf-8")

        extractor = OverviewExtractor(data_root=tmp_path)
        tournament = extractor.run(tournament_id="1200", year=2023, is_archive=True)
        assert tournament.circuit == Circuit.chal

    def test_archive_refresh_skips_cache(self, tmp_path):
        """When refresh=True, don't read cached file even for archive."""
        cached_data = {
            "EventType": "GS",
            "Location": "Melbourne, Australia",
            "Surface": "Hard",
            "InOutdoor": "O",
        }
        path = (
            tmp_path
            / "raw"
            / "atptour"
            / "tournaments"
            / "tour"
            / "580"
            / "2023"
            / "overview.json"
        )
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(cached_data), encoding="utf-8")

        fresh_data = {
            "EventType": "GS",
            "Location": "Melbourne, Australia",
            "Surface": "Hard",
            "InOutdoor": "I",
        }
        extractor = OverviewExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=fresh_data):
            tournament = extractor.run(
                tournament_id="580", year=2023, is_archive=True, refresh=True
            )
        # Should have fetched fresh data (indoor="I") rather than cached (indoor="O")
        assert tournament.indoor == "I"


class TestOverviewExtractorFetch:
    def test_fetches_and_saves_on_active(self, tmp_path):
        """Active tournaments always fetch from API."""
        data = {
            "EventType": "GS",
            "Location": "Melbourne, Australia",
            "Surface": "Hard",
            "InOutdoor": "O",
        }
        extractor = OverviewExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=data):
            tournament = extractor.run(tournament_id="580", year=2026)
        assert tournament.tournament_id == "580"
        assert tournament.circuit == Circuit.tour
        # Verify raw JSON was saved
        saved = (
            tmp_path
            / "raw"
            / "atptour"
            / "tournaments"
            / "tour"
            / "580"
            / "2026"
            / "overview.json"
        )
        assert saved.exists()

    def test_null_response_with_circuit_hint(self, tmp_path):
        """When API returns null but circuit hint provided, return fallback Tournament."""
        extractor = OverviewExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=None):
            tournament = extractor.run(
                tournament_id="999", year=2023, circuit=Circuit.chal
            )
        assert tournament.tournament_id == "999"
        assert tournament.circuit == Circuit.chal
        assert tournament.location == "Unknown"

    def test_null_event_type_with_circuit_hint(self, tmp_path):
        """When API returns data with null EventType, use circuit hint."""
        extractor = OverviewExtractor(data_root=tmp_path)
        with patch.object(
            extractor, "fetch_json", return_value={"EventType": None}
        ):
            tournament = extractor.run(
                tournament_id="999", year=2023, circuit=Circuit.chal
            )
        assert tournament.tournament_id == "999"
        assert tournament.circuit == Circuit.chal
        assert tournament.location == "Unknown"

    def test_null_response_without_hint_raises(self, tmp_path):
        """When API returns null and no circuit hint, raise ValueError."""
        extractor = OverviewExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=None):
            with pytest.raises(ValueError, match="null"):
                extractor.run(tournament_id="999", year=2023)
