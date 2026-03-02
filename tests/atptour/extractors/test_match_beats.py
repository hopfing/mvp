"""Tests for MatchBeatsExtractor."""

import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests

from mvp.atptour.extractors.match_beats import MatchBeatsExtractor
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit


@pytest.fixture
def tournament():
    return Tournament(
        tournament_id="339",
        year=2023,
        circuit=Circuit.tour,
        location="Indian Wells, USA",
    )


@pytest.fixture
def tournament_2021():
    return Tournament(
        tournament_id="339",
        year=2021,
        circuit=Circuit.tour,
        location="Indian Wells, USA",
    )


@pytest.fixture
def extractor(tmp_path):
    return MatchBeatsExtractor(data_root=tmp_path)


class TestMatchBeatsExtractor:
    """Tests for MatchBeatsExtractor."""

    def test_extractor_init(self, tmp_path):
        """Extractor should initialize with data_root."""
        extractor = MatchBeatsExtractor(data_root=tmp_path)
        assert extractor.data_root == tmp_path

    def test_get_match_ids_from_results(self, extractor, tournament, tmp_path):
        """Should read match IDs from staged results parquet."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)

        df = pl.DataFrame({
            "match_id": ["MS001", "MS002", None, "MS003"],
        })
        df.write_parquet(stage_dir / "results.parquet")

        match_ids = extractor._get_match_ids(tournament)

        assert match_ids == ["MS001", "MS002", "MS003"]

    def test_build_url(self, extractor):
        """URL should use uppercase match ID."""
        url = extractor._build_url(year=2023, event_id="339", match_id="ms001")

        assert "MS001" in url
        assert "year/2023" in url
        assert "eventId/339" in url

    def test_skip_pre_2022(self, extractor, tournament_2021, caplog):
        """Should skip tournaments before 2022."""
        import logging

        caplog.set_level(logging.DEBUG)
        extractor.run(tournament_2021)

        log_text = caplog.text.lower()
        assert "pre-2022" in log_text or "skipping matchbeats" in log_text

    def test_no_results_parquet(self, extractor, tournament):
        """Should return empty list when no results parquet."""
        match_ids = extractor._get_match_ids(tournament)
        assert match_ids == []

    def test_skips_existing_json(self, extractor, tournament, tmp_path):
        """Should skip fetching existing match beats files."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        beats_dir.mkdir(parents=True)
        (beats_dir / "MS001.json").write_text("{}")

        with patch.object(extractor.session, "get") as mock:
            extractor.run(tournament, refresh=False)
        mock.assert_not_called()

    def test_refresh_refetches_existing(self, extractor, tournament, tmp_path):
        """Should refetch when refresh=True even if file exists."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        beats_dir.mkdir(parents=True)
        (beats_dir / "MS001.json").write_text("{}")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "lastModified": 1672531200000,
            "response": "encrypted_data",
        }

        with (
            patch.object(extractor.session, "get", return_value=mock_response),
            patch(
                "mvp.atptour.extractors.match_beats.decrypt_response",
                return_value={"isMatchComplete": True, "data": "test"},
            ) as mock_decrypt,
        ):
            extractor.run(tournament, refresh=True)

        mock_decrypt.assert_called_once()

    def test_fetches_and_saves_complete_match(self, extractor, tournament, tmp_path):
        """Should fetch, decrypt, and save complete match data."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "lastModified": 1672531200000,
            "response": "encrypted_data",
        }

        decrypted = {"isMatchComplete": True, "matchWinner": "1", "data": "test"}

        with (
            patch.object(extractor.session, "get", return_value=mock_response),
            patch(
                "mvp.atptour.extractors.match_beats.decrypt_response",
                return_value=decrypted,
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        saved = beats_dir / "MS001.json"
        assert saved.exists()
        data = json.loads(saved.read_text())
        assert data == decrypted

    def test_skips_stub_data(self, extractor, tournament, tmp_path):
        """Should skip matches where isMatchComplete is False."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "lastModified": 1672531200000,
            "response": "encrypted_data",
        }

        with (
            patch.object(extractor.session, "get", return_value=mock_response),
            patch(
                "mvp.atptour.extractors.match_beats.decrypt_response",
                return_value={"isMatchComplete": False},
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        saved = beats_dir / "MS001.json"
        assert not saved.exists()

    def test_request_failure_continues(self, extractor, tournament, tmp_path):
        """Should continue processing after request failure."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001", "MS002"]})
        df.write_parquet(stage_dir / "results.parquet")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "lastModified": 1672531200000,
            "response": "encrypted_data",
        }

        def side_effect(url, timeout=30):
            if "MS001" in url:
                raise requests.RequestException("connection error")
            return mock_response

        with (
            patch.object(extractor.session, "get", side_effect=side_effect),
            patch(
                "mvp.atptour.extractors.match_beats.decrypt_response",
                return_value={"isMatchComplete": True, "data": "test"},
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        assert not (beats_dir / "MS001.json").exists()
        assert (beats_dir / "MS002.json").exists()

    def test_decrypt_failure_continues(self, extractor, tournament, tmp_path):
        """Should continue processing after decryption failure."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001", "MS002"]})
        df.write_parquet(stage_dir / "results.parquet")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "lastModified": 1672531200000,
            "response": "encrypted_data",
        }

        call_count = [0]

        def decrypt_side_effect(encrypted, last_modified):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("decryption failed")
            return {"isMatchComplete": True, "data": "test"}

        with (
            patch.object(extractor.session, "get", return_value=mock_response),
            patch(
                "mvp.atptour.extractors.match_beats.decrypt_response",
                side_effect=decrypt_side_effect,
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        assert not (beats_dir / "MS001.json").exists()
        assert (beats_dir / "MS002.json").exists()

    def test_missing_lastmodified_skipped(self, extractor, tournament, tmp_path):
        """Should skip when response missing lastModified."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "response": "encrypted_data",
        }

        with patch.object(extractor.session, "get", return_value=mock_response):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        saved = beats_dir / "MS001.json"
        assert not saved.exists()

    def test_missing_encrypted_response_skipped(self, extractor, tournament, tmp_path):
        """Should skip when response missing encrypted data."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "lastModified": 1672531200000,
        }

        with patch.object(extractor.session, "get", return_value=mock_response):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        saved = beats_dir / "MS001.json"
        assert not saved.exists()

    def test_get_match_ids_deduplicates(self, extractor, tournament, tmp_path):
        """Should deduplicate match IDs."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001", "MS001", "MS002"]})
        df.write_parquet(stage_dir / "results.parquet")

        match_ids = extractor._get_match_ids(tournament)
        assert match_ids == ["MS001", "MS002"]

    def test_url_structure(self, extractor):
        """Verify full URL structure."""
        url = extractor._build_url(year=2023, event_id="339", match_id="ms001")
        expected = (
            "https://itp-atp-sls.infosys-platforms.com/prod/api/match-beats/data"
            "/year/2023/eventId/339/matchId/MS001"
        )
        assert url == expected
