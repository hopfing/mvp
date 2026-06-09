"""Tests for MatchBeatsExtractor."""

import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests

from mvp.atptour.extractors.match_beats import MatchBeatsExtractor
from mvp.atptour.extractors.match_centre import (
    DataType,
    DataTypeConfig,
    MatchCentreExtractor,
)

# Patch target for decrypt_response (now in match_centre module)
DECRYPT_PATCH = "mvp.atptour.extractors.match_centre.decrypt_response"
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


def make_mock_response(data: dict) -> MagicMock:
    """Create a mock response with json() returning data."""
    mock = MagicMock()
    mock.json.return_value = data
    return mock


def make_status_response(match_beats_available: bool = True) -> dict:
    """Create a status API response."""
    return {
        "lastModified": 1672531200000,
        "response": "encrypted_status",
    }


def make_status_decrypted(match_beats_available: bool = True) -> dict:
    """Create decrypted status data."""
    return {
        "matchCenter": {
            "matchBeats": match_beats_available,
            "strokeSummary": True,
            "rallyAnalysis": True,
        }
    }


def make_data_response() -> dict:
    """Create a data API response."""
    return {
        "lastModified": 1672531200000,
        "response": "encrypted_data",
    }


def make_data_decrypted(is_complete: bool = True) -> dict:
    """Create decrypted match data."""
    return {
        "isMatchComplete": is_complete,
        "matchWinner": "1",
        "data": "test",
    }


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

        # Mock returns status then data
        call_count = [0]

        def get_side_effect(url, timeout=30):
            call_count[0] += 1
            if "status" in url:
                return make_mock_response(make_status_response())
            return make_mock_response(make_data_response())

        # Decrypt returns status then data
        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            if decrypt_calls[0] == 1:
                return make_status_decrypted()
            return make_data_decrypted()

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(
                DECRYPT_PATCH,
                side_effect=decrypt_side_effect,
            ),
        ):
            extractor.run(tournament, refresh=True)

        assert decrypt_calls[0] == 2  # Status + data

    def test_fetches_and_saves_complete_match(self, extractor, tournament, tmp_path):
        """Should fetch, decrypt, and save complete match data."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        def get_side_effect(url, timeout=30):
            if "status" in url:
                return make_mock_response(make_status_response())
            return make_mock_response(make_data_response())

        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            if decrypt_calls[0] == 1:
                return make_status_decrypted()
            return make_data_decrypted()

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(
                DECRYPT_PATCH,
                side_effect=decrypt_side_effect,
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        saved = beats_dir / "MS001.json"
        assert saved.exists()
        data = json.loads(saved.read_text())
        assert data["isMatchComplete"] is True

    def test_skips_stub_data(self, extractor, tournament, tmp_path):
        """Should skip matches where isMatchComplete is False."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        def get_side_effect(url, timeout=30):
            if "status" in url:
                return make_mock_response(make_status_response())
            return make_mock_response(make_data_response())

        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            if decrypt_calls[0] == 1:
                return make_status_decrypted()
            return make_data_decrypted(is_complete=False)

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(
                DECRYPT_PATCH,
                side_effect=decrypt_side_effect,
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        saved = beats_dir / "MS001.json"
        assert not saved.exists()

    def test_skips_when_matchbeats_unavailable(self, extractor, tournament, tmp_path):
        """Should skip when status says matchBeats is not available."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        def get_side_effect(url, timeout=30):
            if "status" in url:
                return make_mock_response(make_status_response())
            return make_mock_response(make_data_response())

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(
                DECRYPT_PATCH,
                return_value=make_status_decrypted(match_beats_available=False),
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        saved = beats_dir / "MS001.json"
        assert not saved.exists()

    def test_status_failure_skips_match(self, extractor, tournament, tmp_path):
        """Should skip match when status request fails."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001", "MS002"]})
        df.write_parquet(stage_dir / "results.parquet")

        call_count = [0]

        def get_side_effect(url, timeout=30):
            call_count[0] += 1
            if "MS001" in url and "status" in url:
                raise requests.RequestException("connection error")
            if "status" in url:
                return make_mock_response(make_status_response())
            return make_mock_response(make_data_response())

        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            if decrypt_calls[0] == 1:
                return make_status_decrypted()
            return make_data_decrypted()

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(
                DECRYPT_PATCH,
                side_effect=decrypt_side_effect,
            ),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        assert not (beats_dir / "MS001.json").exists()
        assert (beats_dir / "MS002.json").exists()

    def test_data_fetch_failure_continues(self, extractor, tournament, tmp_path):
        """Should continue processing after data fetch failure."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001", "MS002"]})
        df.write_parquet(stage_dir / "results.parquet")

        def get_side_effect(url, timeout=30):
            if "status" in url:
                return make_mock_response(make_status_response())
            if "MS001" in url:
                raise requests.RequestException("connection error")
            return make_mock_response(make_data_response())

        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            # First two are status calls, third is data for MS002
            if decrypt_calls[0] <= 2:
                return make_status_decrypted()
            return make_data_decrypted()

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(
                DECRYPT_PATCH,
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
        mock_response.json.return_value = {"response": "encrypted_data"}

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
        mock_response.json.return_value = {"lastModified": 1672531200000}

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

    def test_get_match_status(self, extractor):
        """Should fetch and decrypt status."""
        mock_response = make_mock_response(make_status_response())

        with (
            patch.object(extractor.session, "get", return_value=mock_response),
            patch(
                DECRYPT_PATCH,
                return_value=make_status_decrypted(),
            ),
        ):
            status = extractor._get_match_status(2023, "339", "MS001")

        assert status["matchCenter"]["matchBeats"] is True

    def test_get_match_status_failure_returns_none(self, extractor):
        """Should return None when status request fails."""
        with patch.object(
            extractor.session,
            "get",
            side_effect=requests.RequestException("error"),
        ):
            status = extractor._get_match_status(2023, "339", "MS001")

        assert status is None

    def test_fetch_data(self, extractor):
        """Should fetch and decrypt data."""
        mock_response = make_mock_response(make_data_response())
        config = DataTypeConfig(
            status_flag="matchBeats",
            folder="match_beats",
            endpoint="https://example.com/api",
        )

        with (
            patch.object(extractor.session, "get", return_value=mock_response),
            patch(
                DECRYPT_PATCH,
                return_value=make_data_decrypted(),
            ),
        ):
            data = extractor._fetch_data(config, 2023, "339", "MS001")

        assert data["isMatchComplete"] is True

    def test_fetch_data_failure_returns_none(self, extractor):
        """Should return None when data request fails."""
        config = DataTypeConfig(
            status_flag="matchBeats",
            folder="match_beats",
            endpoint="https://example.com/api",
        )
        with patch.object(
            extractor.session,
            "get",
            side_effect=requests.RequestException("error"),
        ):
            data = extractor._fetch_data(config, 2023, "339", "MS001")

        assert data is None
