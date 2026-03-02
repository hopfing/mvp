"""Tests for MatchCentreExtractor."""

import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from mvp.atptour.extractors.match_centre import (
    DATA_TYPE_CONFIGS,
    DataType,
    MatchCentreExtractor,
)
from mvp.atptour.tournament import Tournament
from mvp.common.enums import Circuit

DECRYPT_PATCH = "mvp.atptour.extractors.match_centre.decrypt_response"


@pytest.fixture
def tournament():
    return Tournament(
        tournament_id="339",
        year=2023,
        circuit=Circuit.tour,
        location="Indian Wells, USA",
    )


@pytest.fixture
def extractor(tmp_path):
    return MatchCentreExtractor(
        data_root=tmp_path,
        data_types=[DataType.MATCH_BEATS, DataType.STROKE_ANALYSIS],
    )


def make_mock_response(data: dict) -> MagicMock:
    """Create a mock response with json() returning data."""
    mock = MagicMock()
    mock.json.return_value = data
    return mock


def make_status_decrypted(
    match_beats: bool = True,
    stroke_summary: bool = True,
    rally_analysis: bool = True,
) -> dict:
    """Create decrypted status data."""
    return {
        "matchCenter": {
            "matchBeats": match_beats,
            "strokeSummary": stroke_summary,
            "rallyAnalysis": rally_analysis,
        }
    }


class TestMatchCentreExtractor:
    """Tests for MatchCentreExtractor."""

    def test_extractor_init_default_data_types(self, tmp_path):
        """Default should only fetch match_beats."""
        extractor = MatchCentreExtractor(data_root=tmp_path)
        assert extractor.data_types == [DataType.MATCH_BEATS]

    def test_extractor_init_custom_data_types(self, tmp_path):
        """Should accept custom data types."""
        extractor = MatchCentreExtractor(
            data_root=tmp_path,
            data_types=[DataType.STROKE_ANALYSIS, DataType.RALLY_ANALYSIS],
        )
        assert DataType.STROKE_ANALYSIS in extractor.data_types
        assert DataType.RALLY_ANALYSIS in extractor.data_types

    def test_fetches_multiple_data_types(self, extractor, tournament, tmp_path):
        """Should fetch all requested data types for a match."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        def get_side_effect(url, timeout=30):
            return make_mock_response({
                "lastModified": 1672531200000,
                "response": "encrypted",
            })

        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            if decrypt_calls[0] == 1:
                # Status
                return make_status_decrypted()
            elif decrypt_calls[0] == 2:
                # Match beats data
                return {"isMatchComplete": True, "data": "beats"}
            else:
                # Stroke analysis data
                return {"matchCompleted": True, "data": "strokes"}

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(DECRYPT_PATCH, side_effect=decrypt_side_effect),
        ):
            extractor.run(tournament, refresh=False)

        # Should have fetched both data types
        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        stroke_dir = tmp_path / "raw" / "atptour" / tournament.path / "stroke_analysis"

        assert (beats_dir / "MS001.json").exists()
        assert (stroke_dir / "MS001.json").exists()

    def test_skips_unavailable_data_types(self, extractor, tournament, tmp_path):
        """Should skip data types not available per status."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        def get_side_effect(url, timeout=30):
            return make_mock_response({
                "lastModified": 1672531200000,
                "response": "encrypted",
            })

        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            if decrypt_calls[0] == 1:
                # Status - stroke_summary not available
                return make_status_decrypted(stroke_summary=False)
            else:
                # Match beats data
                return {"isMatchComplete": True, "data": "beats"}

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(DECRYPT_PATCH, side_effect=decrypt_side_effect),
        ):
            extractor.run(tournament, refresh=False)

        beats_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        stroke_dir = tmp_path / "raw" / "atptour" / tournament.path / "stroke_analysis"

        assert (beats_dir / "MS001.json").exists()
        assert not stroke_dir.exists() or not (stroke_dir / "MS001.json").exists()

    def test_single_status_call_per_match(self, extractor, tournament, tmp_path):
        """Should only call status endpoint once per match."""
        stage_dir = tmp_path / "stage" / "atptour" / tournament.path
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({"match_id": ["MS001"]})
        df.write_parquet(stage_dir / "results.parquet")

        status_calls = [0]
        data_calls = [0]

        def get_side_effect(url, timeout=30):
            if "status" in url:
                status_calls[0] += 1
            else:
                data_calls[0] += 1
            return make_mock_response({
                "lastModified": 1672531200000,
                "response": "encrypted",
            })

        decrypt_calls = [0]

        def decrypt_side_effect(encrypted, last_modified):
            decrypt_calls[0] += 1
            if decrypt_calls[0] == 1:
                return make_status_decrypted()
            elif decrypt_calls[0] == 2:
                return {"isMatchComplete": True, "data": "beats"}
            else:
                return {"matchCompleted": True, "data": "strokes"}

        with (
            patch.object(extractor.session, "get", side_effect=get_side_effect),
            patch(DECRYPT_PATCH, side_effect=decrypt_side_effect),
        ):
            extractor.run(tournament, refresh=False)

        # One status call, two data calls (match_beats + stroke_analysis)
        assert status_calls[0] == 1
        assert data_calls[0] == 2

    def test_data_type_configs_completeness_check(self):
        """Verify all data types have proper config."""
        for dt in DataType:
            assert dt in DATA_TYPE_CONFIGS
            config = DATA_TYPE_CONFIGS[dt]
            assert config.status_flag
            assert config.endpoint
            assert config.folder
