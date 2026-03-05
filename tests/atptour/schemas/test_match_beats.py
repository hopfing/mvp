"""Tests for MatchBeats schema."""

from datetime import datetime

import pytest

from mvp.atptour.schemas.match_beats import (
    MatchBeatsPointRecord,
    PointResult,
    SCHEMA_HASH,
)


@pytest.fixture
def valid_point_data():
    """Valid point data for testing."""
    return {
        "tournament_id": "339",
        "year": 2023,
        "match_id": "MS001",
        "is_doubles": False,
        "p1_id": "A123",
        "p2_id": "B456",
        "set_num": 1,
        "game_num": 1,
        "point_num": 1,
        "point_id": "1_1_1_1",
        "result": "A",
        "scorer": "1",
        "server": "1",
        "serve": 1,
        "source_file": "test.json",
        "parsed_at": datetime.now(),
    }


class TestPointResult:
    """Tests for PointResult enum."""

    def test_all_values(self):
        """Should have all expected result types."""
        assert PointResult.ACE == "A"
        assert PointResult.WINNER == "W"
        assert PointResult.UNFORCED_ERROR == "UE"
        assert PointResult.FORCED_ERROR == "FE"
        assert PointResult.DOUBLE_FAULT == "DF"


class TestMatchBeatsPointRecord:
    """Tests for MatchBeatsPointRecord."""

    def test_valid_minimal_record(self, valid_point_data):
        """Should create record with minimal required fields."""
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.tournament_id == "339"
        assert record.result == PointResult.ACE

    def test_result_normalization(self, valid_point_data):
        """Should normalize result string to enum."""
        valid_point_data["result"] = "a"  # lowercase
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.result == PointResult.ACE

    def test_zero_serve_speed_becomes_none(self, valid_point_data):
        """Should convert 0.0 serve speed to None."""
        valid_point_data["serve_speed"] = 0.0
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.serve_speed is None

    def test_nonzero_serve_speed_preserved(self, valid_point_data):
        """Should preserve non-zero serve speed."""
        valid_point_data["serve_speed"] = 214.0
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.serve_speed == 214.0

    def test_zero_fault_speed_becomes_none(self, valid_point_data):
        """Should convert 0.0 fault speed to None."""
        valid_point_data["fault_serve_speed"] = 0.0
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.fault_serve_speed is None

    def test_serve_validation(self, valid_point_data):
        """Should validate serve is 1 or 2."""
        valid_point_data["serve"] = 1
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.serve == 1

        valid_point_data["serve"] = 2
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.serve == 2

    def test_serve_invalid_value(self, valid_point_data):
        """Should reject invalid serve values."""
        valid_point_data["serve"] = 3
        with pytest.raises(ValueError):
            MatchBeatsPointRecord(**valid_point_data)

    def test_all_result_types(self, valid_point_data):
        """Should accept all result types."""
        for result in ["A", "W", "UE", "FE", "DF"]:
            valid_point_data["result"] = result
            record = MatchBeatsPointRecord(**valid_point_data)
            assert record.result == PointResult(result)

    def test_none_result(self, valid_point_data):
        """Should accept None result (no data available)."""
        valid_point_data["result"] = None
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.result is None

    def test_defaults(self, valid_point_data):
        """Should have sensible defaults."""
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.serve_speed is None
        assert record.p1_rally_shots == 0
        assert record.p2_rally_shots == 0
        assert record.is_break_point is False
        assert record.is_tiebreak is False
        assert record.p1_game_score == "0"
        assert record.p2_game_score == "0"

    def test_full_record(self, valid_point_data):
        """Should create record with all fields."""
        valid_point_data.update({
            "set_winner": "1",
            "game_duration": 120,
            "easy_hold": True,
            "difficult_hold": False,
            "multiple_deuces": False,
            "game_winner": "1",
            "is_tiebreak": False,
            "serve_speed": 214.0,
            "fault_serve_speed": 180.0,
            "p1_rally_shots": 3,
            "p2_rally_shots": 4,
            "is_break_point": True,
            "break_points_in_game": 2,
            "break_points_lost": 1,
            "is_crucial_point": True,
            "p1_game_score": "30",
            "p2_game_score": "40",
            "match_duration_at_point": 3600,
        })
        record = MatchBeatsPointRecord(**valid_point_data)
        assert record.serve_speed == 214.0
        assert record.p1_rally_shots == 3
        assert record.is_break_point is True


class TestSchemaHash:
    """Tests for schema hash."""

    def test_schema_hash_exists(self):
        """Should have schema hash defined."""
        assert SCHEMA_HASH
        assert "match_beats" in SCHEMA_HASH
