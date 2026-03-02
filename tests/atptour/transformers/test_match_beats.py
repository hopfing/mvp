"""Tests for MatchBeats transformer."""

import json
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.schemas.match_beats import PointResult, SCHEMA_HASH
from mvp.atptour.transformers.match_beats import MatchBeatsTransformer
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
def sample_match_data():
    """Sample match data matching real API structure."""
    return {
        "matchId": "MS001",
        "isDoubles": False,
        "eventType": "Men's Singles",
        "maxRally": 10,
        "matchWinner": 1,
        "allStats": True,
        "rallyStats": True,
        "isMatchComplete": True,
        "setsComplete": 2,
        "playerData": {
            "tm1Ply1Id": "A123",
            "tm1Ply1Name": "Player One",
            "tm2Ply1Id": "B456",
            "tm2Ply1Name": "Player Two",
        },
        "setData": [
            {
                "set": 1,
                "gamesComplete": 6,
                "setWinner": 1,
                "gameData": [
                    {
                        "game": 1,
                        "duration": 120,
                        "easyHold": True,
                        "difficultHold": False,
                        "multipleDeuces": False,
                        "gameWinner": 1,
                        "isTieBreak": False,
                        "pointData": [
                            {
                                "point": 1,
                                "pointId": "1_1_1_1",
                                "result": "A",
                                "scorer": "1",
                                "server": "1",
                                "serve": 1,
                                "serveSpeed": 214.0,
                                "faultSrvSpd": 0.0,
                                "tm1Rally": 0,
                                "tm2Rally": 0,
                                "isBrkPt": False,
                                "isCrucialPt": False,
                                "brkPts": 0,
                                "brkPtsLost": 0,
                                "tm1GameScore": "15",
                                "tm2GameScore": "0",
                                "rallyLengthMissing": False,
                                "currentMatchDuration": 0,
                            },
                            {
                                "point": 2,
                                "pointId": "1_1_2_1",
                                "result": "W",
                                "scorer": "1",
                                "server": "1",
                                "serve": 1,
                                "serveSpeed": 195.0,
                                "faultSrvSpd": 0.0,
                                "tm1Rally": 2,
                                "tm2Rally": 3,
                                "isBrkPt": False,
                                "isCrucialPt": False,
                                "brkPts": 0,
                                "brkPtsLost": 0,
                                "tm1GameScore": "30",
                                "tm2GameScore": "0",
                                "rallyLengthMissing": False,
                                "currentMatchDuration": 45,
                            },
                        ],
                    },
                ],
            },
        ],
    }


@pytest.fixture
def transformer(tmp_path, tournament):
    return MatchBeatsTransformer(tournament, data_root=tmp_path)


class TestMatchBeatsTransformer:
    """Tests for MatchBeatsTransformer."""

    def test_transformer_init(self, transformer, tournament):
        """Should initialize with tournament."""
        assert transformer.tournament == tournament

    def test_no_raw_directory(self, transformer, caplog):
        """Should handle missing raw directory gracefully."""
        import logging

        caplog.set_level(logging.DEBUG)
        transformer.run()
        assert "no match_beats directory" in caplog.text.lower()

    def test_empty_raw_directory(self, transformer, tmp_path, tournament, caplog):
        """Should handle empty raw directory gracefully."""
        import logging

        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        caplog.set_level(logging.DEBUG)
        transformer.run()
        assert "no match_beats json files" in caplog.text.lower()

    def test_transforms_single_file(
        self, transformer, tmp_path, tournament, sample_match_data
    ):
        """Should transform a single JSON file to parquet."""
        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        with open(raw_dir / "MS001.json", "w") as f:
            json.dump(sample_match_data, f)

        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "match_beats.parquet"
        assert output.exists()

        df = pl.read_parquet(output)
        assert len(df) == 2  # Two points
        assert df["match_id"][0] == "MS001"
        assert df["p1_id"][0] == "A123"
        assert df["p2_id"][0] == "B456"

    def test_point_data_fields(
        self, transformer, tmp_path, tournament, sample_match_data
    ):
        """Should correctly extract point-level fields."""
        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        with open(raw_dir / "MS001.json", "w") as f:
            json.dump(sample_match_data, f)

        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "match_beats.parquet"
        df = pl.read_parquet(output)

        # First point - ace
        point1 = df.filter(pl.col("point_num") == 1)
        assert point1["result"][0] == PointResult.ACE
        assert point1["serve_speed"][0] == 214.0
        assert point1["p1_game_score"][0] == "15"
        assert point1["p2_game_score"][0] == "0"

        # Second point - winner with rally
        point2 = df.filter(pl.col("point_num") == 2)
        assert point2["result"][0] == PointResult.WINNER
        assert point2["p1_rally_shots"][0] == 2
        assert point2["p2_rally_shots"][0] == 3

    def test_game_context_denormalized(
        self, transformer, tmp_path, tournament, sample_match_data
    ):
        """Should denormalize game context to each point."""
        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        with open(raw_dir / "MS001.json", "w") as f:
            json.dump(sample_match_data, f)

        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "match_beats.parquet"
        df = pl.read_parquet(output)

        assert all(df["game_duration"] == 120)
        assert all(df["easy_hold"] == True)
        assert all(df["game_winner"] == "1")
        assert all(df["set_num"] == 1)

    def test_zero_serve_speed_becomes_null(
        self, transformer, tmp_path, tournament, sample_match_data
    ):
        """Should convert 0.0 serve speed to null in output."""
        # Set serve speed to 0
        sample_match_data["setData"][0]["gameData"][0]["pointData"][0]["serveSpeed"] = 0.0

        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        with open(raw_dir / "MS001.json", "w") as f:
            json.dump(sample_match_data, f)

        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "match_beats.parquet"
        df = pl.read_parquet(output)

        point1 = df.filter(pl.col("point_num") == 1)
        assert point1["serve_speed"][0] is None

    def test_schema_hash_added(
        self, transformer, tmp_path, tournament, sample_match_data
    ):
        """Should add schema hash to output."""
        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        with open(raw_dir / "MS001.json", "w") as f:
            json.dump(sample_match_data, f)

        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "match_beats.parquet"
        df = pl.read_parquet(output)

        assert "schema_hash" in df.columns
        assert all(df["schema_hash"] == SCHEMA_HASH)

    def test_multiple_files(self, transformer, tmp_path, tournament, sample_match_data):
        """Should transform multiple JSON files."""
        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        # Create two match files
        with open(raw_dir / "MS001.json", "w") as f:
            json.dump(sample_match_data, f)

        sample_match_data["matchId"] = "MS002"
        with open(raw_dir / "MS002.json", "w") as f:
            json.dump(sample_match_data, f)

        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "match_beats.parquet"
        df = pl.read_parquet(output)

        assert len(df) == 4  # 2 points per match
        assert set(df["match_id"].to_list()) == {"MS001", "MS002"}

    def test_handles_integer_scorer_server(
        self, transformer, tmp_path, tournament, sample_match_data
    ):
        """Should handle scorer/server as integers."""
        # Set as integers instead of strings
        sample_match_data["setData"][0]["gameData"][0]["pointData"][0]["scorer"] = 1
        sample_match_data["setData"][0]["gameData"][0]["pointData"][0]["server"] = 2

        raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_beats"
        raw_dir.mkdir(parents=True)

        with open(raw_dir / "MS001.json", "w") as f:
            json.dump(sample_match_data, f)

        transformer.run()

        output = tmp_path / "stage" / "atptour" / tournament.path / "match_beats.parquet"
        df = pl.read_parquet(output)

        point1 = df.filter(pl.col("point_num") == 1)
        assert point1["scorer"][0] == "1"
        assert point1["server"][0] == "2"
