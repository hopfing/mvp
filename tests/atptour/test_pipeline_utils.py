import polars as pl
import pytest
from pathlib import Path

from mvp.atptour.pipeline_utils import get_active_players, activity_covers_tournament


class TestGetActivePlayers:
    def test_extracts_player_ids(self, tmp_path):
        # Create a results.parquet with known player IDs
        stage_dir = tmp_path / "tournaments" / "tour" / "580" / "2023"
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "tournament_id": ["580", "580"],
            "year": [2023, 2023],
            "draw_type": ["singles", "singles"],
            "p1_id": ["S0AG", "N409"],
            "p2_id": ["N409", "D875"],
        })
        df.write_parquet(stage_dir / "results.parquet")

        result = get_active_players(tmp_path)
        assert "S0AG" in result
        assert "N409" in result
        assert "D875" in result
        assert ("580", 2023) in result["S0AG"]

    def test_skips_placeholder_ids(self, tmp_path):
        stage_dir = tmp_path / "tournaments" / "tour" / "580" / "2023"
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "tournament_id": ["580"],
            "year": [2023],
            "draw_type": ["singles"],
            "p1_id": ["S0AG"],
            "p2_id": ["0"],
        })
        df.write_parquet(stage_dir / "results.parquet")

        result = get_active_players(tmp_path)
        assert "0" not in result

    def test_excludes_doubles(self, tmp_path):
        stage_dir = tmp_path / "tournaments" / "tour" / "580" / "2023"
        stage_dir.mkdir(parents=True)
        df = pl.DataFrame({
            "tournament_id": ["580", "580"],
            "year": [2023, 2023],
            "draw_type": ["singles", "doubles"],
            "p1_id": ["S0AG", "X123"],
            "p2_id": ["N409", "Y456"],
        })
        df.write_parquet(stage_dir / "results.parquet")

        result = get_active_players(tmp_path)
        assert "S0AG" in result
        assert "N409" in result
        assert "X123" not in result
        assert "Y456" not in result

    def test_empty_dir(self, tmp_path):
        result = get_active_players(tmp_path)
        assert result == {}


class TestActivityCoversTournament:
    def test_found(self):
        data = {"Activity": [{"EventYear": "2023", "Tournaments": [
            {"EventId": 580, "EventType": "GS"}
        ]}]}
        assert activity_covers_tournament(data, 2023, "580") is True

    def test_not_found(self):
        data = {"Activity": [{"EventYear": "2023", "Tournaments": []}]}
        assert activity_covers_tournament(data, 2023, "580") is False

    def test_wrong_year(self):
        data = {"Activity": [{"EventYear": "2022", "Tournaments": [
            {"EventId": 580, "EventType": "GS"}
        ]}]}
        assert activity_covers_tournament(data, 2023, "580") is False

    def test_davis_cup(self):
        data = {"Activity": [{"EventYear": "2023", "Tournaments": [
            {"EventId": 99999, "EventType": "DC"}
        ]}]}
        assert activity_covers_tournament(data, 2023, "8096") is True

    def test_none_json(self):
        assert activity_covers_tournament(None, 2023, "580") is False

    def test_none_activity(self):
        data = {"Activity": None}
        assert activity_covers_tournament(data, 2023, "580") is False
