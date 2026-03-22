"""Tests for PlayerBio stager and transformer (consolidator)."""

import json
from datetime import date, datetime

import polars as pl

from mvp.atptour.schemas.player_bio import PlayerBioRecord
from mvp.atptour.transformers.player_bio import (
    PlayerBioStager,
    PlayerBioTransformer,
    _parse_bio_json,
    _parse_birth_date,
)

SAMPLE_BIO = {
    "FirstName": "Rafael",
    "LastName": "Nadal",
    "BirthDate": "1986-06-03T00:00:00",
    "BirthCity": "Manacor",
    "Nationality": "Spain",
    "NatlId": "ESP",
    "HeightCm": 185,
    "WeightKg": 85,
    "PlayHand": {"Id": "L", "Description": "Left-Handed"},
    "BackHand": {"Id": "2", "Description": "Two-Handed Backhand"},
    "ProYear": 2001,
    "Active": {"Id": "I", "Description": "Inactive"},
    "DblSpecialist": False,
}

SAMPLE_BIO_MINIMAL = {
    "FirstName": "Unknown",
    "LastName": "Player",
    "BirthDate": None,
    "BirthCity": None,
    "Nationality": None,
    "NatlId": None,
    "HeightCm": None,
    "WeightKg": None,
    "PlayHand": None,
    "BackHand": None,
    "ProYear": None,
    "Active": {"Id": "A", "Description": "Active"},
    "DblSpecialist": False,
}


class TestParseBirthDate:
    def test_valid_iso(self):
        assert _parse_birth_date("1986-06-03T00:00:00") == date(1986, 6, 3)

    def test_none(self):
        assert _parse_birth_date(None) is None

    def test_empty(self):
        assert _parse_birth_date("") is None


class TestParseBioJson:
    def test_full_record(self):
        record = _parse_bio_json(
            "N409", SAMPLE_BIO, "players/N409.json", datetime(2026, 2, 24)
        )
        assert record.player_id == "N409"
        assert record.first_name == "Rafael"
        assert record.last_name == "Nadal"
        assert record.birth_date == date(1986, 6, 3)
        assert record.birth_city == "Manacor"
        assert record.nationality == "SPAIN"
        assert record.natl_id == "ESP"
        assert record.height_cm == 185
        assert record.weight_kg == 85
        assert record.right_handed is False  # "L"
        assert record.twohand_backhand is True  # "2"
        assert record.pro_year == 2001
        assert record.is_active is False  # "I"
        assert record.is_dbl_specialist is False
        assert record.source_file == "players/N409.json"
        assert record.parsed_at == datetime(2026, 2, 24)

    def test_minimal_record(self):
        record = _parse_bio_json(
            "X001", SAMPLE_BIO_MINIMAL, "players/X001.json", datetime(2026, 2, 24)
        )
        assert record.player_id == "X001"
        assert record.birth_date is None
        assert record.height_cm is None
        assert record.right_handed is None
        assert record.twohand_backhand is None
        assert record.is_active is True

    def test_returns_player_bio_record(self):
        record = _parse_bio_json(
            "N409", SAMPLE_BIO, "test", datetime(2026, 1, 1)
        )
        assert isinstance(record, PlayerBioRecord)


class TestPlayerBioStager:
    def test_stages_single_player(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text(
            json.dumps(SAMPLE_BIO), encoding="utf-8"
        )

        stager = PlayerBioStager(data_root=tmp_path)
        failed = stager.run()
        assert len(failed) == 0

        parquet = tmp_path / "stage" / "atptour" / "players" / "N409.parquet"
        assert parquet.exists()
        df = pl.read_parquet(parquet)
        assert len(df) == 1
        assert df["player_id"][0] == "N409"
        assert df["first_name"][0] == "Rafael"

    def test_skips_already_staged(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True)
        raw_file = raw_dir / "N409.json"
        raw_file.write_text(json.dumps(SAMPLE_BIO), encoding="utf-8")

        stager = PlayerBioStager(data_root=tmp_path)
        stager.run()

        # Run again — should skip since staged is newer
        stager2 = PlayerBioStager(data_root=tmp_path)
        failed = stager2.run()
        assert len(failed) == 0

    def test_restages_when_raw_newer(self, tmp_path):
        import time

        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True)
        raw_file = raw_dir / "N409.json"
        raw_file.write_text(json.dumps(SAMPLE_BIO), encoding="utf-8")

        stager = PlayerBioStager(data_root=tmp_path)
        stager.run()

        # Touch the raw file to make it newer
        time.sleep(0.05)
        raw_file.write_text(json.dumps(SAMPLE_BIO), encoding="utf-8")

        stager2 = PlayerBioStager(data_root=tmp_path)
        failed = stager2.run()
        assert len(failed) == 0

    def test_stages_multiple_players(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text(
            json.dumps(SAMPLE_BIO), encoding="utf-8"
        )
        (raw_dir / "X001.json").write_text(
            json.dumps(SAMPLE_BIO_MINIMAL), encoding="utf-8"
        )

        stager = PlayerBioStager(data_root=tmp_path)
        failed = stager.run()
        assert len(failed) == 0

        stage_dir = tmp_path / "stage" / "atptour" / "players"
        assert (stage_dir / "N409.parquet").exists()
        assert (stage_dir / "X001.parquet").exists()

    def test_returns_failures(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True)
        # Write invalid JSON
        (raw_dir / "BAD1.json").write_text("not json", encoding="utf-8")

        stager = PlayerBioStager(data_root=tmp_path)
        failed = stager.run()
        assert len(failed) == 1
        assert failed[0][0] == "BAD1"

    def test_no_raw_files(self, tmp_path):
        stager = PlayerBioStager(data_root=tmp_path)
        failed = stager.run()
        assert failed == []


class TestUniquenessAssertion:
    def test_assertion_fires_on_duplicate_pk(self):
        import pytest

        df = pl.DataFrame({
            "player_id": ["N409", "N409"],
        })
        with pytest.raises(ValueError, match="Duplicate primary keys"):
            PlayerBioTransformer.assert_unique(df, ["player_id"], "player_bio")


class TestPlayerBioTransformer:
    def _stage_player(self, tmp_path, player_id, bio_data):
        """Helper to create a staged parquet for a player."""
        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / f"{player_id}.json").write_text(
            json.dumps(bio_data), encoding="utf-8"
        )
        stager = PlayerBioStager(data_root=tmp_path)
        stager.run()

    def test_consolidates_multiple(self, tmp_path):
        self._stage_player(tmp_path, "N409", SAMPLE_BIO)
        self._stage_player(tmp_path, "X001", SAMPLE_BIO_MINIMAL)

        transformer = PlayerBioTransformer(data_root=tmp_path)
        result = transformer.run()

        assert result is not None
        consolidated = tmp_path / "stage" / "atptour" / "players.parquet"
        assert consolidated.exists()
        df = pl.read_parquet(consolidated)
        assert len(df) == 2
        player_ids = set(df["player_id"].to_list())
        assert player_ids == {"N409", "X001"}

    def test_no_staged_files(self, tmp_path):
        transformer = PlayerBioTransformer(data_root=tmp_path)
        result = transformer.run()
        assert result is None

    def test_excludes_consolidated_file(self, tmp_path):
        """Consolidated players.parquet should not be re-read."""
        self._stage_player(tmp_path, "N409", SAMPLE_BIO)

        transformer = PlayerBioTransformer(data_root=tmp_path)
        transformer.run()

        # Run again — should produce same result, not double the rows
        transformer2 = PlayerBioTransformer(data_root=tmp_path)
        result = transformer2.run()
        assert result is not None
        df = pl.read_parquet(result)
        assert len(df) == 1
