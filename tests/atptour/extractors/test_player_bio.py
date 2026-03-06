"""Tests for PlayerBioExtractor."""

import json
from unittest.mock import patch

from mvp.atptour.extractors.player_bio import PlayerBioExtractor

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


class TestPlayerBioExtractorFetch:
    def test_fetches_missing(self, tmp_path):
        extractor = PlayerBioExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=SAMPLE_BIO):
            failed, new_count = extractor.run(["N409"])
        assert len(failed) == 0
        assert new_count == 1
        saved = tmp_path / "raw" / "atptour" / "players" / "N409.json"
        assert saved.exists()
        data = json.loads(saved.read_text(encoding="utf-8"))
        assert data["FirstName"] == "Rafael"

    def test_skips_existing(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text("{}", encoding="utf-8")

        extractor = PlayerBioExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json") as mock:
            extractor.run(["N409"])
        mock.assert_not_called()

    def test_returns_failures(self, tmp_path):
        extractor = PlayerBioExtractor(data_root=tmp_path)
        with patch.object(
            extractor, "fetch_json", side_effect=Exception("timeout")
        ):
            failed, new_count = extractor.run(["N409"])
        assert len(failed) == 1
        assert new_count == 1
        assert failed[0][0] == "N409"
        assert "timeout" in failed[0][1]

    def test_skips_empty_response(self, tmp_path):
        extractor = PlayerBioExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=None):
            failed, new_count = extractor.run(["N409"])
        assert len(failed) == 0
        assert new_count == 1
        saved = tmp_path / "raw" / "atptour" / "players" / "N409.json"
        assert not saved.exists()

    def test_fetches_multiple_skips_existing(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "players"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text("{}", encoding="utf-8")

        extractor = PlayerBioExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=SAMPLE_BIO) as mock:
            failed, new_count = extractor.run(["N409", "S0AG", "D875"])
        assert len(failed) == 0
        assert new_count == 2
        # Should only fetch S0AG and D875, not N409
        assert mock.call_count == 2
        assert (raw_dir / "S0AG.json").exists()
        assert (raw_dir / "D875.json").exists()

    def test_player_ids_uppercased(self, tmp_path):
        """Player IDs should be uppercased for file naming."""
        extractor = PlayerBioExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=SAMPLE_BIO):
            failed, new_count = extractor.run(["n409"])
        assert len(failed) == 0
        assert new_count == 1
        saved = tmp_path / "raw" / "atptour" / "players" / "N409.json"
        assert saved.exists()
