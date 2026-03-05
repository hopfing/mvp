"""Tests for PlayerActivityExtractor."""

import json
import os
import time
from unittest.mock import patch

from mvp.atptour.extractors.player_activity import PlayerActivityExtractor

SAMPLE_ACTIVITY = {
    "Activity": [
        {
            "EventYear": "2023",
            "Tournaments": [
                {
                    "EventId": 580,
                    "EventType": "GS",
                    "Surface": "Clay",
                    "InOutdoor": "O",
                    "PlayerRank": 1,
                    "EventDate": "2023-05-28T00:00:00",
                    "PlayEndDate": "2023-06-11T00:00:00",
                    "Points": 2000,
                    "PrizeUsd": 2630000,
                    "Matches": [
                        {
                            "MatchId": "MS001",
                            "Round": {"ShortName": "R128"},
                            "WinLoss": "W",
                            "Reason": None,
                            "OpponentId": "AB01",
                            "OpponentFirstName": "John",
                            "OpponentLastName": "Doe",
                            "OpponentNatlId": "USA",
                            "OpponentRank": 50,
                            "Set1Player": 6,
                            "Set1Opponent": 4,
                            "Set1Tie": None,
                            "Set2Player": 7,
                            "Set2Opponent": 6,
                            "Set2Tie": 5,
                            "Set3Player": 6,
                            "Set3Opponent": 3,
                            "Set3Tie": None,
                            "Set4Player": None,
                            "Set4Opponent": None,
                            "Set4Tie": None,
                            "Set5Player": None,
                            "Set5Opponent": None,
                            "Set5Tie": None,
                            "HasStats": True,
                            "MatchStatsUrl": "/en/scores/match-stats/ms001",
                            "IsBye": False,
                        }
                    ],
                }
            ],
        }
    ]
}


class TestPlayerActivityExtractorFetch:
    def test_fetches_missing_activity(self, tmp_path):
        extractor = PlayerActivityExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=SAMPLE_ACTIVITY):
            failed = extractor.run({"N409": {("580", 2023)}})
        assert len(failed) == 0
        saved = tmp_path / "raw" / "atptour" / "activity" / "N409.json"
        assert saved.exists()
        data = json.loads(saved.read_text(encoding="utf-8"))
        assert data["Activity"][0]["EventYear"] == "2023"

    def test_skips_existing_with_coverage(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text(
            json.dumps(SAMPLE_ACTIVITY), encoding="utf-8"
        )

        extractor = PlayerActivityExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json") as mock:
            failed = extractor.run({"N409": {("580", 2023)}})
        mock.assert_not_called()
        assert len(failed) == 0

    def test_refetches_when_coverage_missing_and_stale(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True)
        path = raw_dir / "N409.json"
        path.write_text(json.dumps(SAMPLE_ACTIVITY), encoding="utf-8")
        # Backdate mtime so file is older than 24h
        old_time = time.time() - 25 * 60 * 60
        os.utime(path, (old_time, old_time))

        extractor = PlayerActivityExtractor(data_root=tmp_path)
        with patch.object(
            extractor, "fetch_json", return_value=SAMPLE_ACTIVITY
        ) as mock:
            # Request tournament 999 which is NOT in the activity data
            failed = extractor.run({"N409": {("999", 2023)}})
        mock.assert_called_once()
        assert len(failed) == 0

    def test_skips_when_coverage_missing_but_fresh(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text(
            json.dumps(SAMPLE_ACTIVITY), encoding="utf-8"
        )

        extractor = PlayerActivityExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json") as mock:
            # Request tournament 999 which is NOT in the activity data
            # but the file is fresh (just written), so skip
            failed = extractor.run({"N409": {("999", 2023)}})
        mock.assert_not_called()
        assert len(failed) == 0

    def test_returns_failures(self, tmp_path):
        extractor = PlayerActivityExtractor(data_root=tmp_path)
        with patch.object(
            extractor, "fetch_json", side_effect=Exception("timeout")
        ):
            failed = extractor.run({"N409": {("580", 2023)}})
        assert len(failed) == 1
        assert failed[0][0] == "N409"
        assert "timeout" in failed[0][1]

    def test_skips_empty_response(self, tmp_path):
        extractor = PlayerActivityExtractor(data_root=tmp_path)
        with patch.object(extractor, "fetch_json", return_value=None):
            failed = extractor.run({"N409": {("580", 2023)}})
        assert len(failed) == 0
        saved = tmp_path / "raw" / "atptour" / "activity" / "N409.json"
        assert not saved.exists()

    def test_skips_empty_activity_key(self, tmp_path):
        extractor = PlayerActivityExtractor(data_root=tmp_path)
        with patch.object(
            extractor, "fetch_json", return_value={"Activity": None}
        ):
            failed = extractor.run({"N409": {("580", 2023)}})
        assert len(failed) == 0
        saved = tmp_path / "raw" / "atptour" / "activity" / "N409.json"
        assert not saved.exists()
