"""Tests for PlayerActivity stager and transformer (consolidator)."""

import json
from datetime import date, datetime

import polars as pl

from mvp.atptour.schemas.player_activity import PlayerActivityRecord
from mvp.atptour.transformers.player_activity import (
    PlayerActivityStager,
    PlayerActivityTransformer,
    _derive_tiebreak_scores,
    _parse_activity_json,
)

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


class TestDeriveTiebreakScores:
    def test_player_wins_tiebreak(self):
        p_tb, o_tb = _derive_tiebreak_scores(7, 6, 5)
        assert p_tb == 7
        assert o_tb == 5

    def test_opponent_wins_tiebreak(self):
        p_tb, o_tb = _derive_tiebreak_scores(6, 7, 5)
        assert p_tb == 5
        assert o_tb == 7

    def test_no_tiebreak(self):
        p_tb, o_tb = _derive_tiebreak_scores(6, 4, None)
        assert p_tb is None
        assert o_tb is None

    def test_high_tiebreak_score(self):
        p_tb, o_tb = _derive_tiebreak_scores(7, 6, 12)
        assert p_tb == 14
        assert o_tb == 12

    def test_minimum_tiebreak_win(self):
        p_tb, o_tb = _derive_tiebreak_scores(7, 6, 3)
        assert p_tb == 7
        assert o_tb == 3


class TestParseActivityJson:
    def test_parses_full_record(self):
        records = _parse_activity_json(
            "N409", SAMPLE_ACTIVITY, "activity/N409.json", datetime(2026, 2, 24)
        )
        assert len(records) == 1
        r = records[0]
        assert isinstance(r, PlayerActivityRecord)
        assert r.player_id == "N409"
        assert r.year == 2023
        assert r.tournament_id == "580"
        assert r.surface == "Clay"
        assert r.indoor is False
        assert r.tournament_start_date == date(2023, 5, 28)
        assert r.tournament_end_date == date(2023, 6, 11)
        assert r.points == 2000
        assert r.prize_usd == 2630000
        assert r.match_id == "MS001"
        assert r.round.value == "R128"
        assert r.win_loss == "W"
        assert r.reason is None
        assert r.player_rank == 1
        assert r.opp_id == "AB01"
        assert r.opp_first_name == "John"
        assert r.opp_last_name == "Doe"
        assert r.opp_natl_id == "USA"
        assert r.opp_rank == 50
        assert r.player_set1_score == 6
        assert r.opp_set1_score == 4
        assert r.player_set1_tiebreak_score is None
        assert r.opp_set1_tiebreak_score is None
        assert r.player_set2_score == 7
        assert r.opp_set2_score == 6
        assert r.player_set2_tiebreak_score == 7
        assert r.opp_set2_tiebreak_score == 5
        assert r.player_set3_score == 6
        assert r.opp_set3_score == 3
        assert r.has_stats is True
        assert r.match_stats_url == "/en/scores/match-stats/ms001"
        assert r.is_bye is False
        assert r.source_file == "activity/N409.json"
        assert r.parsed_at == datetime(2026, 2, 24)

    def test_empty_data(self):
        records = _parse_activity_json(
            "N409", None, "test", datetime(2026, 1, 1)
        )
        assert records == []

    def test_tournament_id_is_string(self):
        records = _parse_activity_json(
            "N409", SAMPLE_ACTIVITY, "test", datetime(2026, 1, 1)
        )
        assert isinstance(records[0].tournament_id, str)
        assert records[0].tournament_id == "580"


class TestPlayerActivityStager:
    def test_stages_single_player(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text(
            json.dumps(SAMPLE_ACTIVITY), encoding="utf-8"
        )

        stager = PlayerActivityStager(data_root=tmp_path)
        failed = stager.run()
        assert len(failed) == 0

        parquet = tmp_path / "stage" / "atptour" / "activity" / "N409.parquet"
        assert parquet.exists()
        df = pl.read_parquet(parquet)
        assert len(df) == 1
        assert df["player_id"][0] == "N409"
        assert df["tournament_id"][0] == "580"

    def test_skips_already_staged(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True)
        (raw_dir / "N409.json").write_text(
            json.dumps(SAMPLE_ACTIVITY), encoding="utf-8"
        )

        stager = PlayerActivityStager(data_root=tmp_path)
        stager.run()

        # Run again — should skip since staged is newer
        stager2 = PlayerActivityStager(data_root=tmp_path)
        failed = stager2.run()
        assert len(failed) == 0

    def test_restages_when_raw_newer(self, tmp_path):
        import time

        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True)
        raw_file = raw_dir / "N409.json"
        raw_file.write_text(json.dumps(SAMPLE_ACTIVITY), encoding="utf-8")

        stager = PlayerActivityStager(data_root=tmp_path)
        stager.run()

        # Touch the raw file to make it newer
        time.sleep(0.05)
        raw_file.write_text(json.dumps(SAMPLE_ACTIVITY), encoding="utf-8")

        stager2 = PlayerActivityStager(data_root=tmp_path)
        failed = stager2.run()
        assert len(failed) == 0

    def test_returns_failures(self, tmp_path):
        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True)
        (raw_dir / "BAD1.json").write_text("not json", encoding="utf-8")

        stager = PlayerActivityStager(data_root=tmp_path)
        failed = stager.run()
        assert len(failed) == 1
        assert failed[0][0] == "BAD1"

    def test_no_raw_files(self, tmp_path):
        stager = PlayerActivityStager(data_root=tmp_path)
        failed = stager.run()
        assert failed == []


class TestPlayerActivityTransformer:
    def _stage_player(self, tmp_path, player_id, activity_data):
        """Helper to create a staged parquet for a player."""
        raw_dir = tmp_path / "raw" / "atptour" / "activity"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / f"{player_id}.json").write_text(
            json.dumps(activity_data), encoding="utf-8"
        )
        stager = PlayerActivityStager(data_root=tmp_path)
        stager.run()

    def test_consolidates_multiple(self, tmp_path):
        self._stage_player(tmp_path, "N409", SAMPLE_ACTIVITY)
        self._stage_player(tmp_path, "AB01", SAMPLE_ACTIVITY)

        transformer = PlayerActivityTransformer(data_root=tmp_path)
        result = transformer.run()

        assert result is not None
        consolidated = tmp_path / "stage" / "atptour" / "activity.parquet"
        assert consolidated.exists()
        df = pl.read_parquet(consolidated)
        assert len(df) == 2
        player_ids = set(df["player_id"].to_list())
        assert player_ids == {"N409", "AB01"}

    def test_no_staged_files(self, tmp_path):
        transformer = PlayerActivityTransformer(data_root=tmp_path)
        result = transformer.run()
        assert result is None
