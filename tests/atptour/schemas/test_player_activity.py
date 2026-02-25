"""Tests for PlayerActivityRecord schema."""

from datetime import date, datetime

import pytest

from mvp.atptour.schemas.player_activity import (
    SCHEMA_HASH,
    SCHEMA_VERSION,
    PlayerActivityRecord,
)
from mvp.common.enums import ActivityEventType, Circuit

PARSED_AT = datetime(2026, 2, 24)
SOURCE_FILE = "tournaments/tour/580/2023/player_activity/s0ag.json"


def _base_activity(**overrides) -> dict:
    """Minimal valid player activity record."""
    data = {
        "player_id": "s0ag",
        "year": 2023,
        "tournament_id": "580",
        "event_type": "GS",
        "points": 2000,
        "prize_usd": 4500000,
        "match_id": "ms001",
        "round": "Final",
        "win_loss": "W",
        "has_stats": True,
        "is_bye": False,
        "opp_id": "n409",
        "opp_first_name": "Rafael",
        "opp_last_name": "Nadal",
        "source_file": SOURCE_FILE,
        "parsed_at": PARSED_AT,
    }
    data.update(overrides)
    return data


class TestValidRecords:
    def test_basic_record(self):
        record = PlayerActivityRecord(**_base_activity())
        assert record.player_id == "S0AG"
        assert record.year == 2023
        assert record.tournament_id == "580"
        assert record.event_type == ActivityEventType.GS
        assert record.points == 2000
        assert record.round.value == "F"

    def test_all_nullable_fields_none(self):
        record = PlayerActivityRecord(**_base_activity(
            surface=None,
            indoor=None,
            tournament_start_date=None,
            tournament_end_date=None,
            win_loss=None,
            reason=None,
            player_rank=None,
            opp_id=None,
            opp_first_name=None,
            opp_last_name=None,
            opp_natl_id=None,
            opp_rank=None,
            match_stats_url=None,
            is_bye=True,
        ))
        assert record.surface is None
        assert record.indoor is None
        assert record.opp_id is None

    def test_full_record_with_scores(self):
        record = PlayerActivityRecord(**_base_activity(
            surface="Hard",
            indoor="I",
            tournament_start_date="2023-01-16T00:00:00",
            tournament_end_date="2023-01-29T00:00:00",
            player_rank=1,
            opp_rank=14,
            opp_natl_id="ESP",
            reason=None,
            match_stats_url="/en/scores/stats/2023/580/ms001",
            player_set1_score=6,
            opp_set1_score=4,
            player_set2_score=6,
            opp_set2_score=3,
        ))
        assert record.surface == "Hard"
        assert record.indoor is True
        assert record.tournament_start_date == date(2023, 1, 16)
        assert record.tournament_end_date == date(2023, 1, 29)
        assert record.player_set1_score == 6
        assert record.opp_set1_score == 4


class TestFieldValidation:
    def test_player_id_uppercased(self):
        record = PlayerActivityRecord(**_base_activity(player_id="s0ag"))
        assert record.player_id == "S0AG"

    def test_opp_id_uppercased(self):
        record = PlayerActivityRecord(**_base_activity(opp_id="n409"))
        assert record.opp_id == "N409"

    def test_opp_id_none_passes(self):
        record = PlayerActivityRecord(**_base_activity(
            opp_id=None, opp_first_name=None, opp_last_name=None,
            is_bye=True,
        ))
        assert record.opp_id is None

    def test_opp_natl_id_uppercased(self):
        record = PlayerActivityRecord(**_base_activity(opp_natl_id="esp"))
        assert record.opp_natl_id == "ESP"

    def test_opp_natl_id_none_passes(self):
        record = PlayerActivityRecord(**_base_activity(opp_natl_id=None))
        assert record.opp_natl_id is None

    def test_round_normalization(self):
        record = PlayerActivityRecord(**_base_activity(round="Quarterfinals"))
        assert record.round.value == "QF"

    def test_empty_to_none_win_loss(self):
        record = PlayerActivityRecord(**_base_activity(win_loss=""))
        assert record.win_loss is None

    def test_indoor_parsing_true(self):
        record = PlayerActivityRecord(**_base_activity(indoor="I"))
        assert record.indoor is True

    def test_indoor_parsing_false(self):
        record = PlayerActivityRecord(**_base_activity(indoor="O"))
        assert record.indoor is False

    def test_indoor_parsing_empty(self):
        record = PlayerActivityRecord(**_base_activity(indoor=""))
        assert record.indoor is None

    def test_surface_empty_to_none(self):
        record = PlayerActivityRecord(**_base_activity(surface=""))
        assert record.surface is None

    def test_tournament_date_parsing(self):
        record = PlayerActivityRecord(**_base_activity(
            tournament_start_date="2023-01-16T00:00:00",
            tournament_end_date="2023-01-29T00:00:00",
        ))
        assert record.tournament_start_date == date(2023, 1, 16)
        assert record.tournament_end_date == date(2023, 1, 29)

    def test_tournament_date_none(self):
        record = PlayerActivityRecord(**_base_activity(
            tournament_start_date=None,
            tournament_end_date=None,
        ))
        assert record.tournament_start_date is None
        assert record.tournament_end_date is None

    def test_tournament_date_already_date(self):
        record = PlayerActivityRecord(**_base_activity(
            tournament_start_date=date(2023, 1, 16),
        ))
        assert record.tournament_start_date == date(2023, 1, 16)


class TestWinLossValidation:
    def test_win_loss_w(self):
        record = PlayerActivityRecord(**_base_activity(win_loss="W"))
        assert record.win_loss == "W"

    def test_win_loss_l(self):
        record = PlayerActivityRecord(**_base_activity(win_loss="L"))
        assert record.win_loss == "L"

    def test_win_loss_none(self):
        record = PlayerActivityRecord(**_base_activity(win_loss=None))
        assert record.win_loss is None

    def test_win_loss_empty_becomes_none(self):
        record = PlayerActivityRecord(**_base_activity(win_loss=""))
        assert record.win_loss is None

    def test_win_loss_invalid_raises(self):
        with pytest.raises(ValueError, match="win_loss must be"):
            PlayerActivityRecord(**_base_activity(win_loss="D"))


class TestComputedFields:
    def test_circuit_computed_from_event_type(self):
        record = PlayerActivityRecord(**_base_activity(event_type="GS"))
        assert record.circuit == Circuit.tour

    def test_circuit_computed_challenger(self):
        record = PlayerActivityRecord(**_base_activity(event_type="CH"))
        assert record.circuit == Circuit.chal

    def test_circuit_computed_itf(self):
        record = PlayerActivityRecord(**_base_activity(event_type="FU"))
        assert record.circuit == Circuit.itf

    def test_circuit_computed_team(self):
        record = PlayerActivityRecord(**_base_activity(event_type="DC"))
        assert record.circuit == Circuit.team

    def test_match_uid_for_normal_match(self):
        record = PlayerActivityRecord(**_base_activity(
            player_id="S0AG",
            opp_id="N409",
            round="Final",
            year=2023,
            tournament_id="580",
            is_bye=False,
        ))
        assert record.match_uid is not None
        assert "2023_580_SGL_F_" in record.match_uid
        assert "N409" in record.match_uid
        assert "S0AG" in record.match_uid

    def test_match_uid_none_for_bye(self):
        record = PlayerActivityRecord(**_base_activity(
            is_bye=True,
            opp_id=None,
            opp_first_name=None,
            opp_last_name=None,
        ))
        assert record.match_uid is None

    def test_match_uid_none_when_no_opp_id(self):
        record = PlayerActivityRecord(**_base_activity(
            is_bye=False,
            opp_id=None,
            opp_first_name=None,
            opp_last_name=None,
        ))
        assert record.match_uid is None


class TestSetScores:
    def test_all_set_scores_none_by_default(self):
        record = PlayerActivityRecord(**_base_activity())
        for i in range(1, 6):
            assert getattr(record, f"player_set{i}_score") is None
            assert getattr(record, f"opp_set{i}_score") is None
            assert getattr(record, f"player_set{i}_tiebreak_score") is None
            assert getattr(record, f"opp_set{i}_tiebreak_score") is None

    def test_partial_set_scores(self):
        record = PlayerActivityRecord(**_base_activity(
            player_set1_score=6,
            opp_set1_score=4,
            player_set2_score=7,
            opp_set2_score=6,
            player_set2_tiebreak_score=7,
            opp_set2_tiebreak_score=3,
        ))
        assert record.player_set1_score == 6
        assert record.opp_set1_score == 4
        assert record.player_set2_tiebreak_score == 7
        assert record.opp_set2_tiebreak_score == 3
        assert record.player_set3_score is None


class TestFieldCount:
    def test_field_count(self):
        # Context: player_id, year, tournament_id, event_type, surface, indoor,
        #   tournament_start_date, tournament_end_date, points, prize_usd,
        #   match_id, round, win_loss, reason, player_rank (15)
        # Opponent: opp_id, opp_first_name, opp_last_name, opp_natl_id, opp_rank (5)
        # Set scores: 5 sets * 4 fields = 20
        # Flags: has_stats, match_stats_url, is_bye (3)
        # Traceability: source_file, parsed_at (2)
        # Total: 15 + 5 + 20 + 3 + 2 = 45
        assert len(PlayerActivityRecord.model_fields) == 45


class TestSchemaVersioning:
    def test_schema_version_is_semver(self):
        parts = SCHEMA_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_schema_hash_is_hex_string(self):
        assert len(SCHEMA_HASH) == 16
        int(SCHEMA_HASH, 16)

    def test_class_level_schema_version(self):
        assert PlayerActivityRecord.SCHEMA_VERSION == "1.0.0"
