"""Tests for Schedule staged schema."""

from datetime import date, datetime

from mvp.atptour.schemas.schedule import (
    SCHEMA_HASH,
    SCHEMA_VERSION,
    ScheduleRecord,
)
from mvp.common.enums import Circuit, DrawType, Round

PARSED_AT = datetime(2026, 2, 24)
SOURCE_FILE = "tournaments/tour/339/2026/schedule/schedule_20260207_140000.html"
SNAPSHOT_TS = datetime(2026, 2, 7, 14, 0, 0)


def _base_record(**overrides) -> dict:
    """Minimal valid schedule record."""
    data = {
        "tournament_id": "339",
        "year": 2026,
        "circuit": Circuit.tour,
        "draw_type": DrawType.singles,
        "match_date": date(2026, 2, 7),
        "scheduled_datetime": datetime(2026, 2, 7, 14, 0, 0),
        "time_suffix": "Not Before",
        "display_time": "Not Before 3:00 PM",
        "court_name": "Center Court",
        "round": "SF",
        "p1_id": "me82",
        "p1_name": "A. Mannarino",
        "p1_country": "fra",
        "p1_seed_entry": "(1)",
        "p2_id": "d0dt",
        "p2_name": "M. Damm",
        "p2_country": "usa",
        "p2_seed_entry": "(Q)",
        "status": "Vs",
        "score": None,
        "snapshot_timestamp": SNAPSHOT_TS,
        "source_file": SOURCE_FILE,
        "parsed_at": PARSED_AT,
    }
    data.update(overrides)
    return data


class TestValidRecords:
    def test_valid_record(self):
        record = ScheduleRecord(**_base_record())
        assert record.tournament_id == "339"
        assert record.year == 2026
        assert record.circuit == Circuit.tour
        assert record.draw_type == DrawType.singles
        assert record.match_date == date(2026, 2, 7)
        assert record.scheduled_datetime == datetime(2026, 2, 7, 14, 0, 0)
        assert record.time_suffix == "Not Before"
        assert record.display_time == "Not Before 3:00 PM"
        assert record.court_name == "Center Court"
        assert record.round == Round.SF
        assert record.p1_name == "A. Mannarino"
        assert record.p1_seed_entry == "(1)"
        assert record.p2_name == "M. Damm"
        assert record.p2_seed_entry == "(Q)"
        assert record.status == "Vs"
        assert record.score is None
        assert record.match_uid == "2026_339_SGL_SF_D0DT_ME82"
        assert record.snapshot_timestamp == SNAPSHOT_TS
        assert record.source_file == SOURCE_FILE
        assert record.parsed_at == PARSED_AT

    def test_minimal_record(self):
        record = ScheduleRecord(**_base_record(
            scheduled_datetime=None,
            court_name=None,
            p1_seed_entry=None,
            p2_seed_entry=None,
            status=None,
            score=None,
        ))
        assert record.scheduled_datetime is None
        assert record.court_name is None
        assert record.p1_seed_entry is None
        assert record.p2_seed_entry is None
        assert record.status is None
        assert record.score is None

    def test_completed_match_with_score(self):
        record = ScheduleRecord(**_base_record(
            status="Defeats",
            score="76(6) 61",
        ))
        assert record.status == "Defeats"
        assert record.score == "76(6) 61"


class TestFieldValidation:
    def test_player_ids_uppercased(self):
        record = ScheduleRecord(**_base_record(p1_id="me82", p2_id="d0dt"))
        assert record.p1_id == "ME82"
        assert record.p2_id == "D0DT"

    def test_player_ids_already_upper(self):
        record = ScheduleRecord(**_base_record(p1_id="ME82", p2_id="D0DT"))
        assert record.p1_id == "ME82"
        assert record.p2_id == "D0DT"

    def test_country_uppercased(self):
        record = ScheduleRecord(**_base_record(p1_country="fra", p2_country="usa"))
        assert record.p1_country == "FRA"
        assert record.p2_country == "USA"

    def test_country_already_upper(self):
        record = ScheduleRecord(**_base_record(p1_country="FRA", p2_country="USA"))
        assert record.p1_country == "FRA"
        assert record.p2_country == "USA"


class TestFieldCount:
    def test_field_count(self):
        assert len(ScheduleRecord.model_fields) == 23


class TestSchemaVersioning:
    def test_schema_version_is_semver(self):
        parts = SCHEMA_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_schema_hash_is_hex_string(self):
        assert len(SCHEMA_HASH) == 16
        int(SCHEMA_HASH, 16)
