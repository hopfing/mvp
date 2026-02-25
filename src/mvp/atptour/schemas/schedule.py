"""Schedule staged schema."""

from datetime import date, datetime
from typing import ClassVar

from pydantic import BaseModel, computed_field, field_validator

from mvp.atptour.mappings import (
    create_match_uid,
    is_placeholder_id,
    map_player_id,
    normalize_round,
)
from mvp.atptour.schema_helpers import empty_to_none
from mvp.common.enums import Circuit, DrawType, Round
from mvp.common.schema_hash import compute_schema_hash

SCHEMA_VERSION = "2.0.0"


class ScheduleRecord(BaseModel):
    """A single scheduled match entry from an ATP tournament schedule page."""

    SCHEMA_VERSION: ClassVar[str] = SCHEMA_VERSION

    tournament_id: str
    year: int
    circuit: Circuit
    draw_type: DrawType
    match_date: date
    scheduled_datetime: datetime | None
    time_suffix: str
    display_time: str
    court_name: str | None
    round: Round

    p1_id: str
    p1_name: str
    p1_country: str
    p1_seed: int | None = None
    p1_entry: str | None = None

    p2_id: str
    p2_name: str
    p2_country: str
    p2_seed: int | None = None
    p2_entry: str | None = None

    status: str | None
    score: str | None

    snapshot_timestamp: datetime

    # Traceability
    source_file: str
    parsed_at: datetime

    _normalize_round = field_validator("round", mode="before")(normalize_round)
    _empty_to_none = field_validator(
        "court_name", "status", "score", mode="before",
    )(empty_to_none)

    @field_validator("p1_id", "p2_id", mode="before")
    @classmethod
    def _uppercase_player_id(cls, v: str) -> str:
        return map_player_id(v)

    @field_validator("p1_country", "p2_country", mode="before")
    @classmethod
    def _uppercase_country(cls, v: str) -> str:
        return v.upper()

    @computed_field
    @property
    def match_uid(self) -> str | None:
        all_ids = [self.p1_id, self.p2_id]
        if any(is_placeholder_id(pid) for pid in all_ids):
            return None
        return create_match_uid(
            self.year,
            self.tournament_id,
            self.round,
            all_ids,
            is_doubles=(self.draw_type == DrawType.doubles),
        )


SCHEMA_HASH = compute_schema_hash(ScheduleRecord)
ScheduleRecord.SCHEMA_HASH = SCHEMA_HASH
