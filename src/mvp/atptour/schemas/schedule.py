"""Schedule staged schema."""

from datetime import date, datetime
from typing import ClassVar

from pydantic import BaseModel, field_validator

from mvp.atptour.mappings import map_player_id
from mvp.common.enums import Circuit
from mvp.common.schema_hash import compute_schema_hash

SCHEMA_VERSION = "1.0.0"


class ScheduleRecord(BaseModel):
    """A single scheduled match entry from an ATP tournament schedule page."""

    SCHEMA_VERSION: ClassVar[str] = SCHEMA_VERSION

    tournament_id: str
    year: int
    circuit: Circuit
    match_date: date
    scheduled_datetime: datetime | None
    time_suffix: str
    display_time: str
    court_name: str | None
    round: str

    p1_id: str
    p1_name: str
    p1_country: str
    p1_seed_entry: str | None

    p2_id: str
    p2_name: str
    p2_country: str
    p2_seed_entry: str | None

    status: str | None
    score: str | None

    snapshot_timestamp: datetime

    # Traceability
    source_file: str
    parsed_at: datetime

    @field_validator("p1_id", "p2_id", mode="before")
    @classmethod
    def _uppercase_player_id(cls, v: str) -> str:
        return map_player_id(v)

    @field_validator("p1_country", "p2_country", mode="before")
    @classmethod
    def _uppercase_country(cls, v: str) -> str:
        return v.upper()


SCHEMA_HASH = compute_schema_hash(ScheduleRecord)
ScheduleRecord.SCHEMA_HASH = SCHEMA_HASH
