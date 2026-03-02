"""Rankings staged schema."""

from datetime import date, datetime

from pydantic import BaseModel, field_validator

from mvp.atptour.mappings import map_player_id
from mvp.common.schema_hash import compute_schema_hash


class RankingsRecord(BaseModel):
    """A single player ranking from a weekly ATP rankings snapshot."""

    ranking_date: date
    rank: int
    player_id: str
    player_name: str
    nationality: str
    age: int
    points: int
    rank_move: int | None
    points_move: int | None
    tournaments_played: int
    points_dropping: int | None
    next_best: int | None

    # Traceability
    source_file: str
    parsed_at: datetime

    @field_validator("player_id", mode="before")
    @classmethod
    def _uppercase_player_id(cls, v: str) -> str:
        return map_player_id(v)

    @field_validator("nationality", mode="before")
    @classmethod
    def _uppercase_nationality(cls, v: str) -> str:
        return v.upper()


SCHEMA_HASH = compute_schema_hash(RankingsRecord)
RankingsRecord.SCHEMA_HASH = SCHEMA_HASH
