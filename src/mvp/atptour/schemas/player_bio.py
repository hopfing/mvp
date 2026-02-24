"""Player bio staged schema."""

from datetime import date, datetime
from typing import ClassVar

from pydantic import BaseModel, field_validator

from mvp.atptour.mappings import map_player_id
from mvp.atptour.schema_helpers import strip_or_none
from mvp.common.schema_hash import compute_schema_hash

SCHEMA_VERSION = "1.0.0"


class PlayerBioRecord(BaseModel):
    """A single player biographical record from an ATP player profile JSON."""

    SCHEMA_VERSION: ClassVar[str] = SCHEMA_VERSION

    player_id: str
    first_name: str
    last_name: str
    birth_date: date | None = None
    birth_city: str | None = None
    nationality: str | None = None
    natl_id: str | None = None
    height_cm: int | None = None
    weight_kg: int | None = None
    right_handed: bool | None = None
    twohand_backhand: bool | None = None
    pro_year: int | None = None
    is_active: bool
    is_dbl_specialist: bool

    # Traceability
    source_file: str
    parsed_at: datetime

    @field_validator("player_id", mode="before")
    @classmethod
    def _map_player_id(cls, v: str) -> str:
        return map_player_id(v)

    @field_validator("nationality", mode="before")
    @classmethod
    def _uppercase_nationality(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return v.upper()

    @field_validator("natl_id", mode="before")
    @classmethod
    def _uppercase_natl_id(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return v.upper()

    _strip_birth_city = field_validator("birth_city", mode="before")(strip_or_none)

    @field_validator("right_handed", mode="before")
    @classmethod
    def _parse_right_handed(cls, v):
        if v is None or v in ("", "U", "A"):
            return None
        if v == "R":
            return True
        if v == "L":
            return False
        raise ValueError(f"Unexpected PlayHand value '{v}'.")

    @field_validator("twohand_backhand", mode="before")
    @classmethod
    def _parse_twohand_backhand(cls, v):
        if v is None or v in ("", "U", "0"):
            return None
        if v == "2":
            return True
        if v == "1":
            return False
        raise ValueError(f"Unexpected BackHand value '{v}'.")

    @field_validator("is_active", mode="before")
    @classmethod
    def _parse_is_active(cls, v):
        if v == "A":
            return True
        if v in ("I", "D"):
            return False
        if isinstance(v, bool):
            return v
        raise ValueError(f"Unexpected Active value '{v}'.")


SCHEMA_HASH = compute_schema_hash(PlayerBioRecord)
PlayerBioRecord.SCHEMA_HASH = SCHEMA_HASH
