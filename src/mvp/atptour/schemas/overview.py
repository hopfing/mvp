"""Overview (tournament metadata) staged schema."""

from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel, field_validator

from mvp.atptour.schema_helpers import empty_to_none, parse_indoor
from mvp.common.enums import Circuit, TournamentType
from mvp.common.schema_hash import compute_schema_hash

SCHEMA_VERSION = "1.0.0"


class OverviewRecord(BaseModel):
    """A single tournament overview record from an ATP overview JSON file."""

    SCHEMA_VERSION: ClassVar[str] = SCHEMA_VERSION

    # Tournament identity
    tournament_id: str
    year: int
    tournament_name: str
    city: str
    country: str | None
    circuit: Circuit

    # Overview fields
    sponsor_title: str | None
    event_type: TournamentType
    event_type_detail: int
    singles_draw_size: int
    doubles_draw_size: int
    surface: str | None = None
    surface_detail: str | None = None
    indoor: bool | None = None
    prize: str
    total_financial_commitment: str
    location: str

    # Traceability
    source_file: str
    parsed_at: datetime

    # Field validators
    _empty_surface = field_validator(
        "surface", "surface_detail", "sponsor_title",
        mode="before",
    )(empty_to_none)
    _parse_indoor = field_validator("indoor", mode="before")(parse_indoor)


SCHEMA_HASH = compute_schema_hash(OverviewRecord)
OverviewRecord.SCHEMA_HASH = SCHEMA_HASH
