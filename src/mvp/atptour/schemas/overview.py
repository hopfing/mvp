"""Overview (tournament metadata) staged schema."""

from datetime import datetime

from pydantic import BaseModel, field_validator

from mvp.atptour.schema_helpers import empty_to_none, parse_indoor, strip_or_none
from mvp.common.enums import Circuit, Surface, TournamentType
from mvp.common.schema_hash import compute_schema_hash


class OverviewRecord(BaseModel):
    """A single tournament overview record from an ATP overview JSON file."""

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
    surface: Surface | None = None
    surface_detail: str | None = None
    indoor: bool | None = None
    prize: str
    total_financial_commitment: str
    location: str

    # Traceability
    source_file: str
    parsed_at: datetime

    # Field validators
    _strip_surface = field_validator("surface", mode="before")(strip_or_none)
    _empty_strings = field_validator(
        "surface_detail", "sponsor_title",
        mode="before",
    )(empty_to_none)
    _parse_indoor = field_validator("indoor", mode="before")(parse_indoor)


SCHEMA_HASH = compute_schema_hash(OverviewRecord)
OverviewRecord.SCHEMA_HASH = SCHEMA_HASH
