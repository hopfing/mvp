"""Staged oddspapi tick schema — one row per captured price.

One staged parquet per raw capture file, so this is the record: every tick as
captured, with no prematch cut and no `active` cut. Deliberately carries no
`match_uid`, so improving the matcher is a re-join rather than a 35 GB reparse.
"""

from datetime import datetime

from pydantic import BaseModel

from mvp.common.schema_hash import compute_schema_hash


class TickRecord(BaseModel):
    """A single quote for one side of one market at one book."""

    # Identity — fixture, not match: resolution happens downstream.
    fixture_id: str
    book: str
    market: str
    points: float | None
    side: str

    # The quote
    odds: float
    fetched_at: datetime
    active: bool | None
    limit: float | None
    exchange_meta: str | None


SCHEMA_HASH = compute_schema_hash(TickRecord)
TickRecord.SCHEMA_HASH = SCHEMA_HASH
