"""Aggregated oddspapi quote schema — one row per priced line, per book role.

The reduction every consumer actually wants: for each match, market, line and
side, the price at open / formed / close, read at a single instant across the
books of one role. Derived from the staged ticks, which stay the record — anything
needing the intervening prices (in-play, per-book behaviour) reads those.

`role` keeps reference and entry books apart rather than maxing across them:
Pinnacle is the de-vig reference for CLV, not a book a bet can be placed at, so
folding it into a best-across price would quote an unreachable number.
"""

from pydantic import BaseModel

from mvp.common.schema_hash import compute_schema_hash


class QuoteRecord(BaseModel):
    """Open / formed / close for one (match, market, line, side, role)."""

    # Identity
    match_uid: str
    market: str
    points: float | None
    side: str
    role: str

    # Best across the books of this role, each read at one real instant
    best_opening_odds: float | None
    formed_odds: float | None
    best_closing_odds: float | None

    # Distinct books of this role that quoted the series at ANY prematch instant —
    # not the depth at the bucket each of the three prices was read from, which
    # differs per price. Sitting next to them, it is easy to read as the latter.
    n_books: int

    p1_id: str | None
    p2_id: str | None


SCHEMA_HASH = compute_schema_hash(QuoteRecord)
QuoteRecord.SCHEMA_HASH = SCHEMA_HASH
