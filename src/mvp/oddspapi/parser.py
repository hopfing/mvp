"""Parse a captured oddspapi historical file into tick records.

Pure parsing: no filesystem layout, no match resolution, no filtering decisions —
those belong to the transformer. One record per (book, market, outcome, tick).

Every market the book offered is emitted, not a chosen few. The 35 GB parse is the
expensive part of the job and it is paid per RUN, not per market, so extracting one
market at a time would mean paying it once per market for data already in hand.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from mvp.oddspapi.markets import Market

PARSER_VERSION = 1


@dataclass(frozen=True)
class Tick:
    fixture_id: str
    book: str
    market_type: str
    period: str | None
    stage_name: str
    points: float | None
    side: str
    odds: float | None
    ts: datetime
    limit: float | None
    active: bool
    exchange_meta: str | None


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def parse_file(path: Path, index: dict[str, Market]) -> tuple[str | None, list[Tick]]:
    """(fixture_id, ticks). Returns (None, []) for an unreadable file.

    Capture writes these while running, so a half-written file is expected rather
    than exceptional — it is skipped and counted, never raised.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None, []

    fixture_id = payload.get("fixtureId")
    if not fixture_id:
        return None, []

    out: list[Tick] = []
    for book, bm in (payload.get("bookmakers") or {}).items():
        for market_id, mk in ((bm or {}).get("markets") or {}).items():
            market = index.get(str(market_id))
            if market is None:
                continue
            outcomes = (mk or {}).get("outcomes") or {}
            for outcome in market.outcomes:
                series = (outcomes.get(outcome.outcome_id) or {}).get("players", {})
                for arr in series.values():
                    for tick in arr or []:
                        created = tick.get("createdAt")
                        if created is None:
                            continue
                        out.append(Tick(
                            fixture_id=fixture_id,
                            book=book,
                            market_type=market.market_type,
                            period=market.period,
                            stage_name=market.stage_name,
                            points=market.handicap,
                            side=outcome.name or outcome.outcome_id,
                            odds=tick.get("price"),
                            ts=parse_ts(created),
                            limit=tick.get("limit"),
                            active=bool(tick.get("active", True)),
                            # Null on every book seen so far; presumably populated
                            # for exchanges. Carried rather than dropped — the
                            # stage layer is the record.
                            exchange_meta=(
                                None if tick.get("exchangeMeta") is None
                                else str(tick["exchangeMeta"])
                            ),
                        ))
    return fixture_id, out
