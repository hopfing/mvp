"""Market definitions read from the raw markets reference.

685 markets across 26 (marketType, period) groups, nearly all two-sided with a
handicap. Market ids are resolved from `reference/markets_tennis.json` rather than
hardcoded: the ids are stable but numerous, and the handicap lives in the reference
rather than in the tick payload.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from mvp.oddspapi.paths import markets_reference

# Stage filenames for the markets downstream code reads by name. Everything else
# gets a normalised `<markettype>_<period>` name — the transform writes every
# market the books offered, not a hand-picked subset.
STAGE_NAMES: dict[tuple[str, str | None], str] = {
    ("totals-games", "result"): "total_games",
    ("spreads-games", "result"): "game_spread",
    ("moneyline", "result"): "moneyline",
}


@dataclass(frozen=True)
class Outcome:
    outcome_id: str
    name: str


@dataclass(frozen=True)
class Market:
    """One offered market: its id, group, handicap and outcomes."""

    market_id: str
    market_type: str
    period: str | None
    handicap: float | None
    outcomes: tuple[Outcome, ...]

    @property
    def group(self) -> tuple[str, str | None]:
        return (self.market_type, self.period)

    @property
    def stage_name(self) -> str:
        return stage_name(self.market_type, self.period)


def stage_name(market_type: str, period: str | None) -> str:
    known = STAGE_NAMES.get((market_type, period))
    if known:
        return known
    raw = f"{market_type}_{period}" if period else str(market_type)
    return re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")


def load_index() -> dict[str, Market]:
    """marketId -> Market, for every market the reference defines."""
    path = markets_reference()
    if not path.exists():
        raise FileNotFoundError(
            f"markets reference not found at {path}. Raw capture is out of scope; "
            f"place the captured tree under {path.parent.parent}."
        )
    index: dict[str, Market] = {}
    for it in json.loads(path.read_text(encoding="utf-8")):
        mid = it.get("marketId")
        mt = it.get("marketType")
        if mid is None or mt is None:
            continue
        outs = tuple(
            Outcome(str(o["outcomeId"]), str(o.get("outcomeName") or ""))
            for o in (it.get("outcomes") or [])
            if o.get("outcomeId") is not None
        )
        if not outs:
            continue
        hcap = it.get("handicap")
        index[str(mid)] = Market(
            market_id=str(mid),
            market_type=str(mt),
            period=it.get("period"),
            handicap=float(hcap) if hcap is not None else None,
            outcomes=outs,
        )
    return index


def groups(index: dict[str, Market]) -> dict[tuple[str, str | None], int]:
    counts: dict[tuple[str, str | None], int] = {}
    for m in index.values():
        counts[m.group] = counts.get(m.group, 0) + 1
    return counts
