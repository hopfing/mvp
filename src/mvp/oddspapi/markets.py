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


# Outcome names that mean "the fixture's first / second participant". A market is
# participant-sided when its outcomes are exactly these, optionally with a draw.
_PARTICIPANT_OUTCOMES = frozenset({"1", "2"})
_DRAW_OUTCOME = "X"


def participant_sided_markets() -> set[str]:
    """Stage names whose "1"/"2" outcomes are the fixture's participants.

    Derived from the reference rather than hardcoded, so a market the feed adds
    classifies itself.

    The distinction is load-bearing and not obvious from the side value alone:
    `exactsets_result` has outcomes ["2","3","4","5"] for the number of sets a match
    takes, so a row with side="2" there means a two-set match, not participant two.
    Resolving sides to players by matching the string would attribute a set-count
    outcome to a player.

    Measured over the reference: 14 markets qualify (moneyline, game_spread,
    spreads_result, moneyline_p1..p5, spreads_games_p1..p5, moneyline_aces_result),
    and exactsets_result is the only market with a bare "2" that does not.
    """
    out: set[str] = set()
    for m in load_index().values():
        names = {o.name for o in m.outcomes}
        if _PARTICIPANT_OUTCOMES <= names and not (names - _PARTICIPANT_OUTCOMES
                                                   - {_DRAW_OUTCOME}):
            out.add(m.stage_name)
    return out


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
