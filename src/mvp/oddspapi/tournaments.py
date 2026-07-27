"""oddspapi tournamentName -> our tournament_id.

Match resolution needs tournament identity. Without it the only discriminator left
for a pair that met more than once is the date, and `effective_match_date` is
estimated for rounds whose real date we don't have — so 36 fixtures land as
ambiguous and repeat meetings resolve on an estimate.

`_match_tournament` in the shared mapper can't supply it: it compares strings, and
of 184 oddspapi names ZERO match one of ours exactly, so the comparison no-ops and
returns the candidates untouched.

What does work is the structure of the names:

    'ATP Miami, USA Men Singles'                    -> Miami,   tour
    'ATP Challenger Oeiras 1, Portugal Men Singles'  -> Oeiras 1, chal
    'Wimbledon Men Singles'                          -> by name

`city` is the join key, not `(city, country)`: country is null on 343 of our 510
2026 tournaments — including challengers like Pune and Campinas — while city is
populated for every tour and challenger event. oddspapi's `categoryName` (ATP or
Challenger) supplies the circuit, which is what separates the two events a city can
host: our `Madrid 1` is tour and `Madrid 2` is challenger, `Rome 2` tour and
`Rome 1` challenger. The numeric suffix separates same-circuit repeats, and both
sides carry it (`Oeiras 1`..`Oeiras 4`).
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from mvp.common.base_job import get_data_root
from mvp.common.odds_matching import normalize_name

logger = logging.getLogger(__name__)

ALIASES_PATH = Path(__file__).with_name("tournament_aliases.yaml")

# Some names put a comma before it: 'ATP Challenger San Diego, USA, Men Singles'.
_SUFFIX = re.compile(r",?\s*Men\s+Singles\s*$")

# 'ATP Challenger Oeiras 1, Portugal' / 'ATP Miami, USA'. The country is captured
# only to be discarded — see the module docstring.
_PREFIX = re.compile(r"^ATP\s+Challenger\s+|^ATP\s+")
_PAREN = re.compile(r"\s*\([^)]*\)")            # 'Lincoln (NE)' -> 'Lincoln'
_CITY_SEQ = re.compile(r"^(?P<city>.+?)(?:\s+(?P<seq>\d+|[IVX]+))?$")

# 'Shymkent II' against our 'Shymkent 2'.
_ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6}

CIRCUIT_OF_CATEGORY = {"ATP": "tour", "Challenger": "chal"}


@dataclass
class ResolveReport:
    pinned: int = 0          # exactly one tournament_id
    narrowed: int = 0        # a small candidate set, date decides inside it
    by_name: int = 0
    by_city: int = 0
    by_alias: int = 0
    unresolved: dict[str, int] = field(default_factory=dict)
    narrowed_names: dict[str, int] = field(default_factory=dict)


def _load_aliases() -> dict[str, str]:
    """oddspapi tournament text -> our tournament_id, for what structure can't reach.

    Keyed on the name with ' Men Singles' already stripped.
    """
    if not ALIASES_PATH.exists():
        return {}
    raw = yaml.safe_load(ALIASES_PATH.read_text(encoding="utf-8")) or {}
    return {normalize_name(k): str(v) for k, v in raw.items()}


def _norm_city(text: str) -> str:
    """Apostrophe-insensitive: ours is \"'s-Hertogenbosch\", oddspapi's is
    'S-Hertogenbosch'."""
    return normalize_name((text or "").replace("'", "").replace("`", "")).strip()


def strip_suffix(name: str) -> str:
    return _SUFFIX.sub("", name).strip()


def parse_name(name: str) -> tuple[str, int | None]:
    """('ATP Challenger Oeiras 1, Portugal Men Singles') -> ('oeiras', 1).

    Returns the normalized city and the sequence number where present. The country
    segment is dropped: ours is null two thirds of the time.
    """
    text = _PREFIX.sub("", strip_suffix(name)).strip()
    if "," in text:
        text = text.rsplit(",", 1)[0].strip()
    text = _PAREN.sub("", text).strip()
    m = _CITY_SEQ.match(text)
    if not m:
        return _norm_city(text), None
    raw_seq = m.group("seq")
    if raw_seq is None:
        return _norm_city(text), None
    seq = int(raw_seq) if raw_seq.isdigit() else _ROMAN.get(raw_seq.upper())
    if seq is None:                     # not a sequence after all, e.g. a city 'V'
        return _norm_city(text), None
    return _norm_city(m.group("city")), seq


def _our_tournaments() -> pl.DataFrame:
    return (
        pl.scan_parquet(get_data_root() / "aggregate" / "atptour" / "matches.parquet")
        .filter(pl.col("draw_type") == "singles")
        .select("tournament_id", "tournament_name", "city", "circuit", "year")
        .unique()
        .collect()
    )


def _name_seq(tournament_name: str) -> int | None:
    """Trailing number of one of our names: 'Oeiras 3' -> 3, 'Miami' -> None."""
    m = re.search(r"\s(\d+)$", tournament_name or "")
    return int(m.group(1)) if m else None


class TournamentIndex:
    """Our tournaments, keyed the two ways an oddspapi name can be matched."""

    def __init__(self, rows: pl.DataFrame) -> None:
        self.by_name: dict[tuple[str, int], str] = {}
        self.by_city: dict[tuple[str, str, int], list[dict]] = defaultdict(list)
        for r in rows.iter_rows(named=True):
            year = r["year"]
            if r["tournament_name"]:
                self.by_name[(_norm_city(r["tournament_name"]), year)] = (
                    r["tournament_id"]
                )
            if r["city"] and r["circuit"] in ("tour", "chal"):
                self.by_city[(_norm_city(r["city"]), r["circuit"], year)].append(r)

    def lookup(
        self, name: str, circuit: str | None, year: int,
    ) -> tuple[frozenset[str], str]:
        """(candidate tournament_ids, how).

        A SET rather than one id, because a city can host two same-circuit events
        that neither side numbers consistently — we hold two tournaments both named
        'Nottingham 1'. Narrowing to that city's events is still worth having: the
        date then chooses between two candidates instead of every meeting the pair
        ever played. `how` is one of name / city / narrowed / miss.
        """
        direct = self.by_name.get((_norm_city(strip_suffix(name)), year))
        if direct is not None:
            return frozenset({direct}), "name"

        if circuit is None:
            return frozenset(), "miss"
        city, seq = parse_name(name)
        hits = self.by_city.get((city, circuit, year), [])
        if not hits:
            return frozenset(), "miss"
        if len(hits) == 1:
            return frozenset({hits[0]["tournament_id"]}), "city"

        # Same city, same circuit. The sequence number separates them when both
        # sides carry one.
        wanted = [h for h in hits if _name_seq(h["tournament_name"]) == seq]
        if len(wanted) == 1:
            return frozenset({wanted[0]["tournament_id"]}), "city"

        # A city-form oddspapi name refers to the event WE name after the city:
        # London (Queen's) and Wimbledon are both city=London, circuit=tour, and
        # neither carries a number. Anything brand-named on both sides — the majors —
        # matched on the name path above and never reaches here.
        if seq is None:
            named_for_city = [h for h in hits if _norm_city(h["tournament_name"]) == city]
            if len(named_for_city) == 1:
                return frozenset({named_for_city[0]["tournament_id"]}), "city"

        return frozenset(h["tournament_id"] for h in hits), "narrowed"


def build_index() -> TournamentIndex:
    return TournamentIndex(_our_tournaments())


def resolve_fixtures(
    fixtures: list[dict],
    index: TournamentIndex | None = None,
) -> tuple[dict[str, frozenset[str]], ResolveReport]:
    """fixtureId -> candidate tournament_ids.

    A fixture with no entry is left to the player-pair path as before; absence is
    not an error, it just means no tournament constraint is available for it.
    """
    from mvp.oddspapi.matcher import parse_ts

    index = index or build_index()
    aliases = _load_aliases()
    out: dict[str, frozenset[str]] = {}
    report = ResolveReport()

    for f in fixtures:
        name = f.get("tournamentName") or ""
        ts = parse_ts(f.get("trueStartTime")) or parse_ts(f.get("startTime"))
        if not name or ts is None:
            continue
        circuit = CIRCUIT_OF_CATEGORY.get(f.get("categoryName") or "")

        alias = aliases.get(_norm_city(strip_suffix(name)))
        if alias is not None:
            out[f["fixtureId"]] = frozenset({alias})
            report.pinned += 1
            report.by_alias += 1
            continue

        ids, how = index.lookup(name, circuit, ts.year)
        if not ids:
            report.unresolved[name] = report.unresolved.get(name, 0) + 1
            continue
        out[f["fixtureId"]] = ids
        if how == "narrowed":
            report.narrowed += 1
            report.narrowed_names[name] = report.narrowed_names.get(name, 0) + 1
        else:
            report.pinned += 1
            if how == "name":
                report.by_name += 1
            else:
                report.by_city += 1
    return out, report
