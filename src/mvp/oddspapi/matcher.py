"""Resolve oddspapi fixtures to match_uid, once, and cache the result.

`odds/event_map.parquet` is a live-pipeline artifact and is deliberately not
touched: oddspapi rows carry `match_uid` directly, resolved here at ingest time.

The resolution itself reuses the production matching machinery — the only
oddspapi-specific step is converting its "Surname, First (YYYY)" participant naming
to "First Surname" before `normalize_name` sees it.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.common.base_job import get_data_root
from mvp.common.event_mapper import (
    build_match_catalog,
    build_player_lookup,
    map_book_events,
)
from mvp.oddspapi import tournaments
from mvp.oddspapi.paths import crosswalk_path, fixtures_dir

logger = logging.getLogger(__name__)

# Curated name overrides, versioned beside the source like every scraper's
# (`src/mvp/<source>/player_aliases.yaml`). Not optional: without it, players whose
# oddspapi spelling differs from ours fail to resolve and their matches vanish from
# the stage file silently — 165 matches, one whole qualifying draw, in the first run.
ALIASES_PATH = Path(__file__).with_name("player_aliases.yaml")

IN_SCOPE_CATEGORIES = frozenset({"ATP", "Challenger"})
_YEAR_SUFFIX = re.compile(r"\s*\([^)]*\)\s*$")

# Fixture-level facts ride along because ticks carry none of them: a historical
# payload is only {fixtureId, bookmakers}, and a tick is only
# {createdAt, price, limit, active, exchangeMeta}. The prematch boundary and the
# completeness filter both live in the fixtures dump, which this module already
# reads, so they are resolved here rather than looked up twice.
#
# `true_start_time` is the real boundary, not `start_time`: 48% of fixtures begin
# more than 5 minutes after their scheduled time and 1.7% more than an hour, so
# cutting on the schedule misclassifies genuinely prematch ticks as in-play.
CROSSWALK_COLUMNS = ["fixture_id", "match_uid", "p1_id", "p2_id",
                     "p1_book_name", "p2_book_name"]
_TS_COLUMNS = ["start_time", "true_start_time"]
_INT_COLUMNS = ["status_id"]

# Observed statusId values, inferred from co-occurrence with trueStartTime rather
# than from documentation: 2 -> finished (trueStart on 28,329/28,952), None -> not
# yet played (trueStart on 0/13,023), 1 -> started, 3 -> cancelled (trueStart on
# 66/2,365), 0 -> scheduled. Treat as a hint, not a contract.
STATUS_FINISHED = 2
STATUS_CANCELLED = 3


def parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def to_first_last(name: str) -> str:
    """'Cretu, Cezar (2001)' -> 'Cezar Cretu'; drop the trailing disambiguator."""
    name = _YEAR_SUFFIX.sub("", name).strip()
    if "," in name:
        surname, first = name.split(",", 1)
        return f"{first.strip()} {surname.strip()}".strip()
    return name


def _is_singles(f: dict) -> bool:
    return not any(
        "/" in str(f.get(k, "")) for k in ("participant1Name", "participant2Name")
    )


# Bracket placeholders for unplayed matches — "Qf1", "R16P12", "WQF3", "WSF1".
# They are draw slots, not people, so they can never resolve; counting them as
# misses made the crosswalk look 0.3pp worse than it is and buried three real
# unmapped players in a list of thirty placeholders.
_PLACEHOLDER = re.compile(r"^[A-Za-z]{1,4}\d+[A-Za-z]*\d*$")


def _is_placeholder(name: str) -> bool:
    name = name.strip()
    return " " not in name and bool(_PLACEHOLDER.match(name))


def _has_real_participants(f: dict) -> bool:
    return not any(
        _is_placeholder(str(f.get(k, "")))
        for k in ("participant1Name", "participant2Name")
    )


def load_in_scope_fixtures() -> list[dict]:
    """Deduplicated ATP/Challenger singles fixtures from the raw fixtures dumps."""
    seen: dict[str, dict] = {}
    for p in sorted(fixtures_dir().glob("*.json")):
        for f in json.loads(p.read_text(encoding="utf-8")):
            if (
                isinstance(f, dict)
                and f.get("categoryName") in IN_SCOPE_CATEGORIES
                and _is_singles(f)
                and _has_real_participants(f)
                and f.get("fixtureId")
            ):
                seen[f["fixtureId"]] = f
    return list(seen.values())


def resolve_raw(fixtures: list[dict]):
    """Run the mapper and return its full result, for diagnostics.

    `resolve` wraps this into the crosswalk frame. Both go through here so a
    diagnostic can never drift from the real query — building a second `matches`
    select for analysis is how a fix silently fails to show up in its own probe.
    """
    # Tournament identity, so the mapper can constrain on it instead of leaning on
    # the estimated per-match date. 92% of fixtures pin to one tournament_id and
    # another 7% narrow to one city's events; the rest carry no constraint and fall
    # back to the player-pair path exactly as before.
    tournament_ids, trep = tournaments.resolve_fixtures(fixtures)
    logger.info(
        "Tournaments: %d fixtures pinned, %d narrowed, %d unresolved (%d names)",
        trep.pinned, trep.narrowed, sum(trep.unresolved.values()),
        len(trep.unresolved),
    )

    rows = []
    for f in fixtures:
        start = parse_ts(f["startTime"])
        tids = tournament_ids.get(f["fixtureId"])
        for nm in (
            to_first_last(f.get("participant1Name", "")),
            to_first_last(f.get("participant2Name", "")),
        ):
            rows.append({
                "oap_event_id": f["fixtureId"],
                "player_name": nm,
                "tournament": f.get("tournamentName", ""),
                "fetched_at": start,
                "tournament_ids": sorted(tids) if tids else None,
            })
    if not rows:
        return None

    lookup = build_player_lookup(aliases_path=ALIASES_PATH)
    matches = (
        pl.scan_parquet(_matches_path())
        # No season filter: map_book_events already narrows candidates by the
        # event year and then by date, so filtering here would only duplicate
        # that while making the job wrong the moment the raw tree spans two
        # seasons.
        .filter(pl.col("draw_type") == "singles")
        .select([
            "match_uid", "player_id", "opp_id", "tournament_id", "year",
            "draw_p1_id", "tournament_name", "round", "draw_type",
            # Disambiguates a pair that met twice in the season. oddspapi's
            # tournament strings don't align with ours ('French Open Men Singles'
            # vs our Roland Garros naming), so name matching never narrows.
            "effective_match_date",
        ])
        .collect()
    )
    catalog = build_match_catalog(matches)
    return map_book_events(
        pl.DataFrame(rows), "oap_event_id", "oddspapi", lookup, catalog
    )


def resolve(fixtures: list[dict]) -> pl.DataFrame:
    """Map fixtures to match_uid. One row per resolved fixture."""
    return to_crosswalk(resolve_raw(fixtures), fixtures)


def to_crosswalk(res, fixtures: list[dict]) -> pl.DataFrame:
    """Crosswalk frame from an already-computed MappingResult.

    Separate from `resolve` so a caller that wants the diagnostics does not have to
    resolve twice — the resolution reads matches.parquet and rebuilds the player
    lookup, so it is the expensive half.
    """
    if res is None:
        return pl.DataFrame(schema=_crosswalk_schema())
    meta = {f["fixtureId"]: f for f in fixtures}
    return pl.DataFrame([
        {
            "fixture_id": em.event_id,
            "match_uid": em.match_uid,
            "p1_id": em.p1_id,
            "p2_id": em.p2_id,
            "p1_book_name": em.p1_book_name,
            "p2_book_name": em.p2_book_name,
            "start_time": parse_ts(meta[em.event_id].get("startTime")),
            "true_start_time": parse_ts(meta[em.event_id].get("trueStartTime")),
            "status_id": meta[em.event_id].get("statusId"),
        }
        for em in res.event_matches
        if em.event_id in meta
    ], schema=_crosswalk_schema())


def _crosswalk_schema() -> dict:
    schema: dict = {c: pl.Utf8 for c in CROSSWALK_COLUMNS}
    for c in _TS_COLUMNS:
        schema[c] = pl.Datetime(time_unit="us", time_zone="UTC")
    for c in _INT_COLUMNS:
        schema[c] = pl.Int64
    return schema


def _matches_path() -> Path:
    return get_data_root() / "aggregate" / "atptour" / "matches.parquet"


def build(refresh: bool = False) -> pl.DataFrame:
    """Load the cached crosswalk, rebuilding it when missing or when asked.

    matches.parquet is rewritten every 15 minutes by the live pipeline, so a
    rebuild can read a torn snapshot. Caching means one exposure per transform
    session instead of one per market.
    """
    path = crosswalk_path()
    if path.exists() and not refresh:
        cached = pl.read_parquet(path)
        logger.info("Crosswalk: %d fixtures (cached)", len(cached))
        return cached

    fixtures = load_in_scope_fixtures()
    logger.info("Crosswalk: resolving %d in-scope fixtures", len(fixtures))
    raw = resolve_raw(fixtures)
    if raw is not None:
        logger.info(
            "Crosswalk: %d unresolved names, %d no match, %d ambiguous, "
            "%d rejected on date, %d rejected on tournament",
            len(raw.unresolved_names), len(raw.no_match_found),
            len(raw.collisions), len(raw.date_rejected),
            len(raw.tournament_rejected),
        )
        for _eid, a, b, muid, gap in raw.date_rejected[:10]:
            logger.info("  date-rejected: %s vs %s -> %s (%d days)", a, b, muid, gap)
    resolved = to_crosswalk(raw, fixtures)
    path.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_parquet(path)
    rate = len(resolved) / max(len(fixtures), 1)
    logger.info(
        "Crosswalk: resolved %d/%d fixtures (%.1f%%) -> %s",
        len(resolved), len(fixtures), 100 * rate, path,
    )
    return resolved
