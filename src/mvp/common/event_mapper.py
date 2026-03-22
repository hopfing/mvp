"""Map book odds events to internal match_uids using full player database.

Decoupled from predictions — maps ALL book events against our schedule/results
data using player bio names, display name variants, and per-book aliases.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from mvp.common.base_job import get_data_root
from mvp.common.odds_matching import EventMatch, normalize_name

logger = logging.getLogger(__name__)

# Book tournament name prefixes to strip for matching
_CIRCUIT_PREFIXES = [
    "challenger quals. - ",
    "challenger quals - ",
    "challenger - ",
    "atp - ",
    "wta - ",
]


@dataclass
class MappingResult:
    """Result of mapping book events to matches."""

    event_matches: list[EventMatch] = field(default_factory=list)
    unresolved_names: set[str] = field(default_factory=set)
    no_match_found: list[tuple[str, str, str]] = field(default_factory=list)
    collisions: list[tuple[str, str, str, int]] = field(default_factory=list)


def build_player_lookup(
    aliases_path: Path | None = None,
) -> dict[str, str]:
    """Build normalized_name -> player_id lookup from all available sources.

    Layers (highest priority first):
    1. Per-book aliases from aliases_path (if provided)
    2. Display name variants from results data
    3. Bio names (first_name + last_name) from players.parquet

    Args:
        aliases_path: Path to book-specific player_aliases.yaml. None to skip.

    Returns:
        Dict mapping normalized player names to player_ids.
    """
    lookup: dict[str, str] = {}
    collisions: list[tuple[str, str, str]] = []

    # Layer 3 (lowest priority): bio names
    data_root = get_data_root()
    bio_path = data_root / "stage" / "atptour" / "players.parquet"
    if bio_path.exists():
        bio = pl.read_parquet(bio_path, columns=["player_id", "first_name", "last_name"])
        for row in bio.iter_rows(named=True):
            pid = row["player_id"]
            first = row["first_name"] or ""
            last = row["last_name"] or ""
            if first and last:
                normed = normalize_name(f"{first} {last}")
                existing = lookup.get(normed)
                if existing is not None and existing != pid:
                    collisions.append((normed, existing, pid))
                lookup[normed] = pid
        logger.info("Player lookup: %d bio names loaded", len(lookup))

    # Layer 2 (medium priority): display name variants from results
    # These override bio names when they differ (e.g., shortened names)
    _add_display_name_variants(lookup, data_root, collisions)

    # Layer 1 (highest priority): per-book aliases
    if aliases_path is not None and aliases_path.exists():
        with open(aliases_path) as f:
            raw = yaml.safe_load(f) or {}
        alias_count = 0
        for name, pid in raw.items():
            normed = normalize_name(name)
            lookup[normed] = pid.upper().strip()
            alias_count += 1
        logger.info("Player lookup: %d aliases loaded from %s", alias_count, aliases_path.name)

    if collisions:
        for normed, pid1, pid2 in collisions[:10]:
            logger.warning(
                "Name collision: '%s' maps to both %s and %s (keeping %s)",
                normed, pid1, pid2, lookup.get(normed, "?"),
            )
        if len(collisions) > 10:
            logger.warning("... and %d more collisions", len(collisions) - 10)

    logger.info("Player lookup: %d total entries", len(lookup))
    return lookup


def _add_display_name_variants(
    lookup: dict[str, str],
    data_root: Path,
    collisions: list[tuple[str, str, str]],
) -> None:
    """Add display name variants from staged results parquets."""
    results_root = data_root / "stage" / "atptour" / "tournaments"
    if not results_root.exists():
        return

    added = 0
    for results_path in results_root.glob("**/results.parquet"):
        try:
            df = pl.read_parquet(
                results_path, columns=["p1_id", "p1_name", "p2_id", "p2_name"]
            )
        except Exception:
            continue

        for id_col, name_col in [("p1_id", "p1_name"), ("p2_id", "p2_name")]:
            pairs = df.select(id_col, name_col).unique().drop_nulls()
            for row in pairs.iter_rows():
                pid, name = row[0], row[1]
                if not pid or not name:
                    continue
                normed = normalize_name(name)
                existing = lookup.get(normed)
                if existing is not None and existing != pid:
                    collisions.append((normed, existing, pid))
                else:
                    if normed not in lookup:
                        added += 1
                    lookup[normed] = pid

    if added > 0:
        logger.info("Player lookup: %d display name variants added", added)


def build_match_catalog(
    matches_df: pl.DataFrame,
) -> dict[frozenset, list[dict]]:
    """Build an index from player pair -> list of match records.

    Args:
        matches_df: DataFrame with at minimum: match_uid, player_id, opp_id,
                    tournament_id, year. May also have tournament_name.

    Returns:
        Dict mapping frozenset({player_id, opp_id}) to list of
        {match_uid, tournament_id, year, tournament_name?} dicts.
    """
    catalog: dict[frozenset, list[dict]] = {}

    required = {"match_uid", "player_id", "opp_id", "tournament_id", "year"}
    missing = required - set(matches_df.columns)
    if missing:
        raise ValueError(f"matches_df missing required columns: {missing}")

    has_name = "tournament_name" in matches_df.columns
    cols = list(required) + (["tournament_name"] if has_name else [])

    # Deduplicate: same match_uid can appear twice (player + opp perspective)
    deduped = matches_df.select(cols).unique(subset=["match_uid"])

    for row in deduped.iter_rows(named=True):
        pair = frozenset({row["player_id"], row["opp_id"]})
        entry = {
            "match_uid": row["match_uid"],
            "tournament_id": row["tournament_id"],
            "year": row["year"],
        }
        if has_name:
            entry["tournament_name"] = row.get("tournament_name")
        catalog.setdefault(pair, []).append(entry)

    # Log collision warnings (same pair, same tournament+year)
    for pair, entries in catalog.items():
        seen: dict[tuple, int] = {}
        for e in entries:
            key = (e["tournament_id"], e["year"])
            seen[key] = seen.get(key, 0) + 1
        for key, count in seen.items():
            if count > 1:
                pair_str = " vs ".join(sorted(pair))
                logger.warning(
                    "Match catalog collision: %s appears %d times in "
                    "tournament %s year %s (round-robin?)",
                    pair_str, count, key[0], key[1],
                )

    total_matches = sum(len(v) for v in catalog.values())
    logger.info("Match catalog: %d unique pairs, %d matches", len(catalog), total_matches)
    return catalog


def _strip_circuit_prefix(book_tournament: str) -> str:
    """Strip circuit prefixes from book tournament names."""
    lower = book_tournament.strip().lower()
    for prefix in _CIRCUIT_PREFIXES:
        if lower.startswith(prefix):
            return book_tournament[len(prefix):].strip()
    return book_tournament.strip()


def _match_tournament(
    book_tournament: str,
    candidates: list[dict],
) -> list[dict]:
    """Narrow candidates by matching book tournament name to our tournament data.

    Uses simple substring matching of the stripped book tournament name
    against our tournament_name field.
    """
    stripped = _strip_circuit_prefix(book_tournament).lower()
    if not stripped:
        return candidates

    matched = []
    for c in candidates:
        our_name = (c.get("tournament_name") or "").lower()
        # Check if the book's stripped name appears in our tournament name or vice versa
        if stripped in our_name or our_name in stripped:
            matched.append(c)

    return matched if matched else candidates


def map_book_events(
    staged_odds: pl.DataFrame,
    event_id_col: str,
    book: str,
    player_lookup: dict[str, str],
    match_catalog: dict[frozenset, list[dict]],
    existing_event_ids: set[str] | None = None,
) -> MappingResult:
    """Map book odds events to internal match_uids.

    Args:
        staged_odds: Staged odds DataFrame with event_id_col, player_name,
                     tournament columns. Should be pre-filtered/deduped as needed.
        event_id_col: Name of the event ID column (e.g., "dk_event_id").
        book: Book identifier (e.g., "dk", "br", "mgm").
        player_lookup: normalized_name -> player_id mapping.
        match_catalog: frozenset({pid1, pid2}) -> list of match records.
        existing_event_ids: Event IDs already in the event_map (skip these).

    Returns:
        MappingResult with new event matches, unresolved names, and diagnostics.
    """
    if existing_event_ids is None:
        existing_event_ids = set()

    result = MappingResult()

    # Group odds by event (two rows per event = one match)
    book_events: dict[str, list[dict]] = {}
    for row in staged_odds.iter_rows(named=True):
        eid = row[event_id_col]
        if eid in existing_event_ids:
            continue
        book_events.setdefault(eid, []).append(row)

    mapped = 0
    skipped_unresolved = 0
    skipped_no_match = 0
    skipped_ambiguous = 0

    for eid, rows in book_events.items():
        if len(rows) < 2:
            continue

        # Resolve both player names
        name_a = rows[0]["player_name"]
        name_b = rows[1]["player_name"]
        pid_a = player_lookup.get(normalize_name(name_a))
        pid_b = player_lookup.get(normalize_name(name_b))

        if pid_a is None:
            result.unresolved_names.add(name_a)
        if pid_b is None:
            result.unresolved_names.add(name_b)
        if pid_a is None or pid_b is None:
            skipped_unresolved += 1
            continue

        # Look up match by player pair
        pair = frozenset({pid_a, pid_b})
        candidates = match_catalog.get(pair, [])

        if len(candidates) == 0:
            result.no_match_found.append((eid, name_a, name_b))
            skipped_no_match += 1
            continue

        if len(candidates) == 1:
            match = candidates[0]
        else:
            # Multiple candidates — try tournament disambiguation
            book_tournament = rows[0].get("tournament", "")
            narrowed = _match_tournament(book_tournament, candidates)

            if len(narrowed) == 1:
                match = narrowed[0]
            else:
                result.collisions.append((
                    eid, name_a, name_b, len(narrowed or candidates),
                ))
                skipped_ambiguous += 1
                continue

        # Determine p1/p2 book names (p1 = first player in our match data)
        # We don't know p1/p2 order here — store both and let the consumer resolve
        result.event_matches.append(EventMatch(
            match_uid=match["match_uid"],
            event_id=eid,
            p1_book_name=name_a,
            p2_book_name=name_b,
        ))
        mapped += 1

    # Logging
    total = len(book_events)
    logger.info(
        "Event mapper [%s]: %d/%d events mapped, %d unresolved, "
        "%d no match, %d ambiguous",
        book.upper(), mapped, total, skipped_unresolved,
        skipped_no_match, skipped_ambiguous,
    )

    if result.unresolved_names:
        logger.info(
            "Unresolved %s names (%d): %s",
            book.upper(),
            len(result.unresolved_names),
            ", ".join(sorted(result.unresolved_names)[:20]),
        )
        if len(result.unresolved_names) > 20:
            logger.info("  ... and %d more", len(result.unresolved_names) - 20)

    for eid, na, nb, count in result.collisions:
        logger.warning(
            "Ambiguous match for %s event %s (%s vs %s): %d candidates",
            book.upper(), eid, na, nb, count,
        )

    return result
