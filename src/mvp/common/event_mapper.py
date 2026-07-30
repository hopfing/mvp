"""Map book odds events to internal match_uids using full player database.

Decoupled from predictions — maps ALL book events against our schedule/results
data using player bio names, display name variants, and per-book aliases.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from mvp.common.base_job import get_data_root
from mvp.common.odds_matching import EventMatch, normalize_name, normalize_tournament

logger = logging.getLogger(__name__)

# Book tournament name prefixes to strip for matching (order matters — longest first)
_CIRCUIT_PREFIXES = [
    "atp challenger ",
    "challenger quals. - ",
    "challenger quals - ",
    "challenger - ",
    "atp - ",
    "wta - ",
]

# Suffixes like "(ESP)", "- Qualification", "- Clay", "- Hard", "- Grass"
_SUFFIX_PATTERNS = re.compile(
    r"\s*\([A-Z]{2,3}\)"  # (ESP), (FR), etc.
    r"|\s*-\s*(?:qualification|qualifying|qual\.?|clay|hard|grass|carpet|indoor hard)"
    r"|\s*-\s*(?:q[12]|main draw)",
    re.IGNORECASE,
)

# Everything above only fires on prefixes and " - "-delimited suffixes. Books also
# embed the same tokens mid-string ("Shymkent Challenger 2026", "ATP Madrid 2026",
# "Queens Semifinals"), which left the residue that made name comparison unusable as
# a gate. Stripped from BOTH sides so the comparison is symmetric.
_NAME_NOISE = re.compile(
    r"\b20\d\d\b"                                      # season year
    r"|\bchallenger\b|\bmasters\b|\batp\b|\bwta\b|\bitf\b"
    r"|\bmen\b|\bwomen\b"
    r"|\bqualifiers?\b|\bqualifying\b|\bqualification\b|\bquals?\b"
    # Spanish/Portuguese qualifying, prefix-matched: bet365 serves these
    # mojibaked ("ClasificaciÃ³n"), so the tail is not reliably spellable.
    r"|\bclasificaci\w*|\bqualificac\w*"
    r"|\bround\s*\d+\b|\br\d+\b"
    r"|\bsemi\s*finals?\b|\bquarter\s*finals?\b|\bfinals?\b"
    r"|\bclay\b|\bhard\b|\bgrass\b|\bcarpet\b|\bindoor\b|\boutdoor\b",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[-–—,()]+")

# Our own back-to-back disambiguator ("Madrid 1", "Oeiras 3"), which no book carries.
# Dropped for the AGREEMENT test only — picking between Madrid 1 and Madrid 2 is the
# tiebreak's job (`_match_tournament`), not the gate's.
_OUR_VARIANT_INDEX = re.compile(r"\s+\d+$")

# Tournaments whose book name and ours genuinely differ, so no amount of token
# stripping reconciles them. Keys and values are already normalized. Measured
# against the full event map: these cover ~70% of the name disagreements on
# otherwise-healthy mappings. Anything not listed is expected to match on tokens.
#
# Keep this small and only for genuine divergences. An alias for a name that also
# exists in our own data is actively harmful: "centurion" -> "pretoria" looked
# reasonable and broke 188 healthy mappings, because Centurion is a real
# tournament of ours.
_TOURNAMENT_ALIASES = {
    "french open": "roland garros",
    "french open paris": "roland garros",
    "montemar": "alicante",
    "marrakesh": "marrakech",
    "queens": "london",
    "napoli": "naples",
    "bangalore": "bengaluru",
    "de roma": "rome",
}


def _scrub(name: str) -> str:
    """Strip noise tokens and punctuation, then normalize."""
    return normalize_tournament(_PUNCT.sub(" ", _NAME_NOISE.sub(" ", name or "")))


def normalize_book_tournament(book_tournament: str) -> str:
    """Book tournament text -> comparable token string, aliases applied."""
    scrubbed = _scrub(_strip_circuit_prefix(book_tournament or ""))
    return _TOURNAMENT_ALIASES.get(scrubbed, scrubbed)


def normalize_our_tournament(our_name: str) -> str:
    """Our tournament_name -> comparable token string, variant index dropped."""
    return _scrub(_OUR_VARIANT_INDEX.sub("", (our_name or "").strip()))


def _tournament_agrees(book_tournament: str, candidate: dict) -> bool:
    """Whether a book event's tournament text can refer to a candidate's tournament.

    Containment is over WHOLE TOKENS, not raw substrings. A substring test reads
    "Pau" as agreeing with "Sao Paulo" ("pau" sits inside "sao paulo"), and there
    are 381 such nesting pairs among the tournaments we have seen since 2024 —
    "Porto"/"Porto Alegre", "Paris"/"Paris Olympics", and every ATP city against
    its "M15 <city>" ITF twin. Tokens kill the accidents while keeping the real
    prefix relationships, which is what lets "Hertogenbosch" still reach our
    "'s-Hertogenbosch".

    Deliberately permissive: returns True whenever either side normalizes to
    nothing, so a book that supplies no usable tournament text is left exactly as
    it was rather than having every event rejected. It only returns False when
    both sides are legible AND neither is a token-subset of the other — which is
    the case this exists to catch (a "Sao Paulo" event resolving to Liberec).

    Separating "Porto" from "Porto Alegre" is deliberately NOT its job, for the
    same reason "Madrid 1" and "Madrid 2" both pass: choosing among tournaments
    that genuinely share a name belongs to `_match_tournament` and the date.

    That permissiveness is bounded and enumerated. Across the 1,261 tournaments
    seen since 2024, 343 name pairs still nest under tokens; 332 involve an ITF
    M##/W## event, leaving 11: Porto/Porto Alegre, Paris/Paris Olympics, the
    Buenos Aires and Guangzhou venue variants, and Santa Cruz/Santa Cruz de la
    Sierra.
    """
    book = set(normalize_book_tournament(book_tournament).split())
    ours = set(normalize_our_tournament(candidate.get("tournament_name") or "").split())
    if not book or not ours:
        return True
    return book <= ours or ours <= book


@dataclass
class MappingResult:
    """Result of mapping book events to matches."""

    event_matches: list[EventMatch] = field(default_factory=list)
    unresolved_names: set[str] = field(default_factory=set)
    no_match_found: list[tuple[str, str, str]] = field(default_factory=list)
    collisions: list[tuple[str, str, str, int]] = field(default_factory=list)
    # Resolved to a real match, then rejected because its date is too far from the
    # event's. Kept apart from no_match_found: "the pair has no match near this
    # date" is a different diagnosis from "we don't know these players".
    date_rejected: list[tuple[str, str, str, str, int]] = field(default_factory=list)
    # The pair has matches, but none in the tournament the event belongs to.
    tournament_rejected: list[tuple[str, str, str]] = field(default_factory=list)


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
                    tournament_id, year. Should also have draw_p1_id for correct
                    p1/p2 assignment. May also have tournament_name, draw_type,
                    round, and result_type.

    Returns:
        Dict mapping frozenset({player_id, opp_id}) to list of
        {match_uid, tournament_id, year, p1_id, tournament_name?, round?} dicts.
        Completed matches (result_type non-null) are excluded when result_type
        is present.
    """
    catalog: dict[frozenset, list[dict]] = {}

    required = {"match_uid", "player_id", "opp_id", "tournament_id", "year"}
    missing = required - set(matches_df.columns)
    if missing:
        raise ValueError(f"matches_df missing required columns: {missing}")

    # Filter to singles if draw_type column is available
    if "draw_type" in matches_df.columns:
        before = len(matches_df)
        matches_df = matches_df.filter(pl.col("draw_type") == "singles")
        logger.info("Match catalog: filtered to singles (%d -> %d)", before, len(matches_df))

    # Exclude completed matches (result_type is set for completed/retirement/walkover).
    # A book's live prematch event must never map to a finished match.
    if "result_type" in matches_df.columns:
        before = len(matches_df)
        matches_df = matches_df.filter(pl.col("result_type").is_null())
        logger.info(
            "Match catalog: filtered to uncompleted (%d -> %d)", before, len(matches_df),
        )

    has_name = "tournament_name" in matches_df.columns
    has_p1 = "draw_p1_id" in matches_df.columns
    has_round = "round" in matches_df.columns
    # Carried so a caller supplying an event start time can disambiguate two
    # meetings of the same pair by date. Tournament-name matching only works when
    # the book's naming aligns with ours, which for some sources it never does.
    has_date = "effective_match_date" in matches_df.columns
    optional = []
    if has_name:
        optional.append("tournament_name")
    if has_p1:
        optional.append("draw_p1_id")
    if has_round:
        optional.append("round")
    if has_date:
        optional.append("effective_match_date")
    cols = list(required) + optional

    # Deduplicate: same match_uid can appear twice (player + opp perspective)
    deduped = matches_df.select(cols).unique(subset=["match_uid"])

    for row in deduped.iter_rows(named=True):
        pair = frozenset({row["player_id"], row["opp_id"]})
        # Determine p1_id: prefer draw_p1_id, fall back to player_id
        p1_id = row.get("draw_p1_id") or row["player_id"]
        entry = {
            "match_uid": row["match_uid"],
            "tournament_id": row["tournament_id"],
            "year": row["year"],
            "p1_id": p1_id,
        }
        if has_name:
            entry["tournament_name"] = row.get("tournament_name")
        if has_round:
            entry["round"] = row.get("round")
        if has_date:
            entry["effective_match_date"] = row.get("effective_match_date")
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
    """Strip circuit prefixes and suffixes from book tournament names."""
    lower = book_tournament.strip().lower()
    for prefix in _CIRCUIT_PREFIXES:
        if lower.startswith(prefix):
            book_tournament = book_tournament[len(prefix):]
            break
    # Strip suffixes (country codes, surface, qualification)
    result = _SUFFIX_PATTERNS.sub("", book_tournament)
    return result.strip()


# Book-round classification: "main" = main draw, "qual" = qualifying round.
# Used as a defensive gate against qualifier vs main-draw ambiguity for books
# that surface round info in their tournament string (bet365 primarily; betmgm
# has the "Qualification" suffix). Books without any round signal (DK/FD/BR)
# produce None and the gate is a no-op.
_QUAL_PATTERNS = re.compile(
    r"qualification|qualifying|\bquals?\.?\b|\bq-?r\d|\bq\d\b",
    re.IGNORECASE,
)
_MAIN_PATTERNS = re.compile(
    r"\bround\s*\d|\bfinal(?:e|es|s)?\b|\bsemi(?:final)?(?:es|s)?\b"
    r"|\bquarter(?:final)?(?:es|s)?\b|1/\d+\s*final",
    re.IGNORECASE,
)


def _parse_book_round(tournament_text: str) -> str | None:
    """Classify a book's tournament string as "main" or "qual" draw, or None.

    Coarse on purpose: the draw size (and therefore the mapping from "Round 1"
    to a specific R### code) varies by tournament tier. "main" vs "qual" is
    sufficient to prevent a main-draw book event from mapping to a qualifier
    catalog entry (and vice versa), which is the ambiguity we hit in practice.
    """
    if not tournament_text:
        return None
    if _QUAL_PATTERNS.search(tournament_text):
        return "qual"
    if _MAIN_PATTERNS.search(tournament_text):
        return "main"
    return None


def _round_class(catalog_round: str | None) -> str | None:
    """Classify a catalog round code as "main" or "qual"."""
    if not catalog_round:
        return None
    return "qual" if catalog_round.upper().startswith("Q") else "main"


# How far the book's event start may sit from our match date and still count, and
# how much closer the winner must be than the runner-up. A pair meeting twice in a
# season is normally weeks apart, so this stays conservative: an unclear pick is
# left as a collision rather than guessed at.
_DATE_MATCH_MAX_DAYS = 2
_DATE_MATCH_MARGIN_DAYS = 2

# Staleness backstop for the live path (`max_date_gap_days`). Far looser than
# `_DATE_MATCH_MAX_DAYS` because that one picks between candidates while this one
# only rejects the plainly-dead.
#
# Chosen from the measured distribution of |match_date - last prematch snapshot|
# over the 18,622 name-agreeing mappings in the event map: p99 is 1 day and p99.9
# is 8. Rejecting above 10 days costs 11 of those (0.06%) — identical to what a
# 14-day threshold costs, so the extra headroom bought nothing. Tightening
# further is not free: 7 days costs 21 and 5 days costs 43, over half of them
# qualifying rounds, whose `effective_match_date` is estimated.
#
# This does NOT close back-to-back editions in one city (Buenos Aires 1 vs 2).
# Those run days apart, so no affordable threshold separates them; the variant
# index in `_match_tournament` is what handles that case.
LIVE_MAX_DATE_GAP_DAYS = 10


def _date_gap_days(fetched_at, candidate: dict) -> int | None:
    """Days between the event's date and a candidate's match date.

    None when either side has no date — which is what keeps this inert for callers
    that supply no date column at all.
    """
    if fetched_at is None:
        return None
    import datetime as _dt

    d = candidate.get("effective_match_date")
    if isinstance(d, _dt.datetime):
        d = d.date()
    if not isinstance(d, _dt.date):
        return None
    target = fetched_at.date() if hasattr(fetched_at, "date") else fetched_at
    return abs((d - target).days)


def _match_by_date(fetched_at, candidates: list[dict]) -> dict | None:
    """The candidate whose match date is nearest `fetched_at`, or None.

    Returns None unless exactly one candidate is within `_DATE_MATCH_MAX_DAYS`
    AND beats the runner-up by `_DATE_MATCH_MARGIN_DAYS` — no date on the row or
    no dates in the catalog also yields None, leaving behaviour unchanged for
    callers that don't supply one.
    """
    if fetched_at is None:
        return None
    import datetime as _dt

    target = fetched_at.date() if hasattr(fetched_at, "date") else fetched_at
    scored: list[tuple[int, dict]] = []
    for c in candidates:
        d = c.get("effective_match_date")
        if d is None:
            continue
        if isinstance(d, _dt.datetime):
            d = d.date()
        if not isinstance(d, _dt.date):
            continue
        scored.append((abs((d - target).days), c))
    if not scored:
        return None
    scored.sort(key=lambda t: t[0])
    best_days, best = scored[0]
    if best_days > _DATE_MATCH_MAX_DAYS:
        return None
    if len(scored) > 1 and scored[1][0] - best_days < _DATE_MATCH_MARGIN_DAYS:
        return None
    return best


def _match_tournament(
    book_tournament: str,
    candidates: list[dict],
) -> list[dict]:
    """Narrow candidates by matching book tournament name to our tournament data.

    Uses substring matching with accent normalization of the stripped book
    tournament name against our tournament_name field.
    """
    stripped = normalize_tournament(_strip_circuit_prefix(book_tournament))
    if not stripped:
        return candidates

    matched = []
    for c in candidates:
        our_name = normalize_tournament(c.get("tournament_name") or "")
        # Check if the book's stripped name appears in our tournament name or vice versa
        if stripped in our_name or our_name in stripped:
            matched.append(c)

    # If multiple substring matches, try treating bare name as the " 1" variant
    # (books often omit the "1" suffix for the first of back-to-back tournaments)
    if len(matched) > 1:
        exact_or_first = [
            c for c in matched
            if normalize_tournament(c.get("tournament_name") or "") in (stripped, f"{stripped} 1")
        ]
        if exact_or_first:
            matched = exact_or_first

    return matched if matched else candidates


def map_book_events(
    staged_odds: pl.DataFrame,
    event_id_col: str,
    book: str,
    player_lookup: dict[str, str],
    match_catalog: dict[frozenset, list[dict]],
    existing_event_ids: set[str] | None = None,
    max_date_gap_days: int | None = None,
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
        max_date_gap_days: Reject a resolved match sitting further than this from
            the event's own timestamp. Requires `effective_match_date` in the
            catalog; inert without it. None (default) disables the check, keeping
            behaviour unchanged for callers whose dates aren't comparable.

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

        # Filter candidates to the year the odds were fetched
        fetched_at = rows[0].get("fetched_at")
        if fetched_at is not None and len(candidates) > 1:
            odds_year = fetched_at.year
            year_filtered = [c for c in candidates if c["year"] == odds_year]
            if year_filtered:
                candidates = year_filtered

        # Tournament identity, where the caller resolved it. This is the constraint
        # the date was standing in for: `_match_tournament` below compares strings,
        # and for oddspapi ZERO of 184 tournament names match one of ours, so it
        # no-ops and leaves every meeting of the pair in play. A caller that can map
        # its own tournament text to our ids passes them here instead.
        #
        # A set rather than one id, because a city can host two same-circuit events
        # that neither side numbers consistently. Narrowing to that city's events
        # still leaves the date choosing between two candidates rather than all.
        #
        # No-op for callers that don't supply the column — the live scrapers don't.
        tournament_ids = rows[0].get("tournament_ids")
        if tournament_ids:
            wanted = {str(t) for t in tournament_ids}
            in_tournament = [
                c for c in candidates if str(c.get("tournament_id")) in wanted
            ]
            if not in_tournament:
                result.tournament_rejected.append((eid, name_a, name_b))
                skipped_no_match += 1
                continue
            candidates = in_tournament

        # Round gate: if the book's tournament text surfaces a round class
        # (main draw vs qualifier), drop catalog candidates whose round class
        # disagrees. No-op for books without round info in their tournament
        # string. Applied before single-vs-multi candidate branching so both
        # paths benefit.
        book_tournament = rows[0].get("tournament", "")
        book_round = _parse_book_round(book_tournament)
        if book_round is not None:
            round_filtered = [
                c for c in candidates
                if _round_class(c.get("round")) in (None, book_round)
            ]
            if not round_filtered:
                result.no_match_found.append((eid, name_a, name_b))
                skipped_no_match += 1
                continue
            candidates = round_filtered

        # Tournament-name gate. `_match_tournament` below narrows but cannot
        # reject — it returns every candidate when none matches, so a total
        # disagreement and "this book's naming never aligns with ours" are
        # indistinguishable to it. That made the pair the only real key: with one
        # candidate the `else` branch never ran, and a Sao Paulo event took the
        # pair's only open match, four months and a continent away.
        #
        # Applied before the single-vs-multi branch, like the round gate, because
        # the single-candidate path is exactly where nothing else checks. Kept
        # permissive on purpose (see `_tournament_agrees`) — its job is rejecting
        # events that plainly belong to another tournament, not picking between
        # Madrid 1 and Madrid 2, which is still `_match_tournament`'s.
        #
        # Skipped when the caller resolved `tournament_ids` above: an id the caller
        # mapped itself beats our string comparison, and for oddspapi the strings
        # never align anyway. This is the fallback for callers without ids — which
        # is the live scrapers, and they are the ones that needed it.
        if not tournament_ids:
            name_filtered = [
                c for c in candidates if _tournament_agrees(book_tournament, c)
            ]
            if not name_filtered:
                result.tournament_rejected.append((eid, name_a, name_b))
                skipped_no_match += 1
                continue
            candidates = name_filtered

        if len(candidates) == 1:
            match = candidates[0]
        else:
            # Multiple candidates — try tournament disambiguation
            narrowed = _match_tournament(book_tournament, candidates)

            if len(narrowed) == 1:
                match = narrowed[0]
            else:
                # Fall back to the event start date. Tournament-name matching
                # only narrows when the book's naming aligns with ours; where it
                # does not, the string comparison silently no-ops and returns the
                # candidates unchanged. Two meetings of one pair in a season are
                # normally weeks apart, so the date separates them cleanly.
                by_date = _match_by_date(fetched_at, narrowed or candidates)
                if by_date is not None:
                    match = by_date
                else:
                    result.collisions.append((
                        eid, name_a, name_b, len(narrowed or candidates),
                    ))
                    skipped_ambiguous += 1
                    continue

        # SEASON is a constraint, not just a tiebreak. The year filter above only
        # narrows when there are two or more candidates, and keeps them all when
        # none matches the year — so a pair that met once, in a prior season,
        # resolved to that match unchecked. Measured on the oddspapi backfill: 33
        # of 8,946 fixtures took 2026 prices onto 2018-2025 matches, which in a
        # backtest settles a bet against a different match entirely.
        #
        # The date is only the escape hatch, for tournaments running across New
        # Year: those carry the prior season's `year` while being a day or two away.
        # It is NOT the constraint itself — `effective_match_date` is estimated for
        # rounds whose real date we don't have, and gating on a 2-day tolerance
        # rejected 210 fixtures instead of 33, most of them same-season qualifying
        # matches sitting 3 days off their estimate.
        #
        # No-op unless the caller supplies a date: the live scraper path passes no
        # date column, so `fetched_at` is None and this cannot fire.
        cand_year = match.get("year")
        if fetched_at is not None and cand_year is not None and (
            cand_year != fetched_at.year
        ):
            gap = _date_gap_days(fetched_at, match)
            if gap is None or gap > _DATE_MATCH_MAX_DAYS:
                result.date_rejected.append(
                    (eid, name_a, name_b, match.get("match_uid", ""),
                     gap if gap is not None else -1)
                )
                skipped_no_match += 1
                continue

        # Same-season staleness. The season constraint above cannot see it: a
        # March event and a July match share a year, so a book event that went
        # dark months ago still resolved to the pair's next open match. The
        # tolerance is deliberately loose — `effective_match_date` is estimated
        # for rounds whose real date we don't have, and a tight window rejects
        # same-season qualifying matches sitting days off their estimate. This is
        # a staleness backstop, not a disambiguator.
        if max_date_gap_days is not None:
            gap = _date_gap_days(fetched_at, match)
            if gap is not None and gap > max_date_gap_days:
                result.date_rejected.append(
                    (eid, name_a, name_b, match.get("match_uid", ""), gap)
                )
                skipped_no_match += 1
                continue

        # Assign p1/p2 book names and player IDs
        match_p1_id = match.get("p1_id")
        if match_p1_id == pid_a:
            p1_book_name, p2_book_name = name_a, name_b
            p1_id, p2_id = pid_a, pid_b
        else:
            p1_book_name, p2_book_name = name_b, name_a
            p1_id, p2_id = pid_b, pid_a
        result.event_matches.append(EventMatch(
            match_uid=match["match_uid"],
            event_id=eid,
            p1_book_name=p1_book_name,
            p2_book_name=p2_book_name,
            p1_id=p1_id,
            p2_id=p2_id,
            # The book's own order, before the swap above. Which branch that swap
            # took IS "is the book's first participant our p1?", and discarding it
            # left positionally-sided odds unresolvable — see EventMatch's docstring.
            # Additive: `save_event_mappings` names its columns, so event_map.parquet
            # is unchanged.
            participant1_id=pid_a,
            participant2_id=pid_b,
            participant1_name=name_a,
            participant2_name=name_b,
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
