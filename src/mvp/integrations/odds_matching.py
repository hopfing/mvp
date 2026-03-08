"""Match DraftKings odds to predictions by player-pair matching."""

import logging
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

logger = logging.getLogger(__name__)

_TOURNAMENT_PREFIXES = [
    "challenger quals. - ",
    "challenger quals - ",
    "challenger - ",
    "atp - ",
    "wta - ",
]


def normalize_name(name: str) -> str:
    """Normalize a player name for fuzzy matching.

    Strips accents (NFKD decomposition), removes hyphens,
    collapses whitespace, lowercases.
    """
    # NFKD decomposition strips accents
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    # Remove hyphens, collapse whitespace, lowercase
    stripped = stripped.replace("-", " ")
    return " ".join(stripped.lower().split())


def normalize_tournament(tournament: str) -> str:
    """Normalize a tournament name, stripping DK circuit prefixes."""
    # Strip prefixes before normalizing (hyphens in prefixes matter)
    lower = tournament.strip().lower()
    for prefix in _TOURNAMENT_PREFIXES:
        if lower.startswith(prefix):
            tournament = tournament[len(prefix):]
            break
    return normalize_name(tournament)


def load_aliases(path: Path) -> dict[str, str]:
    """Load player alias YAML mapping DK names to our player IDs.

    Returns empty dict if file is missing or empty.
    """
    if not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def get_latest_odds(odds_path: Path) -> pl.DataFrame:
    """Read DK odds parquet and deduplicate to latest snapshot per event+player."""
    if not odds_path.exists():
        return pl.DataFrame()

    df = pl.read_parquet(odds_path)
    if len(df) == 0:
        return df

    # Keep only the latest fetched_at per (dk_event_id, player_name)
    return (
        df.sort("fetched_at")
        .group_by(["dk_event_id", "player_name"])
        .last()
    )


@dataclass
class OddsMatchResult:
    """Result of matching DK odds to predictions."""

    odds: dict[str, dict[str, float]] = field(default_factory=dict)
    unmatched_names: set[str] = field(default_factory=set)


def match_odds_to_predictions(
    odds_df: pl.DataFrame,
    predictions: pl.DataFrame,
    aliases: dict[str, str],
) -> OddsMatchResult:
    """Match DK odds to predictions by player pair.

    Args:
        odds_df: Deduplicated DK odds (from get_latest_odds).
        predictions: DataFrame from predictor.predict().
        aliases: DK name -> our player_id mapping.

    Returns:
        OddsMatchResult with odds map and unmatched DK names.
    """
    if len(odds_df) == 0 or len(predictions) == 0:
        return OddsMatchResult()

    # Build normalized name -> player_id lookup from predictions
    name_to_id: dict[str, str] = {}
    for row in predictions.iter_rows(named=True):
        p1_name = row.get("p1_name") or ""
        p2_name = row.get("p2_name") or ""
        p1_id = row.get("p1_id") or ""
        p2_id = row.get("p2_id") or ""
        if p1_name and p1_id:
            name_to_id[normalize_name(p1_name)] = p1_id
        if p2_name and p2_id:
            name_to_id[normalize_name(p2_name)] = p2_id

    # Build alias lookup: normalized DK name -> player_id
    alias_norm: dict[str, str] = {}
    for dk_name, our_id in aliases.items():
        alias_norm[normalize_name(dk_name)] = our_id.upper().strip()

    def resolve_id(dk_name: str) -> str | None:
        """Resolve a DK player name to our player_id."""
        normed = normalize_name(dk_name)
        # Alias takes priority
        if normed in alias_norm:
            return alias_norm[normed]
        return name_to_id.get(normed)

    # Group DK odds by event (two rows per event = one match)
    dk_events: dict[str, list[dict]] = {}
    for row in odds_df.iter_rows(named=True):
        eid = row["dk_event_id"]
        dk_events.setdefault(eid, []).append(row)

    # Build prediction lookup: frozenset({p1_id, p2_id}) -> prediction row
    pred_by_pair: dict[frozenset, dict] = {}
    for row in predictions.iter_rows(named=True):
        p1_id = row.get("p1_id") or ""
        p2_id = row.get("p2_id") or ""
        if p1_id and p2_id:
            pred_by_pair[frozenset({p1_id, p2_id})] = row

    result: dict[str, dict[str, float]] = {}
    unmatched_names: set[str] = set()
    matched = 0

    for eid, dk_rows in dk_events.items():
        if len(dk_rows) < 2:
            continue

        # Resolve both DK player names to our IDs
        ids_and_odds: list[tuple[str, float]] = []
        for dk_row in dk_rows[:2]:
            pid = resolve_id(dk_row["player_name"])
            if pid is None:
                unmatched_names.add(dk_row["player_name"])
            else:
                ids_and_odds.append((pid, dk_row["odds"]))

        if len(ids_and_odds) != 2:
            continue

        pair = frozenset({ids_and_odds[0][0], ids_and_odds[1][0]})
        pred = pred_by_pair.get(pair)
        if pred is None:
            continue

        match_uid = pred["match_uid"]
        result[match_uid] = {
            ids_and_odds[0][0]: ids_and_odds[0][1],
            ids_and_odds[1][0]: ids_and_odds[1][1],
        }
        matched += 1

    total_events = len(dk_events)
    total_preds = len(predictions)
    logger.info(
        "Odds matching: %d/%d DK events matched to %d predictions",
        matched, total_events, total_preds,
    )
    if unmatched_names:
        logger.info(
            "Unmatched DK names (%d): %s",
            len(unmatched_names),
            ", ".join(sorted(unmatched_names)),
        )

    return OddsMatchResult(odds=result, unmatched_names=unmatched_names)
