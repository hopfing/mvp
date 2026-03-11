"""Match DraftKings odds to predictions by player-pair matching."""

import logging
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from mvp.common.base_job import BaseJob

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
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    stripped = stripped.replace("-", " ")
    return " ".join(stripped.lower().split())


def normalize_tournament(tournament: str) -> str:
    """Normalize a tournament name, stripping DK circuit prefixes."""
    lower = tournament.strip().lower()
    for prefix in _TOURNAMENT_PREFIXES:
        if lower.startswith(prefix):
            tournament = tournament[len(prefix):]
            break
    return normalize_name(tournament)


@dataclass
class EventMatch:
    """Record of a successful match between a book event and a prediction."""

    match_uid: str
    event_id: str
    p1_book_name: str
    p2_book_name: str


@dataclass
class OddsMatchResult:
    """Result of matching DK odds to predictions."""

    odds: dict[str, dict[str, float]] = field(default_factory=dict)
    unmatched_names: set[str] = field(default_factory=set)
    event_matches: list[EventMatch] = field(default_factory=list)


class DraftKingsOddsMatcher(BaseJob):
    """Matches DK odds to predictions using player name resolution."""

    ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="draftkings", data_root=data_root)
        self._name_to_id: dict[str, str] | None = None
        self._aliases: dict[str, str] | None = None

    def _load_players(self) -> dict[str, str]:
        """Build normalized name -> player_id from players.parquet."""
        if self._name_to_id is not None:
            return self._name_to_id

        self._name_to_id = {}
        players_path = self.data_root / "stage" / "atptour" / "players.parquet"
        if players_path.exists():
            players = pl.read_parquet(
                players_path, columns=["player_id", "first_name", "last_name"]
            )
            for row in players.iter_rows(named=True):
                first = row.get("first_name") or ""
                last = row.get("last_name") or ""
                pid = row.get("player_id") or ""
                if first and last and pid:
                    self._name_to_id[normalize_name(f"{first} {last}")] = pid
        return self._name_to_id

    def _load_aliases(self) -> dict[str, str]:
        """Load alias YAML (normalized DK name -> player_id)."""
        if self._aliases is not None:
            return self._aliases

        raw: dict[str, str] = {}
        if self.ALIASES_PATH.exists():
            with open(self.ALIASES_PATH) as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                raw = data

        self._aliases = {
            normalize_name(dk_name): our_id.upper().strip()
            for dk_name, our_id in raw.items()
        }
        return self._aliases

    def _resolve_id(self, dk_name: str) -> str | None:
        """Resolve a DK player name to our player_id."""
        normed = normalize_name(dk_name)
        aliases = self._load_aliases()
        if normed in aliases:
            return aliases[normed]
        return self._load_players().get(normed)

    def get_latest_odds(self) -> pl.DataFrame:
        """Read DK odds parquet, deduplicated to latest per event+player."""
        odds_path = self.build_path("stage", "moneyline.parquet")
        if not odds_path.exists():
            return pl.DataFrame()

        df = pl.read_parquet(odds_path)
        if len(df) == 0:
            return df

        return (
            df.sort("fetched_at")
            .group_by(["dk_event_id", "player_name"])
            .last()
        )

    def match(self, predictions: pl.DataFrame) -> OddsMatchResult:
        """Match DK odds to predictions by player pair.

        Args:
            predictions: DataFrame with p1_id, p2_id, p1_name, p2_name, match_uid.

        Returns:
            OddsMatchResult with odds map and unmatched DK names.
        """
        odds_df = self.get_latest_odds()
        if len(odds_df) == 0 or len(predictions) == 0:
            return OddsMatchResult()

        # Group DK odds by event (two rows per event = one match)
        dk_events: dict[str, list[dict]] = {}
        for row in odds_df.iter_rows(named=True):
            dk_events.setdefault(row["dk_event_id"], []).append(row)

        # Build prediction lookup: frozenset({p1_id, p2_id}) -> prediction row
        pred_by_pair: dict[frozenset, dict] = {}
        for row in predictions.iter_rows(named=True):
            p1_id = row.get("p1_id") or ""
            p2_id = row.get("p2_id") or ""
            if p1_id and p2_id:
                pred_by_pair[frozenset({p1_id, p2_id})] = row

        result: dict[str, dict[str, float]] = {}
        unmatched_names: set[str] = set()
        event_matches: list[EventMatch] = []
        matched = 0

        for eid, dk_rows in dk_events.items():
            if len(dk_rows) < 2:
                continue

            ids_and_odds: list[tuple[str, float]] = []
            for dk_row in dk_rows[:2]:
                pid = self._resolve_id(dk_row["player_name"])
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

            result[pred["match_uid"]] = {
                ids_and_odds[0][0]: ids_and_odds[0][1],
                ids_and_odds[1][0]: ids_and_odds[1][1],
            }
            matched += 1

            p1_id = pred["p1_id"]
            book_names = {pid: dk_rows[i]["player_name"] for i, (pid, _) in enumerate(ids_and_odds)}
            event_matches.append(EventMatch(
                match_uid=pred["match_uid"],
                event_id=eid,
                p1_book_name=book_names.get(p1_id, ""),
                p2_book_name=book_names.get(pred["p2_id"], ""),
            ))

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

        return OddsMatchResult(odds=result, unmatched_names=unmatched_names, event_matches=event_matches)
