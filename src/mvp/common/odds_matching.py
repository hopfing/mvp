"""Shared odds matching utilities used across all book integrations."""

import logging
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml

from mvp.common.base_job import BaseJob


def normalize_name(name: str) -> str:
    """Normalize a player name for fuzzy matching.

    Strips accents (NFKD decomposition), removes hyphens,
    collapses whitespace, lowercases.
    """
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    stripped = stripped.replace("-", " ")
    return " ".join(stripped.lower().split())


@dataclass
class EventMatch:
    """Record of a successful match between a book event and a prediction."""

    match_uid: str
    event_id: str
    p1_book_name: str
    p2_book_name: str


@dataclass
class OddsMatchResult:
    """Result of matching book odds to predictions."""

    odds: dict[str, dict[str, float]] = field(default_factory=dict)
    unmatched_names: set[str] = field(default_factory=set)
    event_matches: list[EventMatch] = field(default_factory=list)


class BaseOddsMatcher(BaseJob):
    """Base class for book-specific odds matchers.

    Subclasses must set:
        event_id_column: column name for the book's event ID
        book_label: short label for log messages (e.g. "DK", "BR", "MGM")
        ALIASES_PATH: path to the per-book player_aliases.yaml
    """

    event_id_column: str
    book_label: str
    ALIASES_PATH: Path

    def __init__(self, domain: str, data_root: Path | None = None):
        super().__init__(domain=domain, data_root=data_root)
        self._aliases: dict[str, str] | None = None
        self._logger = logging.getLogger(f"mvp.{domain}.matcher")

    def _load_aliases(self) -> dict[str, str]:
        """Load alias YAML (normalized book name -> player_id)."""
        if self._aliases is not None:
            return self._aliases

        raw: dict[str, str] = {}
        if self.ALIASES_PATH.exists():
            with open(self.ALIASES_PATH) as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                raw = data

        self._aliases = {
            normalize_name(name): our_id.upper().strip()
            for name, our_id in raw.items()
        }
        return self._aliases

    def _resolve_id(self, name: str, pred_names: dict[str, str]) -> str | None:
        """Resolve a book player name to our player_id."""
        normed = normalize_name(name)
        aliases = self._load_aliases()
        if normed in aliases:
            return aliases[normed]
        return pred_names.get(normed)

    def get_latest_odds(self) -> pl.DataFrame:
        """Read odds parquet, deduplicated to latest per event+player."""
        odds_path = self.build_path("stage", "moneyline.parquet")
        if not odds_path.exists():
            return pl.DataFrame()

        df = pl.read_parquet(odds_path)
        if len(df) == 0:
            return df

        if "event_status" in df.columns:
            df = df.filter(pl.col("event_status") != "STARTED")

        return (
            df.sort("fetched_at")
            .group_by([self.event_id_column, "player_name"])
            .last()
        )

    def match(self, predictions: pl.DataFrame) -> OddsMatchResult:
        """Match book odds to predictions by player pair.

        Args:
            predictions: DataFrame with p1_id, p2_id, p1_name, p2_name, match_uid.

        Returns:
            OddsMatchResult with odds map and unmatched book names.
        """
        odds_df = self.get_latest_odds()
        if len(odds_df) == 0 or len(predictions) == 0:
            return OddsMatchResult()

        # Group odds by event (two rows per event = one match)
        book_events: dict[str, list[dict]] = {}
        for row in odds_df.iter_rows(named=True):
            book_events.setdefault(row[self.event_id_column], []).append(row)

        # Build prediction lookup: frozenset({p1_id, p2_id}) -> prediction row
        # Also build name -> id scoped to today's predictions only
        pred_by_pair: dict[frozenset, dict] = {}
        pred_names: dict[str, str] = {}
        for row in predictions.iter_rows(named=True):
            p1_id = row.get("p1_id") or ""
            p2_id = row.get("p2_id") or ""
            if p1_id and p2_id:
                pred_by_pair[frozenset({p1_id, p2_id})] = row
            for name_col, id_col in [("p1_name", "p1_id"), ("p2_name", "p2_id")]:
                name = row.get(name_col) or ""
                pid = row.get(id_col) or ""
                if name and pid:
                    normed = normalize_name(name)
                    existing = pred_names.get(normed)
                    if existing is not None and existing != pid:
                        self._logger.warning(
                            "Player name collision: '%s' -> %s and %s (keeping %s)",
                            normed, existing, pid, pid,
                        )
                    pred_names[normed] = pid

        result: dict[str, dict[str, float]] = {}
        unmatched_names: set[str] = set()
        event_matches: list[EventMatch] = []
        matched = 0

        for eid, book_rows in book_events.items():
            if len(book_rows) < 2:
                continue

            ids_and_odds: list[tuple[str, float]] = []
            for book_row in book_rows[:2]:
                pid = self._resolve_id(book_row["player_name"], pred_names)
                if pid is None:
                    unmatched_names.add(book_row["player_name"])
                else:
                    ids_and_odds.append((pid, book_row["odds"]))

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
            book_names = {
                pid: book_rows[i]["player_name"]
                for i, (pid, _) in enumerate(ids_and_odds)
            }
            event_matches.append(EventMatch(
                match_uid=pred["match_uid"],
                event_id=eid,
                p1_book_name=book_names.get(p1_id, ""),
                p2_book_name=book_names.get(pred["p2_id"], ""),
            ))

        total_events = len(book_events)
        total_preds = len(predictions)
        self._logger.info(
            "Odds matching: %d/%d %s events matched to %d predictions",
            matched, total_events, self.book_label, total_preds,
        )
        if unmatched_names:
            self._logger.info(
                "Unmatched %s names (%d): %s",
                self.book_label,
                len(unmatched_names),
                ", ".join(sorted(unmatched_names)),
            )

        return OddsMatchResult(
            odds=result, unmatched_names=unmatched_names, event_matches=event_matches,
        )
