"""Shared odds matching utilities used across all book integrations."""

import logging
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

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


class BaseOddsMatcher(BaseJob):
    """Looks up odds for predictions using the persisted event map.

    The event_mapper module handles mapping book events to match_uids.
    This class simply reads the event map and extracts odds for predictions.

    Subclasses must set:
        event_id_column: column name for the book's event ID
        book_label: short label for log messages (e.g. "DK", "BR", "MGM")
    """

    event_id_column: str
    book_label: str

    def __init__(self, domain: str, data_root: Path | None = None):
        super().__init__(domain=domain, data_root=data_root)
        self._logger = logging.getLogger(f"mvp.{domain}.matcher")

    def get_latest_odds(self) -> pl.DataFrame:
        """Read odds from the most recent run only.

        Filters to run_at == max(run_at) so only events from the latest
        pipeline run are included. Falls back to fetched_at if run_at
        column doesn't exist yet (old data).
        """
        odds_path = self.build_path("stage", "moneyline.parquet")
        if not odds_path.exists():
            return pl.DataFrame()

        df = pl.read_parquet(odds_path)
        if len(df) == 0:
            return df

        # Filter to most recent run
        ts_col = "run_at" if "run_at" in df.columns else "fetched_at"
        max_run = df[ts_col].max()
        df = df.filter(pl.col(ts_col) == max_run)

        if "event_status" in df.columns:
            df = df.filter(pl.col("event_status") == "NOT_STARTED")

        return df

    def match(self, predictions: pl.DataFrame) -> OddsMatchResult:
        """Look up odds for predictions using the event map.

        Reads the persisted event map to resolve event_ids to match_uids,
        then uses p1_book_name/p2_book_name to assign odds to the correct side.

        Args:
            predictions: DataFrame with p1_id, p2_id, match_uid.

        Returns:
            OddsMatchResult with odds map keyed by match_uid.
        """
        odds_df = self.get_latest_odds()
        if len(odds_df) == 0 or len(predictions) == 0:
            return OddsMatchResult()

        # Load event map for this book: event_id -> {match_uid, p1_book_name, p2_book_name}
        from mvp.analysis.event_map import load_event_map_with_overrides

        event_map_df = load_event_map_with_overrides()
        book_key = self.book_label.lower()
        book_map = event_map_df.filter(pl.col("book") == book_key)

        emap: dict[str, dict] = {}
        for row in book_map.iter_rows(named=True):
            emap[row["event_id"]] = {
                "match_uid": row["match_uid"],
                "p1_book_name": row["p1_book_name"],
                "p2_book_name": row["p2_book_name"],
            }

        # Build prediction lookup by match_uid
        pred_by_uid: dict[str, dict] = {}
        for row in predictions.iter_rows(named=True):
            uid = row.get("match_uid") or ""
            if uid:
                pred_by_uid[uid] = row

        # Group odds by event
        book_events: dict[str, list[dict]] = {}
        for row in odds_df.iter_rows(named=True):
            book_events.setdefault(row[self.event_id_column], []).append(row)

        result: dict[str, dict[str, float]] = {}
        matched = 0

        for eid, book_rows in book_events.items():
            if len(book_rows) < 2:
                continue

            mapping = emap.get(eid)
            if mapping is None:
                continue

            pred = pred_by_uid.get(mapping["match_uid"])
            if pred is None:
                continue

            p1_id = pred["p1_id"]
            p2_id = pred["p2_id"]

            odds_by_pid: dict[str, float] = {}
            for book_row in book_rows[:2]:
                name = book_row["player_name"]
                if name == mapping["p1_book_name"]:
                    odds_by_pid[p1_id] = book_row["odds"]
                elif name == mapping["p2_book_name"]:
                    odds_by_pid[p2_id] = book_row["odds"]

            if p1_id in odds_by_pid and p2_id in odds_by_pid:
                result[mapping["match_uid"]] = odds_by_pid
                matched += 1

        self._logger.info(
            "Odds lookup: %d %s events matched to %d predictions",
            matched, self.book_label, len(predictions),
        )

        return OddsMatchResult(odds=result)
