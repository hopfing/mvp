"""Shared odds matching utilities used across all book integrations."""

import logging
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.common.base_job import BaseJob


def _strip_accents(text: str) -> str:
    """Strip accents via NFKD decomposition."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def normalize_name(name: str) -> str:
    """Normalize a player name for fuzzy matching.

    Strips accents (NFKD decomposition), removes hyphens,
    collapses whitespace, lowercases.
    """
    stripped = _strip_accents(name)
    stripped = stripped.replace("-", " ")
    return " ".join(stripped.lower().split())


def normalize_tournament(name: str) -> str:
    """Normalize a tournament name for matching.

    Strips accents, replaces hyphens with spaces, collapses whitespace, lowercases.
    """
    result = _strip_accents(name).replace("-", " ")
    return " ".join(result.lower().split())


@dataclass
class EventMatch:
    """Record of a successful match between a book event and a prediction.

    `p1_*`/`p2_*` are OUR convention: the mapper reorders the book's two participants
    to match the match record's p1. `participant1_*`/`participant2_*` are the book's
    own order, as it listed them.

    Both are kept because they answer different questions. A book that names its
    players per row (the scrapers) resolves sides by name and wants ours. A book that
    sides positionally — oddspapi quotes outcome "1" and "2" with no name attached —
    can only resolve a side against the book's own order, and reordering to ours
    silently reattributes it. That happens on 85 of 8,881 oddspapi fixtures.

    Defaults are empty so callers constructing these without the book's order (there
    are none today) stay valid rather than raising.
    """

    match_uid: str
    event_id: str
    p1_book_name: str
    p2_book_name: str
    p1_id: str = ""
    p2_id: str = ""
    participant1_id: str = ""
    participant2_id: str = ""
    participant1_name: str = ""
    participant2_name: str = ""


@dataclass
class OddsMatchResult:
    """Result of matching book odds to predictions."""

    odds: dict[str, dict[str, float]] = field(default_factory=dict)


def _naive(stamp: datetime) -> datetime:
    """Drop tzinfo so stamps staged with and without a zone compare equal."""
    return stamp.replace(tzinfo=None) if stamp.tzinfo is not None else stamp


def latest_run_anchor(matchers: Iterable["BaseOddsMatcher"]) -> datetime | None:
    """The newest run stamp any book staged: the shared stamp of the latest
    tick that produced odds. None when no book has staged rows."""
    stamps = [s for s in (m.latest_run_at() for m in matchers) if s is not None]
    return max(stamps) if stamps else None


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

    def _read_staged_moneyline(self) -> pl.DataFrame:
        odds_path = self.build_path("stage", "moneyline.parquet")
        if not odds_path.exists():
            return pl.DataFrame()
        return pl.read_parquet(odds_path)

    @staticmethod
    def _run_stamp_col(df: pl.DataFrame) -> str:
        """run_at when staged; fetched_at for data that predates the column."""
        return "run_at" if "run_at" in df.columns else "fetched_at"

    def latest_run_at(self) -> datetime | None:
        """This book's newest staged run stamp (tz stripped); None with no rows."""
        df = self._read_staged_moneyline()
        if len(df) == 0:
            return None
        return _naive(df[self._run_stamp_col(df)].max())

    def get_latest_odds(self, anchor: datetime | None = None) -> pl.DataFrame:
        """Read odds from one run only.

        ``anchor`` is the run stamp every book scraped in a pipeline tick
        shares (the books job stamps all scrapers with one ``run_at``). Given
        one, only rows stamped exactly ``anchor`` survive, so a book whose
        fetch failed this tick contributes nothing instead of presenting its
        last good run as the current price. Without an anchor the book's own
        newest run is used. Falls back to fetched_at if the run_at column
        doesn't exist yet (old data).
        """
        df = self._read_staged_moneyline()
        if len(df) == 0:
            return df

        ts_col = self._run_stamp_col(df)
        stamp = pl.col(ts_col)
        if getattr(df.schema[ts_col], "time_zone", None):
            stamp = stamp.dt.replace_time_zone(None)
        target = _naive(anchor if anchor is not None else df[ts_col].max())
        df = df.filter(stamp == target)

        if "event_status" in df.columns:
            df = df.filter(pl.col("event_status") == "NOT_STARTED")

        return df

    def get_opening_odds(self) -> pl.DataFrame:
        """Read the first NOT_STARTED snapshot per (event, player) across all
        runs in the staged moneyline parquet. Mirrors the analysis layer's
        opening-odds derivation but operates on whatever staged data is
        currently available to the live pipeline.
        """
        odds_path = self.build_path("stage", "moneyline.parquet")
        if not odds_path.exists():
            return pl.DataFrame()

        df = pl.read_parquet(odds_path)
        if len(df) == 0:
            return df

        if "event_status" in df.columns:
            df = df.filter(pl.col("event_status") == "NOT_STARTED")
        if len(df) == 0:
            return df

        return (
            df.sort("fetched_at")
            .group_by([self.event_id_column, "player_name"], maintain_order=True)
            .head(1)
        )

    def match(
        self, predictions: pl.DataFrame, anchor: datetime | None = None,
    ) -> OddsMatchResult:
        """Look up pre-match odds from the anchor run (or this book's latest)."""
        return self._match_from_odds(
            predictions, self.get_latest_odds(anchor), label="latest",
        )

    def match_opening(self, predictions: pl.DataFrame) -> OddsMatchResult:
        """Look up opening (first NOT_STARTED) odds for predictions."""
        return self._match_from_odds(predictions, self.get_opening_odds(), label="opening")

    def _match_from_odds(
        self,
        predictions: pl.DataFrame,
        odds_df: pl.DataFrame,
        label: str = "latest",
    ) -> OddsMatchResult:
        """Shared event-map lookup: assign book odds to predictions by side.

        Args:
            predictions: DataFrame with p1_id, p2_id, match_uid.
            odds_df: Book-staged moneyline rows to project (latest or opening).
            label: Tag for log line.

        Returns:
            OddsMatchResult with odds map keyed by match_uid.
        """
        if len(odds_df) == 0 or len(predictions) == 0:
            return OddsMatchResult()

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

        pred_by_uid: dict[str, dict] = {}
        for row in predictions.iter_rows(named=True):
            uid = row.get("match_uid") or ""
            if uid:
                pred_by_uid[uid] = row

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
            "Odds lookup (%s): %d %s events matched to %d predictions",
            label, matched, self.book_label, len(predictions),
        )

        return OddsMatchResult(odds=result)
