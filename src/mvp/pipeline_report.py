"""Pipeline health report builder.

Collects metrics from each pipeline stage and persists as JSONL.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

_TICK_MINUTES = 15


def _tick_id(when: datetime) -> str:
    """Stable id shared by the main and books rows of one cron tick — the
    start time floored to the 15-min cadence (e.g. '2026-06-08T07:15:00').
    Lets the dashboard join a tick's two rows."""
    floored = when.replace(
        minute=(when.minute // _TICK_MINUTES) * _TICK_MINUTES,
        second=0,
        microsecond=0,
    )
    return floored.isoformat()


class PipelineReport:
    """Accumulates pipeline health metrics and writes to JSONL."""

    def __init__(self, job: str = "main") -> None:
        # ``job`` distinguishes the split live processes: "main" (ATP fetch +
        # predict + publish, off-VPN) vs "books" (odds scraping, on-VPN). Both
        # append to the same runs.jsonl with a shared ``tick_id`` (start time
        # floored to the 15-min cadence) so the dashboard can join a tick's two
        # rows; the Health views key off "main".
        now = datetime.now()
        self.data: dict = {
            "timestamp": now.isoformat(),
            "tick_id": _tick_id(now),
            "job": job,
            "tournaments_processed": 0,
            "tournaments_failed": 0,
            "tournament_failures": [],
            "books_fetched": {},
            "unresolved_names": {},
            "predictions_total": 0,
            "predictions_without_odds": [],
            "sheets_sync": {"success": False, "count": 0, "error": None},
            "books_stale": False,
            "errors": [],
        }

    def record_tournaments(
        self,
        processed: int,
        failed: list[tuple[str, int, str]],
    ) -> None:
        self.data["tournaments_processed"] = processed
        self.data["tournaments_failed"] = len(failed)
        self.data["tournament_failures"] = [
            {"name": tid, "year": year, "error": error}
            for tid, year, error in failed
        ]

    def record_book_fetched(self, book: str, count: int) -> None:
        self.data["books_fetched"][book] = count

    def record_unresolved_names(self, book: str, names: set[str]) -> None:
        self.data["unresolved_names"][book] = sorted(names)

    def record_predictions(self, total: int) -> None:
        self.data["predictions_total"] = total

    def record_predictions_without_odds(
        self, items: list[dict[str, str]]
    ) -> None:
        self.data["predictions_without_odds"] = items

    def record_sheets_sync(
        self,
        success: bool,
        count: int,
        error: str | None = None,
    ) -> None:
        self.data["sheets_sync"] = {
            "success": success, "count": count, "error": error
        }

    def record_books_stale(self, stale: bool = True) -> None:
        """Main job: odds were possibly stale because the books job hadn't
        finished staging fresh odds before the mapping/matching stages."""
        self.data["books_stale"] = stale

    def set_errors(self, errors: list[str]) -> None:
        self.data["errors"] = list(errors)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(self.data) + "\n")
