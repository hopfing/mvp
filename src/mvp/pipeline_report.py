"""Pipeline health report builder.

Collects metrics from each pipeline stage and persists as JSONL.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


class PipelineReport:
    """Accumulates pipeline health metrics and writes to JSONL."""

    def __init__(self) -> None:
        self.data: dict = {
            "timestamp": datetime.now().isoformat(),
            "tournaments_processed": 0,
            "tournaments_failed": 0,
            "tournament_failures": [],
            "books_fetched": {},
            "unresolved_names": {},
            "predictions_total": 0,
            "predictions_without_odds": [],
            "sheets_sync": {"success": False, "count": 0, "error": None},
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

    def set_errors(self, errors: list[str]) -> None:
        self.data["errors"] = list(errors)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(self.data) + "\n")
