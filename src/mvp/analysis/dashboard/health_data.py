"""Load pipeline health data for dashboard display."""

from __future__ import annotations

import json
from pathlib import Path


def _runs_path(data_root: Path) -> Path:
    return data_root / "pipeline" / "runs.jsonl"


def _is_main_run(run: dict) -> bool:
    """True for main (ATP fetch + predict) runs. Book-job rows
    (``job == "books"``) are excluded from the pipeline views; rows written
    before the books/main split have no ``job`` field and count as main."""
    return run.get("job", "main") == "main"


def load_latest_run(data_root: Path) -> dict | None:
    """Load the most recent main pipeline run from JSONL. None if no data."""
    path = _runs_path(data_root)
    if not path.exists():
        return None
    latest = None
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            run = json.loads(stripped)
            if _is_main_run(run):
                latest = run
    return latest


def load_all_runs(data_root: Path) -> list[dict]:
    """Load all main pipeline runs, most recent first (book-job rows excluded)."""
    path = _runs_path(data_root)
    if not path.exists():
        return []
    runs = []
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            run = json.loads(stripped)
            if _is_main_run(run):
                runs.append(run)
    runs.reverse()
    return runs
