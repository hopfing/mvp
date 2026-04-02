"""Load pipeline health data for dashboard display."""

from __future__ import annotations

import json
from pathlib import Path


def _runs_path(data_root: Path) -> Path:
    return data_root / "pipeline" / "runs.jsonl"


def load_latest_run(data_root: Path) -> dict | None:
    """Load the most recent pipeline run from JSONL. Returns None if no data."""
    path = _runs_path(data_root)
    if not path.exists():
        return None
    last_line = None
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                last_line = stripped
    if last_line is None:
        return None
    return json.loads(last_line)


def load_all_runs(data_root: Path) -> list[dict]:
    """Load all pipeline runs, most recent first."""
    path = _runs_path(data_root)
    if not path.exists():
        return []
    runs = []
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                runs.append(json.loads(stripped))
    runs.reverse()
    return runs
