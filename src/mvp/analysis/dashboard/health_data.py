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


def _read_rows(data_root: Path) -> list[dict]:
    path = _runs_path(data_root)
    if not path.exists():
        return []
    rows: list[dict] = []
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
    return rows


def _books_by_tick(rows: list[dict]) -> dict[str, dict]:
    """Index book-job rows by tick_id (later rows win for a duplicated tick)."""
    return {
        r["tick_id"]: r
        for r in rows
        if r.get("job") == "books" and r.get("tick_id")
    }


def _with_books(main_run: dict, books_by_tick: dict[str, dict]) -> dict:
    """Read-side join: enrich a main run with its own tick's book fetch counts.

    The books job records ``books_fetched`` on a separate row (split for VPN
    egress); this pairs it back to the main run by ``tick_id`` so each run view
    shows the odds from that tick. Pre-split rows have no ``tick_id`` and carry
    their own ``books_fetched``, so they're left untouched.
    """
    tick = main_run.get("tick_id")
    if tick and tick in books_by_tick:
        main_run["books_fetched"] = books_by_tick[tick].get("books_fetched", {})
    return main_run


def load_latest_run(data_root: Path) -> dict | None:
    """Most recent main run, enriched with its tick's book fetch counts."""
    rows = _read_rows(data_root)
    main = [r for r in rows if _is_main_run(r)]
    if not main:
        return None
    return _with_books(main[-1], _books_by_tick(rows))


def load_all_runs(data_root: Path) -> list[dict]:
    """All main runs, most recent first, each enriched with its tick's books."""
    rows = _read_rows(data_root)
    books_by_tick = _books_by_tick(rows)
    main = [_with_books(r, books_by_tick) for r in rows if _is_main_run(r)]
    main.reverse()
    return main
