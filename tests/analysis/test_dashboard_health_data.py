"""Tests for dashboard health data loading."""

import json
from pathlib import Path


def _write_runs(tmp_path: Path, runs: list[dict]) -> Path:
    jsonl_path = tmp_path / "pipeline" / "runs.jsonl"
    jsonl_path.parent.mkdir(parents=True)
    with open(jsonl_path, "w") as f:
        for run in runs:
            f.write(json.dumps(run) + "\n")
    return tmp_path


def test_load_latest_run(tmp_path):
    from mvp.analysis.dashboard.health_data import load_latest_run

    root = _write_runs(tmp_path, [
        {"timestamp": "2026-04-02T14:00:00", "errors": []},
        {"timestamp": "2026-04-02T14:15:00", "errors": ["something"]},
    ])
    latest = load_latest_run(root)
    assert latest is not None
    assert latest["timestamp"] == "2026-04-02T14:15:00"


def test_load_latest_run_missing_file(tmp_path):
    from mvp.analysis.dashboard.health_data import load_latest_run

    latest = load_latest_run(tmp_path)
    assert latest is None


def test_load_all_runs(tmp_path):
    from mvp.analysis.dashboard.health_data import load_all_runs

    root = _write_runs(tmp_path, [
        {"timestamp": "2026-04-02T14:00:00", "errors": []},
        {"timestamp": "2026-04-02T14:15:00", "errors": []},
        {"timestamp": "2026-04-02T14:30:00", "errors": []},
    ])
    runs = load_all_runs(root)
    assert len(runs) == 3
    # Most recent first
    assert runs[0]["timestamp"] == "2026-04-02T14:30:00"


def test_load_all_runs_missing_file(tmp_path):
    from mvp.analysis.dashboard.health_data import load_all_runs

    runs = load_all_runs(tmp_path)
    assert runs == []


def test_main_run_enriched_with_tick_books(tmp_path):
    from mvp.analysis.dashboard.health_data import load_all_runs, load_latest_run

    root = _write_runs(tmp_path, [
        {"timestamp": "2026-04-02T14:15:01", "tick_id": "2026-04-02T14:15:00",
         "job": "main", "books_fetched": {}, "errors": []},
        {"timestamp": "2026-04-02T14:15:30", "tick_id": "2026-04-02T14:15:00",
         "job": "books", "books_fetched": {"br": 40, "mgm": 0}},
    ])
    # The main run is joined to its tick's books row.
    latest = load_latest_run(root)
    assert latest["job"] == "main"
    assert latest["books_fetched"] == {"br": 40, "mgm": 0}
    assert load_all_runs(root)[0]["books_fetched"] == {"br": 40, "mgm": 0}


def test_book_row_not_returned_as_run(tmp_path):
    from mvp.analysis.dashboard.health_data import load_all_runs, load_latest_run

    root = _write_runs(tmp_path, [
        {"timestamp": "2026-04-02T14:15:01", "tick_id": "2026-04-02T14:15:00",
         "job": "main", "errors": []},
        {"timestamp": "2026-04-02T14:15:30", "tick_id": "2026-04-02T14:15:00",
         "job": "books", "books_fetched": {"br": 40}},
    ])
    assert load_latest_run(root)["job"] == "main"
    runs = load_all_runs(root)
    assert len(runs) == 1 and runs[0]["job"] == "main"


def test_main_run_without_matching_books_row(tmp_path):
    from mvp.analysis.dashboard.health_data import load_latest_run

    root = _write_runs(tmp_path, [
        {"timestamp": "2026-04-02T14:15:01", "tick_id": "2026-04-02T14:15:00",
         "job": "main", "books_fetched": {}, "errors": []},
    ])
    # No books row for the tick -> books_fetched stays empty (not an error).
    assert load_latest_run(root)["books_fetched"] == {}


def test_pre_split_row_keeps_own_books(tmp_path):
    from mvp.analysis.dashboard.health_data import load_latest_run

    # Pre-split row: no job, no tick_id, carries its own books_fetched.
    root = _write_runs(tmp_path, [
        {"timestamp": "2026-04-02T14:00:00", "books_fetched": {"dk": 22},
         "errors": []},
    ])
    assert load_latest_run(root)["books_fetched"] == {"dk": 22}
