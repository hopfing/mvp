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
