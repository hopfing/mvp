"""Tests for pipeline report builder."""

import json
from datetime import datetime
from pathlib import Path

from mvp.pipeline_report import PipelineReport


def test_new_report_has_timestamp():
    report = PipelineReport()
    assert report.data["timestamp"] is not None


def test_record_tournaments():
    report = PipelineReport()
    report.record_tournaments(processed=8, failed=[("houston", 2026, "timeout")])
    assert report.data["tournaments_processed"] == 8
    assert report.data["tournaments_failed"] == 1
    assert report.data["tournament_failures"] == [
        {"name": "houston", "year": 2026, "error": "timeout"}
    ]


def test_record_tournaments_no_failures():
    report = PipelineReport()
    report.record_tournaments(processed=5, failed=[])
    assert report.data["tournaments_processed"] == 5
    assert report.data["tournaments_failed"] == 0
    assert report.data["tournament_failures"] == []


def test_record_books_fetched():
    report = PipelineReport()
    report.record_book_fetched("dk", 45)
    report.record_book_fetched("br", 38)
    report.record_book_fetched("mgm", 0)
    assert report.data["books_fetched"] == {"dk": 45, "br": 38, "mgm": 0}


def test_record_unresolved_names():
    report = PipelineReport()
    report.record_unresolved_names("br", {"Felipe Virgili Berini", "Some Name"})
    report.record_unresolved_names("dk", set())
    assert sorted(report.data["unresolved_names"]["br"]) == [
        "Felipe Virgili Berini", "Some Name"
    ]
    assert report.data["unresolved_names"]["dk"] == []


def test_record_predictions():
    report = PipelineReport()
    report.record_predictions(total=32)
    assert report.data["predictions_total"] == 32


def test_record_predictions_without_odds():
    report = PipelineReport()
    items = [
        {"match_uid": "abc", "tournament": "Houston", "p1": "Sinner", "p2": "Alcaraz"},
    ]
    report.record_predictions_without_odds(items)
    assert report.data["predictions_without_odds"] == items


def test_record_sheets_sync_success():
    report = PipelineReport()
    report.record_sheets_sync(success=True, count=4)
    assert report.data["sheets_sync"] == {
        "success": True, "count": 4, "error": None
    }


def test_record_sheets_sync_failure():
    report = PipelineReport()
    report.record_sheets_sync(success=False, count=0, error="auth expired")
    assert report.data["sheets_sync"] == {
        "success": False, "count": 0, "error": "auth expired"
    }


def test_record_errors():
    report = PipelineReport()
    report.set_errors(["tournament houston: timeout", "DK odds fetch: 500"])
    assert report.data["errors"] == [
        "tournament houston: timeout", "DK odds fetch: 500"
    ]


def test_save_creates_file_and_appends(tmp_path):
    report = PipelineReport()
    report.record_tournaments(processed=3, failed=[])
    report.record_predictions(total=10)

    jsonl_path = tmp_path / "pipeline" / "runs.jsonl"
    report.save(jsonl_path)

    assert jsonl_path.exists()
    lines = jsonl_path.read_text().strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["tournaments_processed"] == 3
    assert data["predictions_total"] == 10


def test_save_appends_to_existing(tmp_path):
    jsonl_path = tmp_path / "pipeline" / "runs.jsonl"
    jsonl_path.parent.mkdir(parents=True)
    jsonl_path.write_text('{"timestamp":"2026-04-02T14:00:00"}\n')

    report = PipelineReport()
    report.save(jsonl_path)

    lines = jsonl_path.read_text().strip().split("\n")
    assert len(lines) == 2
