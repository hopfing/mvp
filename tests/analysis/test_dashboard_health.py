"""Tests for Pipeline Health page."""


def _sample_run():
    return {
        "timestamp": "2026-04-02T14:52:00",
        "tournaments_processed": 8,
        "tournaments_failed": 1,
        "tournament_failures": [
            {"name": "houston", "year": 2026, "error": "timeout"}
        ],
        "books_fetched": {"dk": 45, "br": 38, "mgm": 0, "b365": 42},
        "unresolved_names": {
            "dk": [],
            "br": ["Felipe Virgili Berini"],
            "mgm": [],
            "b365": [],
        },
        "predictions_total": 32,
        "predictions_without_odds": [
            {"match_uid": "abc", "tournament": "Houston", "p1": "Sinner", "p2": "Alcaraz"},
        ],
        "sheets_sync": {"success": True, "count": 4, "error": None},
        "errors": ["tournament houston: timeout"],
    }


def test_format_tournaments():
    from mvp.analysis.dashboard.health import format_tournaments

    run = _sample_run()
    summary, failures = format_tournaments(run)
    assert summary == "8 processed, 1 failed"
    assert len(failures) == 1
    assert failures[0]["name"] == "houston"


def test_format_tournaments_no_failures():
    from mvp.analysis.dashboard.health import format_tournaments

    run = _sample_run()
    run["tournaments_failed"] = 0
    run["tournament_failures"] = []
    summary, failures = format_tournaments(run)
    assert summary == "8 processed, 0 failed"
    assert failures == []


def test_format_books_fetched():
    from mvp.analysis.dashboard.health import format_books_fetched

    run = _sample_run()
    rows = format_books_fetched(run)
    assert len(rows) == 4
    dk_row = next(r for r in rows if r["book"] == "dk")
    assert dk_row["entries"] == 45


def test_format_unresolved_names():
    from mvp.analysis.dashboard.health import format_unresolved_names

    run = _sample_run()
    rows = format_unresolved_names(run)
    assert len(rows) == 1
    assert rows[0]["book"] == "br"
    assert rows[0]["name"] == "Felipe Virgili Berini"


def test_format_unresolved_names_none():
    from mvp.analysis.dashboard.health import format_unresolved_names

    run = _sample_run()
    run["unresolved_names"] = {"dk": [], "br": [], "mgm": [], "b365": []}
    rows = format_unresolved_names(run)
    assert rows == []


def test_format_predictions_without_odds():
    from mvp.analysis.dashboard.health import format_predictions_without_odds

    run = _sample_run()
    summary, items = format_predictions_without_odds(run)
    assert summary == "1/32 predictions without odds from any book"
    assert len(items) == 1


def test_format_predictions_without_odds_none():
    from mvp.analysis.dashboard.health import format_predictions_without_odds

    run = _sample_run()
    run["predictions_without_odds"] = []
    summary, items = format_predictions_without_odds(run)
    assert summary == "0/32 predictions without odds from any book"
    assert items == []
