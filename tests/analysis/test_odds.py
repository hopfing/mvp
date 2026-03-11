"""Tests for per-match-book odds computation."""

import polars as pl
import pytest
from datetime import datetime, timezone


class TestOddsByBook:
    def _make_event_map(self):
        return pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "book": ["dk", "dk"],
            "event_id": ["e1", "e2"],
            "p1_book_name": ["Player A1", "Player B1"],
            "p2_book_name": ["Player A2", "Player B2"],
            "source": ["auto", "auto"],
        })

    def _make_staged_odds(self):
        """Two events, multiple snapshots. e2 goes live in second snapshot."""
        t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        return pl.DataFrame({
            "dk_event_id": ["e1", "e1", "e1", "e1", "e2", "e2", "e2", "e2"],
            "player_name": [
                "Player A1", "Player A2", "Player A1", "Player A2",
                "Player B1", "Player B2", "Player B1", "Player B2",
            ],
            "odds": [2.20, 1.70, 2.10, 1.75, 1.85, 1.95, 1.80, 2.00],
            "event_status": [
                "NOT_STARTED", "NOT_STARTED", "NOT_STARTED", "NOT_STARTED",
                "NOT_STARTED", "NOT_STARTED", "STARTED", "STARTED",
            ],
            "fetched_at": [t1, t1, t2, t2, t1, t1, t2, t2],
        })

    def test_closing_line_is_last_prematch(self):
        from mvp.analysis.odds import compute_odds_by_book

        result = compute_odds_by_book(
            staged_odds=self._make_staged_odds(),
            event_map=self._make_event_map(),
            book="dk",
            event_id_col="dk_event_id",
        )

        m1 = result.filter(pl.col("match_uid") == "m1")
        assert len(m1) == 1
        assert m1["closing_odds_p1"][0] == pytest.approx(2.10)  # t2 snapshot
        assert m1["closing_odds_p2"][0] == pytest.approx(1.75)

    def test_opening_line_is_first_prematch(self):
        from mvp.analysis.odds import compute_odds_by_book

        result = compute_odds_by_book(
            staged_odds=self._make_staged_odds(),
            event_map=self._make_event_map(),
            book="dk",
            event_id_col="dk_event_id",
        )

        m1 = result.filter(pl.col("match_uid") == "m1")
        assert m1["opening_odds_p1"][0] == pytest.approx(2.20)
        assert m1["opening_odds_p2"][0] == pytest.approx(1.70)

    def test_live_odds_excluded_from_closing(self):
        from mvp.analysis.odds import compute_odds_by_book

        result = compute_odds_by_book(
            staged_odds=self._make_staged_odds(),
            event_map=self._make_event_map(),
            book="dk",
            event_id_col="dk_event_id",
        )

        m2 = result.filter(pl.col("match_uid") == "m2")
        assert len(m2) == 1
        # Only one pre-match snapshot, so opening == closing
        assert m2["closing_odds_p1"][0] == pytest.approx(1.85)
        assert m2["n_snapshots"][0] == 1

    def test_has_prematch_flag(self):
        from mvp.analysis.odds import compute_odds_by_book

        # Event with only live odds
        t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
        odds = pl.DataFrame({
            "dk_event_id": ["e3", "e3"],
            "player_name": ["Player C1", "Player C2"],
            "odds": [1.50, 2.50],
            "event_status": ["STARTED", "STARTED"],
            "fetched_at": [t1, t1],
        })
        event_map = pl.DataFrame({
            "match_uid": ["m3"],
            "book": ["dk"],
            "event_id": ["e3"],
            "p1_book_name": ["Player C1"],
            "p2_book_name": ["Player C2"],
            "source": ["auto"],
        })

        result = compute_odds_by_book(
            staged_odds=odds,
            event_map=event_map,
            book="dk",
            event_id_col="dk_event_id",
        )

        assert len(result) == 1
        assert result["has_prematch"][0] is False
        assert result["closing_odds_p1"][0] is None

    def test_movement_direction(self):
        from mvp.analysis.odds import compute_odds_by_book

        result = compute_odds_by_book(
            staged_odds=self._make_staged_odds(),
            event_map=self._make_event_map(),
            book="dk",
            event_id_col="dk_event_id",
        )

        m1 = result.filter(pl.col("match_uid") == "m1")
        # P1 odds went from 2.20 to 2.10 (shortened)
        assert m1["direction_p1"][0] == "SHORTENED"

    def test_movement_pct(self):
        from mvp.analysis.odds import compute_odds_by_book

        result = compute_odds_by_book(
            staged_odds=self._make_staged_odds(),
            event_map=self._make_event_map(),
            book="dk",
            event_id_col="dk_event_id",
        )

        m1 = result.filter(pl.col("match_uid") == "m1")
        expected = (2.10 - 2.20) / 2.20
        assert m1["movement_pct_p1"][0] == pytest.approx(expected, abs=0.001)

    def test_unmatched_events_excluded(self):
        from mvp.analysis.odds import compute_odds_by_book

        # Event e99 not in event_map
        t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
        odds = pl.DataFrame({
            "dk_event_id": ["e99", "e99"],
            "player_name": ["Unknown P1", "Unknown P2"],
            "odds": [1.50, 2.50],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
            "fetched_at": [t1, t1],
        })

        result = compute_odds_by_book(
            staged_odds=odds,
            event_map=self._make_event_map(),
            book="dk",
            event_id_col="dk_event_id",
        )

        assert len(result) == 0
