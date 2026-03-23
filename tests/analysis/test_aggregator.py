"""Tests for odds aggregator."""

import polars as pl
import pytest
from datetime import datetime, timezone


def _make_snapshots():
    """Resolved snapshots: two matches, two books, multiple time points."""
    t1 = datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
    return pl.DataFrame({
        "match_uid": [
            "m1", "m1", "m1", "m1",  # dk t1
            "m1", "m1", "m1", "m1",  # dk t2
            "m1", "m1",              # br t1
        ],
        "book": [
            "dk", "dk", "dk", "dk",
            "dk", "dk", "dk", "dk",
            "br", "br",
        ],
        "side": [
            "p1", "p2", "p1", "p2",
            "p1", "p2", "p1", "p2",
            "p1", "p2",
        ],
        "odds": [
            2.20, 1.70, 2.20, 1.70,
            2.10, 1.75, 2.10, 1.75,
            2.15, 1.72,
        ],
        "fetched_at": [
            t1, t1, t1, t1,
            t2, t2, t2, t2,
            t1, t1,
        ],
        "event_status": ["NOT_STARTED"] * 10,
    })


class TestComputeBookOdds:
    def test_closing_odds(self):
        from mvp.odds.aggregator import compute_book_odds

        snaps = _make_snapshots()
        result = compute_book_odds(snaps, "dk")

        assert len(result) == 1
        m1 = result.filter(pl.col("match_uid") == "m1")
        assert m1["closing_odds_p1"][0] == pytest.approx(2.10)
        assert m1["closing_odds_p2"][0] == pytest.approx(1.75)

    def test_opening_odds(self):
        from mvp.odds.aggregator import compute_book_odds

        result = compute_book_odds(_make_snapshots(), "dk")

        m1 = result.filter(pl.col("match_uid") == "m1")
        assert m1["opening_odds_p1"][0] == pytest.approx(2.20)

    def test_movement_direction(self):
        from mvp.odds.aggregator import compute_book_odds

        result = compute_book_odds(_make_snapshots(), "dk")

        m1 = result.filter(pl.col("match_uid") == "m1")
        # 2.20 -> 2.10 = shortened
        assert m1["direction_p1"][0] == "SHORTENED"

    def test_empty_book_returns_empty(self):
        from mvp.odds.aggregator import compute_book_odds

        result = compute_book_odds(_make_snapshots(), "nonexistent")
        assert len(result) == 0


class TestCrossBookOdds:
    def test_best_closing_is_max_across_books(self):
        from mvp.odds.aggregator import compute_book_odds, compute_cross_book_odds

        snaps = _make_snapshots()
        dk = compute_book_odds(snaps, "dk")
        br = compute_book_odds(snaps, "br")

        cross = compute_cross_book_odds([dk, br])
        assert len(cross) == 1

        m1 = cross.filter(pl.col("match_uid") == "m1")
        # DK closing p1 = 2.10, BR closing p1 = 2.15 → best = 2.15
        assert m1["best_closing_odds_p1"][0] == pytest.approx(2.15)

    def test_worst_closing_is_min_across_books(self):
        from mvp.odds.aggregator import compute_book_odds, compute_cross_book_odds

        snaps = _make_snapshots()
        dk = compute_book_odds(snaps, "dk")
        br = compute_book_odds(snaps, "br")

        cross = compute_cross_book_odds([dk, br])
        m1 = cross.filter(pl.col("match_uid") == "m1")
        # DK closing p1 = 2.10, BR = 2.15 → worst = 2.10
        assert m1["worst_closing_odds_p1"][0] == pytest.approx(2.10)

    def test_n_books(self):
        from mvp.odds.aggregator import compute_book_odds, compute_cross_book_odds

        snaps = _make_snapshots()
        dk = compute_book_odds(snaps, "dk")
        br = compute_book_odds(snaps, "br")

        cross = compute_cross_book_odds([dk, br])
        m1 = cross.filter(pl.col("match_uid") == "m1")
        assert m1["n_books"][0] == 2

    def test_empty_list_returns_empty(self):
        from mvp.odds.aggregator import compute_cross_book_odds

        result = compute_cross_book_odds([])
        assert len(result) == 0


def _make_opening_snapshots():
    """Snapshots with staggered book timing for opening odds tests.

    DK opens at 08:00, BR opens at 10:00.
    Both have snapshots at 10:00 and 12:00 (close).
    """
    return pl.DataFrame({
        "match_uid": ["m1"] * 8,
        "book": [
            "dk", "dk",      # 08:00
            "dk", "dk",      # 10:00
            "br", "br",      # 10:00
            "dk", "dk",      # 12:00 (not needed for opening but present)
        ],
        "player_id": [
            "A", "B",
            "A", "B",
            "A", "B",
            "A", "B",
        ],
        "odds": [
            2.20, 1.70,      # DK @ 08:00
            2.15, 1.73,      # DK @ 10:00
            2.25, 1.68,      # BR @ 10:00
            2.10, 1.75,      # DK @ 12:00
        ],
        "fetched_at": [
            datetime(2026, 3, 10, 8, 2, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 8, 2, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 10, 3, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 10, 3, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 10, 5, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 10, 5, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 12, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 12, 1, tzinfo=timezone.utc),
        ],
        "event_status": ["NOT_STARTED"] * 8,
    })


class TestOpeningOdds:
    def test_first_avail_is_earliest_book(self):
        from mvp.odds.aggregator import compute_opening_odds

        result = compute_opening_odds(_make_opening_snapshots())

        a = result.filter(pl.col("player_id") == "A")
        # DK posted first at 08:00 with odds 2.20 — only book in that round
        assert a["first_avail_odds"][0] == pytest.approx(2.20)

    def test_market_formed_averages_books(self):
        from mvp.odds.aggregator import compute_opening_odds

        result = compute_opening_odds(_make_opening_snapshots())

        a = result.filter(pl.col("player_id") == "A")
        # Market forms at 10:00 (DK + BR). DK=2.15, BR=2.25 → avg=2.20
        assert a["market_formed_odds"][0] == pytest.approx(2.20)

        b = result.filter(pl.col("player_id") == "B")
        # DK=1.73, BR=1.68 → avg=1.705
        assert b["market_formed_odds"][0] == pytest.approx(1.705)

    def test_single_book_match_has_null_market_formed(self):
        from mvp.odds.aggregator import compute_opening_odds

        # Only DK snapshots
        snaps = pl.DataFrame({
            "match_uid": ["m2", "m2"],
            "book": ["dk", "dk"],
            "player_id": ["X", "Y"],
            "odds": [1.50, 2.60],
            "fetched_at": [
                datetime(2026, 3, 10, 8, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 10, 8, 0, tzinfo=timezone.utc),
            ],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
        })
        result = compute_opening_odds(snaps)
        x = result.filter(pl.col("player_id") == "X")
        assert x["first_avail_odds"][0] == pytest.approx(1.50)
        assert x["market_formed_odds"][0] is None

    def test_empty_snapshots_returns_empty(self):
        from mvp.odds.aggregator import compute_opening_odds

        result = compute_opening_odds(pl.DataFrame(schema={
            "match_uid": pl.Utf8,
            "book": pl.Utf8,
            "player_id": pl.Utf8,
            "odds": pl.Float64,
            "fetched_at": pl.Datetime("us", "UTC"),
            "event_status": pl.Utf8,
        }))
        assert len(result) == 0
