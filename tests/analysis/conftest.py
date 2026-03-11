"""Shared fixtures for analysis tests."""

import polars as pl
import pytest
from datetime import datetime, timezone


@pytest.fixture
def sample_predictions():
    """Minimal predictions DataFrame matching predictor output."""
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3"],
        "p1_id": ["A001", "B001", "C001"],
        "p2_id": ["A002", "B002", "C002"],
        "p1_name": ["Player A1", "Player B1", "Player C1"],
        "p2_name": ["Player A2", "Player B2", "Player C2"],
        "p1_win_prob": [0.65, 0.55, 0.70],
        "p2_win_prob": [0.35, 0.45, 0.30],
        "circuit": ["chal", "tour", "chal"],
        "surface": ["Hard", "Clay", "Hard"],
        "round": ["R32", "R16", "QF"],
        "tournament_name": ["Tourney A", "Tourney B", "Tourney A"],
        "effective_match_date": [
            datetime(2026, 3, 10),
            datetime(2026, 3, 10),
            datetime(2026, 3, 11),
        ],
        "p1_elo": [1500.0, 1600.0, 1450.0],
        "p2_elo": [1400.0, 1550.0, 1350.0],
    })


@pytest.fixture
def sample_sheet_data():
    """Minimal sheet data as read from Google Sheets (all Utf8)."""
    return pl.DataFrame({
        "match_uid": ["m1", "m2", "m3"],
        "circuit": ["CH", "ATP", "CH"],
        "p1_odds": ["2.10", "1.80", ""],
        "p2_odds": ["1.75", "2.00", ""],
        "p1_pin": ["2.05", "", ""],
        "p2_pin": ["1.80", "", ""],
        "bet_side": ["P1", "P2", ""],
        "stake": ["10", "15", ""],
        "book": ["DraftKings", "Bet365", ""],
        "bet_result": ["W", "L", ""],
        "net": ["11.00", "-15", ""],
        "notes": ["", "bad beat", ""],
        "result": ["P1", "P2", ""],
    })


@pytest.fixture
def sample_staged_odds():
    """Staged odds parquet with multiple snapshots per event."""
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
    return pl.DataFrame({
        "book": ["dk"] * 8,
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
        "fetched_at": [
            now, now,
            now.replace(hour=14), now.replace(hour=14),
            now, now,
            now.replace(hour=14), now.replace(hour=14),
        ],
    })
