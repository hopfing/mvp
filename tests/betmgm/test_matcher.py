"""Tests for BetMGM odds matcher."""

from datetime import datetime, timezone

import polars as pl
import pytest

from mvp.common.odds_matching import EventMatch


def _make_staged_odds(tmp_path, entries):
    """Write staged odds parquet for testing."""
    df = pl.DataFrame(entries)
    path = tmp_path / "stage" / "betmgm" / "moneyline.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def _make_players(tmp_path):
    """Write players.parquet for name resolution."""
    df = pl.DataFrame({
        "player_id": ["ALCARAZ", "RUUD", "DJOKOVIC"],
        "first_name": ["Carlos", "Casper", "Novak"],
        "last_name": ["Alcaraz", "Ruud", "Djokovic"],
    })
    path = tmp_path / "stage" / "atptour" / "players.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _make_predictions(*matches):
    """Build a predictions DataFrame."""
    rows = []
    for m in matches:
        rows.append({
            "match_uid": m[0],
            "p1_id": m[1],
            "p2_id": m[2],
            "p1_name": m[3],
            "p2_name": m[4],
        })
    return pl.DataFrame(rows)


NOW = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)


class TestBetMGMOddsMatcher:
    def test_matches_by_player_pair(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        _make_players(tmp_path)
        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1"],
            "player_name": ["Carlos Alcaraz", "Casper Ruud"],
            "odds": [1.08, 7.25],
            "fetched_at": [NOW, NOW],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
        })

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "ALCARAZ", "RUUD", "Alcaraz", "Ruud"))
        result = matcher.match(preds)

        assert "M1" in result.odds
        assert result.odds["M1"]["ALCARAZ"] == pytest.approx(1.08)
        assert result.odds["M1"]["RUUD"] == pytest.approx(7.25)

    def test_unmatched_names_reported(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        _make_players(tmp_path)
        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1"],
            "player_name": ["Unknown Player", "Casper Ruud"],
            "odds": [2.0, 1.8],
            "fetched_at": [NOW, NOW],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
        })

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "UNKNOWN", "RUUD", "Unknown", "Ruud"))
        result = matcher.match(preds)

        assert "Unknown Player" in result.unmatched_names

    def test_event_matches_populated(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        _make_players(tmp_path)
        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1"],
            "player_name": ["Carlos Alcaraz", "Casper Ruud"],
            "odds": [1.5, 2.5],
            "fetched_at": [NOW, NOW],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
        })

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "ALCARAZ", "RUUD", "Alcaraz", "Ruud"))
        result = matcher.match(preds)

        assert len(result.event_matches) == 1
        em = result.event_matches[0]
        assert em.match_uid == "M1"
        assert em.event_id == "e1"

    def test_aliases_resolve(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        _make_players(tmp_path)
        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1"],
            "player_name": ["C. Alcaraz Jr", "Casper Ruud"],
            "odds": [1.5, 2.5],
            "fetched_at": [NOW, NOW],
            "event_status": ["NOT_STARTED", "NOT_STARTED"],
        })

        alias_path = tmp_path / "betmgm_aliases.yaml"
        alias_path.write_text("C. Alcaraz Jr: ALCARAZ\n")

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        matcher.ALIASES_PATH = alias_path
        preds = _make_predictions(("M1", "ALCARAZ", "RUUD", "Alcaraz", "Ruud"))
        result = matcher.match(preds)

        assert "M1" in result.odds

    def test_empty_odds_returns_empty(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "A", "B", "A", "B"))
        result = matcher.match(preds)

        assert len(result.odds) == 0

    def test_deduplicates_to_latest(self, tmp_path):
        from mvp.betmgm.matcher import BetMGMOddsMatcher

        _make_players(tmp_path)
        t1 = datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        _make_staged_odds(tmp_path, {
            "mgm_event_id": ["e1", "e1", "e1", "e1"],
            "player_name": ["Carlos Alcaraz", "Casper Ruud", "Carlos Alcaraz", "Casper Ruud"],
            "odds": [1.5, 2.5, 1.6, 2.4],
            "fetched_at": [t1, t1, t2, t2],
            "event_status": ["NOT_STARTED", "NOT_STARTED", "NOT_STARTED", "NOT_STARTED"],
        })

        matcher = BetMGMOddsMatcher(data_root=tmp_path)
        preds = _make_predictions(("M1", "ALCARAZ", "RUUD", "Alcaraz", "Ruud"))
        result = matcher.match(preds)

        assert result.odds["M1"]["ALCARAZ"] == pytest.approx(1.6)
        assert result.odds["M1"]["RUUD"] == pytest.approx(2.4)
