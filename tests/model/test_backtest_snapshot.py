"""The classification backtest scores a window and reads prices that are pinned to
the week's frozen snapshot, and evaluations written before the snapshot was taken
are stale. Twin of tests/projection/iid/test_frozen_inputs.py."""

import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

import mvp.model.backtest as bt
from mvp.model import evaluation


@pytest.fixture
def frozen(tmp_path, monkeypatch):
    """Point every live source and every frozen path at tmp dirs; reset memos."""
    live = tmp_path / "live"
    live.mkdir()
    fz = tmp_path / "frozen"
    pl.DataFrame({"match_uid": ["m1"]}).write_parquet(live / "matches.parquet")
    pl.DataFrame(
        {"match_uid": ["m1"], "player_id": ["p1"], "best_opening_odds": [1.9],
         "formed_odds": [1.85], "best_closing_odds": [1.8]}
    ).write_parquet(live / "odds.parquet")
    monkeypatch.setattr(bt, "MATCHES_PATH", live / "matches.parquet")
    monkeypatch.setattr(bt, "ODDS_PATH", live / "odds.parquet")
    monkeypatch.setattr(bt, "FROZEN_MATCHES_PATH", fz / "matches.parquet")
    monkeypatch.setattr(bt, "FROZEN_MATCHES_MARKER", fz / ".matches_frozen")
    monkeypatch.setattr(bt, "FROZEN_ODDS_PARQUET_PATH", fz / "odds.parquet")
    monkeypatch.setattr(bt, "FROZEN_ODDS_PARQUET_MARKER", fz / ".odds_frozen")
    monkeypatch.setattr(bt, "_frozen_matches_cache", None)
    monkeypatch.setattr(bt, "_frozen_odds_parquet_cache", None)
    return fz


def _set_time(path: Path, when: datetime) -> None:
    t = when.timestamp()
    os.utime(path, (t, t))


class TestBettingPeriodFollowsTheSnapshot:
    def test_default_end_is_the_day_before_the_freeze(self, frozen):
        bt._frozen_matches_path()
        # A freeze taken mid-Wednesday holds only part of Wednesday.
        monday = date.today() - timedelta(days=date.today().weekday())
        freeze = datetime.combine(monday, datetime.min.time()) + timedelta(hours=13)
        _set_time(bt.FROZEN_MATCHES_MARKER, freeze)
        start, end = bt._resolve_betting_period(None, None)
        assert start == bt.BETTING_START_FLOOR
        assert end == monday - timedelta(days=1)

    def test_does_not_depend_on_the_day_it_runs(self, frozen, monkeypatch):
        bt._frozen_matches_path()
        first = bt._resolve_betting_period(None, None)

        real_today = date.today()

        class Later(date):
            @classmethod
            def today(cls):
                return real_today + timedelta(days=3)

        # Same snapshot (memo held, so nothing re-freezes), a later run day: the
        # window must not move. Under the old today-minus-seven rule it moved.
        monkeypatch.setattr(bt, "_frozen_matches_cache", bt.FROZEN_MATCHES_PATH)
        monkeypatch.setattr(bt, "date", Later)
        assert bt._resolve_betting_period(None, None) == first

    def test_explicit_end_overrides(self, frozen):
        bt._frozen_matches_path()
        _, end = bt._resolve_betting_period(None, date(2026, 3, 1))
        assert end == date(2026, 3, 1)

    def test_start_before_the_floor_is_clamped(self, frozen):
        bt._frozen_matches_path()
        start, _ = bt._resolve_betting_period(date(2025, 6, 1), date(2026, 3, 1))
        assert start == bt.BETTING_START_FLOOR

    def test_snapshot_from_before_markers_falls_back_to_the_file_time(self, frozen):
        bt._frozen_matches_path()
        bt.FROZEN_MATCHES_MARKER.unlink()
        monday = date.today() - timedelta(days=date.today().weekday())
        _set_time(bt.FROZEN_MATCHES_PATH, datetime.combine(monday, datetime.min.time()) + timedelta(hours=9))
        _, end = bt._resolve_betting_period(None, None)
        assert end == monday - timedelta(days=1)


class TestOddsParquetIsFrozen:
    def test_copies_the_live_file_and_writes_a_marker(self, frozen):
        p = bt._frozen_odds_parquet_path()
        assert p == bt.FROZEN_ODDS_PARQUET_PATH and p.exists()
        assert bt.FROZEN_ODDS_PARQUET_MARKER.exists()
        assert pl.read_parquet(p)["best_opening_odds"].to_list() == [1.9]

    def test_a_current_copy_is_not_replaced_when_the_live_file_changes(self, frozen, monkeypatch):
        bt._frozen_odds_parquet_path()
        pl.DataFrame(
            {"match_uid": ["m1"], "player_id": ["p1"], "best_opening_odds": [2.5],
             "formed_odds": [2.4], "best_closing_odds": [2.3]}
        ).write_parquet(bt.ODDS_PATH)
        monkeypatch.setattr(bt, "_frozen_odds_parquet_cache", None)  # a new process
        assert pl.read_parquet(bt._frozen_odds_parquet_path())["best_opening_odds"].to_list() == [1.9]

    def test_a_copy_from_an_earlier_week_is_refrozen(self, frozen, monkeypatch):
        bt._frozen_odds_parquet_path()
        _set_time(bt.FROZEN_ODDS_PARQUET_MARKER, datetime.now() - timedelta(days=10))
        pl.DataFrame(
            {"match_uid": ["m1"], "player_id": ["p1"], "best_opening_odds": [2.5],
             "formed_odds": [2.4], "best_closing_odds": [2.3]}
        ).write_parquet(bt.ODDS_PATH)
        monkeypatch.setattr(bt, "_frozen_odds_parquet_cache", None)
        assert pl.read_parquet(bt._frozen_odds_parquet_path())["best_opening_odds"].to_list() == [2.5]

    def test_no_live_file_leaves_no_frozen_copy(self, frozen):
        bt.ODDS_PATH.unlink()
        assert not bt._frozen_odds_parquet_path().exists()


class TestSnapshotTime:
    def test_none_until_both_components_exist(self, frozen):
        assert bt.classification_snapshot_time(create=False) is None
        bt._frozen_matches_path()
        assert bt.classification_snapshot_time(create=False) is None

    def test_is_the_later_of_the_two_freezes(self, frozen):
        t = bt.classification_snapshot_time(create=True)
        assert t is not None
        later = datetime.now() + timedelta(hours=2)
        _set_time(bt.FROZEN_ODDS_PARQUET_MARKER, later)
        assert bt.classification_snapshot_time(create=False) == pytest.approx(later.timestamp())


class TestWipeKeysOnTheFreezeTime:
    @pytest.fixture
    def eval_dirs(self, tmp_path, monkeypatch, frozen):
        root = tmp_path / "artifacts"
        lead = root / "backtests" / "lead"
        evals = root / "model_evaluations"
        mlruns = tmp_path / "mlruns"
        for d in (lead, evals, mlruns):
            d.mkdir(parents=True)
        # Patch the function the wipe calls, not the env var: if this ever stopped
        # taking effect the wipe would run against the real model_evaluations.
        monkeypatch.setattr(evaluation, "get_artifact_root", lambda: root)
        monkeypatch.setattr(bt, "ARTIFACT_ROOT", lead)
        monkeypatch.setattr(evaluation, "MLRUNS_DIR", mlruns)
        monkeypatch.setattr(evaluation, "_week_wiped", False)
        return evals

    def _fp(self, evals: Path, name: str, when: datetime) -> Path:
        d = evals / name
        d.mkdir()
        (d / "backtest.csv").write_text("x", encoding="utf-8")
        _set_time(d / "backtest.csv", when)
        return d

    def test_an_evaluation_from_before_a_refreeze_is_removed(self, eval_dirs):
        bt.classification_snapshot_time(create=True)
        now = datetime.now()
        # Both written this week; the snapshot was rebuilt between them.
        before = self._fp(eval_dirs, "before", now - timedelta(hours=3))
        after = self._fp(eval_dirs, "after", now + timedelta(hours=1))
        _set_time(bt.FROZEN_MATCHES_MARKER, now - timedelta(hours=1))
        _set_time(bt.FROZEN_ODDS_PARQUET_MARKER, now - timedelta(hours=1))
        assert evaluation.wipe_stale_evaluations() == 1
        assert not before.exists() and after.exists()

    def test_the_newest_file_decides_not_the_directory(self, eval_dirs):
        bt.classification_snapshot_time(create=True)
        now = datetime.now()
        _set_time(bt.FROZEN_MATCHES_MARKER, now - timedelta(hours=1))
        _set_time(bt.FROZEN_ODDS_PARQUET_MARKER, now - timedelta(hours=1))
        d = self._fp(eval_dirs, "rewritten", now + timedelta(hours=1))
        _set_time(d, now - timedelta(days=5))  # NTFS leaves this alone on in-place rewrites
        assert evaluation.wipe_stale_evaluations() == 0
        assert d.exists()

    def test_once_per_process(self, eval_dirs):
        bt.classification_snapshot_time(create=True)
        self._fp(eval_dirs, "old", datetime.now() - timedelta(days=9))
        assert evaluation.wipe_stale_evaluations() == 1
        self._fp(eval_dirs, "old2", datetime.now() - timedelta(days=9))
        assert evaluation.wipe_stale_evaluations() == 0


def test_bet_rows_read_the_frozen_odds_not_the_live_file(frozen):
    """The join reads whatever `_frozen_odds_parquet_path()` returns; a live file
    rewritten after the freeze must not change a backtest's prices."""
    frozen_path = bt._frozen_odds_parquet_path()
    pl.DataFrame(
        {"match_uid": ["m1"], "player_id": ["p1"], "best_opening_odds": [9.9],
         "formed_odds": [9.9], "best_closing_odds": [9.9]}
    ).write_parquet(bt.ODDS_PATH)
    assert bt._read_backtest_odds()["best_opening_odds"].to_list() == [1.9]
    assert frozen_path.exists()
