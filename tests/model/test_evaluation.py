"""Tests for eval-artifact wiping (wipe_stale_evaluations).

The wipe clears the three eval dirs of anything written before the current
snapshot was taken, so model-rank only surfaces runs that read the snapshot on
disk. Every path the wipe touches is monkeypatched to a tmp dir, INCLUDING the
frozen snapshot: the wipe freezes before it reads the cutoff, and against the
real paths that would copy the live matches and odds and re-date the snapshot.
Entry mtimes are set explicitly so the boundary is exercised without controlling
the clock.
"""

import datetime as dt
import os
import time
from pathlib import Path

import polars as pl

import mvp.model.backtest as backtest_mod
import mvp.model.evaluation as ev


def _touch_file(path: Path, mtime: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    os.utime(path, (mtime, mtime))


def _touch_dir(path: Path, mtime: float) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.utime(path, (mtime, mtime))


def _snapshot_time() -> float:
    """A freeze time inside the current ISO week, so the wipe treats the
    snapshot as current and does not re-freeze it to now."""
    today = dt.date.today()
    ws = today - dt.timedelta(days=today.weekday())
    week_start = dt.datetime(ws.year, ws.month, ws.day).timestamp()
    return max(week_start + 1, time.time() - 3600)


def _setup_targets(tmp_path, monkeypatch):
    """Point the three wipe targets AND the frozen snapshot at tmp dirs, take a
    snapshot dated `_snapshot_time()`, and reset the once-per-process memo."""
    lead = tmp_path / "backtests" / "lead"
    evals = tmp_path / "data" / "model_evaluations"
    mlruns = tmp_path / "mlruns"
    frozen = tmp_path / "backtests" / "frozen"
    live = tmp_path / "live"
    for d in (lead, evals, mlruns, frozen, live):
        d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(backtest_mod, "ARTIFACT_ROOT", lead)
    monkeypatch.setattr(ev, "get_artifact_root", lambda: tmp_path / "data")
    monkeypatch.setattr(ev, "MLRUNS_DIR", mlruns)
    monkeypatch.setattr(ev, "_week_wiped", False)

    for name in ("matches.parquet", "odds.parquet"):
        pl.DataFrame({"x": [1]}).write_parquet(live / name)
        pl.DataFrame({"x": [1]}).write_parquet(frozen / name)
    monkeypatch.setattr(backtest_mod, "MATCHES_PATH", live / "matches.parquet")
    monkeypatch.setattr(backtest_mod, "ODDS_PATH", live / "odds.parquet")
    monkeypatch.setattr(backtest_mod, "FROZEN_MATCHES_PATH", frozen / "matches.parquet")
    monkeypatch.setattr(backtest_mod, "FROZEN_MATCHES_MARKER", frozen / ".matches_frozen")
    monkeypatch.setattr(backtest_mod, "FROZEN_ODDS_PARQUET_PATH", frozen / "odds.parquet")
    monkeypatch.setattr(backtest_mod, "FROZEN_ODDS_PARQUET_MARKER", frozen / ".odds_frozen")
    monkeypatch.setattr(backtest_mod, "_frozen_matches_cache", None)
    monkeypatch.setattr(backtest_mod, "_frozen_odds_parquet_cache", None)
    snap = _snapshot_time()
    for marker in (".matches_frozen", ".odds_frozen"):
        _touch_file(frozen / marker, snap)
    return lead, evals, mlruns, snap


def test_wipe_removes_what_predates_the_snapshot_keeps_the_rest(tmp_path, monkeypatch):
    lead, evals, mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    stale = snap - 7 * 86400  # last week's snapshot
    fresh = snap + 60         # written after this snapshot was taken

    for root in (lead, evals, mlruns):
        _touch_file(root / "stale_file.csv", stale)
        _touch_dir(root / "stale_dir", stale)
        _touch_file(root / "fresh_file.csv", fresh)
        _touch_dir(root / "fresh_dir", fresh)

    removed = ev.wipe_stale_evaluations()

    assert removed == 6  # 2 stale entries x 3 dirs
    for root in (lead, evals, mlruns):
        assert root.exists()  # the dir itself is preserved
        assert not (root / "stale_file.csv").exists()
        assert not (root / "stale_dir").exists()
        assert (root / "fresh_file.csv").exists()
        assert (root / "fresh_dir").exists()


def test_wipe_boundary_is_the_snapshot_time(tmp_path, monkeypatch):
    lead, _evals, _mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    _touch_file(lead / "at_boundary.csv", snap)      # == cutoff -> kept (>=)
    _touch_file(lead / "just_before.csv", snap - 1)  # < cutoff -> removed

    ev.wipe_stale_evaluations()

    assert (lead / "at_boundary.csv").exists()
    assert not (lead / "just_before.csv").exists()


def test_wipe_removes_this_weeks_entry_written_before_a_refreeze(tmp_path, monkeypatch):
    """Both entries are from this week; only the snapshot time separates them.
    Under the old Monday-00:00 rule both were kept."""
    lead, _evals, _mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    _touch_file(lead / "before_refreeze.csv", snap - 600)
    _touch_file(lead / "after_refreeze.csv", snap + 600)

    assert ev.wipe_stale_evaluations() == 1
    assert not (lead / "before_refreeze.csv").exists()
    assert (lead / "after_refreeze.csv").exists()


def test_wipe_removes_nonempty_stale_dir(tmp_path, monkeypatch):
    _lead, evals, _mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    stale = snap - 86400
    _touch_file(evals / "old_fp" / "backtest.csv", stale)
    _touch_file(evals / "old_fp" / "config.yaml", stale)
    os.utime(evals / "old_fp", (stale, stale))  # stamp dir mtime after its contents

    assert ev.wipe_stale_evaluations() == 1
    assert not (evals / "old_fp").exists()


def test_wipe_reads_the_newest_file_not_the_directory_mtime(tmp_path, monkeypatch):
    """NTFS leaves a directory's mtime alone when a file inside is rewritten in
    place, so a dir re-evaluated after a re-freeze still looks old."""
    _lead, evals, _mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    _touch_file(evals / "rewritten_fp" / "backtest.csv", snap + 60)
    os.utime(evals / "rewritten_fp", (snap - 5 * 86400, snap - 5 * 86400))

    assert ev.wipe_stale_evaluations() == 0
    assert (evals / "rewritten_fp").exists()


def test_wipe_memoized_once_per_process(tmp_path, monkeypatch):
    lead, _evals, _mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    stale = snap - 86400
    _touch_file(lead / "stale1.csv", stale)
    assert ev.wipe_stale_evaluations() == 1

    # A new stale file after the first wipe is left alone — the memo blocks re-runs.
    _touch_file(lead / "stale2.csv", stale)
    assert ev.wipe_stale_evaluations() == 0
    assert (lead / "stale2.csv").exists()


def test_wipe_tolerates_missing_target_dir(tmp_path, monkeypatch):
    lead, _evals, mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    mlruns.rmdir()  # a target that does not exist must not raise
    _touch_file(lead / "stale.csv", snap - 86400)

    assert ev.wipe_stale_evaluations() == 1


def test_wipe_never_touches_the_frozen_snapshot(tmp_path, monkeypatch):
    lead, _evals, _mlruns, snap = _setup_targets(tmp_path, monkeypatch)
    _touch_file(lead / "stale.csv", snap - 86400)
    ev.wipe_stale_evaluations()
    frozen = lead.parent / "frozen"
    assert (frozen / "matches.parquet").exists() and (frozen / "odds.parquet").exists()
