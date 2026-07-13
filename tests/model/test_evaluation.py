"""Tests for weekly eval-artifact wiping (wipe_stale_evaluations).

The wipe clears the three eval dirs of anything older than the current ISO week
so model-rank only surfaces runs built on this week's frozen-matches snapshot.
Targets are monkeypatched to tmp dirs; entry mtimes are set explicitly so the
week boundary is exercised without controlling the clock.
"""

import datetime as dt
import os
from pathlib import Path

import mvp.model.backtest as backtest_mod
import mvp.model.evaluation as ev


def _week_start_cutoff() -> float:
    today = dt.date.today()
    ws = today - dt.timedelta(days=today.weekday())  # Monday of the ISO week
    return dt.datetime(ws.year, ws.month, ws.day).timestamp()


def _touch_file(path: Path, mtime: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    os.utime(path, (mtime, mtime))


def _touch_dir(path: Path, mtime: float) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.utime(path, (mtime, mtime))


def _setup_targets(tmp_path, monkeypatch):
    """Point the three wipe targets at tmp dirs and reset the once-per-process memo."""
    lead = tmp_path / "backtests" / "lead"
    evals = tmp_path / "data" / "model_evaluations"
    mlruns = tmp_path / "mlruns"
    for d in (lead, evals, mlruns):
        d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(backtest_mod, "ARTIFACT_ROOT", lead)
    monkeypatch.setattr(ev, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(ev, "MLRUNS_DIR", mlruns)
    monkeypatch.setattr(ev, "_week_wiped", False)
    return lead, evals, mlruns


def test_wipe_removes_prior_week_keeps_current(tmp_path, monkeypatch):
    lead, evals, mlruns = _setup_targets(tmp_path, monkeypatch)
    cutoff = _week_start_cutoff()
    stale = cutoff - 7 * 86400  # last week
    fresh = cutoff + 3600       # this week (Monday 01:00)

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


def test_wipe_keeps_monday_midnight_boundary(tmp_path, monkeypatch):
    lead, _evals, _mlruns = _setup_targets(tmp_path, monkeypatch)
    cutoff = _week_start_cutoff()
    _touch_file(lead / "at_boundary.csv", cutoff)      # == cutoff -> kept (>=)
    _touch_file(lead / "just_before.csv", cutoff - 1)  # < cutoff -> removed

    ev.wipe_stale_evaluations()

    assert (lead / "at_boundary.csv").exists()
    assert not (lead / "just_before.csv").exists()


def test_wipe_removes_nonempty_stale_dir(tmp_path, monkeypatch):
    _lead, evals, _mlruns = _setup_targets(tmp_path, monkeypatch)
    stale = _week_start_cutoff() - 86400
    _touch_file(evals / "old_fp" / "backtest.csv", stale)
    _touch_file(evals / "old_fp" / "config.yaml", stale)
    os.utime(evals / "old_fp", (stale, stale))  # stamp dir mtime after its contents

    assert ev.wipe_stale_evaluations() == 1
    assert not (evals / "old_fp").exists()


def test_wipe_memoized_once_per_process(tmp_path, monkeypatch):
    lead, _evals, _mlruns = _setup_targets(tmp_path, monkeypatch)
    stale = _week_start_cutoff() - 86400
    _touch_file(lead / "stale1.csv", stale)
    assert ev.wipe_stale_evaluations() == 1

    # A new stale file after the first wipe is left alone — the memo blocks re-runs.
    _touch_file(lead / "stale2.csv", stale)
    assert ev.wipe_stale_evaluations() == 0
    assert (lead / "stale2.csv").exists()


def test_wipe_tolerates_missing_target_dir(tmp_path, monkeypatch):
    lead, _evals, mlruns = _setup_targets(tmp_path, monkeypatch)
    mlruns.rmdir()  # a target that does not exist must not raise
    _touch_file(lead / "stale.csv", _week_start_cutoff() - 86400)

    assert ev.wipe_stale_evaluations() == 1
