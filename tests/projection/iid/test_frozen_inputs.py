"""Projection evaluations read the week's frozen inputs and the sweep wipes
evaluations older than the snapshot, so every row in `iid-rank` is on the same
data. Twin of the classification freeze/wipe (mvp.model.backtest,
mvp.model.evaluation)."""

import os
import time
from pathlib import Path

import polars as pl
import pytest

import mvp.model.backtest as bt
from mvp.oddspapi import board, paths
from mvp.projection.iid import artifacts, rank


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "dataroot"
    (root / "projection_evaluations").mkdir(parents=True)
    monkeypatch.setenv("MVP_DATA_ROOT", str(root))
    monkeypatch.setenv("MVP_ARTIFACT_ROOT", str(root))
    return root


@pytest.fixture
def frozen_odds(tmp_path, monkeypatch):
    """Point the odds freeze at a tmp dir and reset its memo."""
    root = tmp_path / "frozen" / "oddspapi"
    monkeypatch.setattr(bt, "FROZEN_ODDS_ROOT", root)
    monkeypatch.setattr(bt, "FROZEN_ODDS_MARKER", root / ".frozen")
    monkeypatch.setattr(bt, "_frozen_odds_cache", None)
    return root


def _stage(root: Path) -> Path:
    stage = root / "stage" / "oddspapi"
    for book in ("pinnacle", "draftkings"):
        (stage / book).mkdir(parents=True)
        pl.DataFrame({"x": [1]}).write_parquet(stage / book / "total_games.parquet")
    (stage / "ticks" / "g").mkdir(parents=True)
    pl.DataFrame({"x": [1]}).write_parquet(stage / "ticks" / "g" / "raw.parquet")
    pl.DataFrame({"x": [1]}).write_parquet(stage / "_fixture_map.parquet")
    ref = root / "raw" / "oddspapi" / "reference"
    ref.mkdir(parents=True)
    (ref / "markets_tennis.json").write_text("{}", encoding="utf-8")
    return stage


class TestFrozenOdds:
    def test_copies_the_ledger_inputs_and_nothing_else(self, data_root, frozen_odds):
        _stage(data_root)
        out = bt._frozen_odds_root()
        assert out == frozen_odds
        got = sorted(p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file())
        assert got == [
            ".frozen",
            "_fixture_map.parquet",
            "draftkings/total_games.parquet",
            "pinnacle/total_games.parquet",
            "reference/markets_tennis.json",
        ]
        # The marker is written last: nothing copied is newer than it.
        marker = (out / ".frozen").stat().st_mtime
        assert all(p.stat().st_mtime <= marker + 1e-3 for p in out.rglob("*.parquet"))

    def test_current_marker_skips_the_copy(self, data_root, frozen_odds):
        _stage(data_root)
        bt._frozen_odds_root()
        stamp = (frozen_odds / "pinnacle" / "total_games.parquet").stat().st_mtime_ns
        bt._frozen_odds_cache = None
        bt._frozen_odds_root()
        assert (frozen_odds / "pinnacle" / "total_games.parquet").stat().st_mtime_ns == stamp

    def test_stale_marker_refreezes_with_delete_semantics(self, data_root, frozen_odds):
        stage = _stage(data_root)
        bt._frozen_odds_root()
        old = time.time() - 14 * 86400
        os.utime(frozen_odds / ".frozen", (old, old))
        (stage / "draftkings" / "total_games.parquet").unlink()
        bt._frozen_odds_cache = None
        bt._frozen_odds_root()
        assert not (frozen_odds / "draftkings").exists()
        assert (frozen_odds / "pinnacle" / "total_games.parquet").exists()


class TestFrozenStageRedirect:
    def test_board_reads_the_frozen_stage_inside_the_context(self, data_root, tmp_path):
        _stage(data_root)
        frozen = tmp_path / "f"
        (frozen / "pinnacle").mkdir(parents=True)
        pl.DataFrame({"x": [1]}).write_parquet(frozen / "pinnacle" / "total_games.parquet")
        (frozen / "reference").mkdir()
        (frozen / "reference" / "markets_tennis.json").write_text("{}", encoding="utf-8")
        live_path = board.market_path("pinnacle", "total_games")
        assert live_path.parent.parent == paths.stage_root()
        with paths.frozen_stage(frozen):
            assert board.market_path("pinnacle", "total_games").parent.parent == frozen
            assert paths.markets_reference() == frozen / "reference" / "markets_tennis.json"
            assert board.available_books("total_games") == ["pinnacle"]
        assert board.market_path("pinnacle", "total_games") == live_path
        assert paths.markets_reference() != frozen / "reference" / "markets_tennis.json"


def _fp(root: Path, name: str, *, age_days: float) -> Path:
    d = root / "projection_evaluations" / name
    d.mkdir(parents=True)
    (d / "projection.json").write_text("{}", encoding="utf-8")
    t = time.time() - age_days * 86400
    os.utime(d / "projection.json", (t, t))
    return d


class TestWeeklyWipe:
    @pytest.fixture(autouse=True)
    def _reset(self, monkeypatch):
        monkeypatch.setattr(artifacts, "_wiped_roots", set())

    def test_removes_only_dirs_older_than_the_snapshot(self, data_root):
        stale = _fp(data_root, "aaa", age_days=10)
        fresh = _fp(data_root, "bbb", age_days=0)
        empty = data_root / "projection_evaluations" / "ccc"
        empty.mkdir()
        snapshot = time.time() - 3 * 86400
        assert artifacts.wipe_stale_projection_evaluations(snapshot) == 2
        assert not stale.exists() and not empty.exists() and fresh.exists()

    def test_directory_mtime_is_ignored_files_decide(self, data_root):
        d = _fp(data_root, "aaa", age_days=0)
        old = time.time() - 10 * 86400
        os.utime(d, (old, old))  # NTFS leaves this alone on in-place rewrites
        assert artifacts.wipe_stale_projection_evaluations(time.time() - 3 * 86400) == 0
        assert d.exists()

    def test_once_per_process_and_no_snapshot_is_a_noop(self, data_root):
        _fp(data_root, "aaa", age_days=10)
        assert artifacts.wipe_stale_projection_evaluations(None) == 0
        assert artifacts.wipe_stale_projection_evaluations(time.time()) == 1
        _fp(data_root, "bbb", age_days=10)
        assert artifacts.wipe_stale_projection_evaluations(time.time()) == 0


class TestRankHidesStaleRows:
    def _run(self, root: Path, name: str, *, age_days: float) -> None:
        d = _fp(root, name, age_days=age_days)
        (d / "projection.json").write_text(
            '{"metrics": {"iid_crps_total_games": 3.0}, "n_folds": 1, "n_matches": 10, '
            '"fold_metrics": [{"iid_crps_total_games": 3.0}]}',
            encoding="utf-8",
        )
        t = time.time() - age_days * 86400
        os.utime(d / "projection.json", (t, t))
        (d / "source.txt").write_text(f"parent\t{name}\t2026-01-01T00:00:00\n", encoding="utf-8")
        os.utime(d / "source.txt", (t, t))

    def test_stale_hidden_with_footer(self, data_root, monkeypatch):
        self._run(data_root, "old_run", age_days=10)
        self._run(data_root, "new_run", age_days=0)
        monkeypatch.setattr(bt, "frozen_snapshot_mtime", lambda *, create: time.time() - 3 * 86400)
        out = "\n".join(rank.format_rank_table())
        assert "new_run" in out and "old_run" not in out
        assert "1 evaluation(s) from before the current snapshot hidden" in out

    def test_no_snapshot_shows_everything_and_says_so(self, data_root, monkeypatch):
        self._run(data_root, "old_run", age_days=10)
        monkeypatch.setattr(bt, "frozen_snapshot_mtime", lambda *, create: None)
        out = "\n".join(rank.format_rank_table())
        assert "old_run" in out
        assert "No frozen snapshot yet" in out
