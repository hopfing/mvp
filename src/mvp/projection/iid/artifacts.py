"""Fingerprint-keyed evaluation artifacts for IID projection configs.

Every artifact a comparison needs lives in one directory keyed by config content:

    B:/projection_evaluations/<fp>/
        config.yaml             canonical snapshot (collision detector)
        source.txt              which config/run produced this, for grouping
        projection.json         iid-project metrics + per-fold + segments
        total_games_pmf.parquet per-match pmf, the input to CLV scoring
        serve_model.joblib      trained artifact (backtest)
        backtest.csv            per-bet rows (backtest)
        clv.json                sharp-CLV summary (written by the oddspapi scorer)

Content-keying is what makes a sweep over hyperparameter variants meaningful:
stem-keying wrote every variant of a config to the same path, so each run
overwrote the last.

The root is deliberately NOT `model_evaluations/` — see the note on
PROJECTION_EVAL_ROOT in mvp.common.config_hash.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import polars as pl

from mvp.common.config_hash import (
    PROJECTION_EVAL_ROOT,
    append_source,
    compute_iid_fingerprint,
    iid_fingerprint_dir,
    write_iid_config_snapshot,
)
from mvp.projection.iid.config import IIDProjectionConfig

logger = logging.getLogger(__name__)

PROJECTION_JSON = "projection.json"
PMF_PARQUET = "total_games_pmf.parquet"
BACKTEST_CSV = "backtest.csv"
CLV_JSON = "clv.json"
SERVE_MODEL_JOBLIB = "serve_model.joblib"


def fp_for(config: IIDProjectionConfig, config_path: Path) -> str:
    return compute_iid_fingerprint(config, config_path=config_path)


def fp_dir_for(config: IIDProjectionConfig, config_path: Path) -> Path:
    return iid_fingerprint_dir(fp_for(config, config_path))


def record_run(
    config: IIDProjectionConfig,
    config_path: Path,
    *,
    source: str | None = None,
    run_id: str | None = None,
) -> Path:
    """Ensure the fingerprint dir exists with a config snapshot; return it.

    `source` groups sweep trials under their parent config (a sweep passes the
    parent stem, so `iid-rank` can show N variants of one config together).
    Defaults to the config's own stem for a plain single run.
    """
    fp = fp_for(config, config_path)
    fp_dir = iid_fingerprint_dir(fp)
    fp_dir.mkdir(parents=True, exist_ok=True)
    write_iid_config_snapshot(config, fp, config_path=config_path)
    append_source(
        fp,
        source or config_path.stem,
        run_id or config_path.stem,
        root=PROJECTION_EVAL_ROOT,
    )
    return fp_dir


def write_projection_json(fp_dir: Path, result: dict[str, Any]) -> Path:
    """Persist an IIDProjectionRunner result.

    Drops the un-serializable / bulky members (`_config`, `diagnostics`) — the
    diagnostics' segment metrics are already flattened into `metrics` by the
    runner, and the config is snapshotted separately.
    """
    payload = {
        k: v for k, v in result.items() if k not in ("_config", "diagnostics")
    }
    fp_dir.mkdir(parents=True, exist_ok=True)
    path = fp_dir / PROJECTION_JSON
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def read_projection_json(fp_dir: Path) -> dict[str, Any] | None:
    path = fp_dir / PROJECTION_JSON
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_pmf_parquet(fp_dir: Path, pmf: pl.DataFrame) -> Path:
    """Persist the per-match total-games pmf — the input to CLV scoring.

    Mirrors what scripts/oddspapi/iid_project_dump.py produces, except keyed by
    fingerprint instead of a single fixed path that every run overwrote. That
    overwrite is the reason CLV could not previously be compared across configs.
    """
    fp_dir.mkdir(parents=True, exist_ok=True)
    path = fp_dir / PMF_PARQUET
    pmf.write_parquet(path)
    return path


def read_clv_json(fp_dir: Path) -> dict[str, Any] | None:
    path = fp_dir / CLV_JSON
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_sources(fp_dir: Path) -> list[tuple[str, str, str]]:
    """Parse source.txt into (source_name, run_id, timestamp) rows."""
    path = fp_dir / "source.txt"
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 3:
            rows.append((parts[0], parts[1], parts[2]))
    return rows


def discover_fp_dirs() -> list[Path]:
    """All projection-evaluation fingerprint dirs, newest first."""
    from mvp.common.base_job import get_data_root

    root = get_data_root() / PROJECTION_EVAL_ROOT
    if not root.exists():
        return []
    dirs = [p for p in root.iterdir() if p.is_dir()]
    dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return dirs
