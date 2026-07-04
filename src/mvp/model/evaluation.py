"""Shared extraction + refresh layer for model evaluation commands.

Used by both `mvp model-report` and `mvp model-rank`. Reads from three
artifact sources, written by the upstream model / confidence / backtest
commands respectively:

1. Diagnostics JSON — prefer `<fp_dir>/diagnostics.json`, fall back to the
   latest mlrun artifact.
2. `<fp_dir>/validation_results.json` — fall back to
   `B:/confidence/<model>/validation_results.json`.
3. `<fp_dir>/backtest.csv` — fall back to `B:/backtests/lead/<model>.csv`.

`comparison.csv` is deliberately not a source — see
`mvp-docs/specs/2026-05-17-model-evaluation-cli.md`.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from mvp.common.base_job import get_data_root

logger = logging.getLogger(__name__)

MLRUNS_DIR = Path("mlruns")


@dataclass
class ModelArtifacts:
    """Locations + loaded data for one model's three evaluation sources."""

    name: str
    config_path: Path
    diagnostics_path: Path
    diagnostics: dict
    run_id: str
    run_ts: str
    confidence_path: Path
    confidence: dict
    backtest_path: Path
    backtest: pl.DataFrame


def fingerprint_for(config_path: Path) -> str:
    """Compute the content-hash fingerprint for a config YAML."""
    from mvp.common.config_hash import compute_fingerprint
    from mvp.model.config import ExperimentConfig

    cfg = ExperimentConfig.from_file(str(config_path))
    return compute_fingerprint(cfg, config_path=config_path)


def fp_confidence_path(config_path: Path) -> Path:
    from mvp.common.config_hash import fingerprint_dir

    return fingerprint_dir(fingerprint_for(config_path)) / "validation_results.json"


def fp_backtest_path(config_path: Path) -> Path:
    from mvp.common.config_hash import fingerprint_dir

    return fingerprint_dir(fingerprint_for(config_path)) / "backtest.csv"


def fp_diagnostics_path(config_path: Path) -> Path:
    from mvp.common.config_hash import fingerprint_dir

    return fingerprint_dir(fingerprint_for(config_path)) / "diagnostics.json"


# Numeric columns in the per-row backtest CSV. Pinning these prevents polars
# from inferring Utf8 when a short backtest window has all-null/blank values
# in the first N rows (e.g. 2026 cutoff configs whose odds coverage is sparse).
_BACKTEST_NUMERIC_COLS: dict[str, type] = {
    "model_prob": pl.Float64,
    "opening_edge": pl.Float64,
    "closing_edge": pl.Float64,
    "clv": pl.Float64,
    "won": pl.Boolean,
    "pnl_open": pl.Float64,
    "pnl_close": pl.Float64,
    "open_odds": pl.Float64,
    "close_odds": pl.Float64,
}


def read_backtest_csv(path: Path) -> pl.DataFrame:
    """Read the per-row backtest CSV with numeric columns pinned to Float64."""
    return pl.read_csv(
        path,
        infer_schema_length=10000,
        schema_overrides=_BACKTEST_NUMERIC_COLS,
    )


def find_latest_diagnostics(
    model_name: str,
    config_path: Path | None = None,
) -> tuple[Path, str, str]:
    """Locate the latest diagnostics JSON for a model.

    If `config_path` is given, prefer `<fp_dir>/diagnostics.json` when its
    mtime is newer than the latest mlruns artifact (or when no mlruns
    artifact exists for the model).

    Returns (json_path, run_id, run_ts) where run_id is the diagnostics
    source's directory name — the full fingerprint (the `model_evaluations/<fp>/`
    dir, e.g. `12f8c0365c6a`) when the fingerprint copy wins, or the mlrun hash
    otherwise — so it maps directly to the on-disk run. run_ts is the JSON mtime
    formatted YYYY-MM-DD HH:MM.
    """
    candidates: list[tuple[float, Path, str]] = []

    if config_path is not None:
        try:
            fp_diag = fp_diagnostics_path(config_path)
            if fp_diag.exists():
                mtime = fp_diag.stat().st_mtime
                fp_id = fp_diag.parent.name
                candidates.append((mtime, fp_diag, fp_id))
        except Exception:
            logger.exception("Failed fingerprint lookup for %s", config_path)

    if MLRUNS_DIR.exists():
        for exp_dir in MLRUNS_DIR.iterdir():
            if not exp_dir.is_dir() or exp_dir.name.startswith("."):
                continue
            for run_dir in exp_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                artifacts = run_dir / "artifacts"
                yaml_path = artifacts / f"{model_name}.yaml"
                if not yaml_path.exists():
                    continue
                json_files = list(artifacts.glob("*.json"))
                if not json_files:
                    continue
                json_path = max(json_files, key=lambda p: p.stat().st_mtime)
                candidates.append((json_path.stat().st_mtime, json_path, run_dir.name))

    if not candidates:
        raise FileNotFoundError(
            f"No diagnostics JSON found for model '{model_name}' "
            f"(searched fingerprint dir and {MLRUNS_DIR})"
        )

    mtime, json_path, run_id = max(candidates, key=lambda t: t[0])
    run_ts = _dt.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
    return json_path, run_id, run_ts


def load_diagnostics(
    model_name: str,
    config_path: Path | None = None,
) -> tuple[dict, str, str, Path]:
    """Load the latest diagnostics JSON for a model.

    Returns (data, run_id, run_ts, path).
    """
    path, run_id, run_ts = find_latest_diagnostics(model_name, config_path=config_path)
    with open(path) as f:
        data = json.load(f)
    if "segments" not in data:
        raise ValueError(f"Diagnostics file missing 'segments' key: {path}")
    return data, run_id, run_ts, path


def confidence_path(model_name: str) -> Path:
    """Legacy name-scoped path. Prefer `fp_confidence_path(config_path)`."""
    return get_data_root() / "confidence" / model_name / "validation_results.json"


def backtest_path(model_name: str) -> Path:
    """Legacy name-scoped path. Prefer `fp_backtest_path(config_path)`."""
    return get_data_root() / "backtests" / "lead" / f"{model_name}.csv"


def load_confidence(
    model_name: str,
    config_path: Path | None = None,
) -> tuple[dict, Path]:
    """Load `validation_results.json` for a model.

    Prefer the fp-scoped path when `config_path` is given; fall back to the
    legacy name-scoped path.
    """
    if config_path is not None:
        try:
            fp_path = fp_confidence_path(config_path)
            if fp_path.exists():
                with open(fp_path) as f:
                    return json.load(f), fp_path
        except Exception:
            logger.exception("Failed fingerprint confidence lookup for %s", config_path)
    path = confidence_path(model_name)
    if not path.exists():
        raise FileNotFoundError(f"Confidence validation results not found: {path}")
    with open(path) as f:
        data = json.load(f)
    return data, path


def load_backtest(
    model_name: str,
    config_path: Path | None = None,
) -> tuple[pl.DataFrame, Path]:
    """Load the per-row backtest CSV for a model.

    Prefer the fp-scoped path when `config_path` is given; fall back to the
    legacy name-scoped path.
    """
    if config_path is not None:
        try:
            fp_path = fp_backtest_path(config_path)
            if fp_path.exists():
                return read_backtest_csv(fp_path), fp_path
        except Exception:
            logger.exception("Failed fingerprint backtest lookup for %s", config_path)
    path = backtest_path(model_name)
    if not path.exists():
        raise FileNotFoundError(f"Backtest CSV not found: {path}")
    df = read_backtest_csv(path)
    return df, path


def _run_confidence_inline(config_path: Path) -> None:
    """Mirror cmd_confidence's non-voter, refresh-mode logic.

    Trains via ExperimentRunner, builds ConfidenceValidator from results,
    validates, and writes oof.parquet + validation_results.json.
    Subset of cmd_confidence — only the path needed by refresh_pipeline.
    """
    from mvp.cli import _get_ensemble_base_names, _save_validation_json
    from mvp.common.config_hash import (
        compute_fingerprint,
        fingerprint_dir,
        write_config_snapshot,
    )
    from mvp.model.confidence.validator import ConfidenceValidator
    from mvp.model.runner import ExperimentRunner

    base_names = _get_ensemble_base_names(config_path)

    runner = ExperimentRunner(config_path=config_path)
    results = runner.run()
    all_predictions = results["all_predictions"]
    per_model_oof = results.get("per_model_oof") or None
    if not per_model_oof:
        per_model_oof = None

    validator = ConfidenceValidator(
        all_predictions,
        per_model_oof=per_model_oof,
        base_names=base_names,
    )

    fp = compute_fingerprint(runner.config, config_path=config_path)
    fp_dir = fingerprint_dir(fp)
    fp_dir.mkdir(parents=True, exist_ok=True)
    write_config_snapshot(runner.config, fp, config_path=config_path)
    validator._oof.write_parquet(fp_dir / "oof.parquet")
    result = validator.validate()
    _save_validation_json(result, fp_dir / "validation_results.json")


def refresh_pipeline(config_path: Path) -> None:
    """Run the sequential refresh pipeline for a model: model -> confidence -> backtest.

    Hard-fails on any stage's failure — no partial state is acceptable. The
    upstream commands themselves write their artifacts on success; this
    function only orchestrates calls and surfaces failures.
    """
    import contextlib
    import io

    from mvp.model.backtest import run_backtest as run_lead_backtest
    from mvp.model.runner import ExperimentRunner

    logger.info("[refresh 1/3] training model: %s", config_path.stem)
    runner = ExperimentRunner(config_path=config_path)
    runner.run()

    logger.info("[refresh 2/3] confidence validation: %s", config_path.stem)
    _run_confidence_inline(config_path)

    logger.info("[refresh 3/3] backtest: %s", config_path.stem)
    # Suppress backtest's stdout summary (still written to the _summary.txt
    # file by the underlying command) so the report's Section D isn't
    # preceded by an unrelated wall of text.
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        run_lead_backtest(config_path, retrain=True)


def load_artifacts(model_name: str, config_path: Path) -> ModelArtifacts:
    """Load all three artifacts for a model. Hard-fail if any missing."""
    diagnostics, run_id, run_ts, diag_path = load_diagnostics(
        model_name, config_path=config_path
    )
    confidence, conf_path = load_confidence(model_name, config_path=config_path)
    backtest, bt_path = load_backtest(model_name, config_path=config_path)
    return ModelArtifacts(
        name=model_name,
        config_path=config_path,
        diagnostics_path=diag_path,
        diagnostics=diagnostics,
        run_id=run_id,
        run_ts=run_ts,
        confidence_path=conf_path,
        confidence=confidence,
        backtest_path=bt_path,
        backtest=backtest,
    )
