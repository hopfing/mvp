"""Shared extraction + refresh layer for model evaluation commands.

Used by both `mvp model-report` and `mvp model-rank`. Reads from three
artifact sources, written by the upstream model / confidence / backtest
commands respectively:

1. Latest mlrun diagnostics JSON for a model
2. `B:/confidence/<model>/validation_results.json`
3. `B:/backtests/lead/<model>.csv`

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


def find_latest_diagnostics(model_name: str) -> tuple[Path, str, str]:
    """Locate the latest mlrun diagnostics JSON for a model name.

    Walks `mlruns/*/<run>/artifacts/` looking for `<model_name>.yaml`; the
    sibling `*.json` is the diagnostics file. Returns the most recent
    (by mtime) match.

    Returns (json_path, run_id, run_ts) where run_id is the 8-char mlrun
    hash and run_ts is the JSON mtime formatted YYYY-MM-DD HH:MM.
    """
    if not MLRUNS_DIR.exists():
        raise FileNotFoundError(f"mlruns directory not found: {MLRUNS_DIR}")

    candidates: list[tuple[float, Path, str]] = []
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
            candidates.append((json_path.stat().st_mtime, json_path, run_dir.name[:8]))

    if not candidates:
        raise FileNotFoundError(
            f"No mlrun diagnostics JSON found for model '{model_name}' under {MLRUNS_DIR}"
        )

    mtime, json_path, run_id = max(candidates, key=lambda t: t[0])
    run_ts = _dt.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
    return json_path, run_id, run_ts


def load_diagnostics(model_name: str) -> tuple[dict, str, str, Path]:
    """Load the latest mlrun diagnostics JSON for a model.

    Returns (data, run_id, run_ts, path).
    """
    path, run_id, run_ts = find_latest_diagnostics(model_name)
    with open(path) as f:
        data = json.load(f)
    if "segments" not in data:
        raise ValueError(f"Diagnostics file missing 'segments' key: {path}")
    return data, run_id, run_ts, path


def confidence_path(model_name: str) -> Path:
    return get_data_root() / "confidence" / model_name / "validation_results.json"


def backtest_path(model_name: str) -> Path:
    return get_data_root() / "backtests" / "lead" / f"{model_name}.csv"


def load_confidence(model_name: str) -> tuple[dict, Path]:
    """Load `validation_results.json` for a model."""
    path = confidence_path(model_name)
    if not path.exists():
        raise FileNotFoundError(f"Confidence validation results not found: {path}")
    with open(path) as f:
        data = json.load(f)
    return data, path


def load_backtest(model_name: str) -> tuple[pl.DataFrame, Path]:
    """Load the per-row backtest CSV for a model."""
    path = backtest_path(model_name)
    if not path.exists():
        raise FileNotFoundError(f"Backtest CSV not found: {path}")
    df = pl.read_csv(path, infer_schema_length=10000)
    return df, path


def _run_confidence_inline(config_path: Path) -> None:
    """Mirror cmd_confidence's non-voter, refresh-mode logic.

    Trains via ExperimentRunner, builds ConfidenceValidator from results,
    validates, and writes oof.parquet + validation_results.json.
    Subset of cmd_confidence — only the path needed by refresh_pipeline.
    """
    from mvp.cli import _get_ensemble_base_names, _save_validation_json
    from mvp.model.confidence.validator import ConfidenceValidator
    from mvp.model.runner import ExperimentRunner

    config_name = config_path.stem
    oof_dir = get_data_root() / "confidence" / config_name
    oof_path = oof_dir / "oof.parquet"

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
    oof_dir.mkdir(parents=True, exist_ok=True)
    validator._oof.write_parquet(oof_path)
    result = validator.validate()
    results_path = oof_dir / "validation_results.json"
    _save_validation_json(result, results_path)


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
    diagnostics, run_id, run_ts, diag_path = load_diagnostics(model_name)
    confidence, conf_path = load_confidence(model_name)
    backtest, bt_path = load_backtest(model_name)
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
