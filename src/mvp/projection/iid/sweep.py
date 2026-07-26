"""Evaluate a set of tuning trials for an IID projection config.

The projection-side counterpart of scripts/frozen_backtest_sweep.py: pick N
trials from a config's Optuna study, materialize a runnable config per trial,
and evaluate each into its own fingerprint dir so `iid-rank` can compare them.

Why it exists: a tuning study ranks trials on ONE metric, and for this pipeline
that metric is not known to track betting quality. Comparing a set of trials
across several instruments is the point; this produces the artifacts that
comparison reads.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import optuna
import yaml

from mvp.common.base_job import get_data_root
from mvp.model.sweep_select import (
    missing_metric_trials,
    select_diverse,
    select_top,
)
from mvp.model.tuning import _decode_params
from mvp.projection.iid.artifacts import PMF_PARQUET, fp_dir_for
from mvp.projection.iid.config import IIDProjectionConfig

logger = logging.getLogger(__name__)

SWEEP_CONFIG_DIRNAME = "sweep_configs"


def sweep_config_dir() -> Path:
    return get_data_root() / "projections" / "iid" / SWEEP_CONFIG_DIRNAME


@dataclass
class SweepEntry:
    """One materialized trial config, ready to evaluate."""

    unique_stem: str
    parent_stem: str
    trial_number: int
    rank: int
    config_path: Path
    fp: str
    sort_value: float | None = None


@dataclass
class SweepResult:
    entries: list[SweepEntry] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


def load_study(stem: str, state_dir: Path | None = None):
    """Load a config's tuning study. Study name == config stem."""
    tuning_dir = state_dir or (get_data_root() / "tuning")
    db = tuning_dir / f"{stem}.db"
    if not db.exists():
        raise FileNotFoundError(f"no tuning study: {db}")
    study = optuna.load_study(study_name=stem, storage=f"sqlite:///{db}")
    pinned = study.user_attrs.get("pinned_params") or {}
    trials = [
        t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE
    ]
    if not trials:
        raise RuntimeError(f"{stem}: no completed trials")
    return study, pinned, trials


def build_trial_config(base_path: Path, pinned: dict, trial) -> dict:
    """Base config + this trial's hyperparameters merged into serve_model.params.

    Same merge the tuner performs per trial, so a materialized config reproduces
    exactly what the trial scored.
    """
    base = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    if "serve_model" not in base:
        raise ValueError(
            f"{base_path} has no `serve_model:` block — not an IID projection "
            "config. Use scripts/frozen_backtest_sweep.py for classification."
        )
    decoded = _decode_params({**trial.params, **pinned})
    base["serve_model"] = dict(base["serve_model"])
    params = dict(base["serve_model"].get("params") or {})
    params.update(decoded)
    base["serve_model"]["params"] = params
    return base


def select_trials(
    trials: list,
    n: int,
    *,
    select: str = "diverse",
    sort: str | None = None,
) -> list:
    if select == "topn":
        if not sort:
            raise ValueError("select='topn' requires a sort metric")
        n_missing = missing_metric_trials(trials, sort)
        if n_missing:
            logger.warning(
                "%d/%d trials have no '%s' — their ordering is arbitrary",
                n_missing, len(trials), sort,
            )
        return select_top(trials, sort, n)
    return select_diverse(trials, n)


def materialize(
    config_arg: str,
    n_trials: int,
    *,
    select: str = "diverse",
    sort: str | None = None,
    out_dir: Path | None = None,
    state_dir: Path | None = None,
) -> list[SweepEntry]:
    """Write one runnable config per selected trial; return the entries.

    A config with no tuning study materializes as a single entry (itself), so a
    sweep can mix tuned and untuned configs the way the classification script does.
    """
    base_path = _resolve_base(config_arg)
    stem = base_path.stem
    out_dir = out_dir or sweep_config_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    db = (state_dir or (get_data_root() / "tuning")) / f"{stem}.db"
    if not db.exists():
        logger.info("%s: no tuning study — evaluating the config as-is", stem)
        cfg_path = out_dir / f"{stem}.yaml"
        cfg_path.write_text(base_path.read_text(encoding="utf-8"), encoding="utf-8")
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        return [SweepEntry(
            unique_stem=stem, parent_stem=stem, trial_number=-1, rank=1,
            config_path=cfg_path, fp=fp_dir_for(cfg, cfg_path).name,
        )]

    _study, pinned, trials = load_study(stem, state_dir=state_dir)
    chosen = select_trials(trials, n_trials, select=select, sort=sort)
    tag = "h" if select == "topn" else "d"

    entries: list[SweepEntry] = []
    for rank, t in enumerate(chosen, 1):
        unique_stem = f"{stem}__{tag}{rank:02d}_t{t.number}"
        cfg_dict = build_trial_config(base_path, pinned, t)
        cfg_path = out_dir / f"{unique_stem}.yaml"
        cfg_path.write_text(
            yaml.safe_dump(cfg_dict, default_flow_style=False), encoding="utf-8",
        )
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        entries.append(SweepEntry(
            unique_stem=unique_stem,
            parent_stem=stem,
            trial_number=t.number,
            rank=rank,
            config_path=cfg_path,
            fp=fp_dir_for(cfg, cfg_path).name,
            sort_value=t.user_attrs.get(sort) if sort else None,
        ))
    return entries


def run_entry(entry: SweepEntry, *, refresh: bool = False) -> str:
    """Evaluate one materialized trial config into its fingerprint dir.

    Runs the projection AND the backtest. There is no skip-the-backtest mode:
    the backtest is roughly a third the cost of the fold loop (one train + one
    2026 projection, against N folds each doing both) and it produces the only
    instruments — bet rows and the pmf CLV scores against — that a comparison
    actually decides on. A distributional-only run would be the expensive half
    without the useful half.

    Returns "ok" or "skip".
    """
    from mvp.projection.iid.backtest import run_backtest
    from mvp.projection.iid.runner import IIDProjectionRunner

    fp_dir = get_data_root() / "projection_evaluations" / entry.fp
    if not refresh and _is_complete(fp_dir):
        return "skip"

    runner = IIDProjectionRunner(
        config_path=entry.config_path,
        run_name=entry.unique_stem,
        log_to_mlflow=False,
        source=entry.parent_stem,
    )
    runner.run()

    run_backtest(
        entry.config_path,
        retrain=refresh,
        source=entry.parent_stem,
        run_id=entry.unique_stem,
    )
    return "ok"


def _is_complete(fp_dir: Path) -> bool:
    return all(
        (fp_dir / f).exists()
        for f in ("projection.json", "backtest.csv", PMF_PARQUET)
    )


def run_sweep(
    configs: list[str],
    n_trials: int,
    *,
    select: str = "diverse",
    sort: str | None = None,
    dry_run: bool = False,
    refresh: bool = False,
) -> SweepResult:
    result = SweepResult()
    for config_arg in configs:
        entries = materialize(
            config_arg, n_trials, select=select, sort=sort,
        )
        result.entries.extend(entries)
        label = "diverse (maximin over HP space)" if select == "diverse" else f"sort={sort}"
        print(f"\n=== {Path(config_arg).stem}  {label}  selected {len(entries)} ===")
        for e in entries:
            sv = f"  {sort}={e.sort_value:.5f}" if e.sort_value is not None else ""
            print(f"  {e.rank:02d}  trial {e.trial_number:>4}{sv}  -> {e.unique_stem}  [{e.fp}]")

    print(f"\nConfigs written to {sweep_config_dir()}")
    if dry_run:
        print("DRY RUN — configs written, nothing evaluated.")
        return result

    t0 = time.perf_counter()
    for i, entry in enumerate(result.entries, 1):
        elapsed = (time.perf_counter() - t0) / 60
        print(
            f"\n[{i}/{len(result.entries)}] ({elapsed:.1f} min elapsed) {entry.unique_stem} ...",
            flush=True,
        )
        t_entry = time.perf_counter()
        try:
            status = run_entry(entry, refresh=refresh)
        except Exception as exc:  # one bad trial shouldn't kill the sweep
            result.failures.append((entry.unique_stem, str(exc)))
            print(
                f"  [{i}/{len(result.entries)}] FAILED in "
                f"{time.perf_counter() - t_entry:.0f}s: {exc}",
                flush=True,
            )
            continue
        if status == "skip":
            result.skipped.append(entry.unique_stem)
            print(f"  [{i}/{len(result.entries)}] skip — already evaluated", flush=True)
        else:
            print(
                f"  [{i}/{len(result.entries)}] done in "
                f"{time.perf_counter() - t_entry:.0f}s",
                flush=True,
            )
    return result


def _resolve_base(config_arg: str) -> Path:
    p = Path(config_arg)
    if p.suffix == ".yaml" and p.exists():
        return p
    p2 = Path("projections") / f"{p.stem}.yaml"
    if not p2.exists():
        raise FileNotFoundError(
            f"no base config for '{config_arg}': tried {p} and {p2}"
        )
    return p2
