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

from mvp.common.base_job import get_data_root, get_tuning_state_dir
from mvp.model.sweep_select import (
    missing_metric_trials,
    select_diverse,
    select_top,
)
from mvp.model.tuning import (
    _SERVE_BLOCKS,
    NAMESPACE_JOINT,
    _decode_params,
    split_joint_params,
    study_param_namespace,
    tuning_study_key,
)
from mvp.projection.iid.artifacts import (
    BACKTEST_PARQUET,
    PMF_PARQUET,
    SPREAD_BACKTEST_PARQUET,
    SPREAD_PMF_PARQUET,
    fp_dir_for,
)
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


def load_study(
    stem: str, state_dir: Path | None = None, serve_block: str | None = None,
):
    """Load one (config, block) tuning study.

    A two-level config has one study per serve_model param block, since both
    blocks suggest identical param names. Keyed through `tuning_study_key` so
    the sweep opens the same study the tuner wrote — hardcoding the stem here
    would silently sweep the win-branch trials for a config whose `first_in`
    block was the one tuned.
    """
    tuning_dir = state_dir or get_tuning_state_dir()
    db = tuning_dir / f"{stem}.db"
    if not db.exists():
        raise FileNotFoundError(f"no tuning study: {db}")
    key = tuning_study_key(stem, serve_block)
    try:
        study = optuna.load_study(study_name=key, storage=f"sqlite:///{db}")
    except KeyError:
        present = sorted(
            s.study_name
            for s in optuna.get_all_study_summaries(storage=f"sqlite:///{db}")
        )
        raise RuntimeError(
            f"no study '{key}' in {db}; present: {', '.join(present) or '(none)'}"
        ) from None
    pinned = study.user_attrs.get("pinned_params") or {}
    trials = [
        t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE
    ]
    if not trials:
        raise RuntimeError(f"{stem}: no completed trials")
    return study, pinned, trials


def build_trial_config(
    base_path: Path, pinned: dict, trial, serve_block: str | None = None,
    *, joint: bool,
) -> dict:
    """Base config + this trial's hyperparameters merged into its serve_model block.

    Same merge the tuner performs per trial, so a materialized config reproduces
    exactly what the trial scored — which means it must write the block the
    trial was SEARCHING. Writing a `first_in_params` trial into `params` would
    hand the win branches settings tuned for a 68k-row match-grain fit and leave
    the arm the trial was about untouched, so the config would reproduce nothing.

    `joint` is REQUIRED and comes from the study (`study_param_namespace`), not
    from this config file. It used to be re-derived here as "two_level and no
    --serve-block", which is the tuner's predicate minus its third condition:
    a two-level config whose first_in arm has no features is searched FLAT,
    because that arm never fits a model. Trials of such a study carry bare param
    names, the joint branch split them into nothing, and every materialized
    config came out identical to the base — five trials evaluated, none of them
    exercised. Reading it off the study also survives an edit to the config's
    first_in feature lists, which would otherwise change the answer under trials
    that were already written.
    """
    if serve_block is not None and serve_block not in _SERVE_BLOCKS:
        raise ValueError(
            f"serve_block must be None or one of {sorted(_SERVE_BLOCKS)}; "
            f"got {serve_block!r}"
        )
    if joint and serve_block is not None:
        raise ValueError(
            f"joint=True with serve_block={serve_block!r}: a joint study samples "
            "both blocks, so naming one is a contradiction."
        )
    base = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    if "serve_model" not in base:
        raise ValueError(
            f"{base_path} has no `serve_model:` block — not an IID projection "
            "config. Use scripts/frozen_backtest_sweep.py for classification."
        )
    decoded = _decode_params({**trial.params, **pinned})
    base["serve_model"] = dict(base["serve_model"])

    # An empty `first_in_params` inherits `params` at build time
    # (serve_model.py:140), so that is the block's effective starting point.
    win_base = dict(base["serve_model"].get("params") or {})
    fi_base = dict(base["serve_model"].get("first_in_params") or win_base)

    if joint:
        # A joint trial's params are prefixed and describe BOTH blocks. Writing
        # them into `params` unsplit would put `win_max_depth` in the config as
        # a literal key and leave both arms on their old values — the trial
        # would reproduce nothing.
        win_new, fi_new = split_joint_params(decoded)
        win_base.update(win_new)
        fi_base.update(fi_new)
        base["serve_model"]["params"] = win_base
        base["serve_model"]["first_in_params"] = fi_base
        return base

    block = serve_block or "params"
    current = fi_base if block == "first_in_params" else win_base
    current.update(decoded)
    base["serve_model"][block] = current
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
    serve_block: str | None = None,
) -> list[SweepEntry]:
    """Write one runnable config per selected trial; return the entries.

    A config with no tuning study materializes as a single entry (itself), so a
    sweep can mix tuned and untuned configs the way the classification script does.
    """
    base_path = _resolve_base(config_arg)
    stem = base_path.stem
    out_dir = out_dir or sweep_config_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    db = (state_dir or get_tuning_state_dir()) / f"{stem}.db"
    if not db.exists():
        logger.info("%s: no tuning study — evaluating the config as-is", stem)
        cfg_path = out_dir / f"{stem}.yaml"
        cfg_path.write_text(base_path.read_text(encoding="utf-8"), encoding="utf-8")
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        return [SweepEntry(
            unique_stem=stem, parent_stem=stem, trial_number=-1, rank=1,
            config_path=cfg_path, fp=fp_dir_for(cfg, cfg_path).name,
        )]

    study, pinned, trials = load_study(
        stem, state_dir=state_dir, serve_block=serve_block,
    )
    # How the trials on disk are encoded, from the study — never re-derived from
    # the config file. See `build_trial_config`.
    joint = study_param_namespace(study) == NAMESPACE_JOINT
    chosen = select_trials(trials, n_trials, select=select, sort=sort)
    tag = "h" if select == "topn" else "d"

    entries: list[SweepEntry] = []
    for rank, t in enumerate(chosen, 1):
        # Block in the stem, so a params sweep and a first_in sweep of the same
        # config cannot overwrite each other's materialized configs.
        block_tag = "" if serve_block is None else f"_{serve_block[:2]}"
        unique_stem = f"{stem}__{tag}{rank:02d}{block_tag}_t{t.number}"
        cfg_dict = build_trial_config(base_path, pinned, t, serve_block, joint=joint)
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
    _warn_fingerprint_collisions(entries)
    return entries


def _warn_fingerprint_collisions(entries: list[SweepEntry]) -> None:
    """Two entries sharing a fingerprint are one evaluation, not two.

    The fingerprint keys the output dir, so colliding entries write over each
    other and `iid-rank` shows a single row for the pair. That is the SYMPTOM
    the silent param drop presented as — five trials, one dir — and with
    --refresh (which forces every entry to run) there was nothing in the output
    to distinguish it from five real evaluations. A collision is still possible
    without that bug, whenever two trials differ only in params the canonical
    form doesn't hash, so this warns rather than raises.
    """
    by_fp: dict[str, list[str]] = {}
    for e in entries:
        by_fp.setdefault(e.fp, []).append(e.unique_stem)
    for fp, stems in by_fp.items():
        if len(stems) > 1:
            logger.warning(
                "%s: %d entries hash to one fingerprint (%s) and will overwrite "
                "each other — %s. They differ only in fields the canonical form "
                "does not hash, so this is ONE evaluation, not %d.",
                stems[0].split("__")[0], len(stems), fp, ", ".join(stems),
                len(stems),
            )


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
    from mvp.projection.iid.evaluation import run_backtest
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
    """Every artifact a finished trial writes.

    The ledger name is IMPORTED, not spelled. It was a bare "backtest.csv"
    literal while the pmf name was a constant, so renaming the ledger would have
    left this check looking for a file nothing writes — `_is_complete` never
    true, and every sweep entry re-running forever including the finished ones.
    """
    return all(
        (fp_dir / f).exists()
        for f in (
            "projection.json",
            BACKTEST_PARQUET,
            SPREAD_BACKTEST_PARQUET,
            PMF_PARQUET,
            SPREAD_PMF_PARQUET,
        )
    )


def run_sweep(
    configs: list[str],
    n_trials: int,
    *,
    select: str = "diverse",
    sort: str | None = None,
    dry_run: bool = False,
    refresh: bool = False,
    serve_block: str | None = None,
) -> SweepResult:
    result = SweepResult()
    for config_arg in configs:
        entries = materialize(
            config_arg, n_trials, select=select, sort=sort,
            serve_block=serve_block,
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
