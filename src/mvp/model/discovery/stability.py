"""Stability selection over forward feature selection.

Runs the base selector (forward selection) on many resamples of the data and
keeps features selected reproducibly across them. Fold geometry and per-fold
medians are frozen from the full unmasked frame (see
``FastForwardSelector.resample_folds``), so a resample only thins the rows that
populate each fixed fold. Selection frequency therefore measures feature
reproducibility, not drift in the evaluation period.

The Meinshausen-Bühlmann false-selection bound is deliberately NOT used to set
the threshold: it assumes LASSO, i.i.d. half-samples, and no sequential path
dependence, none of which hold for greedy forward selection over
tournament-clustered resamples. The threshold is an empirical knob — inspect the
selection-frequency profile and place it where reproducible and noise features
separate.

NOTE: resamples are independent and run concurrently across a thread pool (see
``max_workers`` on StabilitySelectionConfig). The XGBoost fits release the GIL,
so threads parallelise the real work while sharing the single precomputed
feature matrix — no per-worker copies. Parallelism is results-invariant: each
resample is seeded by its index and the selections are aggregated in index
order, so completion order cannot change the outcome, and ``max_workers=1``
restores the exact sequential path. Cost still scales with ``n_resamples``;
the null-importance pre-filter remains the lever for keeping the searched pool
small.
"""

import hashlib
import json
import logging
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from mvp.model.discovery.config import DiscoveryConfig, StabilitySelectionConfig
from mvp.model.discovery.fast_selection import FastForwardSelector
from mvp.model.discovery.selection import FeatureSelector
from mvp.model.models import get_n_jobs_override

logger = logging.getLogger(__name__)


def _resample_fingerprint(
    config: DiscoveryConfig,
    sconfig: StabilitySelectionConfig,
    all_features: list[str],
    *,
    metric: str,
    direction: str,
    min_features: int,
    max_features: int | None,
    min_delta: float,
    base_features: list[str] | None,
) -> str:
    """Content hash of everything that determines each resample's selected set.

    EXCLUDES ``n_resamples`` (the loop is extendable — more resamples reuse the
    completed ones), ``selection_threshold`` (applied post-hoc at aggregation,
    so re-thresholding reuses the checkpoint), and ``max_workers`` (parallelism
    is results-invariant — resamples are index-seeded and aggregated in index
    order). A mismatch invalidates the checkpoint and the run starts fresh.
    """
    payload = {
        "pool": sorted(all_features),
        "model_type": config.model.type,
        "model_params": config.model.params or {},
        "date_range": {
            "start": str(config.data.date_range.start),
            "end": str(config.data.date_range.end),
        },
        "filters": config.data.filters or {},
        "eval_filters": config.data.eval_filters or {},
        "target": config.target,
        "validation": config.validation.model_dump(),
        "sample_weight": (
            config.sample_weight.model_dump() if config.sample_weight else None
        ),
        "metric": metric,
        "direction": direction,
        "min_features": min_features,
        "max_features": max_features,
        "min_delta": min_delta,
        "base_features": sorted(base_features or []),
        "resample_unit": sconfig.resample_unit,
        "subsample_fraction": sconfig.subsample_fraction,
        "min_fold_rows": sconfig.min_fold_rows,
        "seed": sconfig.random_seed,
    }
    blob = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


def _load_resample_checkpoint(
    path: Path, fingerprint: str
) -> dict[int, dict]:
    """Load completed resamples keyed by index; {} if absent or fingerprint differs."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Stability checkpoint at %s unreadable (%s); starting fresh.", path, e)
        return {}
    if data.get("fingerprint") != fingerprint:
        logger.warning(
            "Stability checkpoint at %s has a different fingerprint "
            "(config changed); ignoring it and starting fresh.", path,
        )
        return {}
    return {int(r["index"]): r for r in data.get("completed", [])}


def _save_resample_checkpoint(
    path: Path, fingerprint: str, results_by_index: dict[int, dict]
) -> None:
    payload = {
        "fingerprint": fingerprint,
        "completed": [results_by_index[i] for i in sorted(results_by_index)],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class StabilityResult:
    """Result of a stability-selection run.

    Attributes:
        selection_frequency: spec -> fraction of effective resamples that
            selected it (only specs selected at least once appear).
        selected_features: specs with frequency >= the configured threshold,
            ordered by frequency descending.
        threshold: the selection-probability threshold applied.
        per_resample_selected: the raw selected set from each effective resample.
        stopping_rounds: number of features each effective resample selected
            (forward selection's stopping depth).
        resample_match_counts: row count of each effective resample.
        fold_skips: number of folds skipped (degenerate) per effective resample.
        n_resamples_requested: B from config.
        n_resamples_effective: resamples that produced at least one usable fold.
    """

    selection_frequency: dict[str, float]
    selected_features: list[str]
    threshold: float
    per_resample_selected: list[list[str]] = field(default_factory=list)
    stopping_rounds: list[int] = field(default_factory=list)
    resample_match_counts: list[int] = field(default_factory=list)
    fold_skips: list[int] = field(default_factory=list)
    n_resamples_requested: int = 0
    n_resamples_effective: int = 0


def _resample_mask(
    rng: np.random.Generator,
    fast: FastForwardSelector,
    config: StabilitySelectionConfig,
) -> np.ndarray:
    """Draw one subsample mask over X_wide rows at the configured unit."""
    n_rows = len(fast.y)
    if config.resample_unit == "tournament":
        if fast.tournament_key is None:
            raise ValueError(
                "resample_unit='tournament' requires tournament_id/year columns, "
                "which were not available in matches.parquet for this run. "
                "Use resample_unit='match' or check the data."
            )
        units = np.unique(fast.tournament_key)
        k = max(1, int(round(len(units) * config.subsample_fraction)))
        chosen = set(rng.choice(units, size=k, replace=False).tolist())
        return np.fromiter(
            (t in chosen for t in fast.tournament_key), dtype=bool, count=n_rows
        )
    # match-level
    k = max(1, int(round(n_rows * config.subsample_fraction)))
    idx = rng.choice(n_rows, size=k, replace=False)
    mask = np.zeros(n_rows, dtype=bool)
    mask[idx] = True
    return mask


def run_stability_selection(
    fast: FastForwardSelector,
    config: StabilitySelectionConfig,
    *,
    metric: str,
    direction: str,
    all_features: list[str],
    min_features: int,
    max_features: int | None,
    min_delta: float = 0.0,
    base_features: list[str] | None = None,
    checkpoint_path: Path | None = None,
) -> StabilityResult:
    """Run stability selection.

    Args:
        fast: a FastForwardSelector that has ALREADY had precompute() run on the
            full, unmasked frame (row_mask=None), so frozen geometry is populated.
        config: stability-selection configuration.
        metric: scoring metric (e.g. "calibration_error").
        direction: "minimize" or "maximize".
        all_features: candidate feature pool for each resample's forward selection.
        min_features / max_features: forward-selection bounds.
        min_delta: forward-selection minimum improvement to accept a candidate.
        base_features: forced-in base features for each resample.
        checkpoint_path: if given, each completed resample is persisted here and a
            resumed run skips resamples already recorded (validated by fingerprint).
            Each resample is seeded independently (seed + index), so resuming
            reproduces exactly the resamples that haven't run yet.

    Returns:
        StabilityResult.
    """
    if not fast.fold_windows:
        raise ValueError(
            "Stability selection requires frozen fold windows — run precompute() "
            "with a date splitter (date_sliding / date_expanding) first."
        )

    fingerprint = _resample_fingerprint(
        fast.config, config, all_features,
        metric=metric, direction=direction,
        min_features=min_features, max_features=max_features,
        min_delta=min_delta, base_features=base_features,
    )
    results_by_index: dict[int, dict] = {}
    if checkpoint_path is not None:
        results_by_index = _load_resample_checkpoint(checkpoint_path, fingerprint)
        if results_by_index:
            logger.info(
                "Stability: resuming from checkpoint — %d/%d resamples already done.",
                len(results_by_index), config.n_resamples,
            )

    pending = [b for b in range(config.n_resamples) if b not in results_by_index]

    # Resample-level parallelism. Resamples are independent (each seeded by its
    # index, aggregated below in index order), so a thread pool changes only
    # throughput, never the selected set. The heavy per-resample work is XGBoost
    # fits, which release the GIL, so threads — not processes — give real
    # parallelism while still sharing the single precomputed X_wide (no
    # per-worker matrix copies). create_scorer / resample_folds only read frozen
    # state, and the scorer's per-fold np.ix_ gather returns a private copy before
    # imputing, so X_wide is never mutated across threads.
    # Resolve the per-fit thread cap the way the model layer does: config
    # model.params n_jobs, else the --n-jobs override. When neither is set the
    # fit falls back to ~all cores (cpu-2), so concurrent fits would
    # oversubscribe — stay sequential there and let an explicit n_jobs cap free
    # cores for resample-level parallelism.
    params_n_jobs = (fast.config.model.params or {}).get("n_jobs")
    explicit_n_jobs = int(params_n_jobs) if params_n_jobs else get_n_jobs_override()
    cpu_count = os.cpu_count() or 4
    if config.max_workers is not None:
        workers = max(1, config.max_workers)
    elif explicit_n_jobs:
        workers = max(1, cpu_count // max(1, explicit_n_jobs))
    else:
        workers = 1
    workers = min(workers, len(pending)) if pending else 1

    loop_t0 = time.perf_counter()
    done_this_run = 0

    def _run_one(b: int) -> dict:
        """Compute one resample's record. Pure with respect to shared state —
        reads only frozen geometry and copies each fold slice, so it is safe to
        run on a worker thread. Per-resample seeding makes it reproducible
        regardless of completion order."""
        rng = np.random.default_rng(config.random_seed + b)
        mask = _resample_mask(rng, fast, config)
        folds, medians, skipped = fast.resample_folds(mask, config.min_fold_rows)
        if not folds:
            return {"index": b, "degenerate": True}
        scorer = fast.create_scorer(metric, folds=folds, fold_medians=medians)
        selector = FeatureSelector(
            scorer=scorer,
            all_features=all_features,
            method="forward",
            direction=direction,
            min_features=min_features,
            max_features=max_features,
            base_features=base_features,
            min_delta=min_delta,
            # Anti-nesting: parallelism lives at the resample layer (this thread
            # pool). Parallelising the inner candidate loop too would multiply
            # thread demand (workers_resample x workers_candidate) and
            # oversubscribe — keep inner selection serial.
            forward_max_workers=1,
        )
        result = selector.run(verbose=False)
        return {
            "index": b,
            "selected": result.selected_features,
            "match_count": int(mask.sum()),
            "fold_skips": skipped,
        }

    def _finalize(record: dict) -> None:
        """Record a completed resample, checkpoint, and log progress. In the
        threaded path the caller holds the lock: the dict mutation, checkpoint
        write, and counter bump must be atomic together."""
        nonlocal done_this_run
        b = record["index"]
        results_by_index[b] = record
        if checkpoint_path is not None:
            _save_resample_checkpoint(checkpoint_path, fingerprint, results_by_index)
        if record.get("degenerate"):
            logger.warning(
                "Resample %d/%d: every fold degenerate (< %d rows); skipping.",
                b + 1, config.n_resamples, config.min_fold_rows,
            )
            return
        # ETA is wall-clock over non-degenerate resamples completed THIS run
        # (resumed ones excluded). Under parallelism each completion stands in
        # for `workers` overlapping resamples, and per-resample cost already
        # varies with stopping depth and subsample size — treat it as approximate.
        done_this_run += 1
        elapsed = time.perf_counter() - loop_t0
        avg = elapsed / done_this_run
        eta_min = avg * (len(pending) - done_this_run) / 60.0
        logger.info(
            "Resample %d/%d done: %d features, %d rows, %d folds skipped "
            "(avg %.0fs/resample wall, ETA ~%.1f min)",
            b + 1, config.n_resamples, len(record["selected"]),
            record["match_count"], record["fold_skips"], avg, eta_min,
        )

    if workers == 1:
        for b in pending:
            _finalize(_run_one(b))
    elif pending:
        logger.info(
            "Stability: %d resamples across %d worker threads (n_jobs=%s per fit).",
            len(pending), workers, explicit_n_jobs if explicit_n_jobs else "default",
        )
        lock = threading.Lock()
        executor = ThreadPoolExecutor(max_workers=workers)
        futures = [executor.submit(_run_one, b) for b in pending]
        try:
            for fut in as_completed(futures):
                record = fut.result()
                with lock:
                    _finalize(record)
        finally:
            # Tear-down doubles as crash recovery. cancel_futures drops resamples
            # that never started (so a doomed batch doesn't burn the rest of the
            # pool), and wait=True joins the in-flight threads. We then checkpoint
            # every resample that DID finish — including any that completed after
            # the failing one and so never reached _finalize in the loop above.
            # On a clean run this is a no-op (all already recorded); on a failure
            # or Ctrl-C it means resuming re-runs only what didn't finish. Threads
            # are joined here, so no lock is needed.
            executor.shutdown(wait=True, cancel_futures=True)
            salvaged = 0
            for fut in futures:
                if fut.cancelled() or fut.exception() is not None:
                    continue
                record = fut.result()
                if record["index"] not in results_by_index:
                    _finalize(record)
                    salvaged += 1
            if salvaged:
                logger.info(
                    "Stability: salvaged %d completed resample(s) to the "
                    "checkpoint before aborting.", salvaged,
                )

    # Aggregate over effective (non-degenerate) resamples, in index order.
    effective_records = [
        results_by_index[i] for i in sorted(results_by_index)
        if not results_by_index[i].get("degenerate")
    ]
    per_resample = [r["selected"] for r in effective_records]
    stopping_rounds = [len(r["selected"]) for r in effective_records]
    match_counts = [r["match_count"] for r in effective_records]
    fold_skips = [r["fold_skips"] for r in effective_records]

    effective = len(effective_records)
    if effective == 0:
        raise RuntimeError(
            "Stability selection produced no usable resamples — every resample "
            "had all folds below min_fold_rows. Lower min_fold_rows or "
            "subsample_fraction."
        )
    if effective < config.n_resamples:
        logger.warning(
            "Only %d/%d resamples were usable (rest had all folds degenerate).",
            effective, config.n_resamples,
        )

    freq: Counter[str] = Counter()
    for selected in per_resample:
        for f in selected:
            freq[f] += 1
    selection_frequency = {
        f: count / effective for f, count in freq.items()
    }
    selected_features = sorted(
        (f for f, p in selection_frequency.items() if p >= config.selection_threshold),
        key=lambda f: selection_frequency[f],
        reverse=True,
    )

    # All resamples done — drop the checkpoint so a fresh re-run doesn't resume.
    if checkpoint_path is not None and checkpoint_path.exists():
        checkpoint_path.unlink()

    return StabilityResult(
        selection_frequency=selection_frequency,
        selected_features=selected_features,
        threshold=config.selection_threshold,
        per_resample_selected=per_resample,
        stopping_rounds=stopping_rounds,
        resample_match_counts=match_counts,
        fold_skips=fold_skips,
        n_resamples_requested=config.n_resamples,
        n_resamples_effective=effective,
    )
