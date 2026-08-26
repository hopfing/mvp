"""Nested calibration of the FS protocol (redesign build item 6).

Plan: mvp-docs/plans/2026-08-25-fs-protocol-redesign.md. Run the FULL
selection protocol (whatever the config specifies — family unit, two-bar
acceptance, min_delta) inside each outer fold, truncated so selection never
sees that fold's test window; then fit the selected set once and score it on
the untouched window through the same fold-fit path the shifted null uses.
Output: per-fold (headline selection gain, realized gain, gap) and the
summary discount — the number to subtract when reading any future run's
headline gain.

Mechanics
---------
- Outer folds are the config's own date-splitter windows (a date splitter is
  required). For outer fold f, the inner run is an ordinary
  ``FeatureDiscovery.run_selection`` with ``data.date_range.end`` set to the
  day before f's test start — the inner schedule re-derives its own expanding
  folds on the truncated range, so the inner protocol is exactly the outer
  protocol, smaller.
- Headline gain = (base-set metric − final metric) on the INNER schedule,
  read from the selection history. Realized gain = (base-set metric −
  selected-set metric) on outer fold f via ``_FoldScorer`` (same gather /
  impute / offset-margin / projection arithmetic as scoring elsewhere).
- Early outer folds whose truncated range yields fewer than
  ``min_inner_folds`` inner folds are skipped and reported as skipped.

Stated simplifications: the realized fit uses the config's model params as-is
(no per-fold hyperparameter tune, no shrunk-final-fit tuning), so the gap
measures selection-induced optimism, not the full deploy pipeline. The outer
feature matrix is held for evaluation while each inner run builds its own —
a transient two-matrix peak; inner instances are dropped with a forced
``gc.collect()`` between folds (model/closure reference cycles do not die
without it).

Run once; expensive by design.
"""

from __future__ import annotations

import gc
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from pathlib import Path

import numpy as np

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.selection import SelectionResult
from mvp.model.discovery.shifted_null import _FoldScorer
from mvp.model.engine import get_feature_columns
from mvp.model.splitters import make_splitter

logger = logging.getLogger(__name__)

_DATE_SPLITTERS = ("date_expanding", "date_sliding")


@dataclass
class NestedFoldResult:
    fold: int
    test_window: str  # "YYYY-MM-DD..YYYY-MM-DD" of the untouched outer window
    inner_end: str  # date selection was truncated to (inclusive)
    n_inner_folds: int
    headline_gain: float | None = None
    realized_gain: float | None = None
    gap: float | None = None
    selected_families: list[str] = field(default_factory=list)
    selected_features: list[str] = field(default_factory=list)
    skipped: str | None = None


@dataclass
class NestedCalibrationReport:
    metric: str
    direction: str
    folds: list[NestedFoldResult]
    mean_gap: float | None
    median_gap: float | None


def headline_gain(
    selection_result: SelectionResult, direction: str
) -> float | None:
    """Selection-time gain over the seeded base set, on the inner schedule.

    None when the run has no step-0 base record (unseeded run) — there is no
    base metric to difference against, and inventing one would make gaps
    incomparable across folds.
    """
    hist = selection_result.history
    if not hist or hist[0].get("action") != "base":
        return None
    sgn = 1.0 if direction == "minimize" else -1.0
    return sgn * (hist[0]["metric"] - selection_result.final_metric)


def summarize_gaps(folds: list[NestedFoldResult]) -> tuple[float | None, float | None]:
    gaps = [f.gap for f in folds if f.gap is not None]
    if not gaps:
        return None, None
    return float(np.mean(gaps)), float(np.median(gaps))


def _inner_fold_count(config: DiscoveryConfig, anchor, inner_end) -> int:
    """Calendar-window count the truncated range would produce (data-presence
    unfiltered — a precheck, not the inner run's actual fold list)."""
    val = config.validation
    splitter = make_splitter(
        val_type=val.type,
        train_months=getattr(val, "train_months", None),
        initial_train_months=getattr(val, "initial_train_months", None),
        test_months=getattr(val, "test_months", None),
    )
    return len(splitter._bounds(anchor, inner_end))


def run_nested_calibration(
    config_path: Path | str,
    matches_path: Path | str | None = None,
    cache_dir: Path | str | None = None,
    run_dir: Path | str | None = None,
    all_features: list[str] | None = None,
    min_inner_folds: int = 2,
    forward_max_workers: int | None = None,
    verbose: bool = False,
) -> NestedCalibrationReport:
    """Run the nested calibration; writes ``nested_calibration_report.json``
    into ``run_dir`` and returns the report."""
    from mvp.model.discovery.discover import FeatureDiscovery

    config_path = Path(config_path)
    config = DiscoveryConfig.from_file(config_path)
    if config.validation.type not in _DATE_SPLITTERS:
        raise ValueError(
            "nested calibration needs a date splitter (date_expanding / "
            f"date_sliding); got validation.type={config.validation.type!r}"
        )
    run_dir = (
        Path(run_dir) if run_dir is not None
        else Path("fs_runs") / f"nested_{config_path.stem}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    metric = config.discovery.metric
    direction = config.discovery.resolved_direction()
    sgn = 1.0 if direction == "minimize" else -1.0

    # Outer engine: full-range precompute; provides the outer fold windows and
    # the realized-gain evaluation path. Held for the whole run (see module
    # docstring on the memory profile).
    outer_fd = FeatureDiscovery(
        config_path=config_path, matches_path=matches_path,
        cache_dir=cache_dir, verbose=verbose,
    )
    pool = outer_fd._build_candidate_pool(all_features)
    outer_fd._create_fast_scorer(pool)
    fast = outer_fd._fast_selector
    windows = fast.fold_windows
    if not windows:
        raise ValueError("outer splitter produced no fold windows with data")
    fold_scorer = _FoldScorer(fast, metric)

    base_specs = list(config.discovery.features.base)
    base_idx = (
        np.array(
            [fast.col_to_idx[c] for c in get_feature_columns(base_specs)],
            dtype=int,
        )
        if base_specs else None
    )

    results: list[NestedFoldResult] = []
    anchor = windows[0][0]
    t0 = time.perf_counter()
    for f, (_tr_s, _tr_e, te_s, te_e) in enumerate(windows):
        inner_end = te_s - timedelta(days=1)
        n_inner = _inner_fold_count(config, anchor, inner_end)
        result = NestedFoldResult(
            fold=f,
            test_window=f"{te_s.isoformat()}..{te_e.isoformat()}",
            inner_end=inner_end.isoformat(),
            n_inner_folds=n_inner,
        )
        if n_inner < min_inner_folds:
            result.skipped = (
                f"{n_inner} inner folds < min_inner_folds={min_inner_folds}"
            )
            results.append(result)
            logger.info("nested fold %d: skipped (%s)", f, result.skipped)
            continue

        logger.info(
            "nested fold %d/%d: selection on ..%s (%d inner folds), "
            "evaluation on %s",
            f + 1, len(windows), result.inner_end, n_inner, result.test_window,
        )
        inner_fd = FeatureDiscovery(
            config_path=config_path, matches_path=matches_path,
            cache_dir=cache_dir, verbose=verbose,
        )
        inner_fd.config.data.date_range.end = inner_end
        if forward_max_workers is not None:
            inner_fd.config.discovery.forward_max_workers = forward_max_workers
        ckpt_dir = run_dir / f"fold{f}"
        ckpt_dir.mkdir(parents=True, exist_ok=True)
        sel = inner_fd.run_selection(
            all_features=list(pool),
            checkpoint_path=ckpt_dir / f"discovery_checkpoint_nested_f{f}.json",
        )
        result.selected_families = list(sel.selected_families)
        result.selected_features = list(sel.selected_features)
        result.headline_gain = headline_gain(sel, direction)

        if sel.selected_features and base_idx is not None:
            sel_idx = np.array(
                [
                    fast.col_to_idx[c]
                    for c in get_feature_columns(sel.selected_features)
                ],
                dtype=int,
            )
            m_base = fold_scorer.score_fold(f, base_idx)
            m_sel = fold_scorer.score_fold(f, sel_idx)
            if m_base is not None and m_sel is not None:
                result.realized_gain = sgn * (m_base - m_sel)
        if result.headline_gain is not None and result.realized_gain is not None:
            result.gap = result.headline_gain - result.realized_gain
        logger.info(
            "nested fold %d: headline=%s realized=%s gap=%s (%.0fs elapsed)",
            f, result.headline_gain, result.realized_gain, result.gap,
            time.perf_counter() - t0,
        )
        results.append(result)
        # Inner runner holds model/closure reference cycles that plain
        # refcounting never frees; without this the matrices accumulate.
        del inner_fd, sel
        gc.collect()

    mean_gap, median_gap = summarize_gaps(results)
    report = NestedCalibrationReport(
        metric=metric, direction=direction, folds=results,
        mean_gap=mean_gap, median_gap=median_gap,
    )
    out = run_dir / "nested_calibration_report.json"
    out.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
    logger.info(
        "nested calibration: %d/%d folds evaluated, mean gap %s, "
        "median gap %s -> %s",
        sum(1 for r in results if r.gap is not None), len(results),
        mean_gap, median_gap, out,
    )
    return report
