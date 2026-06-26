"""Feature selection algorithms."""


import json
import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np

from mvp.model.discovery.checkpoint import (
    SelectionCheckpoint,
    load_checkpoint,
    save_checkpoint,
)

logger = logging.getLogger(__name__)


def _fs_history_path(checkpoint_path: Path | None) -> Path | None:
    """Durable per-round score log derived from the checkpoint path.

    A sibling of the resume checkpoint (``fs_history_<stem>.jsonl``), but —
    unlike the checkpoint — it is append-only and is kept after the run
    completes, so the full per-round candidate scores remain queryable.
    """
    if checkpoint_path is None:
        return None
    name = checkpoint_path.name
    prefix = "discovery_checkpoint_"
    stem = name[len(prefix):] if name.startswith(prefix) else name
    stem = stem.rsplit(".", 1)[0]
    return checkpoint_path.with_name(f"fs_history_{stem}.jsonl")


def _append_fs_history(path: Path | None, entry: dict[str, Any]) -> None:
    """Append one round's record as a JSON line. Best-effort: never fatal.

    ``default=float`` coerces stray numpy scalars (e.g. np.float32 metrics),
    which json cannot serialize natively.
    """
    if path is None:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=float) + "\n")
    except OSError as e:
        logger.warning("Failed to append FS history to %s: %s", path, e)


@dataclass
class SelectionResult:
    """Result from feature selection."""

    selected_features: list[str]
    excluded_features: list[str]
    history: list[dict[str, Any]]
    final_metric: float


class FeatureSelector:
    """Feature selection using various methods.

    Wraps a scorer function that evaluates feature sets and returns
    a metric value. Lower values are better when direction="minimize".
    """

    def __init__(
        self,
        scorer: Callable[[list[str]], float],
        all_features: list[str],
        method: Literal["forward", "recursive", "threshold"] = "forward",
        direction: Literal["minimize", "maximize"] = "minimize",
        min_features: int = 1,
        max_features: int | None = None,
        importance_threshold: float = 0.05,
        importance_fn: Callable[[list[str]], dict[str, float]] | None = None,
        base_features: list[str] | None = None,
        round1_baseline: float | None = None,
        min_delta: float = 0.0,
        forward_max_workers: int | None = None,
    ) -> None:
        """Initialize selector.

        Args:
            scorer: Function that takes feature list and returns metric value.
            all_features: All available features to consider.
            method: Selection method (forward, recursive, threshold).
            direction: Whether to minimize or maximize the metric.
            min_features: Minimum features to keep.
            max_features: Maximum features to select (None = no limit).
            importance_threshold: For threshold method, minimum importance to keep.
            importance_fn: For threshold/recursive, function to compute importance.
            round1_baseline: Optional no-skill baseline used when logging the
                round 1 ranking. Features not beating this baseline (given
                ``direction``) are reported as "below baseline". If None,
                only non-finite scores are filtered.
            min_delta: For forward selection, the minimum absolute improvement
                the best candidate must achieve over the current metric to be
                accepted. 0.0 (default) preserves prior behavior — any strict
                improvement passes.
        """
        self.scorer = scorer
        self.all_features = list(all_features)
        self.method = method
        self.direction = direction
        self.min_features = min_features
        self.max_features = max_features or len(all_features)
        self.importance_threshold = importance_threshold
        self.importance_fn = importance_fn
        self.base_features = list(base_features) if base_features else []
        self.round1_baseline = round1_baseline
        self.min_delta = min_delta
        # Candidate-loop parallelism: number of concurrent candidate fits.
        # None/1 = serial (exact current path). Resolved upstream (discover);
        # stability forces 1 to avoid nesting under its resample pool.
        self.forward_max_workers = forward_max_workers

    def _is_better(self, new_val: float, old_val: float) -> bool:
        """Check if new value is better than old value."""
        if self.direction == "minimize":
            return new_val < old_val
        return new_val > old_val

    def _worst_value(self) -> float:
        """Return the worst possible metric value."""
        if self.direction == "minimize":
            return float("inf")
        return float("-inf")

    def forward_selection(
        self,
        verbose: bool = True,
        checkpoint_path: Path | None = None,
        checkpoint_interval: int = 25,
    ) -> SelectionResult:
        """Select features by iteratively adding the best one.

        Starts with empty set, adds feature that improves metric most,
        repeats until no improvement.

        Args:
            verbose: Print progress.
            checkpoint_path: Path to write/read checkpoint JSON. If the
                file exists when this method is called, the run resumes
                from the saved state. The file is NOT deleted on completion
                here — the caller removes it only after the output config is
                written, so a downstream failure (sweep / final fit / save)
                stays resumable.
            checkpoint_interval: Write checkpoint every N candidate
                evaluations within a round.
        """
        from tqdm import tqdm

        # --- Restore from checkpoint or start fresh ---
        resumed_round_scores: dict[str, float] = {}
        started_at = datetime.now(timezone.utc)
        first_round_logged = False

        cp = load_checkpoint(checkpoint_path) if checkpoint_path else None
        if cp is not None:
            selected: list[str] = [r["feature"] for r in cp.completed_rounds]
            selected_metrics: list[float] = [
                r.get("metric", 0.0) for r in cp.completed_rounds
            ]
            remaining = set(self.all_features) - set(selected)
            best_metric = cp.best_metric
            history: list[dict[str, Any]] = []
            if selected:
                history.append({
                    "step": 0,
                    "action": "base",
                    "features": list(selected),
                    "metric": best_metric,
                })
            resumed_round_scores = dict(cp.current_round_scores)
            started_at = cp.started_at
            # If we already completed a round before checkpointing, round 1
            # ranking has already been logged on the previous run.
            first_round_logged = len(cp.completed_rounds) > 0
            logger.info(
                "Resumed from checkpoint: %d completed rounds "
                "(current metric=%.4f), %d candidates scored in round %d",
                len(cp.completed_rounds),
                best_metric,
                len(resumed_round_scores),
                cp.current_round,
            )
        else:
            selected = list(self.base_features)
            remaining = set(self.all_features) - set(selected)
            history = []
            if selected:
                best_metric = self.scorer(selected)
                # Base features are scored as a set; record the set metric for
                # each so the checkpoint carries a real number, not a 0 stub.
                selected_metrics = [best_metric] * len(selected)
                history.append({
                    "step": 0,
                    "action": "base",
                    "features": list(selected),
                    "metric": best_metric,
                })
            else:
                best_metric = self._worst_value()
                selected_metrics = []

        # Durable per-round score log (append-only, kept after completion).
        # Fresh start wipes any stale log; resume appends to the existing one.
        history_path = _fs_history_path(checkpoint_path)
        if cp is None and history_path is not None and history_path.exists():
            history_path.unlink()

        while remaining and len(selected) < self.max_features:
            round_num = len(selected) + 1
            round_results: list[tuple[str, float]] = []
            scores_this_round: dict[str, float] = {}

            # Seed with any prior scores from the checkpoint for this round.
            # Populate the score set only — the round winner is chosen by a
            # single deterministic reduction over the full set below, so resume
            # insertion order (and, under parallelism, completion order) can't
            # change the pick.
            unevaluated = set(remaining)
            if resumed_round_scores:
                for feat, score in resumed_round_scores.items():
                    if feat not in remaining:
                        continue
                    round_results.append((feat, score))
                    scores_this_round[feat] = score
                unevaluated -= set(scores_this_round.keys())
                logger.info(
                    "  Restored %d/%d candidate scores from checkpoint",
                    len(scores_this_round), len(remaining),
                )
                # Prior scores only apply to the first resumed round
                resumed_round_scores = {}

            to_eval = sorted(unevaluated)
            workers = max(1, self.forward_max_workers or 1)
            eval_count = 0
            round_t0 = time.perf_counter()

            # Pure worker: returns (feature, metric), or (feature, None) on a
            # scorer failure — mirroring the serial `continue`-on-exception. It
            # reads only shared read-only state via self.scorer (which copies
            # each fold slice before touching it), so concurrent calls are safe.
            def _eval(feat: str) -> tuple[str, float | None]:
                try:
                    return feat, self.scorer(selected + [feat])
                except Exception as e:  # noqa: BLE001 — match serial skip-on-error
                    logger.warning("Scorer failed for %s: %s", feat, e)
                    return feat, None

            # All bookkeeping stays on the main thread (no locks needed): record
            # the score and write the periodic checkpoint while the dict is
            # quiescent between results. Cadence is "every N completions" — same
            # crash-loss bound as the old "every N submissions".
            def _record(feat: str, metric: float | None) -> None:
                nonlocal eval_count
                if metric is None:
                    return
                round_results.append((feat, metric))
                scores_this_round[feat] = metric
                eval_count += 1
                if (
                    checkpoint_path is not None
                    and checkpoint_interval > 0
                    and eval_count % checkpoint_interval == 0
                ):
                    self._write_checkpoint(
                        checkpoint_path,
                        started_at=started_at,
                        selected=selected,
                        selected_metrics=selected_metrics,
                        best_metric=best_metric,
                        current_round=round_num,
                        total_candidates=len(remaining),
                        scores=scores_this_round,
                    )

            desc = f"Round {round_num}/{self.max_features}"
            if workers == 1:
                for feature in tqdm(to_eval, desc=desc, leave=False, ncols=120):
                    _record(*_eval(feature))
            else:
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futures = [ex.submit(_eval, f) for f in to_eval]
                    for fut in tqdm(
                        as_completed(futures), total=len(futures),
                        desc=f"{desc} (x{workers})", leave=False, ncols=120,
                    ):
                        _record(*fut.result())

            # Deterministic round winner: strict-improvement reduction over the
            # FULL score set in sorted feature order, seeded at the current
            # metric. Order-independent, so serial and parallel pick the
            # identical feature; ties resolve to the earliest-sorted candidate,
            # and a candidate that merely equals best_metric is not accepted
            # (preserving the stop-on-no-improvement behavior below).
            best_feature = None
            best_feature_metric = best_metric
            for feat in sorted(scores_this_round):
                if self._is_better(scores_this_round[feat], best_feature_metric):
                    best_feature = feat
                    best_feature_metric = scores_this_round[feat]

            if eval_count:
                _dt = time.perf_counter() - round_t0
                logger.info(
                    "  Round %d: scored %d candidates in %.1fs "
                    "(%.2f cand/s, %d worker%s)",
                    round_num, eval_count, _dt,
                    eval_count / _dt if _dt > 0 else 0.0,
                    workers, "" if workers == 1 else "s",
                )

            # If no improvement (or improvement < min_delta), stop
            if best_feature is None:
                delta = -float("inf")
            elif self.direction == "minimize":
                delta = best_metric - best_feature_metric
            else:
                delta = best_feature_metric - best_metric
            if best_feature is None or delta < self.min_delta:
                reason = (
                    "no improvement" if self.min_delta == 0.0
                    else f"improvement {delta:.6f} < min_delta {self.min_delta:.6f}"
                )
                logger.info("FS halting: %s", reason)
                if best_feature is not None:
                    logger.info(
                        "  best rejected candidate: %s -> %.4f",
                        best_feature, best_feature_metric,
                    )
                history.append({
                    "step": len(history) + 1,
                    "action": "stop",
                    "reason": reason,
                    "metric": best_metric,
                    "best_candidate": best_feature,
                    "best_candidate_metric": (
                        best_feature_metric if best_feature is not None else None
                    ),
                })
                _append_fs_history(history_path, {
                    "round": round_num,
                    "action": "stop",
                    "reason": reason,
                    "metric": best_metric,
                    "best_candidate": best_feature,
                    "best_candidate_metric": (
                        best_feature_metric if best_feature is not None else None
                    ),
                    "ranking": sorted(
                        round_results, key=lambda x: x[1],
                        reverse=self.direction == "maximize",
                    ),
                })
                break

            # Add best feature
            selected.append(best_feature)
            selected_metrics.append(best_feature_metric)
            remaining.remove(best_feature)
            best_metric = best_feature_metric

            logger.info("  + %s -> %.4f", best_feature, best_metric)
            Path("discovery_progress.txt").write_text(
                "\n".join(f"{i+1}. {f}" for i, f in enumerate(selected))
            )

            # Sort round results by metric (best first)
            reverse = self.direction == "maximize"
            sorted_results = sorted(round_results, key=lambda x: x[1], reverse=reverse)

            history.append({
                "step": len(history) + 1,
                "action": "add",
                "feature": best_feature,
                "metric": best_metric,
                "round_ranking": sorted_results,
            })
            _append_fs_history(history_path, {
                "round": round_num,
                "action": "add",
                "feature": best_feature,
                "metric": best_metric,
                "ranking": sorted_results,
            })

            # Checkpoint at round boundary (current round advances, scores reset)
            if checkpoint_path is not None:
                self._write_checkpoint(
                    checkpoint_path,
                    started_at=started_at,
                    selected=selected,
                    selected_metrics=selected_metrics,
                    best_metric=best_metric,
                    current_round=round_num + 1,
                    total_candidates=len(remaining),
                    scores={},
                )

            # Log round 1 rankings inline so interrupted runs still surface them
            if not first_round_logged:
                first_round_logged = True
                baseline = self.round1_baseline
                with_signal: list[tuple[str, float]] = []
                for f, m in sorted_results:
                    if not np.isfinite(m):
                        continue
                    if baseline is not None:
                        if self.direction == "minimize" and m >= baseline:
                            continue
                        if self.direction == "maximize" and m <= baseline:
                            continue
                    with_signal.append((f, m))
                n_dropped = len(sorted_results) - len(with_signal)
                label = "with signal" if baseline is not None else "features"
                logger.info("")
                logger.info(
                    "ROUND 1 FEATURE RANKING (%d %s)", len(with_signal), label,
                )
                logger.info("-" * 50)
                for i, (feat, metric) in enumerate(with_signal, 1):
                    logger.info("  %3d. %s: %.4f", i, feat, metric)
                if n_dropped:
                    if baseline is not None:
                        logger.info(
                            "  (%d features below baseline %.4f or rejected)",
                            n_dropped, baseline,
                        )
                    else:
                        logger.info(
                            "  (%d features rejected / returned inf)", n_dropped,
                        )

        # NOTE: the checkpoint is intentionally NOT deleted here. Forward
        # selection completing is not the end of the run — sweep, segment
        # analysis, the final experiment, and the config write all happen
        # downstream and can fail (e.g. OOM in the final fit). Deleting here
        # would orphan a multi-hour run with no resume point. The caller (CLI)
        # removes the checkpoint only after the output config is written, so a
        # crash anywhere in the tail is resumable (restored round scores make
        # the resume cheap — it does not re-run completed rounds).

        return SelectionResult(
            selected_features=selected,
            excluded_features=list(remaining),
            history=history,
            final_metric=best_metric if best_metric != self._worst_value() else 0.0,
        )

    def _write_checkpoint(
        self,
        path: Path,
        *,
        started_at: datetime,
        selected: list[str],
        selected_metrics: list[float],
        best_metric: float,
        current_round: int,
        total_candidates: int,
        scores: dict[str, float],
    ) -> None:
        """Write current selection state to checkpoint file."""
        run_name = path.stem
        # Strip the conventional prefix if present
        prefix = "discovery_checkpoint_"
        if run_name.startswith(prefix):
            run_name = run_name[len(prefix):]

        cp = SelectionCheckpoint(
            run_name=run_name,
            started_at=started_at,
            updated_at=datetime.now(timezone.utc),
            completed_rounds=[
                {"feature": f, "metric": m}
                for f, m in zip(selected, selected_metrics)
            ],
            current_round=current_round,
            total_candidates=total_candidates,
            current_round_scores=scores,
            best_metric=best_metric,
            direction=self.direction,
            max_features=self.max_features,
        )
        save_checkpoint(path, cp)

    def recursive_elimination(self) -> SelectionResult:
        """Select features by iteratively removing the worst one.

        Starts with all features, removes feature that hurts metric least,
        repeats until removal would degrade performance.
        """
        if self.importance_fn is None:
            raise ValueError("recursive elimination requires importance_fn")

        current = list(self.all_features)
        history: list[dict[str, Any]] = []

        # Baseline: all features
        best_metric = self.scorer(current)
        history.append({
            "step": 0,
            "action": "baseline",
            "n_features": len(current),
            "metric": best_metric,
        })

        while len(current) > self.min_features:
            # Compute importance for current set
            importance = self.importance_fn(current)

            # Find least important feature
            least_important = min(current, key=lambda f: importance.get(f, 0))

            # Try removing it
            candidate = [f for f in current if f != least_important]
            try:
                new_metric = self.scorer(candidate)
            except Exception:
                break

            # If performance degrades significantly, stop
            if not self._is_better(new_metric, best_metric) and not np.isclose(
                new_metric, best_metric, rtol=0.01
            ):
                history.append({
                    "step": len(history),
                    "action": "stop",
                    "reason": "removing any feature degrades performance",
                    "metric": best_metric,
                })
                break

            # Remove the feature
            current = candidate
            best_metric = new_metric

            history.append({
                "step": len(history),
                "action": "remove",
                "feature": least_important,
                "importance": importance.get(least_important, 0),
                "metric": best_metric,
            })

        excluded = [f for f in self.all_features if f not in current]
        return SelectionResult(
            selected_features=current,
            excluded_features=excluded,
            history=history,
            final_metric=best_metric,
        )

    def threshold_selection(self) -> SelectionResult:
        """Select features by importance threshold.

        Computes importance once, keeps features above threshold.
        Respects max_features by taking top-N if more pass the threshold.
        """
        if self.importance_fn is None:
            raise ValueError("threshold selection requires importance_fn")

        # Compute importance with all features
        importance = self.importance_fn(self.all_features)

        # Sort by importance descending
        sorted_features = sorted(
            self.all_features,
            key=lambda f: importance.get(f, 0),
            reverse=True,
        )

        # Filter by threshold
        selected = [
            f for f in sorted_features
            if importance.get(f, 0) >= self.importance_threshold
        ]

        # Enforce max_features
        if len(selected) > self.max_features:
            selected = selected[:self.max_features]

        # Ensure minimum features
        if len(selected) < self.min_features:
            selected = sorted_features[:self.min_features]

        excluded = [f for f in self.all_features if f not in selected]

        # Score the selected set
        try:
            final_metric = self.scorer(selected)
        except Exception:
            final_metric = 0.0

        history = [{
            "step": 1,
            "action": "threshold",
            "threshold": self.importance_threshold,
            "importance": importance,
            "selected": selected,
            "metric": final_metric,
        }]

        return SelectionResult(
            selected_features=selected,
            excluded_features=excluded,
            history=history,
            final_metric=final_metric,
        )

    def run(
        self,
        verbose: bool = False,
        checkpoint_path: Path | None = None,
        checkpoint_interval: int | None = None,
    ) -> SelectionResult:
        """Run feature selection using configured method.

        Returns:
            SelectionResult with selected features and history.
        """
        if self.method == "forward":
            kwargs: dict[str, Any] = {
                "verbose": verbose,
                "checkpoint_path": checkpoint_path,
            }
            if checkpoint_interval is not None:
                kwargs["checkpoint_interval"] = checkpoint_interval
            return self.forward_selection(**kwargs)
        elif self.method == "recursive":
            return self.recursive_elimination()
        elif self.method == "threshold":
            return self.threshold_selection()
        else:
            raise ValueError(f"Unknown selection method: {self.method}")


def create_scorer(
    base_config_path: Path | str,
    matches_path: Path | str | None = None,
    cache_dir: Path | str | None = None,
    metric: str = "calibration_error",
) -> Callable[[list[str]], float]:
    """Create a scorer function for feature selection.

    The scorer runs experiments with different feature sets and
    returns the specified metric.

    Args:
        base_config_path: Path to base experiment config.
        matches_path: Path to matches.parquet.
        cache_dir: Path to feature cache directory.
        metric: Metric to optimize.

    Returns:
        Function that takes feature list and returns metric value.
    """
    from mvp.model.config import ExperimentConfig
    from mvp.model.runner import ExperimentRunner

    base_config = ExperimentConfig.from_file(str(base_config_path))

    def scorer(features: list[str]) -> float:
        if not features:
            return float("inf")

        # Create modified config
        config_dict = base_config.model_dump()
        config_dict["features"]["include"] = features
        config = ExperimentConfig.model_validate(config_dict)

        # Run experiment
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            import yaml
            yaml.dump(config.model_dump(), f, default_flow_style=False)
            temp_path = f.name

        try:
            runner = ExperimentRunner(
                config_path=temp_path,
                matches_path=matches_path,
                cache_dir=cache_dir,
            )
            result = runner.run()
            return result["metrics"].get(metric, float("inf"))
        finally:
            Path(temp_path).unlink(missing_ok=True)

    return scorer
