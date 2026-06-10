"""Main discovery orchestration."""


import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import yaml

from mvp.common.base_job import get_local_data_root
from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector
from mvp.model.discovery.importance import compute_importance
from mvp.model.discovery.segments import (
    SegmentImportanceResult,
)
from mvp.model.discovery.null_importance import (
    NullImportanceResult,
    run_null_importance,
)
from mvp.model.discovery.selection import FeatureSelector, SelectionResult
from mvp.model.discovery.stability import StabilityResult, run_stability_selection
from mvp.model.discovery.sweeps import (
    ParameterSweep,
    SweepResult,
    parse_feature_spec,
)
from mvp.model.registry import get_registry

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryResult:
    """Complete result from feature discovery."""

    selected_features: list[str]
    selection_result: SelectionResult | None = None
    sweep_result: SweepResult | None = None
    segment_importance: dict[str, SegmentImportanceResult] = field(
        default_factory=dict
    )
    stability_result: StabilityResult | None = None
    null_importance_result: NullImportanceResult | None = None
    final_metric: float = 0.0
    n_experiments: int = 0
    recommended_config_path: Path | None = None


def _build_disagreement_dataset(
    y_true: np.ndarray,
    pred_0: np.ndarray,
    pred_1: np.ndarray,
    weighting: str = "magnitude",
) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None]:
    """Build target and mask/weights from two models' OOF predictions.

    Args:
        y_true: True outcomes (0/1).
        pred_0: Model 0 predicted probabilities.
        pred_1: Model 1 predicted probabilities.
        weighting: "binary" (filter to disagreements) or "magnitude" (weight all).

    Returns:
        (target, row_mask, sample_weights) where:
        - target: 1 if model 0 was right/closer, 0 if model 1
        - row_mask: boolean mask for binary mode, None for magnitude
        - sample_weights: |pred_0 - pred_1| for magnitude mode, None for binary
    """
    if weighting == "binary":
        side_0 = (pred_0 > 0.5).astype(int)
        side_1 = (pred_1 > 0.5).astype(int)
        mask = side_0 != side_1
        target = (side_0 == y_true).astype(int)
        return target, mask, None
    else:
        err_0 = (pred_0 - y_true) ** 2
        err_1 = (pred_1 - y_true) ** 2
        target = (err_0 < err_1).astype(int)
        weights = np.abs(pred_0 - pred_1)
        return target, None, weights


DEFAULT_day_windows = [0, 7, 14, 30, 60, 90, 180, 365]


def get_all_feature_specs(window_sizes: list[int] | None = None) -> list[str]:
    """Get all registered features with parameter variants.

    For features with a ``days`` parameter, generates variants based on
    *window_sizes*.  Use ``0`` to represent the all-time (no window) variant.

    For mirrorable features (mirror=True), generates both player_* and opp_* versions.
    For diff-style features (mirror=False), only generates player_* version.
    For match-level features (match_level=True), generates unprefixed version.

    Args:
        window_sizes: Window sizes to include.  ``0`` = all-time variant.
            ``None`` = all defaults (alltime + 7…365d).

    Returns:
        List of feature specs like ["player_win_rate", "opp_win_rate", "is_clay", ...].
    """
    day_windows = DEFAULT_day_windows if window_sizes is None else window_sizes
    include_alltime = 0 in day_windows
    sized_windows = [d for d in day_windows if d > 0]

    registry = get_registry()
    feature_names = registry.list_features()

    specs = []
    for name in feature_names:
        feature_def = registry.get(name)

        # Match-level features have no prefix
        if feature_def.match_level:
            if not feature_def.params:
                specs.append(name)
            elif "days" in feature_def.params and len(feature_def.params) == 1:
                if include_alltime:
                    specs.append(name)
                for days in sized_windows:
                    specs.append(f"{name}(days={days})")
            else:
                default_params = {}
                for param in feature_def.params:
                    if param == "days":
                        default_params["days"] = 30
                    elif param == "min_matches":
                        default_params["min_matches"] = 3
                if default_params:
                    params_str = ", ".join(f"{k}={v}" for k, v in default_params.items())
                    specs.append(f"{name}({params_str})")
                else:
                    specs.append(name)
            continue

        # Determine which prefixes to use
        # mirror=True: both player and opp (base features like win_rate)
        # mirror=False: player only (diff features that already combine both)
        prefixes = ["player", "opp"] if feature_def.mirror else ["player"]

        if not feature_def.params:
            for prefix in prefixes:
                specs.append(f"{prefix}_{name}")
        elif "days" in feature_def.params and len(feature_def.params) == 1:
            for prefix in prefixes:
                if include_alltime:
                    specs.append(f"{prefix}_{name}")
                for days in sized_windows:
                    specs.append(f"{prefix}_{name}(days={days})")
        else:
            default_params = {}
            for param in feature_def.params:
                if param == "days":
                    default_params["days"] = 30
                elif param == "min_matches":
                    default_params["min_matches"] = 3
            if default_params:
                params_str = ", ".join(f"{k}={v}" for k, v in default_params.items())
                for prefix in prefixes:
                    specs.append(f"{prefix}_{name}({params_str})")
            else:
                for prefix in prefixes:
                    specs.append(f"{prefix}_{name}")

    return specs


class FeatureDiscovery:
    """Orchestrates automated feature discovery.

    Runs forward selection to find optimal features, optionally
    sweeps parameters, and analyzes segment-level importance.
    """

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
        verbose: bool = False,
    ) -> None:
        """Initialize discovery.

        Args:
            config_path: Path to discovery config YAML.
            matches_path: Path to matches.parquet.
            cache_dir: Path to feature cache.
            mlflow_dir: Path to MLflow tracking directory.
            verbose: Print progress.
        """
        self.config_path = Path(config_path)
        self.config = DiscoveryConfig.from_file(config_path)
        self.matches_path = matches_path
        self.cache_dir = cache_dir
        self.mlflow_dir = mlflow_dir
        self.verbose = verbose

        self._experiment_count = 0

    def _log(self, msg: str) -> None:
        """Log discovery progress."""
        logger.info(msg)

    def _create_temp_config(self, features: list[str]) -> Path:
        """Create temporary experiment config with given features."""
        config_dict = self.config.to_experiment_config_dict(features)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(config_dict, f, default_flow_style=False)
            return Path(f.name)

    def _run_experiment(
        self, features: list[str], log_to_mlflow: bool = False, run_name: str | None = None
    ) -> dict[str, Any]:
        """Run experiment with given features."""
        from mvp.model.runner import ExperimentRunner

        temp_path = self._create_temp_config(features)

        try:
            runner = ExperimentRunner(
                config_path=temp_path,
                matches_path=self.matches_path,
                cache_dir=self.cache_dir,
                mlflow_dir=self.mlflow_dir,
                workflow="discovery",
                run_name=run_name or self.config_path.stem,
                log_to_mlflow=log_to_mlflow,
            )
            result = runner.run()
            self._experiment_count += 1
            return result
        finally:
            temp_path.unlink(missing_ok=True)

    def _collect_oof_predictions(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Run ensemble and collect per-model OOF predictions.

        Returns:
            (y_true, pred_0, pred_1) arrays concatenated across folds.
        """
        from mvp.model.runner import ExperimentRunner

        meta_config = self.config.discovery.meta_discovery
        runner = ExperimentRunner(
            config_path=meta_config.ensemble_config,
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
            log_to_mlflow=False,
        )
        result = runner.run()

        per_model_oof = result["per_model_oof"]
        all_predictions = result["all_predictions"]

        n_base = len(per_model_oof[0])
        if n_base != 2:
            raise ValueError(
                f"Meta-discovery requires exactly 2 base models, got {n_base}"
            )

        pred_0 = np.concatenate([fold[0] for fold in per_model_oof])
        pred_1 = np.concatenate([fold[1] for fold in per_model_oof])
        y_true = np.concatenate([p["y_true"] for p in all_predictions])
        row_keys = pl.concat([
            p["df"].select("match_uid", "player_id") for p in all_predictions
        ])

        return y_true, pred_0, pred_1, row_keys

    def _create_fast_scorer(
        self, all_features: list[str], metric: str | None = None
    ) -> callable:
        """Create a fast scorer for forward selection.

        Precomputes all candidate features into one numpy matrix so each
        candidate evaluation is just column slicing + model fit.
        """
        target_metric = metric or self.config.discovery.metric
        fast = FastForwardSelector(
            config=self.config,
            all_feature_specs=all_features,
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )

        if self.config.discovery.meta_discovery is not None:
            meta_config = self.config.discovery.meta_discovery
            self._log("Collecting OOF predictions from ensemble...")
            y_true, pred_0, pred_1, row_keys = self._collect_oof_predictions()

            self._log(f"Building disagreement dataset (weighting={meta_config.weighting})...")
            target, mask, weights = _build_disagreement_dataset(
                y_true, pred_0, pred_1, weighting=meta_config.weighting
            )

            if mask is not None:
                n_disagree = int(mask.sum())
                self._log(
                    f"  {n_disagree}/{len(y_true)} matches with disagreement "
                    f"({n_disagree / len(y_true):.1%})"
                )

            fast.precompute(
                override_y=target,
                row_mask=mask,
                sample_weights=weights,
                row_keys=row_keys,
            )
        else:
            fast.precompute()

        return fast.create_scorer(target_metric)

    def _create_scorer(self, metric: str | None = None) -> callable:
        """Create scorer function for selection."""
        target_metric = metric or self.config.discovery.metric

        def scorer(features: list[str]) -> float:
            if not features:
                return float("inf")

            try:
                result = self._run_experiment(features)
                return result["metrics"].get(target_metric, float("inf"))
            except Exception as e:
                if self.verbose:
                    self._log(f"    FAILED {features}: {e}")
                return float("inf")

        return scorer

    def _create_importance_fn(
        self, features: list[str]
    ) -> callable:
        """Create importance function for selection.

        Uses the last fold's trained model from _run_experiment to compute
        feature importance without retraining.
        """
        method = self.config.discovery.importance_method

        def importance_fn(current_features: list[str]) -> dict[str, float]:
            result = self._run_experiment(current_features)
            model = result["last_fold_model"]
            X_test = result["last_fold_X_test"]
            y_test = result["last_fold_y_test"]
            feature_cols = result["feature_columns"]

            return compute_importance(model, X_test, y_test, feature_cols, method=method)

        return importance_fn

    def _build_candidate_pool(
        self, all_features: list[str] | None = None
    ) -> list[str]:
        """Build the candidate feature pool from registry + config filters.

        Applies window_sizes, paramed_only, include, compute_only, and exclude.
        Shared by forward selection and stability selection so both search the
        same pool.
        """
        feat_cfg = self.config.discovery.features

        if all_features is None:
            all_features = get_all_feature_specs(window_sizes=feat_cfg.window_sizes)

        if feat_cfg.paramed_only:
            n_before = len(all_features)
            all_features = [f for f in all_features if "(days=" in f]
            self._log(
                f"paramed_only=True: filtered {n_before} → {len(all_features)} "
                "features (kept only specs with days param)"
            )

        if feat_cfg.include:
            included = set(feat_cfg.include)
            all_features = [f for f in all_features if f in included]
            self._log(f"Restricted to {len(all_features)} features via include")

        if feat_cfg.compute_only:
            compute_only = set(feat_cfg.compute_only)
            all_features = [f for f in all_features if f not in compute_only]

        if feat_cfg.exclude:
            excluded = set(feat_cfg.exclude)
            all_features = [f for f in all_features if f not in excluded]
            self._log(f"Excluding {len(excluded)} features: {list(excluded)}")

        return all_features

    def run_stability(self) -> StabilityResult:
        """Run stability selection over the candidate pool.

        Precomputes the feature matrix once on the full (unmasked) frame, freezing
        fold geometry and per-fold medians, then runs forward selection over
        ``n_resamples`` tournament-level subsamples and aggregates per-spec
        selection frequency.
        """
        stab_cfg = self.config.discovery.stability_selection
        assert stab_cfg is not None
        feat_cfg = self.config.discovery.features
        ni_cfg = self.config.discovery.null_importance

        all_features = self._build_candidate_pool()

        # Precompute once on the full frame — reused by both the null-importance
        # screen and stability selection.
        fast = FastForwardSelector(
            config=self.config,
            all_feature_specs=all_features,
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )
        fast.precompute()

        # PHASE 0: Null-importance pre-filter (optional) — shrink the pool that
        # stability selection then searches.
        self._null_importance_result = None
        if ni_cfg is not None:
            self._log("PHASE 0: Null-importance pre-filter")
            ni_cache_dir = (
                get_local_data_root() / "discovery" / "null_importance_cache"
            )
            ni_result = run_null_importance(
                fast,
                all_features=all_features,
                config=ni_cfg,
                cache_dir=ni_cache_dir,
            )
            self._log_null_importance_report(ni_result)
            self._null_importance_result = ni_result
            all_features = ni_result.kept_features
            if not all_features:
                raise RuntimeError(
                    "Null-importance dropped every feature — loosen alpha or "
                    "check the candidate pool."
                )

        self._log("PHASE 1: Stability Selection")
        self._log(
            f"  {len(all_features)} candidate features, "
            f"{stab_cfg.n_resamples} resamples at {stab_cfg.resample_unit} level "
            f"(fraction={stab_cfg.subsample_fraction})"
        )

        stab_checkpoint = Path(
            f"discovery_stability_checkpoint_{self.config_path.stem}.json"
        )
        result = run_stability_selection(
            fast,
            stab_cfg,
            metric=self.config.discovery.metric,
            direction=self.config.discovery.resolved_direction(),
            all_features=all_features,
            min_features=feat_cfg.min,
            max_features=feat_cfg.max,
            min_delta=self.config.discovery.min_delta,
            base_features=feat_cfg.base or None,
            checkpoint_path=stab_checkpoint,
        )

        self._log_stability_report(result)
        return result

    def _log_null_importance_report(self, result: NullImportanceResult) -> None:
        """Log the null-importance screen: how many kept, and what was dropped."""
        self._log(
            f"  kept {len(result.kept_features)} / "
            f"{len(result.kept_features) + len(result.dropped_features)} features "
            f"(alpha={result.alpha}, {result.n_runs} runs)"
        )
        ranked_keep = sorted(
            result.kept_features,
            key=lambda f: result.real_importance[f],
            reverse=True,
        )
        for feat in ranked_keep:
            self._log(
                f"    keep  p={result.p_value[feat]:.3f}  "
                f"imp={result.real_importance[feat]:.4f}  {feat}"
            )
        self._log(f"  dropped {len(result.dropped_features)} features below alpha")

    def _log_stability_report(self, result: StabilityResult) -> None:
        """Log the stability-selection profile and diagnostics."""
        self._log("")
        self._log(
            f"STABILITY PROFILE ({result.n_resamples_effective}/"
            f"{result.n_resamples_requested} resamples effective)"
        )
        self._log("-" * 50)
        ranked = sorted(
            result.selection_frequency.items(), key=lambda kv: kv[1], reverse=True
        )
        for feat, freq in ranked:
            mark = "*" if freq >= result.threshold else " "
            self._log(f"  {mark} {freq:5.2f}  {feat}")
        self._log(
            f"  (threshold π={result.threshold}; {len(result.selected_features)} "
            "features selected)"
        )
        if result.stopping_rounds:
            sr = np.array(result.stopping_rounds)
            self._log(
                f"  stopping round: min={sr.min()} median={int(np.median(sr))} "
                f"max={sr.max()}"
            )
        if result.resample_match_counts:
            mc = np.array(result.resample_match_counts)
            self._log(
                f"  resample rows: min={mc.min()} median={int(np.median(mc))} "
                f"max={mc.max()}"
            )

    def run_selection(
        self,
        all_features: list[str] | None = None,
        checkpoint_path: Path | None = None,
        checkpoint_interval: int | None = None,
    ) -> SelectionResult:
        """Run feature selection phase.

        Args:
            all_features: Features to consider. If None, uses all registered.
            checkpoint_path: Path to write/read forward selection checkpoint.
                Only applies to the forward selection method.
            checkpoint_interval: Override for how often to write checkpoints
                during forward selection. If None, uses selector default.

        Returns:
            SelectionResult with selected features.
        """
        feat_cfg = self.config.discovery.features
        all_features = self._build_candidate_pool(all_features)

        self._log("PHASE 1: Feature Selection")

        base = feat_cfg.base
        method = self.config.discovery.selection_method

        if method == "recursive" and base:
            all_features = list(base)
            self._log(f"Starting recursive elimination from {len(all_features)} base features...")
        else:
            self._log(f"Starting with {len(all_features)} features from registry...")

        if method == "forward":
            scorer = self._create_fast_scorer(all_features)
        else:
            scorer = self._create_scorer()
        importance_fn = self._create_importance_fn(all_features)

        # No-skill baselines for the round 1 feature ranking filter
        # (features that don't beat these are reported as "below baseline")
        no_skill_baselines = {
            "log_loss": 0.693,
            "calibration_error": 0.50,
            "accuracy": 0.50,
            "roc_auc": 0.50,
        }
        round1_baseline = no_skill_baselines.get(self.config.discovery.metric)
        # When MTL is active, the scorer returns multi-task loss (primary
        # log_loss + sum_i weight_i * MSE_std(aux_i)) — different scale than
        # raw primary log_loss. Bump the baseline by the aux contribution:
        # standardized targets have unit variance, so the no-model MSE per
        # aux is ~1.0; multiplied by per-target weights and summed. Without
        # this, all candidates would report as "below baseline" because the
        # 0.693 primary baseline is well under the multi-task loss values.
        if round1_baseline is not None and self.config.mtl is not None:
            aux_weights_sum = sum(
                float((self.config.model.params or {}).get(f"weight_{name}", 0.1))
                for name in self.config.mtl.auxiliary_targets
            )
            round1_baseline = round1_baseline + aux_weights_sum

        selector = FeatureSelector(
            scorer=scorer,
            all_features=all_features,
            method=method,
            direction=self.config.discovery.resolved_direction(),
            importance_fn=importance_fn,
            min_features=feat_cfg.min,
            max_features=feat_cfg.max,
            importance_threshold=self.config.discovery.importance_threshold,
            base_features=base,
            round1_baseline=round1_baseline,
            min_delta=self.config.discovery.min_delta,
        )

        result = selector.run(
            verbose=True,
            checkpoint_path=checkpoint_path,
            checkpoint_interval=checkpoint_interval,
        )

        self._log(f"Selected {len(result.selected_features)} features")
        for step in result.history:
            if step.get("action") == "add":
                self._log(
                    f"  Step {step['step']}: Added {step['feature']} "
                    f"-> {self.config.discovery.metric}={step['metric']:.4f}"
                )
            elif step.get("action") == "stop":
                self._log(f"  Stopped: {step.get('reason', 'no improvement')}")
                best_cand = step.get("best_candidate")
                best_cand_metric = step.get("best_candidate_metric")
                if best_cand is not None and best_cand_metric is not None:
                    self._log(
                        f"  Best rejected candidate: {best_cand} "
                        f"-> {self.config.discovery.metric}={best_cand_metric:.4f}"
                    )

        return result

    def run_sweep(
        self, features: list[str], sweep_params: dict | None = None
    ) -> SweepResult:
        """Run parameter sweep on selected features.

        Args:
            features: Features to sweep.
            sweep_params: Custom sweep params. If None, uses defaults.

        Returns:
            SweepResult with best parameters.
        """
        self._log("PHASE 2: Parameter Tuning")

        if sweep_params is None:
            # Build default sweep params based on features
            sweep_params = {}
            for spec in features:
                name, params = parse_feature_spec(spec)
                if "window_days" in params:
                    sweep_params[name] = {"window_days": [7, 14, 21, 30, 60, 90]}

        if not sweep_params:
            self._log("  No sweepable parameters found")
            return SweepResult(
                best_params={},
                best_metric=0.0,
                all_results=[],
                n_combinations=0,
            )

        # Create temp config with current features
        temp_path = self._create_temp_config(features)

        try:
            sweep = ParameterSweep(
                base_config_path=temp_path,
                sweep_params=sweep_params,
                metric=self.config.discovery.metric,
                direction=self.config.discovery.resolved_direction(),
                matches_path=self.matches_path,
                cache_dir=self.cache_dir,
            )
            result = sweep.run(verbose=True)

            for name, params in result.best_params.items():
                for param, value in params.items():
                    self._log(f"  {name}.{param}: Best={value}")

            return result
        finally:
            temp_path.unlink(missing_ok=True)

    def run_segment_analysis(
        self, features: list[str]
    ) -> dict[str, SegmentImportanceResult]:
        """Run segment-level analysis.

        Args:
            features: Features to analyze.

        Returns:
            Dict mapping segment column to importance result.
        """
        self._log("PHASE 3: Segment Analysis")

        temp_path = self._create_temp_config(features)

        try:
            # Run experiment to get model + data for importance
            result = self._run_experiment(features)
            diagnostics = result.get("diagnostics")

            # Note: Full segment importance requires access to trained model
            # and data, which we don't have direct access to here.
            # This would need refactoring of runner to expose these.
            # For now, we return segment metrics from diagnostics.

            self._log("  Feature importance by circuit:")
            if diagnostics and "circuit" in diagnostics.segments:
                for circuit, metrics in diagnostics.segments["circuit"].items():
                    self._log(f"    {circuit}: accuracy={metrics.get('accuracy', 0):.3f}")

            return {}  # Simplified for now

        finally:
            temp_path.unlink(missing_ok=True)

    def _run_stability_workflow(self) -> DiscoveryResult:
        """Run the stability-selection workflow and assemble a DiscoveryResult."""
        stability_result = self.run_stability()
        ni_result = getattr(self, "_null_importance_result", None)
        selected = stability_result.selected_features

        if not selected:
            self._log(
                "No features cleared the stability threshold "
                f"(π={stability_result.threshold}). Inspect the profile above and "
                "lower selection_threshold if appropriate."
            )
            self._last_result = DiscoveryResult(
                selected_features=[],
                stability_result=stability_result,
                null_importance_result=ni_result,
                n_experiments=self._experiment_count,
            )
            return self._last_result

        # Score the selected set once for reporting (logs to MLflow).
        final_result = self._run_experiment(selected, log_to_mlflow=True)
        final_metric = final_result["metrics"].get(self.config.discovery.metric, 0.0)

        self._log("")
        self._log("RESULTS")
        self._log("-" * 30)
        self._log(f"Stability-selected feature set ({len(selected)} features):")
        for f in selected:
            self._log(f"  - {f}  ({stability_result.selection_frequency[f]:.2f})")
        self._log(f"Final {self.config.discovery.metric}: {final_metric:.4f}")

        self._last_result = DiscoveryResult(
            selected_features=selected,
            stability_result=stability_result,
            null_importance_result=ni_result,
            final_metric=final_metric,
            n_experiments=self._experiment_count,
        )
        return self._last_result

    def run(
        self,
        checkpoint_path: Path | None = None,
        checkpoint_interval: int | None = None,
    ) -> DiscoveryResult:
        """Run complete discovery workflow.

        Args:
            checkpoint_path: Path to write/read forward selection checkpoint.
            checkpoint_interval: Override for checkpoint write frequency.

        Returns:
            DiscoveryResult with all findings.
        """
        self._log(f"Discovery: {self.config_path.stem}")
        self._log("=" * 60)

        # Stability selection is a distinct Phase 1 that supersedes the single
        # forward-selection pass; it returns its own feature set and profile.
        if self.config.discovery.stability_selection is not None:
            return self._run_stability_workflow()

        # Phase 1: Selection
        selection_result = self.run_selection(
            checkpoint_path=checkpoint_path,
            checkpoint_interval=checkpoint_interval,
        )
        selected = selection_result.selected_features

        if not selected:
            self._log("No features selected. Check your data and configuration.")
            self._last_result = DiscoveryResult(
                selected_features=[],
                selection_result=selection_result,
                n_experiments=self._experiment_count,
            )
            return self._last_result

        # Phase 2: Sweeps (optional)
        sweep_result = None
        final_features = selected

        if self.config.discovery.sweep_params:
            sweep_result = self.run_sweep(selected)
            if sweep_result.best_params:
                # Apply best params to features
                from mvp.model.discovery.sweeps import ParameterSweep

                temp_sweep = ParameterSweep(
                    base_config_path=self.config_path,
                    sweep_params={},
                )
                final_features = temp_sweep._apply_params(
                    selected, sweep_result.best_params
                )

        # Phase 3: Segment analysis (optional)
        segment_importance = {}
        if self.config.discovery.segment_analysis:
            segment_importance = self.run_segment_analysis(final_features)

        # Get final metric (this one logs to MLflow)
        final_result = self._run_experiment(final_features, log_to_mlflow=True)
        final_metric = final_result["metrics"].get(
            self.config.discovery.metric, 0.0
        )

        # Summary
        self._log("")
        self._log("RESULTS")
        self._log("-" * 30)
        self._log(f"Feature set ({len(final_features)} features):")
        for f in final_features:
            self._log(f"  - {f}")
        self._log(f"Final {self.config.discovery.metric}: {final_metric:.4f}")
        self._log(f"Total experiments: {self._experiment_count}")

        self._last_result = DiscoveryResult(
            selected_features=final_features,
            selection_result=selection_result,
            sweep_result=sweep_result,
            segment_importance=segment_importance,
            final_metric=final_metric,
            n_experiments=self._experiment_count,
        )
        return self._last_result

    def save_config(self, output_path: Path | str) -> None:
        """Save discovered config to file.

        Args:
            output_path: Path to save YAML config.
        """
        if not hasattr(self, "_last_result"):
            raise RuntimeError("Run discovery first before saving config")

        result = self._last_result
        config_dict = self.config.to_experiment_config_dict(result.selected_features)

        # Add selection history for post-hoc analysis
        if result.selection_result and result.selection_result.history:
            history_entries = []
            prev_metric = None
            for step in result.selection_result.history:
                if step.get("action") not in ("add", "base"):
                    continue
                metric = step["metric"]
                delta = round(metric - prev_metric, 6) if prev_metric is not None else None
                entry: dict[str, Any] = {
                    "step": step["step"],
                    "feature": step.get("feature", "(base)"),
                    "metric": round(metric, 6),
                }
                if delta is not None:
                    entry["delta"] = delta
                history_entries.append(entry)
                prev_metric = metric
            config_dict["selection_history"] = history_entries

        with open(output_path, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False)

        self._log(f"Saved config to: {output_path}")
