"""Feature selection algorithms."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal

import numpy as np

from mvp.model.discovery.importance import compute_importance


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
        """
        self.scorer = scorer
        self.all_features = list(all_features)
        self.method = method
        self.direction = direction
        self.min_features = min_features
        self.max_features = max_features or len(all_features)
        self.importance_threshold = importance_threshold
        self.importance_fn = importance_fn

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

    def forward_selection(self) -> SelectionResult:
        """Select features by iteratively adding the best one.

        Starts with empty set, adds feature that improves metric most,
        repeats until no improvement.
        """
        selected: list[str] = []
        remaining = set(self.all_features)
        history: list[dict[str, Any]] = []

        # Baseline: empty feature set (use worst value)
        best_metric = self._worst_value()

        while remaining and len(selected) < self.max_features:
            best_feature = None
            best_feature_metric = best_metric

            # Try adding each remaining feature
            for feature in remaining:
                candidate = selected + [feature]
                try:
                    metric = self.scorer(candidate)
                except Exception:
                    continue

                if self._is_better(metric, best_feature_metric):
                    best_feature = feature
                    best_feature_metric = metric

            # If no improvement, stop
            if best_feature is None or not self._is_better(
                best_feature_metric, best_metric
            ):
                history.append({
                    "step": len(history) + 1,
                    "action": "stop",
                    "reason": "no improvement",
                    "metric": best_metric,
                })
                break

            # Add best feature
            selected.append(best_feature)
            remaining.remove(best_feature)
            best_metric = best_feature_metric

            history.append({
                "step": len(history) + 1,
                "action": "add",
                "feature": best_feature,
                "metric": best_metric,
            })

        return SelectionResult(
            selected_features=selected,
            excluded_features=list(remaining),
            history=history,
            final_metric=best_metric if best_metric != self._worst_value() else 0.0,
        )

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
        """
        if self.importance_fn is None:
            raise ValueError("threshold selection requires importance_fn")

        # Compute importance with all features
        importance = self.importance_fn(self.all_features)

        # Filter by threshold
        selected = [
            f for f in self.all_features
            if importance.get(f, 0) >= self.importance_threshold
        ]

        # Ensure minimum features
        if len(selected) < self.min_features:
            sorted_features = sorted(
                self.all_features,
                key=lambda f: importance.get(f, 0),
                reverse=True,
            )
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

    def run(self) -> SelectionResult:
        """Run feature selection using configured method.

        Returns:
            SelectionResult with selected features and history.
        """
        if self.method == "forward":
            return self.forward_selection()
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
