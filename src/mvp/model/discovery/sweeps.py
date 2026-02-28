"""Parameter sweep for feature tuning."""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


@dataclass
class SweepResult:
    """Result from parameter sweep."""

    best_params: dict[str, Any]
    best_metric: float
    all_results: list[dict[str, Any]]
    n_combinations: int


@dataclass
class SweepConfig:
    """Configuration for parameter sweep."""

    base_config_path: Path
    sweep_params: dict[str, dict[str, list[Any]]]
    metric: str = "calibration_error"
    direction: Literal["minimize", "maximize"] = "minimize"
    max_combinations: int | None = None


def parse_feature_spec(spec: str) -> tuple[str, dict[str, Any]]:
    """Parse feature spec string into name and params.

    Args:
        spec: Feature specification like "win_rate(window_days=30)".

    Returns:
        Tuple of (feature_name, params_dict).

    Examples:
        >>> parse_feature_spec("win_rate(window_days=30)")
        ('win_rate', {'window_days': 30})
        >>> parse_feature_spec("h2h_record()")
        ('h2h_record', {})
    """
    match = re.match(r"(\w+)\((.*)\)", spec)
    if not match:
        return spec, {}

    name = match.group(1)
    params_str = match.group(2).strip()

    if not params_str:
        return name, {}

    params = {}
    for param in params_str.split(","):
        param = param.strip()
        if "=" in param:
            key, value = param.split("=", 1)
            key = key.strip()
            value = value.strip()
            # Try to parse value
            if value.lower() == "true":
                params[key] = True
            elif value.lower() == "false":
                params[key] = False
            else:
                try:
                    params[key] = int(value)
                except ValueError:
                    try:
                        params[key] = float(value)
                    except ValueError:
                        params[key] = value

    return name, params


def build_feature_spec(name: str, params: dict[str, Any]) -> str:
    """Build feature spec string from name and params.

    Args:
        name: Feature name.
        params: Parameter dictionary.

    Returns:
        Feature specification string.

    Examples:
        >>> build_feature_spec("win_rate", {"window_days": 30})
        'win_rate(window_days=30)'
    """
    if not params:
        return f"{name}()"

    param_strs = [f"{k}={v}" for k, v in sorted(params.items())]
    return f"{name}({', '.join(param_strs)})"


class ParameterSweep:
    """Grid search over feature parameters.

    Sweeps over all combinations of specified parameter values
    and identifies the best configuration.
    """

    def __init__(
        self,
        base_config_path: Path | str,
        sweep_params: dict[str, dict[str, list[Any]]],
        metric: str = "calibration_error",
        direction: Literal["minimize", "maximize"] = "minimize",
        max_combinations: int | None = None,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        """Initialize parameter sweep.

        Args:
            base_config_path: Path to base experiment config.
            sweep_params: Nested dict of feature -> param -> values to try.
                Example: {"win_rate": {"window_days": [7, 14, 30, 60]}}
            metric: Metric to optimize.
            direction: Whether to minimize or maximize.
            max_combinations: Maximum combinations to try (None = no limit).
            matches_path: Path to matches.parquet.
            cache_dir: Path to feature cache.
        """
        self.base_config_path = Path(base_config_path)
        self.sweep_params = sweep_params
        self.metric = metric
        self.direction = direction
        self.max_combinations = max_combinations
        self.matches_path = matches_path
        self.cache_dir = cache_dir

    def _generate_combinations(self) -> list[dict[str, dict[str, Any]]]:
        """Generate all parameter combinations.

        Returns:
            List of param dicts, each mapping feature -> param -> value.
        """
        if not self.sweep_params:
            return [{}]

        # Build list of (feature, param, values) tuples
        sweep_items: list[tuple[str, str, list[Any]]] = []
        for feature, params in self.sweep_params.items():
            for param, values in params.items():
                sweep_items.append((feature, param, values))

        if not sweep_items:
            return [{}]

        # Generate cartesian product
        all_values = [item[2] for item in sweep_items]
        combinations = list(itertools.product(*all_values))

        # Limit if needed
        if self.max_combinations and len(combinations) > self.max_combinations:
            combinations = combinations[:self.max_combinations]

        # Convert to dict format
        result = []
        for combo in combinations:
            param_dict: dict[str, dict[str, Any]] = {}
            for i, value in enumerate(combo):
                feature, param, _ = sweep_items[i]
                if feature not in param_dict:
                    param_dict[feature] = {}
                param_dict[feature][param] = value
            result.append(param_dict)

        return result

    def _apply_params(
        self,
        features: list[str],
        param_combo: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Apply parameter values to feature list.

        Args:
            features: Original feature specs.
            param_combo: Parameter values to apply.

        Returns:
            Modified feature specs with new parameter values.
        """
        result = []
        for spec in features:
            name, params = parse_feature_spec(spec)

            if name in param_combo:
                # Update params with sweep values
                params = {**params, **param_combo[name]}

            result.append(build_feature_spec(name, params))

        return result

    def _is_better(self, new_val: float, old_val: float) -> bool:
        """Check if new value is better than old."""
        if self.direction == "minimize":
            return new_val < old_val
        return new_val > old_val

    def run(self, verbose: bool = False) -> SweepResult:
        """Run the parameter sweep.

        Args:
            verbose: Print progress.

        Returns:
            SweepResult with best parameters and all results.
        """
        from mvp.model.config import ExperimentConfig
        from mvp.model.runner import ExperimentRunner
        import tempfile
        import yaml

        base_config = ExperimentConfig.from_file(str(self.base_config_path))
        combinations = self._generate_combinations()

        if verbose:
            print(f"Testing {len(combinations)} parameter combinations...")

        all_results: list[dict[str, Any]] = []
        best_metric = float("inf") if self.direction == "minimize" else float("-inf")
        best_params: dict[str, Any] = {}

        for i, param_combo in enumerate(combinations):
            # Apply params to features
            modified_features = self._apply_params(
                base_config.features.include,
                param_combo,
            )

            # Create modified config
            config_dict = base_config.model_dump()
            config_dict["features"]["include"] = modified_features
            config = ExperimentConfig.model_validate(config_dict)

            # Run experiment
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            ) as f:
                yaml.dump(config.model_dump(), f, default_flow_style=False)
                temp_path = f.name

            try:
                runner = ExperimentRunner(
                    config_path=temp_path,
                    matches_path=self.matches_path,
                    cache_dir=self.cache_dir,
                )
                result = runner.run()
                metric_value = result["metrics"].get(self.metric, float("inf"))

                all_results.append({
                    "params": param_combo,
                    "features": modified_features,
                    "metric": metric_value,
                    "all_metrics": result["metrics"],
                })

                if self._is_better(metric_value, best_metric):
                    best_metric = metric_value
                    best_params = param_combo

                if verbose:
                    print(f"  [{i+1}/{len(combinations)}] {param_combo} -> {self.metric}={metric_value:.4f}")

            except Exception as e:
                all_results.append({
                    "params": param_combo,
                    "features": modified_features,
                    "error": str(e),
                })
                if verbose:
                    print(f"  [{i+1}/{len(combinations)}] {param_combo} -> ERROR: {e}")

            finally:
                Path(temp_path).unlink(missing_ok=True)

        # Sort results by metric
        valid_results = [r for r in all_results if "metric" in r]
        if valid_results:
            valid_results.sort(
                key=lambda r: r["metric"],
                reverse=(self.direction == "maximize"),
            )

        return SweepResult(
            best_params=best_params,
            best_metric=best_metric,
            all_results=all_results,
            n_combinations=len(combinations),
        )


# Default sweep ranges for common feature parameters
DEFAULT_SWEEP_RANGES: dict[str, dict[str, list[Any]]] = {
    "window_days": {
        "window_days": [7, 14, 21, 30, 60, 90, 180],
    },
    "cap": {
        "cap": [5, 8, 10, 15, 20],
    },
}
