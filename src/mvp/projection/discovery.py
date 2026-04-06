"""Feature discovery for game projection."""


import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from mvp.model.discovery.discover import get_all_feature_specs
from mvp.model.discovery.selection import FeatureSelector, SelectionResult
from mvp.projection.config import ProjectionDiscoveryConfig

logger = logging.getLogger(__name__)


@dataclass
class ProjectionDiscoveryResult:
    """Result from projection feature discovery."""

    selected_features: list[str]
    selection_result: SelectionResult | None = None
    final_metric: float = 0.0
    n_experiments: int = 0


class ProjectionDiscovery:
    """Orchestrates feature discovery for game projection.

    Runs forward selection with regression scoring (MAE) to find
    optimal features for the projection pipeline.
    """

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
        verbose: bool = False,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = ProjectionDiscoveryConfig.from_file(config_path)
        self.matches_path = matches_path
        self.cache_dir = cache_dir
        self.mlflow_dir = mlflow_dir
        self.verbose = verbose
        self._experiment_count = 0

    def _log(self, msg: str) -> None:
        logger.info(msg)

    def _create_temp_config(self, features: list[str]) -> Path:
        """Create temporary projection config with given features."""
        config_dict = self.config.to_projection_config_dict(features)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(config_dict, f, default_flow_style=False)
            return Path(f.name)

    def _run_experiment(
        self, features: list[str], log_to_mlflow: bool = False
    ) -> dict[str, Any]:
        """Run projection experiment with given features."""
        from mvp.projection.runner import ProjectionRunner

        temp_path = self._create_temp_config(features)
        try:
            runner = ProjectionRunner(
                config_path=temp_path,
                matches_path=self.matches_path,
                cache_dir=self.cache_dir,
                mlflow_dir=self.mlflow_dir,
                workflow="projection_discovery",
                run_name=self.config_path.stem,
                log_to_mlflow=log_to_mlflow,
            )
            result = runner.run()
            self._experiment_count += 1
            return result
        finally:
            temp_path.unlink(missing_ok=True)

    def _create_fast_scorer(self, all_features: list[str]) -> callable:
        """Create a fast scorer that precomputes all features once."""
        from mvp.projection.fast_selection import FastProjectionSelector

        target_metric = self.config.discovery.metric
        fast = FastProjectionSelector(
            config=self.config,
            all_feature_specs=all_features,
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )
        fast.precompute()
        return fast.create_scorer(target_metric)

    def _create_slow_scorer(self) -> callable:
        """Create scorer that runs full ProjectionRunner per candidate."""
        target_metric = self.config.discovery.metric
        engine_logger = logging.getLogger("mvp.model.engine")
        runner_logger = logging.getLogger("mvp.projection.runner")

        def scorer(features: list[str]) -> float:
            if not features:
                return float("inf")
            prev_engine = engine_logger.level
            prev_runner = runner_logger.level
            engine_logger.setLevel(logging.WARNING)
            runner_logger.setLevel(logging.WARNING)
            try:
                result = self._run_experiment(features)
                return result["metrics"].get(target_metric, float("inf"))
            except Exception as e:
                if self.verbose:
                    self._log(f"    FAILED {features}: {e}")
                return float("inf")
            finally:
                engine_logger.setLevel(prev_engine)
                runner_logger.setLevel(prev_runner)

        return scorer

    def run(self) -> ProjectionDiscoveryResult:
        """Run projection feature discovery.

        Returns:
            ProjectionDiscoveryResult with selected features.
        """
        self._log(f"Projection Discovery: {self.config_path.stem}")
        self._log("=" * 60)

        feat_cfg = self.config.discovery.features
        all_features = get_all_feature_specs(
            window_sizes=feat_cfg.window_sizes
        )

        if feat_cfg.include:
            included = set(feat_cfg.include)
            all_features = [f for f in all_features if f in included]
            self._log(f"Restricted to {len(all_features)} features via include")

        if feat_cfg.exclude:
            excluded = set(feat_cfg.exclude)
            all_features = [f for f in all_features if f not in excluded]
            self._log(f"Excluding {len(excluded)} features")

        method = self.config.discovery.selection_method
        base = feat_cfg.base

        if method == "forward":
            self._log(f"Precomputing {len(all_features)} features for fast forward selection...")
            scorer = self._create_fast_scorer(all_features)
        else:
            self._log(f"Starting {method} selection from {len(all_features)} features...")
            scorer = self._create_slow_scorer()

        selector = FeatureSelector(
            scorer=scorer,
            all_features=all_features,
            method=method,
            direction=self.config.discovery.direction,
            min_features=feat_cfg.min,
            max_features=feat_cfg.max,
            importance_threshold=self.config.discovery.importance_threshold,
            base_features=base,
        )

        selection_result = selector.run(verbose=True)
        selected = selection_result.selected_features

        if not selected:
            self._log("No features selected.")
            return ProjectionDiscoveryResult(
                selected_features=[],
                selection_result=selection_result,
                n_experiments=self._experiment_count,
            )

        # Final run with selected features (logs to MLflow)
        final_result = self._run_experiment(selected, log_to_mlflow=True)
        final_metric = final_result["metrics"].get(
            self.config.discovery.metric, 0.0
        )

        # Round 1 feature ranking (if available)
        if selection_result and selection_result.history:
            round_1 = selection_result.history[0]
            if round_1.get("action") == "add" and "round_ranking" in round_1:
                ranking = round_1["round_ranking"]
                self._log("")
                self._log(f"ROUND 1 FEATURE RANKING ({len(ranking)} features)")
                self._log("-" * 50)
                for i, (feat, metric) in enumerate(ranking, 1):
                    self._log(f"  {i:3}. {feat}: {metric:.4f}")

        self._log("")
        self._log("RESULTS")
        self._log("-" * 30)
        self._log(f"Feature set ({len(selected)} features):")
        for f in selected:
            self._log(f"  - {f}")
        self._log(f"Final {self.config.discovery.metric}: {final_metric:.4f}")
        self._log(f"Total experiments: {self._experiment_count}")

        return ProjectionDiscoveryResult(
            selected_features=selected,
            selection_result=selection_result,
            final_metric=final_metric,
            n_experiments=self._experiment_count,
        )

    def save_config(self, output_path: Path | str, result: ProjectionDiscoveryResult) -> None:
        """Save discovered config to file."""
        config_dict = self.config.to_projection_config_dict(result.selected_features)
        with open(output_path, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False)
        self._log(f"Saved config to: {output_path}")
