"""Experiment runner for training and evaluating models."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

# Suppress numpy warnings about all-NaN slices during median imputation
warnings.filterwarnings("ignore", message="All-NaN slice encountered")

from mvp.model.config import EnsembleParams, ExperimentConfig
from mvp.model.diagnostics import Diagnostics, EnsembleDiagnostics
from mvp.model.engine import FeatureEngine, get_feature_columns
from mvp.model.metrics import compute_metrics
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.models import EnsembleModel, get_model
from mvp.model.splitters import (
    BaseSplitter,
    ExpandingWindowSplitter,
    SlidingWindowSplitter,
)


class ExperimentRunner:
    """Runner for executing experiments."""

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
        workflow: str = "training",
        run_name: str | None = None,
        log_to_mlflow: bool = True,
    ) -> None:
        """Initialize runner.

        Args:
            config_path: Path to experiment config YAML.
            matches_path: Path to matches.parquet.
            cache_dir: Optional cache directory for features.
            mlflow_dir: Optional MLflow tracking directory.
            workflow: MLflow experiment name ("training" or "discovery").
            run_name: Override for MLflow run name. Defaults to filename.
            log_to_mlflow: Whether to log to MLflow. Set False for intermediate runs.
        """
        self.config_path = Path(config_path)
        self.config = ExperimentConfig.from_file(str(config_path))
        self.matches_path = Path(
            matches_path or "data/aggregate/atptour/matches.parquet"
        )
        self.cache_dir = Path(cache_dir or "data/features/cache")
        self.mlflow_dir = Path(mlflow_dir) if mlflow_dir else None
        self.workflow = workflow
        self.run_name = run_name or self.config_path.stem
        self.log_to_mlflow = log_to_mlflow

        self.engine = FeatureEngine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )

    def _get_splitter(self) -> BaseSplitter:
        """Get the appropriate splitter for validation strategy."""
        val = self.config.validation
        if val.type == "walk_forward":
            # n_splits mode of ExpandingWindowSplitter
            return ExpandingWindowSplitter(
                n_splits=val.n_splits,
                min_train_size=val.min_train_size,
                test_size=val.test_size,
            )
        elif val.type == "expanding_window":
            # step_size mode of ExpandingWindowSplitter
            if val.initial_train_size is None or val.step_size is None:
                raise ValueError(
                    "expanding_window requires initial_train_size and step_size"
                )
            return ExpandingWindowSplitter(
                initial_train_size=val.initial_train_size,
                step_size=val.step_size,
            )
        elif val.type == "sliding_window":
            if val.train_size is None:
                raise ValueError("sliding_window requires train_size")
            return SlidingWindowSplitter(
                train_size=val.train_size,
                test_size=val.test_size,
                step_size=val.step_size,
            )
        else:
            raise ValueError(f"Unknown validation type: {val.type}")

    def _resolve_ensemble(self) -> tuple[list[str], list[dict[str, Any]]]:
        """Resolve ensemble config into union features and base model specs.

        Returns:
            (union_feature_specs, base_model_specs) where each spec has
            type, params, weight, feature_indices.
        """
        ensemble_params = EnsembleParams.model_validate(self.config.model.params)

        all_feature_specs: list[str] = []
        base_model_specs: list[dict[str, Any]] = []

        for ref in ensemble_params.base_models:
            base_config = ExperimentConfig.from_file(ref.config)
            if base_config.features is None:
                raise ValueError(f"Base model {ref.config} has no features section")
            for spec in base_config.features.include:
                if spec not in all_feature_specs:
                    all_feature_specs.append(spec)
            base_model_specs.append({
                "type": base_config.model.type,
                "params": base_config.model.params or {},
                "weight": ref.weight,
                "feature_specs": base_config.features.include,
            })

        union_cols = get_feature_columns(all_feature_specs)
        for spec in base_model_specs:
            base_cols = get_feature_columns(spec["feature_specs"])
            spec["feature_indices"] = [union_cols.index(c) for c in base_cols]
            del spec["feature_specs"]

        return all_feature_specs, base_model_specs

    def run(self) -> dict[str, Any]:
        """Execute the experiment.

        Returns:
            Dictionary with metrics and metadata.
        """
        import mlflow

        if self.log_to_mlflow:
            if self.mlflow_dir:
                mlflow_uri = f"file:///{str(self.mlflow_dir).replace(chr(92), '/')}"
                mlflow.set_tracking_uri(mlflow_uri)
            logger = ExperimentLogger(experiment_name=self.workflow)
        else:
            logger = None

        # Resolve ensemble or standard features
        is_ensemble = self.config.model.type == "ensemble"
        base_model_specs: list[dict[str, Any]] | None = None
        if is_ensemble:
            feature_specs, base_model_specs = self._resolve_ensemble()
        else:
            assert self.config.features is not None
            feature_specs = self.config.features.include

        # Compute features
        df = self.engine.compute(feature_specs)

        # Filter by date range
        df = df.filter(
            (pl.col("effective_match_date") >= self.config.data.date_range.start)
            & (pl.col("effective_match_date") <= self.config.data.date_range.end)
        )

        # Apply additional filters (e.g., draw_type: "singles")
        # These are applied AFTER feature computation so workload features
        # include doubles appearances before filtering to singles-only
        if self.config.data.filters:
            for col, value in self.config.data.filters.items():
                if isinstance(value, list):
                    df = df.filter(pl.col(col).is_in(value))
                else:
                    df = df.filter(pl.col(col) == value)

        # Drop rows with no outcome (e.g., future/unfinished matches)
        df = df.filter(pl.col("won").is_not_null())

        # Get feature columns from config
        feature_cols = get_feature_columns(feature_specs)

        if not feature_cols:
            raise ValueError("No feature columns found after computing features")

        # Get splitter
        splitter = self._get_splitter()

        # Train and evaluate
        all_metrics: list[dict[str, float]] = []
        all_train_metrics: list[dict[str, float]] = []
        all_predictions: list[dict[str, Any]] = []
        all_per_model_predictions: list[list[np.ndarray]] = [] if is_ensemble else []

        run_context = logger.start_run(run_name=self.run_name) if logger else None
        if run_context:
            run_context.__enter__()
            logger.log_params({
                "model_type": self.config.model.type,
                "validation_type": self.config.validation.type,
                "n_features": len(feature_cols),
                "n_splits": self.config.validation.n_splits,
            })

        try:
            for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(df)):
                train_df = df[train_idx]
                test_df = df[test_idx]

                X_train = train_df.select(
                    pl.col(c).cast(pl.Float64) for c in feature_cols
                ).to_numpy()
                y_train = train_df["won"].to_numpy().astype(int)
                X_test = test_df.select(
                    pl.col(c).cast(pl.Float64) for c in feature_cols
                ).to_numpy()
                y_test = test_df["won"].to_numpy().astype(int)

                # Handle missing values with median imputation (0 for all-NaN cols)
                medians = np.nanmedian(X_train, axis=0)
                medians = np.where(np.isnan(medians), 0.0, medians)
                X_train = np.where(np.isnan(X_train), medians, X_train)
                X_test = np.where(np.isnan(X_test), medians, X_test)

                # Train model
                model = get_model(
                    self.config.model.type,
                    self.config.model.params or {},
                )
                if is_ensemble and base_model_specs is not None:
                    assert isinstance(model, EnsembleModel)
                    model.configure(base_model_specs)
                model.fit(X_train, y_train)

                # Predict and evaluate on test
                y_prob = model.predict_proba(X_test)
                metrics = compute_metrics(y_test, y_prob)
                all_metrics.append(metrics)

                # Predict and evaluate on train (for overfitting detection)
                y_prob_train = model.predict_proba(X_train)
                train_metrics = compute_metrics(y_train, y_prob_train)
                all_train_metrics.append(train_metrics)

                # Collect predictions for diagnostics
                all_predictions.append({
                    "df": test_df,
                    "y_true": y_test,
                    "y_prob": y_prob,
                })

                if is_ensemble and isinstance(model, EnsembleModel):
                    all_per_model_predictions.append(
                        model.predict_proba_per_model(X_test)
                    )

                # Log fold metrics
                if logger:
                    logger.log_metrics(
                        {f"fold_{fold_idx}_{k}": v for k, v in metrics.items()}
                    )

            # Average metrics across folds
            avg_metrics = {
                k: float(np.mean([m[k] for m in all_metrics]))
                for k in all_metrics[0].keys()
            }
            avg_train_metrics = {
                k: float(np.mean([m[k] for m in all_train_metrics]))
                for k in all_train_metrics[0].keys()
            }

            # Compute diagnostics
            diagnostics = Diagnostics()
            diagnostic_results = diagnostics.compute_all(all_predictions)

            # Compute ensemble-specific diagnostics
            ensemble_diagnostic_results = None
            if is_ensemble and all_per_model_predictions:
                n_base = len(all_per_model_predictions[0])
                per_model_preds = [
                    np.concatenate([fold[i] for fold in all_per_model_predictions])
                    for i in range(n_base)
                ]
                combined_y_true = np.concatenate(
                    [p["y_true"] for p in all_predictions]
                )
                combined_y_prob = np.concatenate(
                    [p["y_prob"] for p in all_predictions]
                )
                assert base_model_specs is not None
                ensemble_params = EnsembleParams.model_validate(
                    self.config.model.params
                )
                weights = np.array([s["weight"] for s in base_model_specs])
                weights = weights / weights.sum()
                base_names = [
                    ref.config for ref in ensemble_params.base_models
                ]
                ediag = EnsembleDiagnostics()
                ensemble_diagnostic_results = ediag.compute(
                    combined_y_true,
                    combined_y_prob,
                    per_model_preds,
                    weights,
                    base_names,
                    strategy=ensemble_params.strategy,
                )
                diagnostic_results.ensemble = ensemble_diagnostic_results

            # Merge diagnostic metrics (calibration_error, etc.) into avg_metrics
            avg_metrics.update(diagnostic_results.metrics)

            run_id = None
            if logger:
                logger.log_metrics(avg_metrics)
                logger.log_metrics({f"train_{k}": v for k, v in avg_train_metrics.items()})
                logger.log_metrics(diagnostic_results.metrics)

                # Log diagnostic JSON artifact
                import tempfile
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as f:
                    f.write(diagnostic_results.to_json())
                    temp_path = f.name
                logger.log_artifact(temp_path)
                run_id = logger.run_id

        finally:
            if run_context:
                run_context.__exit__(None, None, None)

        return {
            "metrics": avg_metrics,
            "train_metrics": avg_train_metrics,
            "fold_metrics": all_metrics,
            "n_folds": len(all_metrics),
            "feature_columns": feature_cols,
            "run_id": run_id,
            "diagnostics": diagnostic_results,
            "last_fold_model": model,
            "last_fold_X_test": X_test,
            "last_fold_y_test": y_test,
        }
