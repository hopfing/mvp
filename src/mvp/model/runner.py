"""Experiment runner for training and evaluating models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from mvp.model.config import ExperimentConfig
from mvp.model.diagnostics import Diagnostics
from mvp.model.engine import FeatureEngine, get_feature_columns
from mvp.model.metrics import compute_metrics
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.models import get_model
from mvp.model.splitters import (
    BaseSplitter,
    ExpandingWindowSplitter,
    WalkForwardSplitter,
)


class ExperimentRunner:
    """Runner for executing experiments."""

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
    ) -> None:
        """Initialize runner.

        Args:
            config_path: Path to experiment config YAML.
            matches_path: Path to matches.parquet.
            cache_dir: Optional cache directory for features.
            mlflow_dir: Optional MLflow tracking directory.
        """
        self.config = ExperimentConfig.from_file(str(config_path))
        self.matches_path = Path(
            matches_path or "data/aggregate/atptour/matches.parquet"
        )
        self.cache_dir = Path(cache_dir or "data/features/cache")
        self.mlflow_dir = Path(mlflow_dir) if mlflow_dir else None

        self.engine = FeatureEngine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )

    def _get_splitter(self) -> BaseSplitter:
        """Get the appropriate splitter for validation strategy."""
        val = self.config.validation
        if val.type == "walk_forward":
            return WalkForwardSplitter(
                n_splits=val.n_splits,
                min_train_size=val.min_train_size,
                test_size=val.test_size,
            )
        elif val.type == "expanding_window":
            if val.initial_train_size is None or val.step_size is None:
                raise ValueError(
                    "expanding_window requires initial_train_size and step_size"
                )
            return ExpandingWindowSplitter(
                initial_train_size=val.initial_train_size,
                step_size=val.step_size,
            )
        else:
            raise ValueError(f"Unknown validation type: {val.type}")

    def run(self) -> dict[str, Any]:
        """Execute the experiment.

        Returns:
            Dictionary with metrics and metadata.
        """
        import mlflow

        if self.mlflow_dir:
            # Use forward slashes for file URI on all platforms
            mlflow_uri = f"file:///{str(self.mlflow_dir).replace(chr(92), '/')}"
            mlflow.set_tracking_uri(mlflow_uri)

        logger = ExperimentLogger(experiment_name=self.config.name)

        # Compute features
        df = self.engine.compute(self.config.features.include)

        # Filter by date range
        df = df.filter(
            (pl.col("effective_match_date") >= self.config.data.date_range.start)
            & (pl.col("effective_match_date") <= self.config.data.date_range.end)
        )

        # Get feature columns from config
        feature_cols = get_feature_columns(self.config.features.include)

        if not feature_cols:
            raise ValueError("No feature columns found after computing features")

        # Get splitter
        splitter = self._get_splitter()

        # Train and evaluate
        all_metrics: list[dict[str, float]] = []
        all_predictions: list[dict[str, Any]] = []

        with logger.start_run(run_name=self.config.name):
            # Log config as params
            logger.log_params(
                {
                    "model_type": self.config.model.type,
                    "validation_type": self.config.validation.type,
                    "n_features": len(feature_cols),
                    "n_splits": self.config.validation.n_splits,
                }
            )

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

                # Handle missing values with median imputation
                medians = np.nanmedian(X_train, axis=0)
                X_train = np.where(np.isnan(X_train), medians, X_train)
                X_test = np.where(np.isnan(X_test), medians, X_test)

                # Train model
                model = get_model(
                    self.config.model.type,
                    self.config.model.params or {},
                )
                model.fit(X_train, y_train)

                # Predict and evaluate
                y_prob = model.predict_proba(X_test)
                metrics = compute_metrics(y_test, y_prob)
                all_metrics.append(metrics)

                # Collect predictions for diagnostics
                all_predictions.append({
                    "df": test_df,
                    "y_true": y_test,
                    "y_prob": y_prob,
                })

                # Log fold metrics
                logger.log_metrics(
                    {f"fold_{fold_idx}_{k}": v for k, v in metrics.items()}
                )

            # Average metrics across folds
            avg_metrics = {
                k: float(np.mean([m[k] for m in all_metrics]))
                for k in all_metrics[0].keys()
            }
            logger.log_metrics(avg_metrics)

            # Compute diagnostics
            diagnostics = Diagnostics()
            diagnostic_results = diagnostics.compute_all(all_predictions)

            # Log diagnostic metrics
            logger.log_metrics(diagnostic_results.metrics)

            # Merge diagnostic metrics (calibration_error, etc.) into avg_metrics
            avg_metrics.update(diagnostic_results.metrics)

            # Log diagnostic JSON artifact
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                f.write(diagnostic_results.to_json())
                temp_path = f.name
            logger.log_artifact(temp_path)

            run_id = logger.run_id

        return {
            "metrics": avg_metrics,
            "fold_metrics": all_metrics,
            "n_folds": len(all_metrics),
            "feature_columns": feature_cols,
            "run_id": run_id,
            "diagnostics": diagnostic_results,
        }
