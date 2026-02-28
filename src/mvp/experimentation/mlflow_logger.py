"""MLflow logging for experiments."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import mlflow


class ExperimentLogger:
    """Logger for experiment tracking with MLflow."""

    def __init__(self, experiment_name: str) -> None:
        """Initialize logger.

        Args:
            experiment_name: Name of the MLflow experiment.
        """
        self.experiment_name = experiment_name
        mlflow.set_experiment(experiment_name)
        self._run: mlflow.ActiveRun | None = None

    @contextmanager
    def start_run(self, run_name: str | None = None) -> Iterator[None]:
        """Start an MLflow run.

        Args:
            run_name: Optional name for the run.
        """
        with mlflow.start_run(run_name=run_name) as run:
            self._run = run
            try:
                yield
            finally:
                self._run = None

    def log_params(self, params: dict[str, Any]) -> None:
        """Log parameters.

        Args:
            params: Dictionary of parameter name -> value.
        """
        mlflow.log_params(params)

    def log_metrics(
        self, metrics: dict[str, float], step: int | None = None
    ) -> None:
        """Log metrics.

        Args:
            metrics: Dictionary of metric name -> value.
            step: Optional step number for tracking over time.
        """
        mlflow.log_metrics(metrics, step=step)

    def log_artifact(self, local_path: str) -> None:
        """Log an artifact file.

        Args:
            local_path: Path to the file to log.
        """
        mlflow.log_artifact(local_path)

    @property
    def run_id(self) -> str | None:
        """Get current run ID."""
        return self._run.info.run_id if self._run else None
