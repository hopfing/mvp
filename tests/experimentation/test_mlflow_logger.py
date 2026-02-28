"""Tests for MLflow logging."""

from pathlib import Path

import mlflow
import pytest

from mvp.experimentation.mlflow_logger import ExperimentLogger


class TestExperimentLogger:
    """Tests for ExperimentLogger."""

    @pytest.fixture
    def temp_mlflow_uri(self, tmp_path: Path) -> str:
        """Create temp SQLite database for MLflow tracking."""
        db_path = tmp_path / "mlflow.db"
        return f"sqlite:///{db_path}"

    def test_log_params(self, temp_mlflow_uri: str):
        """Log parameters to MLflow."""
        mlflow.set_tracking_uri(temp_mlflow_uri)

        logger = ExperimentLogger(experiment_name="test_exp")
        with logger.start_run(run_name="test_run"):
            logger.log_params({"learning_rate": 0.1, "max_depth": 3})

        runs = mlflow.search_runs(experiment_names=["test_exp"])
        assert len(runs) == 1
        assert runs.iloc[0]["params.learning_rate"] == "0.1"
        assert runs.iloc[0]["params.max_depth"] == "3"

    def test_log_metrics(self, temp_mlflow_uri: str):
        """Log metrics to MLflow."""
        mlflow.set_tracking_uri(temp_mlflow_uri)

        logger = ExperimentLogger(experiment_name="test_exp")
        with logger.start_run(run_name="test_run"):
            logger.log_metrics({"accuracy": 0.85, "log_loss": 0.42})

        runs = mlflow.search_runs(experiment_names=["test_exp"])
        assert runs.iloc[0]["metrics.accuracy"] == 0.85
        assert runs.iloc[0]["metrics.log_loss"] == 0.42

    def test_run_id_available(self, temp_mlflow_uri: str):
        """Run ID is available during run."""
        mlflow.set_tracking_uri(temp_mlflow_uri)

        logger = ExperimentLogger(experiment_name="test_exp")
        with logger.start_run(run_name="test_run"):
            assert logger.run_id is not None
            assert len(logger.run_id) > 0
