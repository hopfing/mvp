"""Integration tests for experiment runner."""

import importlib
import random
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from mvp.experimentation.runner import ExperimentRunner


class TestRunnerIntegration:
    """Integration tests with realistic data."""

    @pytest.fixture(autouse=True)
    def ensure_features_registered(self):
        """Ensure features are registered before each test.

        Other tests may clear the registry, so we clear it and reload
        the feature modules to re-run the @feature decorators.
        """
        import mvp.experimentation.features.h2h
        import mvp.experimentation.features.ranking
        import mvp.experimentation.features.serve
        import mvp.experimentation.features.win_rate
        from mvp.experimentation.registry import get_registry

        get_registry().clear()
        importlib.reload(mvp.experimentation.features.h2h)
        importlib.reload(mvp.experimentation.features.ranking)
        importlib.reload(mvp.experimentation.features.serve)
        importlib.reload(mvp.experimentation.features.win_rate)

    @pytest.fixture
    def realistic_matches(self, tmp_path: Path) -> Path:
        """Create realistic matches parquet file."""
        random.seed(42)
        n_matches = 500

        rows = []
        base_date = date(2024, 1, 1)

        for i in range(n_matches):
            match_date = base_date + timedelta(days=i // 5)
            player_rank = random.randint(1, 200)
            opp_rank = random.randint(1, 200)
            p_win = 0.6 if player_rank < opp_rank else 0.4
            won = random.random() < p_win

            # First row: player perspective
            rows.append(
                {
                    "match_uid": f"M{i:04d}",
                    "player_id": f"P{i % 20:02d}",
                    "opp_id": f"P{(i + 10) % 20:02d}",
                    "effective_match_date": match_date,
                    "won": won,
                    "player_ranking_points": 1000 - player_rank * 4,
                    "opp_ranking_points": 1000 - opp_rank * 4,
                }
            )

            # Second row: opponent perspective (mirror)
            rows.append(
                {
                    "match_uid": f"M{i:04d}",
                    "player_id": f"P{(i + 10) % 20:02d}",
                    "opp_id": f"P{i % 20:02d}",
                    "effective_match_date": match_date,
                    "won": not won,
                    "player_ranking_points": 1000 - opp_rank * 4,
                    "opp_ranking_points": 1000 - player_rank * 4,
                }
            )

        df = pl.DataFrame(rows)
        path = tmp_path / "matches.parquet"
        df.write_parquet(path)
        return path

    def test_full_experiment_flow(
        self,
        realistic_matches: Path,
        tmp_path: Path,
    ):
        """Run complete experiment and verify results."""
        import mlflow

        config_str = """
name: integration_test
description: Test experiment with ranking features
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - ranking_points_diff
model:
  type: logistic
  params:
    C: 1.0
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
metrics:
  primary: log_loss
  secondary:
    - accuracy
    - brier_score
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_str)

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=config_path,
            matches_path=realistic_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )

        results = runner.run()

        # Verify result structure
        assert "metrics" in results
        assert "fold_metrics" in results
        assert "feature_columns" in results
        assert results["n_folds"] == 2

        # Verify metric ranges
        assert 0.3 < results["metrics"]["accuracy"] < 0.9
        assert results["metrics"]["log_loss"] > 0
        assert 0 <= results["metrics"]["brier_score"] <= 0.25

    def test_xgboost_model_experiment(
        self,
        realistic_matches: Path,
        tmp_path: Path,
    ):
        """Run experiment with XGBoost model."""
        import mlflow

        config_str = """
name: xgboost_test
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - ranking_points_diff
model:
  type: xgboost
  params:
    n_estimators: 10
    max_depth: 3
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_str)

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=config_path,
            matches_path=realistic_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )

        results = runner.run()

        assert results["n_folds"] == 2
        assert 0 <= results["metrics"]["accuracy"] <= 1
        assert results["metrics"]["log_loss"] > 0

    def test_expanding_window_validation(
        self,
        realistic_matches: Path,
        tmp_path: Path,
    ):
        """Run experiment with expanding window validation."""
        import mlflow

        config_str = """
name: expanding_window_test
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - ranking_points_diff
model:
  type: logistic
validation:
  type: expanding_window
  initial_train_size: 200
  step_size: 100
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_str)

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=config_path,
            matches_path=realistic_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )

        results = runner.run()

        # Should have multiple folds from expanding window
        assert results["n_folds"] >= 1
        assert "metrics" in results
        assert 0 <= results["metrics"]["accuracy"] <= 1

    def test_mlflow_logging(
        self,
        realistic_matches: Path,
        tmp_path: Path,
    ):
        """Verify MLflow logs experiment properly."""
        import mlflow

        config_str = """
name: mlflow_logging_test
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - ranking_points_diff
model:
  type: logistic
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_str)

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=config_path,
            matches_path=realistic_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )

        results = runner.run()

        # Verify run was logged
        assert "run_id" in results

        # Verify we can retrieve the run
        run = mlflow.get_run(results["run_id"])
        assert run is not None
        assert run.data.params["model_type"] == "logistic"
        assert run.data.params["validation_type"] == "walk_forward"

        # Verify metrics were logged
        assert "accuracy" in run.data.metrics
        assert "log_loss" in run.data.metrics

    def test_runner_produces_diagnostics(
        self,
        realistic_matches: Path,
        tmp_path: Path,
    ):
        """Runner produces diagnostics with correct structure."""
        import mlflow

        from mvp.experimentation.diagnostics import DiagnosticResults

        config_str = """
name: diagnostics_test
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - ranking_points_diff
model:
  type: logistic
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_str)

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=config_path,
            matches_path=realistic_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )

        results = runner.run()

        # Verify diagnostics is in results
        assert "diagnostics" in results
        diagnostics = results["diagnostics"]
        assert isinstance(diagnostics, DiagnosticResults)

        # Verify structure has all components
        assert diagnostics.segments is not None
        assert diagnostics.calibration is not None
        assert diagnostics.errors is not None
        assert diagnostics.temporal is not None

        # Verify calibration has expected keys
        assert "calibration_error" in diagnostics.calibration
        assert "calibration_max_error" in diagnostics.calibration
        assert "buckets" in diagnostics.calibration

        # Verify errors has expected keys
        assert "summary" in diagnostics.errors
        assert "error_rate_80plus" in diagnostics.errors
        assert "high_confidence_errors" in diagnostics.errors

        # Verify temporal has expected keys
        assert "periods" in diagnostics.temporal
        assert "temporal_drift" in diagnostics.temporal

        # Verify metrics can be flattened
        metrics = diagnostics.metrics
        assert isinstance(metrics, dict)
        assert "calibration_error" in metrics
        assert "temporal_drift" in metrics

        # Verify JSON serialization works
        json_str = diagnostics.to_json()
        import json

        parsed = json.loads(json_str)
        assert "segments" in parsed
        assert "calibration" in parsed
        assert "errors" in parsed
        assert "temporal" in parsed

        # Verify MLflow logged diagnostic metrics
        run = mlflow.get_run(results["run_id"])
        assert "calibration_error" in run.data.metrics
        assert "temporal_drift" in run.data.metrics
