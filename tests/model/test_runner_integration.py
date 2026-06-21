"""Integration tests for experiment runner."""

import importlib
import random
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from mvp.model.runner import ExperimentRunner


class TestRunnerIntegration:
    """Integration tests with realistic data."""

    @pytest.fixture(autouse=True)
    def ensure_features_registered(self, isolated_registry):
        """Ensure features are registered before each test."""
        import mvp.model.features.h2h
        import mvp.model.features.ranking
        import mvp.model.features.serve
        import mvp.model.features.win_rate

        importlib.reload(mvp.model.features.h2h)
        importlib.reload(mvp.model.features.ranking)
        importlib.reload(mvp.model.features.serve)
        importlib.reload(mvp.model.features.win_rate)

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
                    "player_rankings_points": 1000 - player_rank * 4,
                    "opp_rankings_points": 1000 - opp_rank * 4,
                    "circuit": "tour",
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
                    "player_rankings_points": 1000 - opp_rank * 4,
                    "opp_rankings_points": 1000 - player_rank * 4,
                    "circuit": "tour",
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
    - player_ranking_points_diff
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

    def test_mtl_full_flow_with_retirements(self, tmp_path: Path):
        """Full runner.run() on an MTL config with retirements/walkovers present.

        Regression guard for the gap that let a deploy-fit crash slip through:
        the fit must succeed (incompletes dropped from TRAINING, kept in the
        test fold) rather than feeding null aux labels into XGBoost. Before the
        train-only-completeness fix, retirements reached the fit and XGBoost
        raised "Label contains NaN".
        """
        import mlflow

        random.seed(7)
        base_date = date(2024, 1, 1)
        rows = []
        for i in range(260):
            match_date = base_date + timedelta(days=i // 3)
            pr, orank = random.randint(1, 200), random.randint(1, 200)
            won = random.random() < (0.6 if pr < orank else 0.4)
            r = random.random()
            if r < 0.03:        # walkover — excluded everywhere
                reason, rtype, sp = "W/O", "walkover", None
                P, O = [None] * 5, [None] * 5
            elif r < 0.11:      # retirement — dropped from training, kept in test
                reason, rtype, sp = "RET", None, 1
                P, O = [6, None, None, None, None], [3, None, None, None, None]
            else:               # complete
                reason, rtype, sp = None, None, 2
                P, O = [6, 6, None, None, None], [4, 3, None, None, None]
            rp_p, rp_o = 1000 - pr * 4, 1000 - orank * 4

            def mk(pid, opid, w, rp, op, sets_p, sets_o):
                d = {
                    "match_uid": f"M{i:04d}", "player_id": pid, "opp_id": opid,
                    "effective_match_date": match_date, "won": w,
                    "player_rankings_points": rp, "opp_rankings_points": op,
                    "circuit": "tour", "surface": "hard", "round": "R32",
                    "reason": reason, "result_type": rtype,
                    "sets_played": sp, "best_of": 3,
                }
                for s in range(5):
                    d[f"player_set{s + 1}_games"] = sets_p[s]
                    d[f"opp_set{s + 1}_games"] = sets_o[s]
                return d

            rows.append(mk(f"P{i % 20:02d}", f"P{(i + 10) % 20:02d}", won, rp_p, rp_o, P, O))
            rows.append(mk(f"P{(i + 10) % 20:02d}", f"P{i % 20:02d}", not won, rp_o, rp_p, O, P))

        matches_path = tmp_path / "mtl_matches.parquet"
        pl.DataFrame(rows).write_parquet(matches_path)

        config_str = """
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: xgboost
mtl:
  auxiliary_targets:
    - set_margin
    - set_count
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
metrics:
  primary: log_loss
  secondary:
    - accuracy
"""
        config_path = tmp_path / "mtl_config.yaml"
        config_path.write_text(config_str)
        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=config_path,
            matches_path=matches_path,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )

        results = runner.run()  # must not raise "Label contains NaN"
        assert results["n_folds"] == 2
        assert results["metrics"]["log_loss"] > 0

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
    - player_ranking_points_diff
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
    - player_ranking_points_diff
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
    - player_ranking_points_diff
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

        from mvp.model.diagnostics import DiagnosticResults

        config_str = """
name: diagnostics_test
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
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
