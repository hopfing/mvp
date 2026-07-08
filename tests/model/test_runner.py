"""Tests for experiment runner."""

import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from mvp.model.runner import (
    ExperimentRunner,
    _calibrated_objective_metrics,
    _reporting_calibrated_holdout,
)


class TestExperimentRunner:
    """Tests for ExperimentRunner."""

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
    def sample_matches(self, tmp_path: Path) -> Path:
        """Create sample matches parquet file."""
        df = pl.DataFrame(
            {
                "match_uid": [f"M{i}" for i in range(200)],
                "player_id": [f"P{i % 10}" for i in range(200)],
                "opp_id": [f"P{(i + 5) % 10}" for i in range(200)],
                "effective_match_date": [
                    f"2024-01-{(i % 28) + 1:02d}" for i in range(200)
                ],
                "won": [i % 2 == 0 for i in range(200)],
                "player_rankings_points": [1000 - i for i in range(200)],
                "opp_rankings_points": [500 + i for i in range(200)],
                "circuit": ["tour" for _ in range(200)],
            }
        ).with_columns(pl.col("effective_match_date").str.to_datetime())
        path = tmp_path / "matches.parquet"
        df.write_parquet(path)
        return path

    @pytest.fixture
    def sample_config(self, tmp_path: Path) -> Path:
        """Create sample experiment config."""
        config_str = """
name: test_experiment
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
  min_train_size: 50
  test_size: 25
"""
        path = tmp_path / "config.yaml"
        path.write_text(config_str)
        return path

    def test_runner_init(self, sample_config: Path, sample_matches: Path):
        """Runner initializes from config file."""
        runner = ExperimentRunner(
            config_path=sample_config,
            matches_path=sample_matches,
        )
        assert runner.run_name == "config"  # Derived from filename config.yaml

    def test_runner_init_with_defaults(self, sample_config: Path, sample_matches: Path):
        """Runner uses default paths when not specified."""
        runner = ExperimentRunner(
            config_path=sample_config,
            matches_path=sample_matches,
        )
        from mvp.common.base_job import get_local_data_root

        assert runner.cache_dir == get_local_data_root() / "features" / "cache"
        assert runner.mlflow_dir is None

    def test_runner_init_with_custom_paths(
        self, sample_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Runner accepts custom cache and mlflow directories."""
        cache_dir = tmp_path / "custom_cache"
        mlflow_dir = tmp_path / "custom_mlflow"

        runner = ExperimentRunner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=cache_dir,
            mlflow_dir=mlflow_dir,
        )

        assert runner.cache_dir == cache_dir
        assert runner.mlflow_dir == mlflow_dir

    def test_runner_engine_initialized(
        self, sample_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Runner initializes FeatureEngine with correct paths."""
        cache_dir = tmp_path / "cache"

        runner = ExperimentRunner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=cache_dir,
        )

        assert runner.engine is not None
        assert runner.engine.matches_path == sample_matches
        assert runner.engine.cache_dir == cache_dir

    def test_runner_run(
        self,
        sample_config: Path,
        sample_matches: Path,
        tmp_path: Path,
    ):
        """Runner executes full pipeline."""
        import mlflow

        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")

        runner = ExperimentRunner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            mlflow_dir=mlflow_dir,
        )
        results = runner.run()

        assert "metrics" in results
        assert "accuracy" in results["metrics"]
        assert "log_loss" in results["metrics"]
        assert 0 <= results["metrics"]["accuracy"] <= 1
        assert results["n_folds"] == 2
        assert "run_id" in results


class TestReportingCalibratedHoldout:
    """Deployment-frame (global-Platt) holdout metrics helper."""

    def test_single_class_oof_returns_none(self):
        """A single-class tuning OOF can't fit Platt; the reporting extra must
        return (None, None) rather than raise and abort the whole tuning study."""
        oof_y_true = np.zeros(50, dtype=int)  # single class → LogisticRegression fails
        oof_y_prob = np.linspace(0.1, 0.9, 50)
        holdout = [
            {"y_true": np.array([0, 1, 0, 1]), "y_prob": np.array([0.3, 0.7, 0.4, 0.6])}
        ]
        overall, per_fold = _reporting_calibrated_holdout(
            oof_y_true, oof_y_prob, holdout, None
        )
        assert overall is None
        assert per_fold is None

    def test_normal_two_class_oof_produces_metrics(self):
        """Two-class OOF yields calibrated overall + one metric dict per fold."""
        rng = np.random.default_rng(0)
        oof_y_true = rng.integers(0, 2, 200)
        oof_y_prob = np.clip(rng.random(200), 1e-6, 1 - 1e-6)
        holdout = [
            {
                "y_true": rng.integers(0, 2, 40),
                "y_prob": np.clip(rng.random(40), 1e-6, 1 - 1e-6),
            }
            for _ in range(2)
        ]
        overall, per_fold = _reporting_calibrated_holdout(
            oof_y_true, oof_y_prob, holdout, None
        )
        assert overall is not None and "log_loss" in overall
        assert per_fold is not None and len(per_fold) == 2


class TestCalibratedObjectiveMetrics:
    """Calibrated-frame tuning objective (nested out-of-fold Platt)."""

    @staticmethod
    def _fold(rng, n):
        return {
            "y_true": rng.integers(0, 2, n),
            "y_prob": np.clip(rng.random(n), 1e-6, 1 - 1e-6),
        }

    def test_two_class_multi_fold_returns_metrics(self):
        """>=2 two-class folds → pooled OOF-calibrated metric dict."""
        rng = np.random.default_rng(0)
        folds = [self._fold(rng, 60) for _ in range(3)]
        y_true = np.concatenate([f["y_true"] for f in folds])
        out = _calibrated_objective_metrics(folds, y_true, None)
        assert out is not None and "log_loss" in out

    def test_single_fold_returns_none(self):
        """One fold can't be nested (no complement to fit on) → None, not crash."""
        rng = np.random.default_rng(0)
        folds = [self._fold(rng, 60)]
        assert _calibrated_objective_metrics(folds, folds[0]["y_true"], None) is None

    def test_single_class_complement_returns_none(self):
        """If a fold's complement is single-class the nested Platt can't fit; the
        objective degrades to None (caller falls back to raw) rather than crashing."""
        folds = [
            {"y_true": np.zeros(50, dtype=int), "y_prob": np.linspace(0.1, 0.9, 50)},
            {"y_true": np.ones(50, dtype=int), "y_prob": np.linspace(0.1, 0.9, 50)},
        ]
        y_true = np.concatenate([f["y_true"] for f in folds])
        assert _calibrated_objective_metrics(folds, y_true, None) is None
