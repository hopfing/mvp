"""Tests for projection regression metrics."""

import numpy as np

from mvp.projection.metrics import compute_regression_metrics


class TestComputeRegressionMetrics:
    """Tests for compute_regression_metrics."""

    def test_perfect_predictions(self):
        y_true = np.array([10.0, 15.0, 20.0, 25.0])
        y_pred = np.array([10.0, 15.0, 20.0, 25.0])
        metrics = compute_regression_metrics(y_true, y_pred)
        assert metrics["mae"] == 0.0
        assert metrics["rmse"] == 0.0
        assert metrics["r_squared"] == 1.0
        assert metrics["median_ae"] == 0.0

    def test_known_values(self):
        y_true = np.array([10.0, 20.0, 30.0])
        y_pred = np.array([12.0, 18.0, 33.0])
        metrics = compute_regression_metrics(y_true, y_pred)
        # Residuals: -2, 2, -3 -> abs: 2, 2, 3
        assert abs(metrics["mae"] - 7.0 / 3) < 1e-10
        assert abs(metrics["rmse"] - np.sqrt(17.0 / 3)) < 1e-10
        assert metrics["median_ae"] == 2.0

    def test_r_squared_positive(self):
        """R² should be positive when predictions are better than the mean."""
        np.random.seed(42)
        y_true = np.random.randn(100) * 5 + 15
        y_pred = y_true + np.random.randn(100) * 0.5
        metrics = compute_regression_metrics(y_true, y_pred)
        assert metrics["r_squared"] > 0.9

    def test_r_squared_negative(self):
        """R² can be negative when predictions are worse than the mean."""
        y_true = np.array([10.0, 20.0, 30.0])
        y_pred = np.array([100.0, 100.0, 100.0])
        metrics = compute_regression_metrics(y_true, y_pred)
        assert metrics["r_squared"] < 0

    def test_all_metrics_present(self):
        y_true = np.array([1.0, 2.0, 3.0, 4.0])
        y_pred = np.array([1.5, 2.5, 2.5, 3.5])
        metrics = compute_regression_metrics(y_true, y_pred)
        assert "mae" in metrics
        assert "rmse" in metrics
        assert "r_squared" in metrics
        assert "median_ae" in metrics

    def test_constant_target(self):
        """R² is 0 when all targets are the same (ss_tot = 0)."""
        y_true = np.array([5.0, 5.0, 5.0])
        y_pred = np.array([4.0, 5.0, 6.0])
        metrics = compute_regression_metrics(y_true, y_pred)
        assert metrics["r_squared"] == 0.0
