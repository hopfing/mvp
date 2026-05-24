"""Tests for metrics calculation."""

import numpy as np

from mvp.model.metrics import compute_metrics


class TestComputeMetrics:
    """Tests for compute_metrics function."""

    def test_perfect_predictions(self):
        """Perfect predictions yield optimal metrics."""
        y_true = np.array([1, 1, 0, 0])
        y_prob = np.array([0.99, 0.99, 0.01, 0.01])

        metrics = compute_metrics(y_true, y_prob)

        assert metrics["accuracy"] == 1.0
        assert metrics["log_loss"] < 0.1
        assert metrics["brier_score"] < 0.01
        assert metrics["roc_auc"] == 1.0

    def test_random_predictions(self):
        """Random predictions yield ~0.5 accuracy."""
        y_true = np.array([1, 1, 1, 1, 0, 0, 0, 0])
        y_prob = np.array([0.5] * 8)

        metrics = compute_metrics(y_true, y_prob)

        assert metrics["accuracy"] == 0.5
        assert 0.6 < metrics["log_loss"] < 0.8
        assert metrics["brier_score"] == 0.25

    def test_all_metrics_present(self):
        """All expected metrics are returned."""
        y_true = np.array([1, 0, 1, 0])
        y_prob = np.array([0.8, 0.2, 0.6, 0.4])

        metrics = compute_metrics(y_true, y_prob)

        assert "accuracy" in metrics
        assert "log_loss" in metrics
        assert "brier_score" in metrics
        assert "roc_auc" in metrics

    def test_asymmetric_logloss_lambda_override(self):
        """lambda_over=None uses default (2.0); explicit override changes the value."""
        # Overconfident-side error: y=0 but p=0.9 → weight = lambda_over.
        y_true = np.array([0, 0, 1, 1])
        y_prob = np.array([0.9, 0.9, 0.5, 0.5])

        default = compute_metrics(y_true, y_prob)["asymmetric_logloss"]
        higher = compute_metrics(y_true, y_prob, lambda_over=4.0)["asymmetric_logloss"]
        lower = compute_metrics(y_true, y_prob, lambda_over=1.0)["asymmetric_logloss"]

        # Larger lambda penalizes the overconfident wrong picks more heavily.
        assert lower < default < higher
        # None falls back to default 2.0 exactly.
        assert compute_metrics(y_true, y_prob, lambda_over=None)["asymmetric_logloss"] == default
