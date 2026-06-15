"""Tests for metrics calculation."""

import numpy as np
from sklearn.metrics import brier_score_loss

from mvp.model.metrics import (
    compute_beta_tail_score,
    compute_metrics,
    compute_partial_auc_tail,
    compute_restricted_logloss,
    compute_threshold_weighted_brier,
    compute_weighted_concordance,
)


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

    def test_float32_extremes_emit_no_warning(self):
        """float32 predictions at exactly 0.0/1.0 must not trigger a
        divide-by-zero in any metric (the 1-1e-15 clip bound rounds to 1.0 in
        float32, so the clip must cast to float64 first)."""
        import warnings

        y_true = np.array([1, 0, 1, 0])
        y_prob = np.array([1.0, 0.0, 0.999, 0.001], dtype=np.float32)
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            metrics = compute_metrics(y_true, y_prob)
        assert all(np.isfinite(v) for v in metrics.values())

    def test_tail_metrics_present(self):
        """All tail-sensitive objectives are returned by compute_metrics."""
        y_true = np.array([1, 0, 1, 0, 1, 0])
        y_prob = np.array([0.8, 0.2, 0.6, 0.4, 0.9, 0.1])

        metrics = compute_metrics(y_true, y_prob)

        for key in (
            "beta_tail_score",
            "beta_tail_score_sharp",
            "threshold_weighted_brier",
            "restricted_logloss",
            "weighted_concordance",
            "partial_auc_tail",
        ):
            assert key in metrics
            assert np.isfinite(metrics[key])


class TestBetaTailScore:
    """Tests for the Beta-family proper tail score."""

    def test_a_b_one_equals_half_brier(self):
        """a = b = 1 recovers exactly half the Brier score (closed-form check)."""
        rng = np.random.default_rng(0)
        y_true = rng.integers(0, 2, size=500)
        y_prob = rng.uniform(0, 1, size=500)

        beta_score = compute_beta_tail_score(y_true, y_prob, a=1.0, b=1.0)
        assert np.isclose(beta_score, 0.5 * brier_score_loss(y_true, y_prob), atol=1e-9)

    def test_confident_correct_beats_confident_wrong(self):
        """Confident-correct predictions score far better than confident-wrong
        (same shape, so magnitudes are comparable)."""
        y_true = np.array([1, 1, 0, 0])
        good = compute_beta_tail_score(y_true, np.array([0.95, 0.95, 0.05, 0.05]))
        bad = compute_beta_tail_score(y_true, np.array([0.05, 0.05, 0.95, 0.95]))
        assert good < bad


class TestThresholdWeightedBrier:
    """Tests for the threshold-weighted (tail-grid) Brier."""

    def test_perfect_near_zero(self):
        y_true = np.array([1, 1, 0, 0])
        y_prob = np.array([0.99, 0.99, 0.01, 0.01])
        assert compute_threshold_weighted_brier(y_true, y_prob) < 1e-3

    def test_confident_correct_beats_confident_wrong(self):
        y_true = np.array([1, 1, 0, 0])
        good = compute_threshold_weighted_brier(y_true, np.array([0.9, 0.9, 0.1, 0.1]))
        bad = compute_threshold_weighted_brier(y_true, np.array([0.1, 0.1, 0.9, 0.9]))
        assert good < bad


class TestRestrictedLogloss:
    """Tests for the coverage-guarded confident-region log loss."""

    def test_collapse_to_half_is_penalized(self):
        """Collapsing all predictions to 0.5 empties the scored set and the
        coverage guard makes it worse than a genuinely confident model."""
        y_true = np.array([1, 1, 0, 0, 1, 1, 0, 0])
        collapsed = np.full(8, 0.5)
        confident = np.array([0.9, 0.9, 0.1, 0.1, 0.9, 0.9, 0.1, 0.1])
        assert compute_restricted_logloss(y_true, confident) < compute_restricted_logloss(
            y_true, collapsed
        )


class TestWeightedConcordance:
    """Tests for confidence-weighted Somers' D."""

    def test_perfect_ranking_is_one(self):
        y_true = np.array([1, 1, 0, 0])
        y_prob = np.array([0.9, 0.8, 0.2, 0.1])
        assert np.isclose(compute_weighted_concordance(y_true, y_prob), 1.0)

    def test_reversed_ranking_is_negative_one(self):
        y_true = np.array([1, 1, 0, 0])
        y_prob = np.array([0.1, 0.2, 0.8, 0.9])
        assert np.isclose(compute_weighted_concordance(y_true, y_prob), -1.0)

    def test_equals_somers_d_when_confidence_uniform(self):
        """With equal |p-0.5| on every sample the weights factor out and
        weighted D reduces to plain Somers' D = 2*AUC - 1, ties included."""
        from sklearn.metrics import roc_auc_score

        # All predictions equidistant from 0.5 (0.6 or 0.4) → uniform weights,
        # with a non-trivial (tied, imperfect) ranking.
        y_true = np.array([1, 1, 0, 0, 1, 0])
        y_prob = np.array([0.6, 0.4, 0.6, 0.4, 0.6, 0.4])
        somers_d = 2.0 * roc_auc_score(y_true, y_prob) - 1.0
        assert np.isclose(compute_weighted_concordance(y_true, y_prob), somers_d)

    def test_single_class_returns_zero(self):
        assert compute_weighted_concordance(np.array([1, 1, 1]), np.array([0.7, 0.8, 0.9])) == 0.0


class TestPartialAucTail:
    """Tests for two-corner tail partial AUC."""

    def test_perfect_separation_near_one(self):
        y_true = np.array([1, 1, 1, 0, 0, 0])
        y_prob = np.array([0.9, 0.85, 0.8, 0.2, 0.15, 0.1])
        assert compute_partial_auc_tail(y_true, y_prob) > 0.95

    def test_chance_near_half(self):
        rng = np.random.default_rng(1)
        y_true = rng.integers(0, 2, size=2000)
        y_prob = rng.uniform(0, 1, size=2000)
        assert abs(compute_partial_auc_tail(y_true, y_prob) - 0.5) < 0.1
