"""Metrics calculation for experiments."""


import numpy as np
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)


def compute_calibration_error(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute weighted mean calibration error for probabilities >= 0.50."""
    mask = y_prob >= 0.50
    y_true_filtered = y_true[mask]
    y_prob_filtered = y_prob[mask]

    if len(y_true_filtered) == 0:
        return 0.0

    bucket_edges = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00]
    errors = []
    weights = []

    for i in range(len(bucket_edges) - 1):
        low, high = bucket_edges[i], bucket_edges[i + 1]
        if i == len(bucket_edges) - 2:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered <= high)
        else:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered < high)

        if not bucket_mask.any():
            continue

        predicted_mean = float(np.mean(y_prob_filtered[bucket_mask]))
        actual = float(np.mean(y_true_filtered[bucket_mask]))
        n = int(bucket_mask.sum())
        error = abs(predicted_mean - actual)

        errors.append(error)
        weights.append(n)

    if not errors:
        return 0.0
    return float(np.average(errors, weights=weights))


def compute_signed_calibration(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute signed calibration for probabilities >= 0.50.

    Positive = underconfident (actual win rate > predicted).
    Negative = overconfident (actual win rate < predicted).
    Same bucketing as compute_calibration_error.
    """
    mask = y_prob >= 0.50
    y_true_filtered = y_true[mask]
    y_prob_filtered = y_prob[mask]

    if len(y_true_filtered) == 0:
        return 0.0

    bucket_edges = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00]
    errors = []
    weights = []

    for i in range(len(bucket_edges) - 1):
        low, high = bucket_edges[i], bucket_edges[i + 1]
        if i == len(bucket_edges) - 2:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered <= high)
        else:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered < high)

        if not bucket_mask.any():
            continue

        predicted_mean = float(np.mean(y_prob_filtered[bucket_mask]))
        actual = float(np.mean(y_true_filtered[bucket_mask]))
        n = int(bucket_mask.sum())
        error = actual - predicted_mean  # positive = underconfident

        errors.append(error)
        weights.append(n)

    if not errors:
        return 0.0
    return float(np.average(errors, weights=weights))


def compute_error_rate_80plus(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute error rate for predictions at 80%+ confidence."""
    y_pred = (y_prob >= 0.5).astype(int)
    is_error = y_pred != y_true
    tier_mask = y_prob >= 0.80
    tier_total = int(tier_mask.sum())
    if tier_total == 0:
        return 0.0
    tier_errors = int((tier_mask & is_error).sum())
    return tier_errors / tier_total


def compute_metrics(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    threshold: float = 0.5,
) -> dict[str, float]:
    """Compute classification metrics.

    Args:
        y_true: True binary labels.
        y_prob: Predicted probabilities for positive class.
        threshold: Classification threshold for accuracy.

    Returns:
        Dictionary of metric name -> value.
    """
    y_pred = (y_prob >= threshold).astype(int)

    # Clip probabilities to avoid log(0)
    y_prob_clipped = np.clip(y_prob, 1e-15, 1 - 1e-15)

    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "log_loss": float(log_loss(y_true, y_prob_clipped)),
        "brier_score": float(brier_score_loss(y_true, y_prob)),
        "roc_auc": float(roc_auc_score(y_true, y_prob)),
        "calibration_error": compute_calibration_error(y_true, y_prob),
        "error_rate_80plus": compute_error_rate_80plus(y_true, y_prob),
    }
