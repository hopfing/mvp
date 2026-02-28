"""Metrics calculation for experiments."""

from __future__ import annotations

import numpy as np
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)


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
    }
