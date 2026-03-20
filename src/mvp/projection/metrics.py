"""Regression metrics for game projection."""


import numpy as np


def compute_regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    """Compute regression metrics.

    Returns:
        Dict with mae, rmse, r_squared, median_ae.
    """
    residuals = y_true - y_pred
    mae = float(np.mean(np.abs(residuals)))
    rmse = float(np.sqrt(np.mean(residuals**2)))
    median_ae = float(np.median(np.abs(residuals)))

    ss_res = float(np.sum(residuals**2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    return {
        "mae": mae,
        "rmse": rmse,
        "r_squared": r_squared,
        "median_ae": median_ae,
    }
