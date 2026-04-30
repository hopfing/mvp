"""Per-line and aggregated scoring for line-market binary predictions.

Per-line primitives delegate to sklearn (`log_loss`, `brier_score_loss`).
Aggregation across lines (mean / max / sum) is the only logic local to this
module — that's the part the lines proxy needs that doesn't exist elsewhere.
"""

import numpy as np

from sklearn.metrics import brier_score_loss, log_loss as _sk_log_loss


def _per_line(
    fn,
    preds: dict[float, np.ndarray],
    labels: dict[float, np.ndarray],
) -> dict[float, float]:
    return {line: float(fn(labels[line], preds[line])) for line in preds}


def _per_line_log_loss(preds, labels):
    return _per_line(lambda y, p: _sk_log_loss(y, p, labels=[0, 1]), preds, labels)


def _per_line_brier(preds, labels):
    return _per_line(brier_score_loss, preds, labels)


def _per_line_cal_err(preds, labels):
    return {
        line: float(abs(np.mean(preds[line]) - np.mean(labels[line])))
        for line in preds
    }


def log_loss(preds, labels) -> float:
    """Mean binary log loss across lines."""
    return float(np.mean(list(_per_line_log_loss(preds, labels).values())))


def brier_score(preds, labels) -> float:
    """Mean Brier score across lines."""
    return float(np.mean(list(_per_line_brier(preds, labels).values())))


def cal_max(preds, labels) -> float:
    """Max absolute calibration error across lines."""
    return float(max(_per_line_cal_err(preds, labels).values()))


def cal_sum(preds, labels) -> float:
    """Sum of absolute calibration errors across lines."""
    return float(sum(_per_line_cal_err(preds, labels).values()))


_SCORERS = {
    "log_loss": log_loss,
    "cal_max":  cal_max,
    "cal_sum":  cal_sum,
    "brier_score": brier_score,
}


def score(name: str, preds, labels) -> float:
    """Dispatch to the named scorer."""
    if name not in _SCORERS:
        raise ValueError(f"Unknown lines metric: {name!r}; expected one of {sorted(_SCORERS)}")
    return _SCORERS[name](preds, labels)


def per_line_diagnostics(
    preds: dict[float, np.ndarray], labels: dict[float, np.ndarray],
) -> dict[str, dict[float, float]]:
    """Per-line diagnostics for post-FS reporting."""
    return {
        "log_loss":  _per_line_log_loss(preds, labels),
        "cal_err":   _per_line_cal_err(preds, labels),
        "brier_score": _per_line_brier(preds, labels),
        "mean_pred": {L: float(np.mean(preds[L])) for L in preds},
        "empirical": {L: float(np.mean(labels[L])) for L in labels},
    }
