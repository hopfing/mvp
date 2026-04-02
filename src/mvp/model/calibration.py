"""Platt scaling calibration for predicted probabilities."""

import numpy as np
from sklearn.linear_model import LogisticRegression


class PlattCalibrator:
    """Platt scaling: fits a logistic regression on logit-transformed probabilities."""

    def __init__(self) -> None:
        self._model: LogisticRegression | None = None

    @property
    def is_fitted(self) -> bool:
        return self._model is not None

    @property
    def slope(self) -> float:
        if self._model is None:
            raise ValueError("Calibrator not fitted")
        return float(self._model.coef_[0, 0])

    @property
    def intercept(self) -> float:
        if self._model is None:
            raise ValueError("Calibrator not fitted")
        return float(self._model.intercept_[0])

    def fit(self, y_prob: np.ndarray, y_true: np.ndarray) -> "PlattCalibrator":
        """Fit calibrator on predicted probabilities and true labels.

        Args:
            y_prob: Predicted probabilities (1-D).
            y_true: True binary labels (1-D).

        Returns:
            self for chaining.
        """
        clipped = np.clip(y_prob, 1e-7, 1 - 1e-7)
        logits = np.log(clipped / (1 - clipped)).reshape(-1, 1)
        self._model = LogisticRegression(solver="lbfgs", max_iter=1000)
        self._model.fit(logits, y_true)
        return self

    def transform(self, y_prob: np.ndarray) -> np.ndarray:
        """Apply calibration to predicted probabilities.

        Returns input unchanged if not fitted (graceful no-op).
        """
        if self._model is None:
            return y_prob
        clipped = np.clip(y_prob, 1e-7, 1 - 1e-7)
        logits = np.log(clipped / (1 - clipped)).reshape(-1, 1)
        return self._model.predict_proba(logits)[:, 1]
