"""Platt scaling calibration for predicted probabilities."""

import numpy as np
import polars as pl
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


class SegmentedPlattCalibrator:
    """Per-segment Platt scaling with a global fallback.

    Uses the same segmentation as the calibration-by-segment diagnostic —
    pass a list of column names; the cartesian product of values defines
    the cells. Each cell with at least `min_n` training rows gets its own
    Platt fit; smaller cells (and any segment not seen at fit time) route
    through the global fallback.
    """

    def __init__(self, segments: list[str], min_n: int = 200) -> None:
        self.segments = list(segments)
        self.min_n = min_n
        self._per_segment: dict[str, PlattCalibrator] = {}
        self._global: PlattCalibrator = PlattCalibrator()

    @property
    def is_fitted(self) -> bool:
        return self._global.is_fitted

    @property
    def n_segments(self) -> int:
        return len(self._per_segment)

    def _build_keys(self, df: pl.DataFrame) -> np.ndarray:
        # Local import avoids module-level circular: diagnostics.py <-> calibration.py
        from mvp.model.diagnostics import _augment_segment_columns

        df = _augment_segment_columns(df, self.segments)
        seg_arrays = [df[col].fill_null("").to_numpy() for col in self.segments]
        n_rows = len(df)
        return np.array([
            "|".join(str(seg_arrays[c][i]) for c in range(len(self.segments)))
            for i in range(n_rows)
        ])

    def fit(
        self,
        y_prob: np.ndarray,
        y_true: np.ndarray,
        df: pl.DataFrame,
    ) -> "SegmentedPlattCalibrator":
        """Fit a global Platt plus a per-segment Platt for each cell with n >= min_n."""
        self._global.fit(y_prob, y_true)
        keys = self._build_keys(df)
        for key in set(keys.tolist()):
            mask = keys == key
            n = int(mask.sum())
            if n < self.min_n:
                continue
            if len(np.unique(y_true[mask])) < 2:
                # PlattCalibrator's LogisticRegression needs both classes present
                continue
            self._per_segment[key] = PlattCalibrator().fit(
                y_prob[mask], y_true[mask]
            )
        return self

    def transform(
        self, y_prob: np.ndarray, df: pl.DataFrame
    ) -> np.ndarray:
        """Apply per-segment calibration; fall back to global for unmatched segments."""
        if not self.is_fitted:
            return y_prob
        keys = self._build_keys(df)
        out = self._global.transform(y_prob).copy()
        for key, cal in self._per_segment.items():
            mask = keys == key
            if mask.any():
                out[mask] = cal.transform(y_prob[mask])
        return out
