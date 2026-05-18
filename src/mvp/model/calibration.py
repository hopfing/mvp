"""Calibration for predicted probabilities (Platt scaling + isotonic regression)."""

import numpy as np
import polars as pl
from sklearn.isotonic import IsotonicRegression
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


class IsotonicCalibrator:
    """Isotonic regression calibration: non-parametric monotonic fit.

    Unlike Platt (which constrains the fit to a logistic shape), isotonic
    fits an arbitrary monotonic step function. Useful when the underlying
    miscalibration shape doesn't match a sigmoid.
    """

    def __init__(self) -> None:
        self._model: IsotonicRegression | None = None

    @property
    def is_fitted(self) -> bool:
        return self._model is not None

    @property
    def n_thresholds(self) -> int:
        """Number of distinct breakpoints in the fitted monotonic curve."""
        if self._model is None:
            raise ValueError("Calibrator not fitted")
        return int(len(self._model.X_thresholds_))

    @property
    def y_min(self) -> float:
        if self._model is None:
            raise ValueError("Calibrator not fitted")
        return float(self._model.y_thresholds_.min())

    @property
    def y_max(self) -> float:
        if self._model is None:
            raise ValueError("Calibrator not fitted")
        return float(self._model.y_thresholds_.max())

    def grid_sample(
        self, grid: tuple[float, ...] = (0.1, 0.3, 0.5, 0.7, 0.9)
    ) -> list[float]:
        """Apply the calibrator to a fixed input grid; useful for at-a-glance shape logging."""
        if self._model is None:
            raise ValueError("Calibrator not fitted")
        return [float(v) for v in self._model.predict(np.array(grid))]

    def fit(self, y_prob: np.ndarray, y_true: np.ndarray) -> "IsotonicCalibrator":
        """Fit calibrator on predicted probabilities and true labels.

        out_of_bounds="clip" so test-time probabilities outside the train
        range (rare but possible) map to the nearest endpoint rather than
        raising.
        """
        self._model = IsotonicRegression(
            y_min=0.0, y_max=1.0, out_of_bounds="clip"
        )
        self._model.fit(y_prob, y_true)
        return self

    def transform(self, y_prob: np.ndarray) -> np.ndarray:
        """Apply calibration to predicted probabilities.

        Returns input unchanged if not fitted (graceful no-op).
        """
        if self._model is None:
            return y_prob
        return self._model.predict(y_prob)


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


class SegmentedIsotonicCalibrator:
    """Per-segment isotonic regression with a global isotonic fallback.

    Same structure as `SegmentedPlattCalibrator` (per-cell fit, `min_n`
    threshold, global fallback) but uses non-parametric isotonic regression
    per cell instead of logistic Platt.
    """

    def __init__(self, segments: list[str], min_n: int = 200) -> None:
        self.segments = list(segments)
        self.min_n = min_n
        self._per_segment: dict[str, IsotonicCalibrator] = {}
        self._global: IsotonicCalibrator = IsotonicCalibrator()

    @property
    def is_fitted(self) -> bool:
        return self._global.is_fitted

    @property
    def n_segments(self) -> int:
        return len(self._per_segment)

    def mean_n_thresholds(self) -> float:
        """Average breakpoint count across per-segment fits (0 if none)."""
        if not self._per_segment:
            return 0.0
        return sum(c.n_thresholds for c in self._per_segment.values()) / len(
            self._per_segment
        )

    def max_n_thresholds(self) -> int:
        """Max breakpoint count across per-segment fits (0 if none)."""
        if not self._per_segment:
            return 0
        return max(c.n_thresholds for c in self._per_segment.values())

    def _build_keys(self, df: pl.DataFrame) -> np.ndarray:
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
    ) -> "SegmentedIsotonicCalibrator":
        """Fit a global isotonic plus a per-segment isotonic for each cell with n >= min_n."""
        self._global.fit(y_prob, y_true)
        keys = self._build_keys(df)
        for key in set(keys.tolist()):
            mask = keys == key
            n = int(mask.sum())
            if n < self.min_n:
                continue
            if len(np.unique(y_true[mask])) < 2:
                # IsotonicRegression also needs label variance to fit meaningfully
                continue
            self._per_segment[key] = IsotonicCalibrator().fit(
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


def make_calibrator(cal_cfg):
    """Construct the calibrator implied by a CalibrationConfig.

    Returns the right (segmented, global) variant for the configured
    method. The caller is responsible for fitting and applying.
    """
    if cal_cfg is None:
        return None
    method = getattr(cal_cfg, "method", "platt")
    segments = cal_cfg.segments
    min_n = cal_cfg.min_n
    if method == "isotonic":
        if segments:
            return SegmentedIsotonicCalibrator(segments=segments, min_n=min_n)
        return IsotonicCalibrator()
    # default / "platt"
    if segments:
        return SegmentedPlattCalibrator(segments=segments, min_n=min_n)
    return PlattCalibrator()


def _is_segmented(cal) -> bool:
    return isinstance(cal, (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator))


def fit_calibrator_with_nested_cv(
    tuning_predictions: list[dict],
    cal_cfg,
):
    """Fit a deployed calibrator on all tuning OOF preds AND apply nested
    fold-i-out calibrators to each fold's preds.

    For each tuning fold i: fit a separate calibrator on the OTHER folds'
    OOF preds and apply it to fold i's preds. This ensures every tuning
    pred used for diagnostics is calibrated by a fitter that hasn't seen
    it — critical for high-DoF calibrators (isotonic) that would
    otherwise trivially achieve perfect in-sample calibration.

    Modifies `tuning_predictions` in place: each dict's ``y_prob`` is
    replaced with the nested-CV-calibrated value. Returns the deployed
    calibrator (fit on all tuning OOF) — the caller should apply it to
    holdout preds and save it as the production-deployed artifact.

    Edge case: a single tuning fold can't be nested. In that case the
    deployed calibrator is applied to the fold (in-sample) and a warning
    is implicit — diagnostics from a 1-fold run already have other
    limitations.
    """
    combined_y_prob = np.concatenate([p["y_prob"] for p in tuning_predictions])
    combined_y_true = np.concatenate([p["y_true"] for p in tuning_predictions])

    deployed = make_calibrator(cal_cfg)
    if deployed is None:
        return None
    segmented = _is_segmented(deployed)
    if segmented:
        combined_df = pl.concat(
            [p["df"] for p in tuning_predictions], how="diagonal_relaxed"
        )
        deployed.fit(combined_y_prob, combined_y_true, combined_df)
    else:
        deployed.fit(combined_y_prob, combined_y_true)

    n = len(tuning_predictions)
    if n < 2:
        # Single fold: can't nest. Apply deployed in-sample (rare path).
        for pred in tuning_predictions:
            if segmented:
                pred["y_prob"] = deployed.transform(pred["y_prob"], pred["df"])
            else:
                pred["y_prob"] = deployed.transform(pred["y_prob"])
        return deployed

    for i, pred in enumerate(tuning_predictions):
        other_probs = np.concatenate(
            [tuning_predictions[j]["y_prob"] for j in range(n) if j != i]
        )
        other_labels = np.concatenate(
            [tuning_predictions[j]["y_true"] for j in range(n) if j != i]
        )
        fold_cal = make_calibrator(cal_cfg)
        if segmented:
            other_df = pl.concat(
                [tuning_predictions[j]["df"] for j in range(n) if j != i],
                how="diagonal_relaxed",
            )
            fold_cal.fit(other_probs, other_labels, other_df)
            pred["y_prob"] = fold_cal.transform(pred["y_prob"], pred["df"])
        else:
            fold_cal.fit(other_probs, other_labels)
            pred["y_prob"] = fold_cal.transform(pred["y_prob"])
    return deployed
