"""Match-grain line-prob estimator: K independent binary classifiers per market.

Fits one xgboost (or logistic) classifier per line in `lines`, all on the same
feature matrix, with per-line label vectors. Predict returns a `{line: probs}`
dict aligned to the input row order.

Per-line independence means line probs are not guaranteed monotone across
lines (e.g., P(over 19.5) > P(over 20.5) might fail). That's acceptable for
FS scoring on per-line metrics; it would matter if these probs were used to
price multi-line bundles, which they aren't — this model is a discovery
proxy only.
"""

from typing import Any, Literal

import numpy as np

from sklearn.linear_model import LogisticRegression


ModelType = Literal["logistic", "xgboost"]


class LineModel:
    """K binary classifiers, one per line, sharing a feature matrix."""

    def __init__(
        self,
        model_type: ModelType,
        lines: list[float],
        params: dict[str, Any] | None = None,
    ) -> None:
        if not lines:
            raise ValueError("LineModel requires non-empty lines")
        self.model_type = model_type
        self.lines = [float(L) for L in lines]
        self.params = dict(params or {})
        self._models: dict[float, Any] = {}
        self._feature_names: list[str] | None = None

    def fit(
        self,
        X: np.ndarray,
        labels: dict[float, np.ndarray],
        feature_names: list[str] | None = None,
    ) -> None:
        """Fit one classifier per line.

        Args:
            X: (n_rows, n_features) float matrix.
            labels: {line: (n_rows,) int array of 0/1 labels}. Must contain a
                key for every line passed to __init__.
            feature_names: optional names; stored for diagnostics only.
        """
        missing = [L for L in self.lines if L not in labels]
        if missing:
            raise ValueError(f"labels missing for lines: {missing}")
        self._feature_names = list(feature_names) if feature_names else None
        self._models = {L: self._build() for L in self.lines}
        for L in self.lines:
            self._models[L].fit(X, labels[L])

    def predict_line_probs(self, X: np.ndarray) -> dict[float, np.ndarray]:
        """Predict P(over | features) per line per row."""
        if not self._models:
            raise RuntimeError("LineModel.predict_line_probs called before fit()")
        return {L: self._models[L].predict_proba(X)[:, 1] for L in self.lines}

    def _build(self) -> Any:
        if self.model_type == "xgboost":
            from xgboost import XGBClassifier
            return XGBClassifier(**self.params)
        if self.model_type == "logistic":
            return LogisticRegression(**self.params)
        raise ValueError(f"Unknown model_type: {self.model_type!r}")
