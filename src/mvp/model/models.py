"""Model wrappers for experiments."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import numpy as np


class BaseModel(ABC):
    """Base class for model wrappers."""

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        """Fit the model."""
        pass

    @abstractmethod
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Predict probabilities for positive class."""
        pass


class XGBoostModel(BaseModel):
    """XGBoost classifier wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "random_state": 42,
            **params,
        }
        self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        import xgboost as xgb

        self._model = xgb.XGBClassifier(**self.params)
        self._model.fit(X, y)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)[:, 1]


class LogisticModel(BaseModel):
    """Logistic regression classifier wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, "max_iter": 1000, **params}
        self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        from sklearn.linear_model import LogisticRegression

        self._model = LogisticRegression(**self.params)
        self._model.fit(X, y)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)[:, 1]


class RandomForestModel(BaseModel):
    """Random forest classifier wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, "n_jobs": -1, **params}
        self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        from sklearn.ensemble import RandomForestClassifier

        self._model = RandomForestClassifier(**self.params)
        self._model.fit(X, y)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)[:, 1]


class _SklearnWrapper:
    """Sklearn-compatible wrapper for permutation importance."""

    _estimator_type = "classifier"

    def __init__(self, ensemble: EnsembleModel) -> None:
        self._ensemble = ensemble
        self.classes_ = np.array([0, 1])

    def fit(self, X: np.ndarray, y: np.ndarray) -> _SklearnWrapper:
        self._ensemble.fit(X, y)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        probs = self._ensemble.predict_proba(X)
        return (probs >= 0.5).astype(int)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        probs = self._ensemble.predict_proba(X)
        return np.column_stack([1 - probs, probs])

    def __sklearn_tags__(self):
        from sklearn.base import BaseEstimator
        tags = BaseEstimator.__sklearn_tags__(self)
        tags.estimator_type = "classifier"
        return tags


class EnsembleModel(BaseModel):
    """Ensemble model that combines multiple base models."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = params
        self._sub_models: list[BaseModel] = []
        self._feature_indices: list[list[int]] = []
        self._weights: np.ndarray = np.array([])
        strategy = params.get("strategy", "average")
        self._strategy = strategy
        self._fitted = False

    def configure(self, base_model_specs: list[dict[str, Any]]) -> None:
        """Set up sub-models from resolved specs.

        Each spec: {type, params, feature_indices: list[int]}
        """
        self._sub_models = []
        self._feature_indices = []
        weights = []
        for spec in base_model_specs:
            sub = get_model(spec["type"], spec.get("params") or {})
            self._sub_models.append(sub)
            self._feature_indices.append(spec["feature_indices"])
            weights.append(spec.get("weight", 1.0))
        w = np.array(weights, dtype=np.float64)
        self._weights = w / w.sum()

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        if not self._sub_models:
            raise RuntimeError("EnsembleModel not configured. Call configure() first.")
        for sub, indices in zip(self._sub_models, self._feature_indices):
            sub.fit(X[:, indices], y)
        self._fitted = True

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        preds = self._predict_all(X)
        if self._strategy == "weighted_average":
            return np.average(preds, axis=0, weights=self._weights)
        return np.mean(preds, axis=0)

    def predict_proba_per_model(self, X: np.ndarray) -> list[np.ndarray]:
        """Return individual predictions from each sub-model."""
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        return self._predict_all(X)

    def _predict_all(self, X: np.ndarray) -> list[np.ndarray]:
        return [
            sub.predict_proba(X[:, idx])
            for sub, idx in zip(self._sub_models, self._feature_indices)
        ]

    @property
    def _model(self) -> _SklearnWrapper:
        """Sklearn-compatible wrapper for permutation importance."""
        return _SklearnWrapper(self)


def get_model(model_type: str, params: dict[str, Any]) -> BaseModel:
    """Factory function to get model wrapper.

    Args:
        model_type: Type of model ("xgboost", "logistic", "random_forest").
        params: Model parameters.

    Returns:
        Model wrapper instance.

    Raises:
        ValueError: If model type is unknown.
    """
    if model_type == "xgboost":
        return XGBoostModel(params)
    elif model_type == "logistic":
        return LogisticModel(params)
    elif model_type == "random_forest":
        return RandomForestModel(params)
    elif model_type == "ensemble":
        return EnsembleModel(params)
    else:
        raise ValueError(f"Unknown model type: {model_type}")
