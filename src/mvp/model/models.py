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
    else:
        raise ValueError(f"Unknown model type: {model_type}")
