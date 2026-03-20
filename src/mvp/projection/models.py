"""Regression model wrappers for game projection."""


import os
from abc import ABC, abstractmethod
from typing import Any

import numpy as np


def _default_n_jobs() -> int:
    """Return a capped n_jobs value, leaving 2 cores for the OS."""
    cpu_count = os.cpu_count() or 4
    return max(1, cpu_count - 2)


class RegressionModel(ABC):
    """Base class for regression model wrappers."""

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        pass

    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        pass


class XGBRegressorModel(RegressionModel):
    """XGBoost regressor wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {
            "objective": "reg:squarederror",
            "eval_metric": "rmse",
            "n_jobs": _default_n_jobs(),
            "random_state": 42,
            **params,
        }
        self._model = None

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        import xgboost as xgb

        self._model = xgb.XGBRegressor(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict(X)


class LinearRegressionModel(RegressionModel):
    """Linear regression wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {**params}
        self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        from sklearn.linear_model import LinearRegression

        self._model = LinearRegression(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict(X)


class RidgeModel(RegressionModel):
    """Ridge regression wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, **params}
        self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        from sklearn.linear_model import Ridge

        self._model = Ridge(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict(X)


def get_regression_model(model_type: str, params: dict[str, Any]) -> RegressionModel:
    """Factory function to get regression model wrapper."""
    if model_type == "xgb_regressor":
        return XGBRegressorModel(params)
    elif model_type == "linear":
        return LinearRegressionModel(params)
    elif model_type == "ridge":
        return RidgeModel(params)
    else:
        raise ValueError(f"Unknown regression model type: {model_type}")
