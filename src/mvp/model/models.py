"""Model wrappers for experiments."""


from abc import ABC, abstractmethod
from typing import Any

import numpy as np


class BaseModel(ABC):
    """Base class for model wrappers."""

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
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

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        import xgboost as xgb

        self._model = xgb.XGBClassifier(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)[:, 1]


class LogisticModel(BaseModel):
    """Logistic regression classifier wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, "max_iter": 1000, **params}
        self._model = None
        self._mean: np.ndarray | None = None
        self._std: np.ndarray | None = None

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        from sklearn.linear_model import LogisticRegression

        self._mean = X.mean(axis=0)
        self._std = X.std(axis=0)
        self._std[self._std == 0] = 1.0
        X = (X - self._mean) / self._std
        self._model = LogisticRegression(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        X = (X - self._mean) / self._std
        return self._model.predict_proba(X)[:, 1]


class RandomForestModel(BaseModel):
    """Random forest classifier wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, "n_jobs": -1, **params}
        self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        from sklearn.ensemble import RandomForestClassifier

        self._model = RandomForestClassifier(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)[:, 1]


class _SklearnWrapper:
    """Sklearn-compatible wrapper for permutation importance."""

    _estimator_type = "classifier"

    def __init__(self, ensemble: "EnsembleModel") -> None:
        self._ensemble = ensemble
        self.classes_ = np.array([0, 1])

    def fit(self, X: np.ndarray, y: np.ndarray) -> "_SklearnWrapper":
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
        self._meta_model = None
        self._meta_feature_names: list[str] = []
        self._meta_feature_indices: list[int] = []
        self._meta_scaler: tuple[np.ndarray, np.ndarray] | None = None

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

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
        per_model_data: list[tuple[np.ndarray, np.ndarray] | None] | None = None,
    ) -> None:
        if not self._sub_models:
            raise RuntimeError("EnsembleModel not configured. Call configure() first.")
        for i, (sub, indices) in enumerate(zip(self._sub_models, self._feature_indices)):
            if per_model_data and per_model_data[i] is not None:
                X_sub, y_sub = per_model_data[i]
                sub.fit(X_sub[:, indices], y_sub)
            else:
                sub.fit(X[:, indices], y)
        self._fitted = True

    def set_meta_feature_indices(self, indices: list[int]) -> None:
        """Store indices into X for meta-feature columns."""
        self._meta_feature_indices = indices

    def set_meta_feature_names(self, names: list[str]) -> None:
        """Store base model names for coefficient reporting."""
        self._meta_feature_names = names

    def fit_meta(self, X_meta: np.ndarray, y_meta: np.ndarray) -> None:
        """Fit stacking meta-model on out-of-fold base predictions."""
        if X_meta.shape[0] < 2:
            raise ValueError("Need at least 2 OOF samples to fit meta-model")
        from sklearn.linear_model import LogisticRegression

        self._meta_model = LogisticRegression(max_iter=1000, random_state=42)
        self._meta_model.fit(X_meta, y_meta)

    def get_meta_coefficients(self) -> tuple[float, dict[str, float]]:
        """Return meta-model intercept and per-base-model coefficients."""
        if self._meta_model is None:
            raise RuntimeError("Meta-model not fitted")
        intercept = float(self._meta_model.intercept_[0])
        coefs = {}
        for name, coef in zip(self._meta_feature_names, self._meta_model.coef_[0]):
            coefs[name] = float(coef)
        return intercept, coefs

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        preds = self._predict_all(X)
        if self._strategy == "stacking":
            if self._meta_model is None:
                raise RuntimeError("Meta-model not fitted. Call fit_meta() first.")
            parts = list(preds)
            if self._meta_feature_indices:
                meta_feat = X[:, self._meta_feature_indices]
                if self._meta_scaler is not None:
                    mean, std = self._meta_scaler
                    meta_feat = (meta_feat - mean) / std
                parts.append(meta_feat)
            return self._meta_model.predict_proba(np.column_stack(parts))[:, 1]
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
