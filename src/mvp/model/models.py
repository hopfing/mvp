"""Model wrappers for experiments."""


import os
from abc import ABC, abstractmethod
from typing import Any

import numpy as np


def _default_n_jobs() -> int:
    """Return a capped n_jobs value, leaving 2 cores for the OS."""
    cpu_count = os.cpu_count() or 4
    return max(1, cpu_count - 2)


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
        eval_set: list[tuple[np.ndarray, np.ndarray]] | None = None,
        early_stopping_rounds: int | None = 10,
    ) -> None:
        import xgboost as xgb

        self._model = xgb.XGBClassifier(**self.params)
        fit_kwargs: dict[str, Any] = {"sample_weight": sample_weight}
        if eval_set is not None:
            fit_kwargs["eval_set"] = eval_set
            fit_kwargs["verbose"] = False
            if early_stopping_rounds is not None:
                self._model.set_params(early_stopping_rounds=early_stopping_rounds)
        self._model.fit(X, y, **fit_kwargs)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)[:, 1]


class LogisticModel(BaseModel):
    """Logistic regression classifier wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, "max_iter": 1000, **params}
        self._model = None

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> None:
        from sklearn.linear_model import LogisticRegression

        self._model = LogisticRegression(**self.params)
        self._model.fit(X, y, sample_weight=sample_weight)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)[:, 1]


class RandomForestModel(BaseModel):
    """Random forest classifier wrapper."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = {"random_state": 42, "n_jobs": _default_n_jobs(), **params}
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
        self._meta_model_params: dict[str, Any] = params.get("meta_model_params", {})
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
        per_model_data: (
            list[tuple[np.ndarray, np.ndarray, np.ndarray | None] | None] | None
        ) = None,
    ) -> None:
        if not self._sub_models:
            raise RuntimeError("EnsembleModel not configured. Call configure() first.")
        for i, (sub, indices) in enumerate(zip(self._sub_models, self._feature_indices)):
            if per_model_data and per_model_data[i] is not None:
                entry = per_model_data[i]
                X_sub, y_sub = entry[0], entry[1]
                w_sub = entry[2] if len(entry) > 2 else None
                sub.fit(X_sub[:, indices], y_sub, sample_weight=w_sub)
            else:
                sub.fit(X[:, indices], y, sample_weight=sample_weight)
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

        self._meta_model = LogisticRegression(
            max_iter=1000, random_state=42, **self._meta_model_params
        )
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


class NeuralNetModel(BaseModel):
    """PyTorch MLP wrapper for tabular classification."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.hidden_layers: list[int] = params.get("hidden_layers", [64, 32])
        self.dropout: float = params.get("dropout", 0.3)
        self.learning_rate: float = params.get("learning_rate", 0.001)
        self.batch_size: int = params.get("batch_size", 512)
        self.epochs: int = params.get("epochs", 200)
        self.patience: int = params.get("patience", 15)
        self.batch_norm: bool = params.get("batch_norm", False)
        self._module = None
        self._device = None
        self._n_features = None

    def _build_module(self, n_features: int):
        import torch.nn as nn

        layers: list[nn.Module] = []
        in_dim = n_features
        for hidden_dim in self.hidden_layers:
            layers.append(nn.Linear(in_dim, hidden_dim))
            if self.batch_norm:
                layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(nn.ReLU())
            if self.dropout > 0:
                layers.append(nn.Dropout(self.dropout))
            in_dim = hidden_dim
        layers.append(nn.Linear(in_dim, 1))
        layers.append(nn.Sigmoid())
        return nn.Sequential(*layers)

    def _get_device(self):
        import torch

        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        import torch
        from torch.utils.data import DataLoader, TensorDataset

        self._device = self._get_device()
        self._n_features = X.shape[1]
        self._module = self._build_module(self._n_features).to(self._device)

        # Temporal train/val split (last 15%)
        val_size = max(1, int(len(X) * 0.15))
        X_train, X_val = X[:-val_size], X[-val_size:]
        y_train, y_val = y[:-val_size], y[-val_size:]
        w_train = sample_weight[:-val_size] if sample_weight is not None else None

        X_t = torch.tensor(X_train, dtype=torch.float32, device=self._device)
        y_t = torch.tensor(y_train, dtype=torch.float32, device=self._device).unsqueeze(1)
        X_v = torch.tensor(X_val, dtype=torch.float32, device=self._device)
        y_v = torch.tensor(y_val, dtype=torch.float32, device=self._device).unsqueeze(1)

        if w_train is not None:
            w_t = torch.tensor(w_train, dtype=torch.float32, device=self._device).unsqueeze(1)
        else:
            w_t = None

        optimizer = torch.optim.Adam(self._module.parameters(), lr=self.learning_rate)
        loss_fn = torch.nn.BCELoss(reduction="none")

        dataset = TensorDataset(X_t, y_t) if w_t is None else TensorDataset(X_t, y_t, w_t)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True)

        best_val_loss = float("inf")
        best_state = None
        wait = 0

        for _epoch in range(self.epochs):
            self._module.train()
            for batch in loader:
                if w_t is not None:
                    xb, yb, wb = batch
                else:
                    xb, yb = batch
                    wb = None

                optimizer.zero_grad()
                pred = self._module(xb)
                loss = loss_fn(pred, yb)
                if wb is not None:
                    loss = (loss * wb).mean()
                else:
                    loss = loss.mean()
                loss.backward()
                optimizer.step()

            # Validation
            self._module.eval()
            with torch.no_grad():
                val_pred = self._module(X_v)
                val_loss = loss_fn(val_pred, y_v).mean().item()

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {
                    k: v.cpu().clone() for k, v in self._module.state_dict().items()
                }
                wait = 0
            else:
                wait += 1
                if wait >= self.patience:
                    break

        if best_state is not None:
            self._module.load_state_dict(best_state)
        self._module.eval()

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        import torch

        if self._module is None:
            raise RuntimeError("Model not fitted")
        self._module.eval()
        with torch.no_grad():
            X_t = torch.tensor(X, dtype=torch.float32, device=self._device)
            preds = self._module(X_t).squeeze(1).cpu().numpy()
        return preds

    def __getstate__(self):
        state = self.__dict__.copy()
        if self._module is not None:
            state["_module_state_dict"] = {
                k: v.cpu() for k, v in self._module.state_dict().items()
            }
        else:
            state["_module_state_dict"] = None
        state["_module"] = None
        state["_device"] = None
        return state

    def __setstate__(self, state):
        module_state = state.pop("_module_state_dict", None)
        self.__dict__.update(state)
        self._device = self._get_device()
        if module_state is not None and self._n_features is not None:
            self._module = self._build_module(self._n_features).to(self._device)
            self._module.load_state_dict(
                {k: v.to(self._device) for k, v in module_state.items()}
            )
            self._module.eval()


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
    elif model_type == "neural_net":
        return NeuralNetModel(params)
    else:
        raise ValueError(f"Unknown model type: {model_type}")
