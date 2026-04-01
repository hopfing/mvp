"""Model wrappers for experiments."""


import os
from abc import ABC, abstractmethod
from typing import Any

import numpy as np

_n_jobs_override: int | None = None


def set_n_jobs_override(n: int | None) -> None:
    """Set a global n_jobs override for all models."""
    global _n_jobs_override
    _n_jobs_override = n


def _default_n_jobs() -> int:
    """Return n_jobs: global override if set, else cpu_count - 2."""
    if _n_jobs_override is not None:
        return _n_jobs_override
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


def _make_embedding_mlp():
    """Lazy factory to avoid top-level torch import."""
    import torch
    import torch.nn as nn

    class EmbeddingMLP(nn.Module):
        def __init__(
            self,
            n_features: int,
            n_players: int,
            embedding_dim: int,
            hidden_layers: list[int],
            dropout: float,
            batch_norm: bool,
            layer_norm: bool = False,
            dual_embedding: bool = False,
        ):
            super().__init__()
            self.embedding = nn.Embedding(n_players + 1, embedding_dim, padding_idx=0)
            self.dual_embedding = dual_embedding
            n_emb = 2 * embedding_dim if dual_embedding else embedding_dim
            mlp_input_dim = n_features + n_emb
            layers: list[nn.Module] = []
            in_dim = mlp_input_dim
            for hidden_dim in hidden_layers:
                layers.append(nn.Linear(in_dim, hidden_dim))
                if layer_norm:
                    layers.append(nn.LayerNorm(hidden_dim))
                elif batch_norm:
                    layers.append(nn.BatchNorm1d(hidden_dim))
                layers.append(nn.ReLU())
                if dropout > 0:
                    layers.append(nn.Dropout(dropout))
                in_dim = hidden_dim
            layers.append(nn.Linear(in_dim, 1))
            layers.append(nn.Sigmoid())
            self.mlp = nn.Sequential(*layers)

        def forward(
            self,
            x: torch.Tensor,
            player_idx: torch.Tensor,
            opp_idx: torch.Tensor | None = None,
        ) -> torch.Tensor:
            player_emb = self.embedding(player_idx)
            parts = [x, player_emb]
            if opp_idx is not None:
                opp_emb = self.embedding(opp_idx)
                parts.append(opp_emb)
            combined = torch.cat(parts, dim=1)
            return self.mlp(combined)

    return EmbeddingMLP


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
        self.embedding_dim: int = params.get("embedding_dim", 0)
        self.embedding_col_idx: int | None = params.get("embedding_col_idx", None)
        self.opp_embedding_col_idx: int | None = params.get("opp_embedding_col_idx", None)
        self.n_players: int = params.get("n_players", 0)
        self.shuffle: bool = params.get("shuffle", True)
        self.finetune_frac: float = params.get("finetune_frac", 0.0)
        self.finetune_lr: float = params.get("finetune_lr", 0.0001)
        self.finetune_epochs: int = params.get("finetune_epochs", 100)
        self.finetune_patience: int = params.get("finetune_patience", 10)
        self.device: str | None = params.get("device", None)
        self.label_smoothing: float = params.get("label_smoothing", 0.0)
        self.weight_decay: float = params.get("weight_decay", 0.0)
        self.grad_clip_norm: float | None = params.get("grad_clip_norm", None)
        self.lr_scheduler: str | None = params.get("lr_scheduler", None)
        self.lr_scheduler_factor: float = params.get("lr_scheduler_factor", 0.5)
        self.lr_scheduler_patience: int = params.get("lr_scheduler_patience", 5)
        self.layer_norm: bool = params.get("layer_norm", False)
        if self.batch_norm and self.layer_norm:
            raise ValueError(
                "batch_norm and layer_norm are mutually exclusive; enable only one"
            )
        if self.opp_embedding_col_idx is not None and not (
            self.embedding_dim > 0 and self.n_players > 0 and self.embedding_col_idx is not None
        ):
            raise ValueError(
                "opp_embedding_col_idx requires embedding_dim, embedding_col_idx, "
                "and n_players to also be set"
            )
        self._module = None
        self._device = None
        self._n_features = None

    @property
    def _has_embeddings(self) -> bool:
        return self.embedding_dim > 0 and self.n_players > 0 and self.embedding_col_idx is not None

    @property
    def _has_opp_embeddings(self) -> bool:
        return self._has_embeddings and self.opp_embedding_col_idx is not None

    def _build_module(self, n_features: int):
        import torch.nn as nn

        if self._has_embeddings:
            EmbeddingMLP = _make_embedding_mlp()
            return EmbeddingMLP(
                n_features=n_features,
                n_players=self.n_players,
                embedding_dim=self.embedding_dim,
                hidden_layers=self.hidden_layers,
                dropout=self.dropout,
                batch_norm=self.batch_norm,
                layer_norm=self.layer_norm,
                dual_embedding=self._has_opp_embeddings,
            )

        layers: list[nn.Module] = []
        in_dim = n_features
        for hidden_dim in self.hidden_layers:
            layers.append(nn.Linear(in_dim, hidden_dim))
            if self.layer_norm:
                layers.append(nn.LayerNorm(hidden_dim))
            elif self.batch_norm:
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

        if self.device is not None:
            return torch.device(self.device)
        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")

    def _split_embedding_col(
        self, X: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None]:
        """Separate embedding ID column(s) from feature columns."""
        if not self._has_embeddings:
            return X, None, None
        cols_to_remove = [self.embedding_col_idx]
        opp_ids = None
        if self._has_opp_embeddings:
            cols_to_remove.append(self.opp_embedding_col_idx)
            opp_ids = X[:, self.opp_embedding_col_idx].astype(int)
        emb_ids = X[:, self.embedding_col_idx].astype(int)
        X_features = np.delete(X, cols_to_remove, axis=1)
        return X_features, emb_ids, opp_ids

    def _make_optimizer(self, params, lr: float):
        """Create optimizer — AdamW when weight_decay > 0, else Adam."""
        import torch

        if self.weight_decay > 0:
            return torch.optim.AdamW(params, lr=lr, weight_decay=self.weight_decay)
        return torch.optim.Adam(params, lr=lr)

    def _forward(self, X_t, emb_t=None, opp_emb_t=None):
        """Forward pass handling plain MLP, single embedding, and dual embedding."""
        if emb_t is not None:
            return self._module(X_t, emb_t, opp_emb_t)
        return self._module(X_t)

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        sample_weight: np.ndarray | None = None,
    ) -> None:
        import torch
        from torch.utils.data import DataLoader, TensorDataset

        self._device = self._get_device()

        # Separate embedding column before counting features
        X_features, emb_ids, opp_emb_ids = self._split_embedding_col(X)
        self._n_features = X_features.shape[1]
        self._module = self._build_module(self._n_features).to(self._device)

        # Temporal train/val split (last 15%)
        val_size = max(1, int(len(X_features) * 0.15))
        X_train, X_val = X_features[:-val_size], X_features[-val_size:]
        y_train, y_val = y[:-val_size], y[-val_size:]
        w_train = sample_weight[:-val_size] if sample_weight is not None else None

        X_t = torch.tensor(X_train, dtype=torch.float32, device=self._device)
        y_t = torch.tensor(y_train, dtype=torch.float32, device=self._device).unsqueeze(1)
        X_v = torch.tensor(X_val, dtype=torch.float32, device=self._device)
        y_v = torch.tensor(y_val, dtype=torch.float32, device=self._device).unsqueeze(1)

        # Embedding index tensors
        if emb_ids is not None:
            emb_t = torch.tensor(emb_ids[:-val_size], dtype=torch.long, device=self._device)
            emb_v = torch.tensor(emb_ids[-val_size:], dtype=torch.long, device=self._device)
        else:
            emb_t = None
            emb_v = None
        if opp_emb_ids is not None:
            opp_emb_t = torch.tensor(opp_emb_ids[:-val_size], dtype=torch.long, device=self._device)
            opp_emb_v = torch.tensor(opp_emb_ids[-val_size:], dtype=torch.long, device=self._device)
        else:
            opp_emb_t = None
            opp_emb_v = None

        if w_train is not None:
            w_t = torch.tensor(w_train, dtype=torch.float32, device=self._device).unsqueeze(1)
        else:
            w_t = None

        optimizer = self._make_optimizer(self._module.parameters(), self.learning_rate)
        scheduler = None
        if self.lr_scheduler == "plateau":
            scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                optimizer,
                mode="min",
                factor=self.lr_scheduler_factor,
                patience=self.lr_scheduler_patience,
            )
        loss_fn = torch.nn.BCELoss(reduction="none")

        # Build dataset with optional embedding and weight tensors
        tensors = [X_t, y_t]
        if emb_t is not None:
            tensors.append(emb_t)
        if opp_emb_t is not None:
            tensors.append(opp_emb_t)
        if w_t is not None:
            tensors.append(w_t)
        dataset = TensorDataset(*tensors)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=self.shuffle)

        best_val_loss = float("inf")
        best_state = None
        wait = 0
        has_emb = emb_t is not None
        has_w = w_t is not None
        has_opp_emb = opp_emb_t is not None

        for _epoch in range(self.epochs):
            self._module.train()
            for batch in loader:
                idx = 0
                xb = batch[idx]
                idx += 1
                yb = batch[idx]
                idx += 1
                eb = batch[idx] if has_emb else None
                if has_emb:
                    idx += 1
                oeb = batch[idx] if has_opp_emb else None
                if has_opp_emb:
                    idx += 1
                wb = batch[idx] if has_w else None

                if self.label_smoothing > 0:
                    yb = yb * (1 - self.label_smoothing) + 0.5 * self.label_smoothing
                optimizer.zero_grad()
                pred = self._forward(xb, eb, oeb)
                loss = loss_fn(pred, yb)
                if wb is not None:
                    loss = (loss * wb).mean()
                else:
                    loss = loss.mean()
                loss.backward()
                if self.grad_clip_norm is not None:
                    torch.nn.utils.clip_grad_norm_(
                        self._module.parameters(), max_norm=self.grad_clip_norm
                    )
                optimizer.step()

            # Validation
            self._module.eval()
            with torch.no_grad():
                val_pred = self._forward(X_v, emb_v, opp_emb_v)
                val_loss = loss_fn(val_pred, y_v).mean().item()

            if scheduler is not None:
                scheduler.step(val_loss)
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

        # Phase 2: fine-tune on recent data only
        if self.finetune_frac > 0:
            n_total = len(X_train)
            n_recent = max(1, int(n_total * self.finetune_frac))
            ft_X = X_t[-n_recent:]
            ft_y = y_t[-n_recent:]
            ft_emb = emb_t[-n_recent:] if emb_t is not None else None
            ft_opp_emb = opp_emb_t[-n_recent:] if opp_emb_t is not None else None
            ft_w = w_t[-n_recent:] if w_t is not None else None

            ft_tensors = [ft_X, ft_y]
            if ft_emb is not None:
                ft_tensors.append(ft_emb)
            if ft_opp_emb is not None:
                ft_tensors.append(ft_opp_emb)
            if ft_w is not None:
                ft_tensors.append(ft_w)
            ft_dataset = TensorDataset(*ft_tensors)
            ft_loader = DataLoader(ft_dataset, batch_size=self.batch_size, shuffle=True)

            ft_optimizer = self._make_optimizer(self._module.parameters(), self.finetune_lr)
            ft_scheduler = None
            if self.lr_scheduler == "plateau":
                ft_scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                    ft_optimizer,
                    mode="min",
                    factor=self.lr_scheduler_factor,
                    patience=self.lr_scheduler_patience,
                )
            best_val_loss = float("inf")
            best_state = None
            wait = 0

            for _epoch in range(self.finetune_epochs):
                self._module.train()
                for batch in ft_loader:
                    idx = 0
                    xb = batch[idx]
                    idx += 1
                    yb = batch[idx]
                    idx += 1
                    eb = batch[idx] if has_emb else None
                    if has_emb:
                        idx += 1
                    oeb = batch[idx] if has_opp_emb else None
                    if has_opp_emb:
                        idx += 1
                    wb = batch[idx] if has_w else None

                    if self.label_smoothing > 0:
                        yb = yb * (1 - self.label_smoothing) + 0.5 * self.label_smoothing
                    ft_optimizer.zero_grad()
                    pred = self._forward(xb, eb, oeb)
                    loss = loss_fn(pred, yb)
                    if wb is not None:
                        loss = (loss * wb).mean()
                    else:
                        loss = loss.mean()
                    loss.backward()
                    if self.grad_clip_norm is not None:
                        torch.nn.utils.clip_grad_norm_(
                            self._module.parameters(), max_norm=self.grad_clip_norm
                        )
                    ft_optimizer.step()

                self._module.eval()
                with torch.no_grad():
                    val_pred = self._forward(X_v, emb_v, opp_emb_v)
                    val_loss = loss_fn(val_pred, y_v).mean().item()

                if ft_scheduler is not None:
                    ft_scheduler.step(val_loss)
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    best_state = {
                        k: v.cpu().clone() for k, v in self._module.state_dict().items()
                    }
                    wait = 0
                else:
                    wait += 1
                    if wait >= self.finetune_patience:
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
            X_features, emb_ids, opp_emb_ids = self._split_embedding_col(X)
            X_t = torch.tensor(X_features, dtype=torch.float32, device=self._device)
            emb_t = None
            opp_emb_t = None
            if emb_ids is not None:
                emb_t = torch.tensor(emb_ids, dtype=torch.long, device=self._device)
            if opp_emb_ids is not None:
                opp_emb_t = torch.tensor(opp_emb_ids, dtype=torch.long, device=self._device)
            preds = self._forward(X_t, emb_t, opp_emb_t).squeeze(1).cpu().numpy()
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
