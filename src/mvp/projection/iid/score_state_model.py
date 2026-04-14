"""Score-state-dependent serve model: P(point_won_by_server | features).

Operates at point grain (one row per point). Features mix:
  - match-level (broadcast to every point in a match, server perspective)
  - point-level (vary per point: score state, serve_num, flags)

The output is a calibrated per-point probability. Chain integration (feeding
this into `p_service_game_win` as a score-state-aware callable) happens in
Phase 3; this module only trains and evaluates the point-grain classifier.
"""

from abc import ABC, abstractmethod
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier


class ScoreStateServeModel(ABC):
    """Point-grain serve-win classifier. Trains/predicts on feature matrices."""

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray) -> None: ...

    @abstractmethod
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return P(point_won_by_server) per row (1-D float64, in [0, 1])."""

    @abstractmethod
    def coef_summary(self) -> dict[str, Any] | None:
        """Per-feature coefficient / importance summary for interpretable forms.

        Returns None for models without natively-interpretable coefficients.
        """


class LogisticScoreStateModel(ScoreStateServeModel):
    """Logistic regression on standardized features.

    Standardization handled inside fit/predict so that NaN → mean imputation
    and std-normalization follow the same contract as MatchupServeModel.
    """

    def __init__(self, feature_names: list[str], params: dict[str, Any] | None = None) -> None:
        if not feature_names:
            raise ValueError("feature_names must be non-empty")
        self.feature_names = list(feature_names)
        # sklearn defaults work reasonably; allow override via config.
        base_params: dict[str, Any] = {
            "max_iter": 1000,
            "solver": "lbfgs",
            "C": 1.0,
        }
        base_params.update(params or {})
        self._params = base_params
        self._model: LogisticRegression | None = None
        self._mean: np.ndarray | None = None
        self._std: np.ndarray | None = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        if X.shape[1] != len(self.feature_names):
            raise ValueError(
                f"X has {X.shape[1]} columns but feature_names has {len(self.feature_names)}"
            )
        valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
        X_valid = X[valid].astype(np.float64)
        y_valid = y[valid].astype(np.int64)
        if len(X_valid) == 0:
            raise ValueError("no valid training rows after dropping NaN/non-finite")

        self._mean = X_valid.mean(axis=0)
        self._std = X_valid.std(axis=0)
        self._std = np.where(self._std == 0, 1.0, self._std)
        X_scaled = (X_valid - self._mean) / self._std

        self._model = LogisticRegression(**self._params)
        self._model.fit(X_scaled, y_valid)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None or self._mean is None or self._std is None:
            raise RuntimeError("LogisticScoreStateModel.predict_proba called before fit")
        X_f = X.astype(np.float64)
        X_f = np.where(np.isnan(X_f), self._mean, X_f)
        X_scaled = (X_f - self._mean) / self._std
        # predict_proba returns shape (N, 2); pick class-1 (point won by server).
        return self._model.predict_proba(X_scaled)[:, 1]

    def coef_summary(self) -> dict[str, Any] | None:
        if self._model is None:
            return None
        coefs = self._model.coef_.ravel()
        intercept = float(self._model.intercept_[0])
        return {
            "intercept": intercept,
            "coefs": dict(zip(self.feature_names, [float(c) for c in coefs], strict=True)),
        }


class XGBoostScoreStateModel(ScoreStateServeModel):
    """XGBoost binary classifier on the raw feature matrix.

    No standardization (XGBoost is scale-invariant). NaN handled natively.
    """

    def __init__(self, feature_names: list[str], params: dict[str, Any] | None = None) -> None:
        if not feature_names:
            raise ValueError("feature_names must be non-empty")
        self.feature_names = list(feature_names)
        base_params: dict[str, Any] = {
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "n_jobs": -1,
            "random_state": 42,
            "tree_method": "hist",
        }
        base_params.update(params or {})
        self._params = base_params
        self._model: XGBClassifier | None = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        if X.shape[1] != len(self.feature_names):
            raise ValueError(
                f"X has {X.shape[1]} columns but feature_names has {len(self.feature_names)}"
            )
        X_f = X.astype(np.float32)
        y_i = y.astype(np.int64)
        self._model = XGBClassifier(**self._params)
        self._model.fit(X_f, y_i)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("XGBoostScoreStateModel.predict_proba called before fit")
        return self._model.predict_proba(X.astype(np.float32))[:, 1]

    def coef_summary(self) -> dict[str, Any] | None:
        if self._model is None:
            return None
        importances = self._model.feature_importances_
        return {
            "feature_importances": dict(
                zip(self.feature_names, [float(i) for i in importances], strict=True)
            ),
        }


def build_score_state_model(
    *,
    type_: str,
    feature_names: list[str],
    params: dict[str, Any] | None = None,
) -> ScoreStateServeModel:
    if type_ == "logistic":
        return LogisticScoreStateModel(feature_names=feature_names, params=params)
    if type_ == "xgboost":
        return XGBoostScoreStateModel(feature_names=feature_names, params=params)
    raise ValueError(f"unknown score-state model type: {type_}")
