"""Feature importance computation methods."""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.inspection import permutation_importance as sklearn_permutation_importance

from mvp.model.models import BaseModel


def gain_importance(
    model: BaseModel,
    feature_names: list[str],
) -> dict[str, float]:
    """Compute gain-based feature importance from tree models.

    Uses the built-in feature_importances_ from XGBoost/LightGBM which
    measures the total gain from splits using each feature.

    Args:
        model: Trained model wrapper (must be tree-based like XGBoost).
        feature_names: List of feature names matching model input columns.

    Returns:
        Dictionary mapping feature names to importance scores (normalized to sum to 1).

    Raises:
        ValueError: If model doesn't support gain importance.
    """
    underlying = getattr(model, "_model", None)
    if underlying is None:
        raise ValueError("Model has no underlying _model attribute")

    if not hasattr(underlying, "feature_importances_"):
        raise ValueError(
            f"Model type {type(underlying).__name__} does not support gain importance. "
            "Use permutation_importance instead."
        )

    importances = underlying.feature_importances_

    if len(importances) != len(feature_names):
        raise ValueError(
            f"Number of importances ({len(importances)}) doesn't match "
            f"number of feature names ({len(feature_names)})"
        )

    total = importances.sum()
    if total > 0:
        normalized = importances / total
    else:
        normalized = importances

    return {name: float(imp) for name, imp in zip(feature_names, normalized)}


def permutation_importance(
    model: BaseModel,
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    n_repeats: int = 10,
    random_state: int = 42,
) -> dict[str, float]:
    """Compute permutation feature importance.

    Measures importance by shuffling each feature and measuring the drop
    in model accuracy. More reliable than gain but slower.

    Args:
        model: Trained model wrapper.
        X: Feature matrix (n_samples, n_features).
        y: Target array (n_samples,).
        feature_names: List of feature names matching X columns.
        n_repeats: Number of times to shuffle each feature.
        random_state: Random seed for reproducibility.

    Returns:
        Dictionary mapping feature names to importance scores (normalized to sum to 1).
    """
    if X.shape[1] != len(feature_names):
        raise ValueError(
            f"Number of columns ({X.shape[1]}) doesn't match "
            f"number of feature names ({len(feature_names)})"
        )

    underlying = getattr(model, "_model", None)
    if underlying is None:
        raise ValueError("Model has no underlying _model attribute")

    result = sklearn_permutation_importance(
        underlying,
        X,
        y,
        n_repeats=n_repeats,
        random_state=random_state,
        scoring="accuracy",
    )

    importances = result.importances_mean
    importances = np.maximum(importances, 0)

    total = importances.sum()
    if total > 0:
        normalized = importances / total
    else:
        normalized = importances

    return {name: float(imp) for name, imp in zip(feature_names, normalized)}


def shap_importance(
    model: BaseModel,
    X: np.ndarray,
    feature_names: list[str],
    sample_size: int = 10000,
    random_state: int = 42,
) -> dict[str, float]:
    """Compute SHAP-based feature importance.

    Uses SHAP values to measure per-feature contribution. Most accurate
    but slowest method. Samples data for performance.

    Args:
        model: Trained model wrapper.
        X: Feature matrix (n_samples, n_features).
        feature_names: List of feature names matching X columns.
        sample_size: Number of samples to use (for performance).
        random_state: Random seed for sampling.

    Returns:
        Dictionary mapping feature names to importance scores (normalized to sum to 1).

    Raises:
        ImportError: If shap package is not installed.
    """
    try:
        import shap
    except ImportError:
        raise ImportError(
            "SHAP is not installed. Install with: pip install shap"
        )

    if X.shape[1] != len(feature_names):
        raise ValueError(
            f"Number of columns ({X.shape[1]}) doesn't match "
            f"number of feature names ({len(feature_names)})"
        )

    underlying = getattr(model, "_model", None)
    if underlying is None:
        raise ValueError("Model has no underlying _model attribute")

    if len(X) > sample_size:
        rng = np.random.default_rng(random_state)
        indices = rng.choice(len(X), size=sample_size, replace=False)
        X_sample = X[indices]
    else:
        X_sample = X

    explainer = shap.TreeExplainer(underlying)
    shap_values = explainer.shap_values(X_sample)

    if isinstance(shap_values, list):
        shap_values = shap_values[1]

    importances = np.abs(shap_values).mean(axis=0)

    total = importances.sum()
    if total > 0:
        normalized = importances / total
    else:
        normalized = importances

    return {name: float(imp) for name, imp in zip(feature_names, normalized)}


def compute_importance(
    model: BaseModel,
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    method: str = "permutation",
    **kwargs: Any,
) -> dict[str, float]:
    """Compute feature importance using specified method.

    Args:
        model: Trained model wrapper.
        X: Feature matrix (n_samples, n_features).
        y: Target array (n_samples,).
        feature_names: List of feature names matching X columns.
        method: Importance method ("gain", "permutation", "shap").
        **kwargs: Additional arguments passed to the importance method.

    Returns:
        Dictionary mapping feature names to importance scores.

    Raises:
        ValueError: If method is unknown.
    """
    if method == "gain":
        return gain_importance(model, feature_names)
    elif method == "permutation":
        return permutation_importance(model, X, y, feature_names, **kwargs)
    elif method == "shap":
        return shap_importance(model, X, feature_names, **kwargs)
    else:
        raise ValueError(
            f"Unknown importance method: {method}. "
            "Choose from: gain, permutation, shap"
        )
