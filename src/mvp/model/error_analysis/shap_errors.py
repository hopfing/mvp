"""3C — SHAP-on-errors meta-model.

Fit XGBoost predicting the signed residual `(y_true - y_prob)` as a continuous
target. Use SHAP importance to rank features by contribution to systematic
directional bias.

The signed-residual framing surfaces directional miscalibration (which way
the model is wrong), distinct from squared residual (which conflates direction
with variance) and sign-only (which discards magnitude).
"""
from __future__ import annotations

import logging
from typing import Sequence

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)


def shap_on_errors(
    df: pl.DataFrame,
    *,
    prob_col: str = "y_prob",
    target_col: str = "y_test",
    feature_cols: Sequence[str] | None = None,
    n_estimators: int = 300,
    max_depth: int = 4,
    learning_rate: float = 0.05,
    random_state: int = 42,
) -> pl.DataFrame:
    """Fit XGBoost on signed residual, return SHAP-based feature importance.

    Returns:
        DataFrame sorted by mean_abs_shap descending. Columns: feature,
        mean_abs_shap, mean_signed_shap, rank.

    `mean_signed_shap` indicates the direction in which the feature pulls the
    residual: positive → feature contributes to model being too low
    (underconfident); negative → feature contributes to model being too high
    (overconfident).
    """
    try:
        import shap
        import xgboost as xgb
    except ImportError as e:
        raise RuntimeError(
            "SHAP-on-errors requires xgboost and shap; install both."
        ) from e

    if feature_cols is None:
        from mvp.model.error_analysis.analyses import _identify_feature_columns

        feature_cols = _identify_feature_columns(df)
    else:
        feature_cols = [c for c in feature_cols if c in df.columns]

    if not feature_cols:
        raise ValueError("No feature columns identified for SHAP analysis.")

    X = df.select(
        pl.col(c).cast(pl.Float64) for c in feature_cols
    ).to_numpy()
    y_prob = df[prob_col].to_numpy().astype(np.float64)
    y_true = df[target_col].to_numpy().astype(np.float64)
    residual = y_true - y_prob  # signed; positive = underconfident

    logger.info(
        "Fitting meta-XGB on signed residual: %d rows x %d features",
        len(residual), len(feature_cols),
    )

    model = xgb.XGBRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        random_state=random_state,
        n_jobs=-1,
        tree_method="hist",
        enable_categorical=False,
    )
    model.fit(X, residual)

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    # For XGBRegressor, shap_values shape is (n_samples, n_features)
    mean_abs = np.abs(shap_values).mean(axis=0)
    mean_signed = shap_values.mean(axis=0)

    rows = [
        {
            "feature": feat,
            "mean_abs_shap": float(mean_abs[i]),
            "mean_signed_shap": float(mean_signed[i]),
        }
        for i, feat in enumerate(feature_cols)
    ]
    df_out = (
        pl.DataFrame(rows)
        .sort("mean_abs_shap", descending=True)
        .with_row_index("rank", offset=1)
    )
    return df_out
