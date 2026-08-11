"""Offset (base_margin) fitting, shared by the discovery, training and
production paths.

The model's level contribution for one feature is fixed as a per-row starting
log-odds, so no tree split can re-earn it. Everything here exists to make one
rule hard to break:

    Take the offset column from the SAME X the adjacent fit/predict_proba call
    receives -- post-imputation, post-scaling.

A 2-parameter logistic is invariant to a consistent affine rescale of its single
input, so raw or standardized both work; mixing them does not. Fitting on raw Elo
gives a slope near ln(10)/400 = 0.0058, and applying that to a standardized
column (~2.5 instead of ~400) yields a margin near 0.014 instead of 2.3 -- the
offset silently collapses to a no-op while the trees expect a strong starting
point. No exception, just wrong probabilities. Hence `fit_offset` and
`offset_margin` both take (X, col_idx) rather than a bare column: the caller has
to name the matrix it is using.
"""

from typing import TYPE_CHECKING

import numpy as np
from sklearn.linear_model import LogisticRegression

if TYPE_CHECKING:  # pragma: no cover
    from mvp.model.config import OffsetConfig


def resolve_offset_col(feature_cols: list[str], feature: str) -> int:
    """Index of the offset feature within `feature_cols`.

    Resolve against `feature_cols`, never the wider `augmented_cols`
    (`feature_cols + aux_base_col_names`): the two have different truncation
    points, and X is truncated to `len(feature_cols)` before the model sees it.
    """
    try:
        return feature_cols.index(feature)
    except ValueError:
        raise ValueError(
            f"offset feature {feature!r} is not in feature_cols. It must be in "
            f"features.include and must not be compute_only. Got "
            f"{len(feature_cols)} columns."
        ) from None


def fit_offset(
    X: np.ndarray, col_idx: int, y: np.ndarray, cfg: "OffsetConfig"
) -> LogisticRegression:
    """Fit the offset model on one column of `X`. TRAIN ROWS ONLY.

    `X` must be the same matrix passed to the model's `fit` for these rows.
    Fitting the offset on rows the model is scored against contaminates every
    downstream metric, so callers must slice to the training fold first.
    """
    # C=1e6 is effectively unregularized, and deliberately so. sklearn's default
    # C=1.0 penalizes the COEFFICIENT, whose magnitude depends on the input's
    # scale: on raw Elo the slope is ~0.0058 and the penalty is negligible, but
    # on a standardized column it is ~0.87 and gets shrunk ~11%. That would make
    # the offset's strength depend on which representation it happened to be fit
    # on, and would give FS (raw X_wide) and production (scaled X) different
    # offsets from identical data. Two parameters over tens of thousands of rows
    # do not need shrinkage.
    params = {"max_iter": 1000, "C": 1e6, **(cfg.params or {})}
    col = np.asarray(X[:, col_idx], dtype=np.float64).reshape(-1, 1)
    model = LogisticRegression(**params)
    model.fit(col, y)
    return model


def offset_margin(model: LogisticRegression, X: np.ndarray, col_idx: int) -> np.ndarray:
    """Raw log-odds for the rows of `X` -- exactly base_margin's space.

    `X` must be in the same representation the offset was fit on (see module
    docstring).
    """
    col = np.asarray(X[:, col_idx], dtype=np.float64).reshape(-1, 1)
    return model.decision_function(col)
