"""SHAP-based one-shot feature ranking.

Trains one XGBoost per fold on the full feature pool, extracts per-feature
SHAP values via XGBoost's built-in `pred_contribs`, and aggregates mean |SHAP|
across folds and rows. Output is a ranking complementary to forward selection's
greedy ranking: features penalized here are redundant given the full pool,
features rewarded contribute uniquely.
"""

import logging
import time
from pathlib import Path

import numpy as np
import polars as pl

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector
from mvp.model.engine import get_feature_columns

logger = logging.getLogger(__name__)


class ShapRanker:
    """Rank features by mean |SHAP| in the presence of the full feature pool."""

    def __init__(
        self,
        config: DiscoveryConfig,
        all_feature_specs: list[str],
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.config = config
        self.all_feature_specs = all_feature_specs
        self.fast = FastForwardSelector(
            config=config,
            all_feature_specs=all_feature_specs,
            matches_path=matches_path,
            cache_dir=cache_dir,
        )

    def precompute(self) -> None:
        """Load data, compute features, build folds (reuses FS precompute)."""
        self.fast.precompute()

    def rank(self) -> pl.DataFrame:
        """Train per-fold XGB on full feature set, return ranked DataFrame."""
        import xgboost as xgb

        fs = self.fast
        if fs.X_wide is None:
            raise RuntimeError("Must call precompute() before rank()")
        if self.config.model.type != "xgboost":
            raise ValueError(
                f"ShapRanker requires model.type='xgboost', "
                f"got {self.config.model.type}"
            )

        col_names = get_feature_columns(self.all_feature_specs)
        col_indices = np.array([fs.col_to_idx[c] for c in col_names])

        model_params = dict(self.config.model.params or {})
        n_estimators = model_params.pop("n_estimators", 100)
        xgb_params = {
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            **model_params,
        }

        sum_abs_shap = np.zeros(len(col_indices))
        total_rows = 0

        logger.info(
            "SHAP ranking: %d features over %d folds",
            len(col_indices), len(fs.folds),
        )

        for fold_idx, (train_idx, test_idx) in enumerate(fs.folds):
            t0 = time.perf_counter()
            X_train = fs.X_wide[np.ix_(train_idx, col_indices)].copy()
            X_test = fs.X_wide[np.ix_(test_idx, col_indices)].copy()
            y_train = fs.y[train_idx]

            medians = fs.fold_medians[fold_idx][col_indices]
            X_train = np.where(np.isnan(X_train), medians, X_train)
            X_test = np.where(np.isnan(X_test), medians, X_test)

            dtrain = xgb.DMatrix(X_train, label=y_train)
            dtest = xgb.DMatrix(X_test)
            if fs.sample_weights is not None:
                dtrain.set_weight(fs.sample_weights[train_idx])

            booster = xgb.train(
                xgb_params, dtrain, num_boost_round=n_estimators,
            )

            # pred_contribs: (n_rows, n_features + 1); last column is bias
            contribs = booster.predict(dtest, pred_contribs=True)
            sum_abs_shap += np.abs(contribs[:, :-1]).sum(axis=0)
            total_rows += len(test_idx)

            logger.info(
                "  Fold %d/%d: %d test rows in %.1fs",
                fold_idx + 1, len(fs.folds),
                len(test_idx), time.perf_counter() - t0,
            )

        mean_abs_shap = sum_abs_shap / total_rows
        ranking = (
            pl.DataFrame({
                "feature_spec": self.all_feature_specs,
                "column_name": col_names,
                "mean_abs_shap": mean_abs_shap,
            })
            .sort("mean_abs_shap", descending=True)
            .with_row_index("rank", offset=1)
        )
        return ranking
