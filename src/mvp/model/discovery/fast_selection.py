"""Fast forward selection using precomputed feature matrix."""

import warnings
from collections.abc import Callable
from pathlib import Path

import numpy as np
import polars as pl

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.engine import FeatureEngine, get_feature_columns
from mvp.model.metrics import compute_metrics
from mvp.model.models import get_model
from mvp.model.splitters import make_splitter

warnings.filterwarnings("ignore", message="All-NaN slice encountered")


class FastForwardSelector:
    """Precomputes all candidate features into one numpy matrix for fast scoring.

    Instead of creating a new ExperimentRunner per candidate (which reloads data,
    recomputes features, etc.), this class does the expensive work once and then
    each candidate evaluation is just numpy column slicing + model fit/predict.
    """

    def __init__(
        self,
        config: DiscoveryConfig,
        all_feature_specs: list[str],
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.config = config
        self.all_feature_specs = all_feature_specs
        self.matches_path = Path(
            matches_path or "data/aggregate/atptour/matches.parquet"
        )
        self.cache_dir = Path(cache_dir or "data/features/cache")

        self.X_wide: np.ndarray | None = None
        self.y: np.ndarray | None = None
        self.sample_weights: np.ndarray | None = None
        self.col_to_idx: dict[str, int] = {}
        self.folds: list[tuple[list[int], list[int]]] = []
        self.fold_medians: list[np.ndarray] = []

    def precompute(
        self,
        override_y: np.ndarray | None = None,
        row_mask: np.ndarray | None = None,
        sample_weights: np.ndarray | None = None,
        row_keys: pl.DataFrame | None = None,
    ) -> None:
        """Run the expensive one-time computation.

        Loads data, computes all features, applies filters/date range,
        generates fold indices, and precomputes per-fold medians.

        Args:
            override_y: Replace target variable after loading.
            row_mask: Boolean mask to filter rows after loading.
            sample_weights: Per-sample weights for model fitting.
            row_keys: DataFrame with (match_uid, player_id) to filter
                and reorder rows. override_y/row_mask/sample_weights
                are aligned to these rows (applied after filtering).
        """
        engine = FeatureEngine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )
        df = engine.compute(self.all_feature_specs)

        if self.config.data.filters:
            for col, value in self.config.data.filters.items():
                if isinstance(value, list):
                    df = df.filter(pl.col(col).is_in(value))
                else:
                    df = df.filter(pl.col(col) == value)

        dr = self.config.data.date_range
        df = df.filter(
            (pl.col("effective_match_date") >= dr.start)
            & (pl.col("effective_match_date") <= dr.end)
        )

        df = df.filter(pl.col("won").is_not_null())

        if row_keys is not None:
            keyed = row_keys.with_row_index("_order")
            df = (
                df.join(keyed, on=["match_uid", "player_id"], how="inner")
                .sort("_order")
                .drop("_order")
            )

        all_col_names = get_feature_columns(self.all_feature_specs)
        self.col_to_idx = {c: i for i, c in enumerate(all_col_names)}

        self.X_wide = (
            df.select(pl.col(c).cast(pl.Float64) for c in all_col_names)
            .to_numpy()
        )
        self.y = df["won"].to_numpy().astype(int)

        if override_y is not None:
            self.y = override_y
        if row_mask is not None:
            self.X_wide = self.X_wide[row_mask]
            self.y = self.y[row_mask]
            if sample_weights is not None:
                sample_weights = sample_weights[row_mask]
            df = df.filter(pl.Series(row_mask))
        self.sample_weights = sample_weights

        val = self.config.validation
        splitter = make_splitter(
            val_type=val.type,
            n_splits=val.n_splits,
            min_train_size=val.min_train_size,
            test_size=val.test_size,
            initial_train_size=val.initial_train_size,
            step_size=val.step_size,
            train_size=val.train_size,
        )
        self.folds = list(splitter.split(df))

        self.fold_medians = []
        for train_idx, _test_idx in self.folds:
            train_data = self.X_wide[train_idx]
            medians = np.nanmedian(train_data, axis=0)
            medians = np.where(np.isnan(medians), 0.0, medians)
            self.fold_medians.append(medians)

    def create_scorer(self, metric: str) -> Callable[[list[str]], float]:
        """Return a fast scorer function that evaluates feature subsets.

        Args:
            metric: Metric name to extract from compute_metrics (e.g. "log_loss").

        Returns:
            Callable that takes a list of feature specs and returns the metric value.
        """
        X_wide = self.X_wide
        y = self.y
        sample_weights = self.sample_weights
        col_to_idx = self.col_to_idx
        folds = self.folds
        fold_medians = self.fold_medians
        model_type = self.config.model.type
        model_params = self.config.model.params or {}
        scale = model_type in ("logistic",)

        def scorer(features: list[str]) -> float:
            if not features:
                return float("inf")

            try:
                col_names = get_feature_columns(features)
                col_indices = np.array([col_to_idx[c] for c in col_names])
            except KeyError:
                return float("inf")

            fold_metrics = []
            for fold_idx, (train_idx, test_idx) in enumerate(folds):
                X_train = X_wide[np.ix_(train_idx, col_indices)].copy()
                X_test = X_wide[np.ix_(test_idx, col_indices)].copy()
                y_train, y_test = y[train_idx], y[test_idx]

                medians = fold_medians[fold_idx][col_indices]
                X_train = np.where(np.isnan(X_train), medians, X_train)
                X_test = np.where(np.isnan(X_test), medians, X_test)

                if scale:
                    mean = X_train.mean(axis=0)
                    std = X_train.std(axis=0)
                    std[std == 0] = 1.0
                    X_train = (X_train - mean) / std
                    X_test = (X_test - mean) / std

                model = get_model(model_type, model_params)
                fit_kwargs: dict = {}
                if sample_weights is not None:
                    fit_kwargs["sample_weight"] = sample_weights[train_idx]
                if model_type == "xgboost":
                    fit_kwargs["eval_set"] = [(X_test, y_test)]
                model.fit(X_train, y_train, **fit_kwargs)
                y_prob = model.predict_proba(X_test)
                fold_metrics.append(compute_metrics(y_test, y_prob)[metric])

            return float(np.mean(fold_metrics))

        return scorer
