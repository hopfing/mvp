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
        self.col_to_idx: dict[str, int] = {}
        self.folds: list[tuple[list[int], list[int]]] = []
        self.fold_medians: list[np.ndarray] = []

    def precompute(self) -> None:
        """Run the expensive one-time computation.

        Loads data, computes all features, applies filters/date range,
        generates fold indices, and precomputes per-fold medians.
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

        all_col_names = get_feature_columns(self.all_feature_specs)
        self.col_to_idx = {c: i for i, c in enumerate(all_col_names)}

        self.X_wide = (
            df.select(pl.col(c).cast(pl.Float64) for c in all_col_names)
            .to_numpy()
        )
        self.y = df["won"].to_numpy().astype(int)

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
        col_to_idx = self.col_to_idx
        folds = self.folds
        fold_medians = self.fold_medians
        model_type = self.config.model.type
        model_params = self.config.model.params or {}

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

                model = get_model(model_type, model_params)
                model.fit(X_train, y_train)
                y_prob = model.predict_proba(X_test)
                fold_metrics.append(compute_metrics(y_test, y_prob)[metric])

            return float(np.mean(fold_metrics))

        return scorer
