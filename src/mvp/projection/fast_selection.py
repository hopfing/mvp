"""Fast forward selection using precomputed feature matrix for projection."""

import logging
import time
import warnings
from collections.abc import Callable
from pathlib import Path

import numpy as np
import polars as pl

from mvp.model.config import apply_filters
from mvp.model.engine import check_memory, get_feature_columns, make_fs_engine
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.model.imputation import build_imputation
from mvp.model.registry import get_registry
from mvp.model.splitters import make_splitter
from mvp.projection.config import ProjectionDiscoveryConfig
from mvp.projection.metrics import crps_from_quantiles
from mvp.projection.models import get_regression_model

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message="All-NaN slice encountered")
warnings.filterwarnings("ignore", message="invalid value encountered", category=RuntimeWarning)


def _make_regression_metric_fn(metric: str) -> Callable[[np.ndarray, np.ndarray], float]:
    """Return a function that computes a single regression metric."""

    def mae(yt: np.ndarray, yp: np.ndarray) -> float:
        return float(np.mean(np.abs(yt - yp)))

    def rmse(yt: np.ndarray, yp: np.ndarray) -> float:
        return float(np.sqrt(np.mean((yt - yp) ** 2)))

    def r_squared(yt: np.ndarray, yp: np.ndarray) -> float:
        ss_res = float(np.sum((yt - yp) ** 2))
        ss_tot = float(np.sum((yt - np.mean(yt)) ** 2))
        return 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    def median_ae(yt: np.ndarray, yp: np.ndarray) -> float:
        return float(np.median(np.abs(yt - yp)))

    metric_fns: dict[str, Callable[[np.ndarray, np.ndarray], float]] = {
        "mae": mae,
        "rmse": rmse,
        "r_squared": r_squared,
        "median_ae": median_ae,
    }
    if metric not in metric_fns:
        from mvp.projection.metrics import compute_regression_metrics
        return lambda yt, yp: compute_regression_metrics(yt, yp)[metric]
    return metric_fns[metric]


class FastProjectionSelector:
    """Precomputes all candidate features for fast projection scoring.

    Same pattern as FastForwardSelector but for regression targets
    (total games won) and regression models.
    """

    def __init__(
        self,
        config: ProjectionDiscoveryConfig,
        all_feature_specs: list[str],
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.config = config
        self.all_feature_specs = all_feature_specs
        from mvp.common.base_job import get_data_root, get_local_data_root

        self.matches_path = Path(matches_path) if matches_path else (
            get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        )
        self.cache_dir = Path(cache_dir) if cache_dir else (
            get_local_data_root() / "features" / "cache"
        )

        self.X_wide: np.ndarray | None = None
        self.y: np.ndarray | None = None
        self.col_to_idx: dict[str, int] = {}
        self.folds: list[tuple[np.ndarray, np.ndarray]] = []
        self.circuit: np.ndarray | None = None
        self.fold_medians: list[np.ndarray] = []

    def precompute(self) -> None:
        """Run the expensive one-time computation."""
        engine = make_fs_engine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )
        compute_only = self.config.discovery.features.compute_only
        all_specs = self.all_feature_specs + [
            s for s in compute_only if s not in self.all_feature_specs
        ]

        extra_columns = [
            "won", "reason", "sets_played", "best_of",
            "circuit", "surface", "round", "match_uid", "player_id",
            "player_set1_games", "player_set2_games",
            "player_set3_games", "player_set4_games", "player_set5_games",
            "opp_set1_games", "opp_set2_games",
            "opp_set3_games", "opp_set4_games", "opp_set5_games",
        ]
        if self.config.data.filters:
            for col in self.config.data.filters:
                if col not in extra_columns:
                    extra_columns.append(col)

        # Phase A: ensure all features are cached
        cache_key = engine.ensure_cached(
            all_specs, extra_columns=extra_columns,
        )

        # Phase B: load lightweight base DataFrame for filtering
        structural_cols = list(dict.fromkeys(
            ["match_uid", "player_id", "opp_id", "effective_match_date"]
            + extra_columns
        ))
        available = set(
            pl.scan_parquet(self.matches_path).collect_schema().names()
        )
        structural_cols = [c for c in structural_cols if c in available]
        df = pl.read_parquet(self.matches_path, columns=structural_cols)

        dr = self.config.data.date_range
        df = df.filter(
            (pl.col("effective_match_date") >= dr.start)
            & (pl.col("effective_match_date") <= dr.end)
        )

        # Filter incomplete matches (same as ProjectionRunner._resolve_target)
        df = df.filter(
            pl.col("player_set1_games").is_not_null()
            & pl.col("player_set2_games").is_not_null()
        )
        if "reason" in df.columns:
            df = df.filter(
                pl.col("reason").fill_null("").is_in(["W/O", "RET", "DEF", "UNP"]).not_()
            )

        # Compute target
        target_mode = self.config.model.target
        if target_mode == "match_games":
            target_col = "_target_match_games"
            df = df.with_columns(
                (total_games_won() + total_games_lost())
                .cast(pl.Float64)
                .alias(target_col)
            )
            df = df.sort(["match_uid", "player_id"]).unique(
                subset=["match_uid"], keep="first", maintain_order=True,
            )
        else:
            target_col = "_target_total_games"
            df = df.with_columns(
                total_games_won().cast(pl.Float64).alias(target_col)
            )
        df = df.filter(pl.col(target_col).is_not_null())

        logger.info("Filtered to %d rows, loading features from cache", len(df))
        check_memory("precompute: after filtering")

        # Phase C: load features from cache
        df = engine.load_features_numpy(all_specs, df, cache_key)

        # Apply filters AFTER features are loaded
        if self.config.data.filters:
            df = apply_filters(df, self.config.data.filters)

        all_col_names = get_feature_columns(self.all_feature_specs)
        self._build_result = build_imputation(self.all_feature_specs, get_registry())
        augmented_col_names = all_col_names + self._build_result.aux_base_col_names
        self.col_to_idx = {c: i for i, c in enumerate(augmented_col_names)}

        logger.info(
            "Extracting %d features (%d aux) x %d rows to numpy",
            len(all_col_names), len(self._build_result.aux_base_col_names), len(df),
        )
        t0 = time.perf_counter()
        self.X_wide = (
            df.select(pl.col(c).cast(pl.Float64) for c in augmented_col_names)
            .to_numpy()
        )
        self.y = df[target_col].to_numpy().astype(float)
        self.circuit = df["circuit"].to_numpy()
        logger.info("Numpy extraction complete in %.1fs", time.perf_counter() - t0)

        val = self.config.validation
        splitter = make_splitter(
            val_type=val.type,
            n_splits=val.n_splits,
            min_train_size=val.min_train_size,
            test_size=val.test_size,
            initial_train_size=val.initial_train_size,
            step_size=val.step_size,
            train_size=val.train_size,
            test_start=getattr(val, "test_start", None),
        )
        self.folds = [
            (np.array(train_idx), np.array(test_idx))
            for train_idx, test_idx in splitter.split(df)
        ]

        logger.info("Precomputing per-fold medians for %d folds", len(self.folds))
        t0 = time.perf_counter()
        self.fold_medians = []
        for train_idx, _test_idx in self.folds:
            medians = np.nanmedian(self.X_wide[train_idx], axis=0)
            medians = np.where(np.isnan(medians), 0.0, medians)
            self.fold_medians.append(medians)
        logger.info("Per-fold medians computed in %.1fs", time.perf_counter() - t0)

    def create_scorer(self, metric: str) -> Callable[[list[str]], float]:
        """Return a fast scorer function that evaluates feature subsets."""
        X_wide = self.X_wide
        y = self.y
        col_to_idx = self.col_to_idx
        folds = self.folds
        fold_medians = self.fold_medians
        model_type = self.config.model.type
        model_params = self.config.model.params or {}

        is_crps = metric == "crps"
        quantile_alphas = model_params.get("quantile_alpha", [])
        if is_crps and not isinstance(quantile_alphas, list):
            quantile_alphas = []

        metric_fn = None if is_crps else _make_regression_metric_fn(metric)

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

                mean = X_train.mean(axis=0)
                std = X_train.std(axis=0)
                std[std == 0] = 1.0
                X_train = (X_train - mean) / std
                X_test = (X_test - mean) / std

                model = get_regression_model(model_type, model_params)
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)

                if is_crps and y_pred.ndim == 2 and quantile_alphas:
                    fold_metrics.append(
                        crps_from_quantiles(y_test, y_pred, quantile_alphas)
                    )
                else:
                    if y_pred.ndim == 2:
                        y_pred = y_pred[:, y_pred.shape[1] // 2]
                    fold_metrics.append(metric_fn(y_test, y_pred))

            return float(np.mean(fold_metrics))

        return scorer
