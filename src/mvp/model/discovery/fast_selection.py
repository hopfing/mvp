"""Fast forward selection using precomputed feature matrix."""

import logging
import time
import warnings
from collections.abc import Callable
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.linear_model import LogisticRegression

from mvp.model.config import apply_filters
from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.engine import check_memory, get_feature_columns, make_fs_engine
from mvp.model.imputation import build_imputation
from mvp.model.metrics import compute_metrics
from mvp.model.models import get_model
from mvp.model.registry import get_registry
from mvp.model.splitters import make_splitter

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message="All-NaN slice encountered")


def _make_metric_fn(metric: str) -> Callable[[np.ndarray, np.ndarray], float]:
    """Return a function that computes a single metric.

    Avoids the overhead of compute_metrics() which calculates all 6 metrics
    when only one is needed per iteration.
    """
    from sklearn.metrics import (
        accuracy_score,
        brier_score_loss,
        log_loss,
        roc_auc_score,
    )

    from mvp.model.metrics import compute_calibration_error, compute_error_rate_80plus

    metric_fns: dict[str, Callable[[np.ndarray, np.ndarray], float]] = {
        "log_loss": lambda yt, yp: float(
            log_loss(yt, np.clip(yp, 1e-15, 1 - 1e-15))
        ),
        "accuracy": lambda yt, yp: float(
            accuracy_score(yt, (yp >= 0.5).astype(int))
        ),
        "brier_score": lambda yt, yp: float(brier_score_loss(yt, yp)),
        "roc_auc": lambda yt, yp: float(roc_auc_score(yt, yp)),
        "calibration_error": lambda yt, yp: compute_calibration_error(yt, yp),
        "error_rate_80plus": lambda yt, yp: compute_error_rate_80plus(yt, yp),
    }
    if metric not in metric_fns:
        # Fall back to full compute_metrics for unknown metrics
        return lambda yt, yp: compute_metrics(yt, yp)[metric]
    return metric_fns[metric]


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
        from mvp.common.base_job import get_data_root, get_local_data_root

        self.matches_path = Path(matches_path) if matches_path else (
            get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        )
        self.cache_dir = Path(cache_dir) if cache_dir else (
            get_local_data_root() / "features" / "cache"
        )

        self.X_wide: np.ndarray | None = None
        self.y: np.ndarray | None = None
        self.sample_weights: np.ndarray | None = None
        self.col_to_idx: dict[str, int] = {}
        self.folds: list[tuple[np.ndarray, np.ndarray]] = []
        self.circuit: np.ndarray | None = None
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
        engine = make_fs_engine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )
        compute_only = self.config.discovery.features.compute_only
        all_specs = self.all_feature_specs + [
            s for s in compute_only if s not in self.all_feature_specs
        ]

        # Extra columns needed for filtering, target resolution, etc.
        extra_columns = [
            "won", "reason", "sets_played", "best_of",
            "circuit", "surface", "round",
        ]
        if self.config.data.filters:
            for col in self.config.data.filters:
                if col not in extra_columns:
                    extra_columns.append(col)

        # Phase A: ensure all features are cached (memory-bounded batches)
        cache_key = engine.ensure_cached(
            all_specs, extra_columns=extra_columns,
        )

        # Phase B: load a lightweight base DataFrame for filtering
        structural_cols = [
            "match_uid", "player_id", "opp_id", "effective_match_date",
        ] + extra_columns
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

        # Walkovers are voided bets — never valid training data for any target
        if "reason" in df.columns:
            df = df.filter(pl.col("reason").fill_null("").ne("W/O"))
        # Resolve target column
        target = getattr(self.config, "target", "won")
        if target == "deciding_set":
            target_col = "_target_deciding_set"
            df = df.filter(pl.col("sets_played").is_not_null())
            if "reason" in df.columns:
                reason = pl.col("reason").fill_null("")
                df = df.filter(
                    ~reason.is_in(["DEF", "UNP"])
                    & ~(
                        reason.is_in(["RET"])
                        & (pl.col("sets_played") < pl.col("best_of"))
                    )
                )
            df = df.with_columns(
                (pl.col("sets_played") == pl.col("best_of"))
                .cast(pl.Int64)
                .alias(target_col)
            )
        else:
            target_col = "won"
        df = df.filter(pl.col(target_col).is_not_null())

        if row_keys is not None:
            keyed = row_keys.with_row_index("_order")
            df = (
                df.join(keyed, on=["match_uid", "player_id"], how="inner")
                .sort("_order")
                .drop("_order")
            )

        logger.info("Filtered to %d rows, loading features from cache", len(df))
        check_memory("precompute: after filtering")

        # Phase C: load features from cache onto the filtered DataFrame
        # Use all_specs (includes compute_only) so filter columns are available
        df = engine.load_features_numpy(
            all_specs, df, cache_key,
        )

        # Apply filters AFTER features are loaded (filters may reference computed features)
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
        self.y = df[target_col].to_numpy().astype(int)
        self.circuit = df["circuit"].to_numpy()
        logger.info("Numpy extraction complete in %.1fs", time.perf_counter() - t0)

        if override_y is not None:
            self.y = override_y
        if row_mask is not None:
            self.X_wide = self.X_wide[row_mask]
            self.y = self.y[row_mask]
            self.circuit = self.circuit[row_mask]
            if sample_weights is not None:
                sample_weights = sample_weights[row_mask]
            df = df.filter(pl.Series(row_mask))
        # Compute recency weights from config if no explicit weights were provided
        if sample_weights is None and getattr(self.config, "sample_weight", None) is not None:
            from mvp.model.weighting import compute_sample_weights
            dates = df["effective_match_date"].to_numpy()
            sample_weights = compute_sample_weights(dates, self.config.sample_weight)
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
            test_start=getattr(val, "test_start", None),
            train_months=getattr(val, "train_months", None),
            test_months=getattr(val, "test_months", None),
            start_date=getattr(val, "start_date", None),
            end_date=getattr(val, "end_date", None),
            train_start_date=getattr(val, "train_start_date", None),
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
        scale = model_type in ("logistic", "neural_net")

        # For logistic regression, bypass the LogisticModel wrapper to avoid
        # redundant scaling (the scorer already scales) and per-call import
        # overhead. Use sklearn LogisticRegression directly.
        use_fast_logistic = model_type == "logistic"
        if use_fast_logistic:
            lr_params = {"random_state": 42, "max_iter": 1000, **model_params}

        # Build a single-metric function to avoid computing all 6 metrics
        # when we only need one.
        metric_fn = _make_metric_fn(metric)

        def scorer(features: list[str]) -> float:
            if not features:
                return float("inf")

            try:
                col_names = get_feature_columns(features)
                col_indices = np.array([col_to_idx[c] for c in col_names])
            except KeyError as e:
                logger.warning("Column lookup failed for %s: %s", features, e)
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
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", RuntimeWarning)
                        mean = X_train.mean(axis=0)
                        std = X_train.std(axis=0)
                        std[std == 0] = 1.0
                        X_train = (X_train - mean) / std
                        X_test = (X_test - mean) / std

                if use_fast_logistic:
                    model = LogisticRegression(**lr_params)
                    sw = sample_weights[train_idx] if sample_weights is not None else None
                    model.fit(X_train, y_train, sample_weight=sw)
                    y_prob = model.predict_proba(X_test)[:, 1]
                else:
                    model = get_model(model_type, model_params)
                    fit_kwargs: dict = {}
                    if sample_weights is not None:
                        fit_kwargs["sample_weight"] = sample_weights[train_idx]
                    model.fit(X_train, y_train, **fit_kwargs)
                    y_prob = model.predict_proba(X_test)

                fold_metrics.append(metric_fn(y_test, y_prob))

            return float(np.mean(fold_metrics))

        return scorer
