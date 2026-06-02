"""Fast forward selection using precomputed feature matrix."""

import logging
import re
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
from mvp.model.features._score_helpers import (
    sets_lost as _sets_lost,
    sets_won as _sets_won,
    total_games_lost as _total_games_lost,
    total_games_won as _total_games_won,
)
from mvp.model.imputation import build_imputation
from mvp.model.metrics import compute_metrics
from mvp.model.models import XGBoostMTLModel, _sigmoid, get_model
from mvp.model.registry import FeatureRegistry, get_registry
from mvp.model.splitters import make_splitter

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message="All-NaN slice encountered")

# Model types that natively accept NaN inputs. impute=None features may only be
# scored under these; passing NaN to any other model type is an FS contract
# violation and surfaces as a ValueError at scorer construction time.
NAN_TOLERANT_MODEL_TYPES = frozenset({"xgboost"})

_WINDOWED_SUFFIX_RE = re.compile(r"^(.+)_(\d+)d$")


def _resolve_column_impute(
    col_name: str, registry: FeatureRegistry,
) -> tuple[str, float]:
    """Resolve a column name to (strategy, constant) for FS-time NaN handling.

    Returns one of:
      ("passthrough", 0.0)  — feature registered with impute=None; leave NaN
      ("constant",    v)    — feature registered with impute=<float>; fill with v
      ("median",      0.0)  — feature registered with impute="median" (default);
                              fill with per-fold median (constant unused)

    Aux base columns and unmapped names fall back to "median" — they're never
    selected for scoring directly, but they sit in X_wide so a defensible fill
    strategy is still needed.
    """
    if col_name.startswith("player_"):
        feat_name = col_name[len("player_"):]
    elif col_name.startswith("opp_"):
        feat_name = col_name[len("opp_"):]
    else:
        feat_name = col_name

    match = _WINDOWED_SUFFIX_RE.match(feat_name)
    if match:
        feat_name = match.group(1)

    try:
        feat_def = registry.get(feat_name)
    except KeyError:
        return ("median", 0.0)

    impute = feat_def.impute
    if impute is None:
        return ("passthrough", 0.0)
    if impute == "median":
        return ("median", 0.0)
    return ("constant", float(impute))


def _compute_mtl_loss(
    model: XGBoostMTLModel,
    X_test: np.ndarray,
    y_test_primary: np.ndarray,
    y_aux_test: np.ndarray,
) -> float:
    """Multi-task FS scoring loss: ``log_loss(primary) + sum_i w_i * MSE(aux_i)``.

    Aux MSE computed on the **standardized scale** (mle review 2026-06-01):
    raw booster output is already on the standardized scale the model trained
    against — we do NOT call `predict_aux()` because that inverse-transforms
    back to original scale; we read the booster output directly. `y_aux_test`
    is re-standardized using the model's training-fold `_aux_mean` /
    `_aux_std` so both sides of the MSE are on the same scale. Result: per-
    target MSE is O(1) (unit-variance), matching what the loss weights were
    tuned against. Without standardization, large-range aux (game_margin
    variance ~50) would dominate small-range aux (set_count variance ~0.25).
    """
    import xgboost as xgb

    raw = model._booster.predict(xgb.DMatrix(X_test))  # [n, num_target] standardized
    # Primary head: sigmoid → log_loss
    p_primary = _sigmoid(raw[:, 0])
    eps = 1e-15
    p_clip = np.clip(p_primary, eps, 1.0 - eps)
    primary_ll = -float(
        np.mean(
            y_test_primary * np.log(p_clip)
            + (1.0 - y_test_primary) * np.log(1.0 - p_clip)
        )
    )

    # Aux heads: standardize y_aux using training-fold params, weighted MSE
    # against raw booster aux output (also standardized).
    p_aux_std = raw[:, 1:]
    y_aux_std = (y_aux_test - model._aux_mean) / model._aux_std
    aux_loss = 0.0
    # model.loss_weights[0] is the primary weight (training-only); aux
    # weights at indices 1..N apply to the scoring term per target.
    for i in range(p_aux_std.shape[1]):
        mse_i = float(np.mean((p_aux_std[:, i] - y_aux_std[:, i]) ** 2))
        aux_loss += float(model.loss_weights[i + 1]) * mse_i

    return primary_ll + aux_loss


def _make_metric_fn(
    metric: str,
    lambda_over: float | None = None,
) -> Callable[[np.ndarray, np.ndarray], float]:
    """Return a function that computes a single metric.

    Avoids the overhead of compute_metrics() which calculates all 6 metrics
    when only one is needed per iteration.

    `lambda_over` mirrors `model.params.lambda_over` from the YAML so that
    `asymmetric_logloss` evaluates the same loss surface used at training.
    None falls back to compute_asymmetric_logloss's default.
    """
    from sklearn.metrics import (
        accuracy_score,
        brier_score_loss,
        log_loss,
        roc_auc_score,
    )

    from mvp.model.metrics import (
        compute_asymmetric_logloss,
        compute_calibration_error,
        compute_error_rate_80plus,
    )

    asym_kwargs = {"lambda_over": lambda_over} if lambda_over is not None else {}

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
        "asymmetric_logloss": lambda yt, yp: compute_asymmetric_logloss(yt, yp, **asym_kwargs),
    }
    if metric not in metric_fns:
        # Fall back to full compute_metrics for unknown metrics
        return lambda yt, yp: compute_metrics(yt, yp, lambda_over=lambda_over)[metric]
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
        # Aux target values for MTL feature selection. None under single-task
        # configs. Shape `[n_rows, num_aux]` when MTL is active. Column order
        # mirrors `self.aux_target_names` (friendly names like "game_margin"),
        # not the internal `_aux_*` derived-column names.
        self.y_aux: np.ndarray | None = None
        self.aux_target_names: list[str] | None = None
        self.sample_weights: np.ndarray | None = None
        self.col_to_idx: dict[str, int] = {}
        self.folds: list[tuple[np.ndarray, np.ndarray]] = []
        self.circuit: np.ndarray | None = None
        self.fold_medians: list[np.ndarray] = []
        # Per-column FS-time fill strategy and constant value, indexed parallel
        # to col_to_idx. Built in precompute() from the registry, consumed by
        # the scorer to honor each feature's declared impute contract instead
        # of blanket median-filling. See _resolve_column_impute.
        self.fill_strategies: list[str] = []
        self.fill_constants: np.ndarray | None = None

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

        # MTL detection: same flag the runner uses. Drives the completeness
        # gate (RET/DEF/UNP exclusion), aux target derivation, and the
        # per-set game columns that the score helpers need.
        is_mtl = getattr(self.config, "mtl", None) is not None
        mtl_aux_targets = (
            list(self.config.mtl.auxiliary_targets) if is_mtl else []
        )

        # Extra columns needed for filtering, target resolution, etc.
        extra_columns = [
            "won", "reason", "sets_played", "best_of",
            "circuit", "surface", "round",
        ]
        # MTL aux derivation reads per-set game counts via score helpers
        # (game_margin = sum player_set{i}_games − sum opp_set{i}_games,
        # set_margin via sets_won/sets_lost). Without these in the projection
        # list, the with_columns call would fail with ColumnNotFoundError.
        if is_mtl:
            for i in range(1, 6):
                for prefix in ("player", "opp"):
                    col = f"{prefix}_set{i}_games"
                    if col not in extra_columns:
                        extra_columns.append(col)
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

        # Walkovers are voided bets — never valid training data for any target.
        # When MTL is active, additionally exclude RET / DEF / UNP because aux
        # targets require completed match scores. Same gate as the runner uses.
        if "reason" in df.columns:
            invalid_reasons = {"W/O"}
            if is_mtl:
                invalid_reasons |= {"RET", "DEF", "UNP"}
            df = df.filter(
                ~pl.col("reason").fill_null("").is_in(invalid_reasons)
            )
        # When MTL is active, also require sets_played not null. Necessary
        # for any aux target derivation.
        if is_mtl:
            df = df.filter(pl.col("sets_played").is_not_null())
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

        # MTL: derive auxiliary regression target columns and apply secondary
        # completeness gate. Same expressions the runner's _resolve_target
        # uses, so FS sees the same aux values that the training path does.
        aux_col_names: list[str] = []
        if is_mtl and mtl_aux_targets:
            aux_exprs = {
                "game_margin": (
                    "_aux_game_margin",
                    _total_games_won() - _total_games_lost(),
                ),
                "set_margin": (
                    "_aux_set_margin",
                    _sets_won() - _sets_lost(),
                ),
                "set_count": (
                    "_aux_set_count",
                    pl.col("sets_played").cast(pl.Int64),
                ),
            }
            for aux_name in mtl_aux_targets:
                col_name, expr = aux_exprs[aux_name]
                df = df.with_columns(expr.alias(col_name))
                aux_col_names.append(col_name)
            # Drop rows where any aux value is null (catches edge cases where
            # per-set games are partially missing despite sets_played being set).
            df = df.drop_nulls(subset=aux_col_names)
            self.aux_target_names = list(mtl_aux_targets)

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
        registry = get_registry()
        self._build_result = build_imputation(self.all_feature_specs, registry)
        augmented_col_names = all_col_names + self._build_result.aux_base_col_names
        self.col_to_idx = {c: i for i, c in enumerate(augmented_col_names)}

        # Resolve each column's FS-time fill strategy from its registered
        # impute setting. Done once here so the scorer hot loop only has to
        # index into precomputed arrays.
        self.fill_strategies = []
        fill_constants_list: list[float] = []
        passthrough_cols: list[str] = []
        constant_cols: list[str] = []
        for c in augmented_col_names:
            strat, const = _resolve_column_impute(c, registry)
            self.fill_strategies.append(strat)
            fill_constants_list.append(const)
            if strat == "passthrough":
                passthrough_cols.append(c)
            elif strat == "constant":
                constant_cols.append(c)
        self.fill_constants = np.asarray(fill_constants_list, dtype=np.float64)
        n_median = len(augmented_col_names) - len(passthrough_cols) - len(constant_cols)
        logger.info(
            "FS fill strategies: %d passthrough (NaN-pass), %d constant, "
            "%d median (default)",
            len(passthrough_cols), len(constant_cols), n_median,
        )
        if passthrough_cols:
            # List explicitly so the user can eyeball-verify the right
            # features are being treated as NaN-passthrough — silent
            # mismatches here would mean FS evaluates a different signal
            # than production training. Capped at 50 names to avoid log
            # spam; the count above is authoritative.
            preview = passthrough_cols[:50]
            suffix = f" (+{len(passthrough_cols) - 50} more)" if len(passthrough_cols) > 50 else ""
            logger.info("FS passthrough columns: %s%s", preview, suffix)

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
        # MTL: extract aux y as a 2D `[n_rows, num_aux]` float array. Column
        # order mirrors `mtl_aux_targets` (and therefore `self.aux_target_names`).
        if is_mtl and aux_col_names:
            self.y_aux = (
                df.select(aux_col_names).to_numpy().astype(np.float64)
            )
        logger.info("Numpy extraction complete in %.1fs", time.perf_counter() - t0)

        if override_y is not None:
            self.y = override_y
        if row_mask is not None:
            self.X_wide = self.X_wide[row_mask]
            self.y = self.y[row_mask]
            self.circuit = self.circuit[row_mask]
            if self.y_aux is not None:
                self.y_aux = self.y_aux[row_mask]
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
            initial_train_months=getattr(val, "initial_train_months", None),
            test_months=getattr(val, "test_months", None),
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
        fill_strategies = self.fill_strategies
        fill_constants = self.fill_constants
        model_type = self.config.model.type
        model_params = self.config.model.params or {}
        scale = model_type in ("logistic", "neural_net")
        nan_tolerant = model_type in NAN_TOLERANT_MODEL_TYPES

        # MTL state captured for the scorer closure. When MTL is active, the
        # scorer instantiates XGBoostMTLModel (vector-leaf + custom objective)
        # instead of routing through get_model. `target_names_mtl` is the
        # full target list in column order (primary + aux), passed to the
        # model so its weight_{target_name} extraction lines up with the
        # config's `model.params.weight_*` keys.
        is_mtl = self.config.mtl is not None
        y_aux = self.y_aux if is_mtl else None
        target_names_mtl = (
            [self.config.target, *self.config.mtl.auxiliary_targets]
            if is_mtl else None
        )
        # For non-NaN-tolerant models, impute=None features must be filled
        # before the model sees them. Production training for these models
        # applies a median imputer at the wrapper level (models._apply_median_imputer),
        # so falling back to per-fold median here keeps FS evaluation
        # consistent with production training behavior for that model type.
        passthrough_fallback = None if nan_tolerant else "median"
        if not nan_tolerant:
            logger.info(
                "Non-NaN-tolerant model '%s' selected for FS — impute=None "
                "features will be median-filled at scoring time (matches "
                "production wrapper behavior, not XGB-style NaN passthrough)",
                model_type,
            )

        # For logistic regression, bypass the LogisticModel wrapper to avoid
        # redundant scaling (the scorer already scales) and per-call import
        # overhead. Use sklearn LogisticRegression directly.
        use_fast_logistic = model_type == "logistic"
        if use_fast_logistic:
            lr_params = {"random_state": 42, "max_iter": 1000, **model_params}

        # Build a single-metric function to avoid computing all 6 metrics
        # when we only need one. Pass lambda_over from model params so
        # asymmetric_logloss mirrors the training-side objective.
        metric_fn = _make_metric_fn(metric, lambda_over=model_params.get("lambda_over"))

        def scorer(features: list[str]) -> float:
            if not features:
                return float("inf")

            try:
                col_names = get_feature_columns(features)
                col_indices = np.array([col_to_idx[c] for c in col_names])
            except KeyError as e:
                logger.warning("Column lookup failed for %s: %s", features, e)
                return float("inf")

            # Partition selected columns by FS-time fill strategy so the inner
            # loop honors each feature's declared impute contract. A column
            # registered as impute=None must reach an NaN-tolerant model still
            # carrying NaN; for non-NaN-tolerant models it falls back to
            # median-fill, mirroring the wrapper-level imputation those models
            # do at production training time.
            sel_strategies = [
                fill_strategies[i] if fill_strategies[i] != "passthrough"
                else (passthrough_fallback or "passthrough")
                for i in col_indices
            ]
            passthrough_positions = [
                p for p, s in enumerate(sel_strategies) if s == "passthrough"
            ]
            constant_positions = [
                p for p, s in enumerate(sel_strategies) if s == "constant"
            ]
            median_positions = [
                p for p, s in enumerate(sel_strategies) if s == "median"
            ]
            constant_values = (
                fill_constants[col_indices[constant_positions]]
                if constant_positions else None
            )

            fold_metrics = []
            for fold_idx, (train_idx, test_idx) in enumerate(folds):
                X_train = X_wide[np.ix_(train_idx, col_indices)].copy()
                X_test = X_wide[np.ix_(test_idx, col_indices)].copy()
                y_train, y_test = y[train_idx], y[test_idx]

                if constant_positions:
                    for offset, pos in enumerate(constant_positions):
                        val = constant_values[offset]
                        col_train = X_train[:, pos]
                        col_test = X_test[:, pos]
                        col_train[np.isnan(col_train)] = val
                        col_test[np.isnan(col_test)] = val
                if median_positions:
                    fold_med = fold_medians[fold_idx]
                    for pos in median_positions:
                        val = fold_med[col_indices[pos]]
                        col_train = X_train[:, pos]
                        col_test = X_test[:, pos]
                        col_train[np.isnan(col_train)] = val
                        col_test[np.isnan(col_test)] = val
                # passthrough_positions: intentionally untouched — NaN is the
                # contract for impute=None features; XGB consumes it natively.

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
                elif is_mtl:
                    # MTL: vector-leaf XGBoostMTLModel trained on 2D y
                    # [primary, *aux]. y_aux is already in friendly column
                    # order matching `target_names_mtl[1:]`. Per-target loss
                    # weights come from `model_params` via the model's
                    # weight_{target_name} extraction. predict_proba returns
                    # primary head only (BaseModel contract preserved).
                    assert y_aux is not None and target_names_mtl is not None
                    y_train_2d = np.column_stack([
                        y_train.astype(np.float64),
                        y_aux[train_idx],
                    ])
                    model = XGBoostMTLModel(
                        model_params,
                        target_names=target_names_mtl,
                        feature_names=col_names,
                    )
                    fit_kwargs = {}
                    if sample_weights is not None:
                        fit_kwargs["sample_weight"] = sample_weights[train_idx]
                    model.fit(X_train, y_train_2d, **fit_kwargs)
                    y_prob = model.predict_proba(X_test)
                else:
                    model = get_model(model_type, model_params, feature_names=col_names)
                    fit_kwargs: dict = {}
                    if sample_weights is not None:
                        fit_kwargs["sample_weight"] = sample_weights[train_idx]
                    model.fit(X_train, y_train, **fit_kwargs)
                    y_prob = model.predict_proba(X_test)

                # MTL: score on the full multi-task loss per design (primary
                # log_loss + weighted standardized aux MSE). Single-task uses
                # the existing single-target metric_fn against y_prob.
                if is_mtl:
                    assert y_aux is not None
                    fold_metrics.append(
                        _compute_mtl_loss(model, X_test, y_test, y_aux[test_idx])
                    )
                else:
                    fold_metrics.append(metric_fn(y_test, y_prob))

            return float(np.mean(fold_metrics))

        return scorer
