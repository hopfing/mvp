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
from mvp.model.completeness import is_incomplete_match
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
NAN_TOLERANT_MODEL_TYPES = frozenset({"xgboost", "lightgbm"})

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
        OPTIMIZABLE_METRICS,
        compute_asymmetric_logloss,
        compute_beta_tail_score,
        compute_calibration_error,
        compute_calibration_error_max,
        compute_error_rate_80plus,
        compute_partial_auc_tail,
        compute_restricted_logloss,
        compute_threshold_weighted_brier,
        compute_weighted_concordance,
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
        "calibration_error_max": lambda yt, yp: compute_calibration_error_max(yt, yp),
        "error_rate_80plus": lambda yt, yp: compute_error_rate_80plus(yt, yp),
        "asymmetric_logloss": lambda yt, yp: compute_asymmetric_logloss(yt, yp, **asym_kwargs),
        # Tail-sensitive objectives. beta_tail_score_sharp reuses the a=b=0.25
        # variant; pass it through compute_beta_tail_score with the sharper shape.
        "beta_tail_score": lambda yt, yp: compute_beta_tail_score(yt, yp),
        "beta_tail_score_sharp": lambda yt, yp: compute_beta_tail_score(yt, yp, a=0.25, b=0.25),
        "threshold_weighted_brier": lambda yt, yp: compute_threshold_weighted_brier(yt, yp),
        "restricted_logloss": lambda yt, yp: compute_restricted_logloss(yt, yp),
        "weighted_concordance": lambda yt, yp: compute_weighted_concordance(yt, yp),
        "partial_auc_tail": lambda yt, yp: compute_partial_auc_tail(yt, yp),
    }
    # Drift guard: the config-load objective validator trusts OPTIMIZABLE_METRICS
    # to mirror these keys exactly. Keep them in lockstep.
    assert set(metric_fns) == OPTIMIZABLE_METRICS, (
        "OPTIMIZABLE_METRICS out of sync with _make_metric_fn: "
        f"{set(metric_fns) ^ OPTIMIZABLE_METRICS}"
    )
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
        # Row-aligned boolean mask over X_wide selecting the eval_filters slice.
        # None when no eval_filters is set (scoring uses the whole test fold).
        self.eval_mask: np.ndarray | None = None
        self.col_to_idx: dict[str, int] = {}
        self.folds: list[tuple[np.ndarray, np.ndarray]] = []
        self.circuit: np.ndarray | None = None
        self.fold_medians: list[np.ndarray] = []
        # Frozen geometry for stability selection. row_dates is the per-row
        # effective_match_date (aligned to X_wide); tournament_key is the
        # per-row resample unit (tournament_id + year). fold_windows holds the
        # date-window tuples derived once from the full unmasked frame so that
        # resampled subsets are assigned to identical folds. All populated in
        # precompute(); empty/None under non-date splitters.
        self.row_dates: np.ndarray | None = None
        self.tournament_key: np.ndarray | None = None
        self.fold_windows: list[tuple] = []
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
        # Recomputable diffs (e.g. a screened `include` that kept a diff but
        # dropped its player/opp parts) need those base columns present for
        # imputation. build_imputation derives exactly which base specs are
        # required; fold them into the load so the augmented-matrix select
        # below can't reference a column the loader was never asked to fetch.
        self._build_result = build_imputation(self.all_feature_specs, get_registry())
        extra_specs = [
            s
            for s in compute_only + self._build_result.aux_base_specs
            if s not in self.all_feature_specs
        ]
        all_specs = self.all_feature_specs + list(dict.fromkeys(extra_specs))

        # MTL detection: same flag the runner uses. Drives the completeness
        # gate (RET/DEF/UNP exclusion), aux target derivation, and the
        # per-set game columns that the score helpers need.
        is_mtl = getattr(self.config, "mtl", None) is not None
        mtl_aux_targets = (
            list(self.config.mtl.auxiliary_targets) if is_mtl else []
        )

        # Extra columns needed for filtering, target resolution, etc.
        # tournament_id / year are the resample unit for stability selection;
        # loaded when available (availability-filtered below) and otherwise
        # silently absent — stability's tournament-level resampling guards on
        # their presence.
        extra_columns = [
            "won", "reason", "result_type", "sets_played", "best_of",
            "circuit", "surface", "round", "tournament_id", "year",
        ]
        # MTL aux derivation reads raw matches.parquet columns directly.
        # Without these in the projection list, the with_columns call would
        # fail with ColumnNotFoundError.
        if is_mtl:
            for i in range(1, 6):
                for prefix in ("player", "opp"):
                    col = f"{prefix}_set{i}_games"
                    if col not in extra_columns:
                        extra_columns.append(col)
            aux_required: dict[str, list[str]] = {
                "total_pts_won_diff": ["pts_total_pts_won", "opp_pts_total_pts_won"],
                "service_pts_won_pct_diff": [
                    "pts_service_pts_won", "opp_pts_service_pts_won",
                    "pts_service_pts_played", "opp_pts_service_pts_played",
                ],
                "return_pts_won_pct_diff": [
                    "pts_return_pts_won", "opp_pts_return_pts_won",
                    "pts_return_pts_played", "opp_pts_return_pts_played",
                ],
                "first_serve_won_pct_diff": [
                    "svc_first_serve_pts_won", "opp_svc_first_serve_pts_won",
                    "svc_first_serve_pts_played", "opp_svc_first_serve_pts_played",
                ],
                "bp_save_pct_diff": [
                    "svc_bp_saved", "opp_svc_bp_saved",
                    "svc_bp_faced", "opp_svc_bp_faced",
                ],
                "svc_serve_rating_diff": ["svc_serve_rating", "opp_svc_serve_rating"],
                "ret_return_rating_diff": ["ret_return_rating", "opp_ret_return_rating"],
                "duration_seconds": ["duration_seconds"],
            }
            for aux_name in (mtl_aux_targets or []):
                for col in aux_required.get(aux_name, []):
                    if col not in extra_columns:
                        extra_columns.append(col)
        for filt in (self.config.data.filters, self.config.data.eval_filters):
            if filt:
                for col in filt:
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
        df = df.filter(~is_incomplete_match(df.columns, is_mtl))
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
                "total_pts_won_diff": (
                    "_aux_total_pts_won_diff",
                    (pl.col("pts_total_pts_won") - pl.col("opp_pts_total_pts_won")).cast(pl.Float64),
                ),
                "service_pts_won_pct_diff": (
                    "_aux_service_pts_won_pct_diff",
                    (
                        pl.when(pl.col("pts_service_pts_played") > 0)
                        .then(pl.col("pts_service_pts_won") / pl.col("pts_service_pts_played"))
                        .otherwise(None)
                        - pl.when(pl.col("opp_pts_service_pts_played") > 0)
                        .then(pl.col("opp_pts_service_pts_won") / pl.col("opp_pts_service_pts_played"))
                        .otherwise(None)
                    ).cast(pl.Float64),
                ),
                "return_pts_won_pct_diff": (
                    "_aux_return_pts_won_pct_diff",
                    (
                        pl.when(pl.col("pts_return_pts_played") > 0)
                        .then(pl.col("pts_return_pts_won") / pl.col("pts_return_pts_played"))
                        .otherwise(None)
                        - pl.when(pl.col("opp_pts_return_pts_played") > 0)
                        .then(pl.col("opp_pts_return_pts_won") / pl.col("opp_pts_return_pts_played"))
                        .otherwise(None)
                    ).cast(pl.Float64),
                ),
                "first_serve_won_pct_diff": (
                    "_aux_first_serve_won_pct_diff",
                    (
                        pl.when(pl.col("svc_first_serve_pts_played") > 0)
                        .then(pl.col("svc_first_serve_pts_won") / pl.col("svc_first_serve_pts_played"))
                        .otherwise(None)
                        - pl.when(pl.col("opp_svc_first_serve_pts_played") > 0)
                        .then(pl.col("opp_svc_first_serve_pts_won") / pl.col("opp_svc_first_serve_pts_played"))
                        .otherwise(None)
                    ).cast(pl.Float64),
                ),
                "bp_save_pct_diff": (
                    "_aux_bp_save_pct_diff",
                    (
                        pl.when(pl.col("svc_bp_faced") > 0)
                        .then(pl.col("svc_bp_saved") / pl.col("svc_bp_faced"))
                        .otherwise(None)
                        - pl.when(pl.col("opp_svc_bp_faced") > 0)
                        .then(pl.col("opp_svc_bp_saved") / pl.col("opp_svc_bp_faced"))
                        .otherwise(None)
                    ).cast(pl.Float64),
                ),
                "svc_serve_rating_diff": (
                    "_aux_svc_serve_rating_diff",
                    (pl.col("svc_serve_rating") - pl.col("opp_svc_serve_rating")).cast(pl.Float64),
                ),
                "ret_return_rating_diff": (
                    "_aux_ret_return_rating_diff",
                    (pl.col("ret_return_rating") - pl.col("opp_ret_return_rating")).cast(pl.Float64),
                ),
                "set1_games_diff": (
                    "_aux_set1_games_diff",
                    pl.when(
                        pl.col("player_set1_games").is_not_null()
                        & pl.col("opp_set1_games").is_not_null()
                    )
                    .then((pl.col("player_set1_games") - pl.col("opp_set1_games")).cast(pl.Float64))
                    .otherwise(None),
                ),
                "set2_games_diff": (
                    "_aux_set2_games_diff",
                    pl.when(
                        pl.col("player_set2_games").is_not_null()
                        & pl.col("opp_set2_games").is_not_null()
                    )
                    .then((pl.col("player_set2_games") - pl.col("opp_set2_games")).cast(pl.Float64))
                    .otherwise(None),
                ),
                "duration_seconds": (
                    "_aux_duration_seconds",
                    pl.col("duration_seconds").cast(pl.Float64),
                ),
                "wl_continuous_proxy": (
                    "_aux_wl_continuous_proxy",
                    (pl.col(target_col).cast(pl.Float64) * 2.0 - 1.0),
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
        # _build_result was computed up front (so its aux base specs could be
        # folded into the load above); reuse it for the augmented columns.
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
        # float32 (not float64): XGBoost's DMatrix is float32 internally, so it
        # already downcasts — the model sees identical values, and the feature
        # matrix halves in RAM. NaN (null) is preserved, so passthrough impute
        # still works. Median/scaling differ only at ~1e-7, below any split.
        self.X_wide = (
            df.select(pl.col(c).cast(pl.Float32) for c in augmented_col_names)
            .to_numpy()
        )
        self.y = df[target_col].to_numpy().astype(int)
        self.circuit = df["circuit"].to_numpy()
        # Frozen-geometry inputs for stability selection. Dates drive fold
        # assignment of resampled rows; tournament_key is the resample unit.
        self.row_dates = df["effective_match_date"].cast(pl.Date).to_numpy()
        if "tournament_id" in df.columns and "year" in df.columns:
            self.tournament_key = (
                df.select(
                    pl.concat_str(
                        [pl.col("tournament_id").cast(pl.Utf8),
                         pl.col("year").cast(pl.Utf8)],
                        separator="_",
                    )
                ).to_series().to_numpy()
            )
        else:
            self.tournament_key = None
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
            self.row_dates = self.row_dates[row_mask]
            if self.tournament_key is not None:
                self.tournament_key = self.tournament_key[row_mask]
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

        # eval_filters: build a row-aligned mask selecting the scoring slice.
        # `df` is now aligned to X_wide / the fold index space, so the mask can
        # be indexed by fold test_idx in the scorer. The fit is unaffected — only
        # the test fold is restricted before the metric is computed.
        if self.config.data.eval_filters:
            idx_col = "_eval_row_idx"
            surviving = (
                apply_filters(
                    df.with_row_index(idx_col), self.config.data.eval_filters
                )[idx_col].to_numpy()
            )
            eval_mask = np.zeros(len(df), dtype=bool)
            eval_mask[surviving] = True
            if not eval_mask.any():
                raise ValueError(
                    "data.eval_filters matched 0 rows — FS scoring would have "
                    "no evaluation set. Check the eval_filters spec and that its "
                    "columns are available (raw columns are auto-loaded; computed "
                    "features must be in discovery.features.compute_only)."
                )
            self.eval_mask = eval_mask
            logger.info(
                "eval_filters active: scoring on %d/%d rows (%.1f%%)",
                int(eval_mask.sum()), len(eval_mask),
                100.0 * eval_mask.sum() / len(eval_mask),
            )

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
        # Freeze fold date-windows from this (full, unmasked) frame so stability
        # selection can assign resampled subsets to identical folds. Only date
        # splitters expose date_windows(); other splitters leave this empty and
        # stability selection is unavailable (guarded at the orchestration layer).
        if hasattr(splitter, "date_windows"):
            self.fold_windows = splitter.date_windows(df)

        logger.info("Precomputing per-fold medians for %d folds", len(self.folds))
        t0 = time.perf_counter()
        self.fold_medians = []
        for train_idx, _test_idx in self.folds:
            medians = np.nanmedian(self.X_wide[train_idx], axis=0)
            medians = np.where(np.isnan(medians), 0.0, medians)
            self.fold_medians.append(medians)
        logger.info("Per-fold medians computed in %.1fs", time.perf_counter() - t0)

    def create_scorer(
        self,
        metric: str,
        folds: list[tuple[np.ndarray, np.ndarray]] | None = None,
        fold_medians: list[np.ndarray] | None = None,
        n_jobs: int | None = None,
    ) -> Callable[[list[str]], float]:
        """Return a fast scorer function that evaluates feature subsets.

        Args:
            metric: Metric name to extract from compute_metrics (e.g. "log_loss").
            folds: Optional per-resample folds (global row-index pairs into
                X_wide). Defaults to the full-frame folds. Stability selection
                passes a thinned set here; medians stay frozen via fold_medians.
            fold_medians: Optional per-fold medians aligned to *folds*. Defaults
                to the full-frame frozen medians. When passing resampled folds
                that skip some windows, pass the matching median subset so
                fold_idx alignment holds.

        Returns:
            Callable that takes a list of feature specs and returns the metric value.
        """
        X_wide = self.X_wide
        y = self.y
        sample_weights = self.sample_weights
        eval_mask = self.eval_mask
        col_to_idx = self.col_to_idx
        folds = self.folds if folds is None else folds
        fold_medians = self.fold_medians if fold_medians is None else fold_medians
        fill_strategies = self.fill_strategies
        fill_constants = self.fill_constants
        model_type = self.config.model.type
        model_params = self.config.model.params or {}
        # Per-fit thread cap for candidate-loop parallelism: a fresh dict so the
        # shared config isn't mutated, spread-last in the model wrapper so it
        # wins over a config-pinned n_jobs. Output is thread-deterministic on
        # this XGBoost build (verified), so this changes speed, never selection.
        if n_jobs is not None:
            model_params = {**model_params, "n_jobs": int(n_jobs)}
        scale = model_type in ("logistic", "neural_net")
        nan_tolerant = model_type in NAN_TOLERANT_MODEL_TYPES

        # MTL state captured for the scorer closure. When MTL is active, the
        # scorer instantiates XGBoostMTLModel (vector-leaf + custom objective)
        # instead of routing through get_model. `target_names_mtl` is the
        # full target list in column order (primary + aux), passed to the
        # model so its weight_{target_name} extraction lines up with the
        # config's `model.params.weight_*` keys.
        is_mtl = self.config.mtl is not None
        mtl_select_on = self.config.mtl.select_on if is_mtl else "combined"
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
            # n_jobs was injected into model_params as the per-fit thread share
            # for XGB; on LogisticRegression it is a deprecated no-op (sklearn
            # 1.8+, removed in 1.10). Discovery applies the share as a BLAS cap
            # instead (see _BLAS_THREADED_MODEL_TYPES), so strip it here.
            lr_params.pop("n_jobs", None)

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
                # eval_filters: restrict the test fold to the scoring slice. The
                # model still fits on the full train fold; only the metric is
                # computed on the slice. A fold with no matching rows is skipped.
                if eval_mask is not None:
                    test_idx = test_idx[eval_mask[test_idx]]
                    if test_idx.size == 0:
                        continue
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

                # MTL combined: score the full multi-task loss (primary log_loss
                # + weighted standardized aux MSE). MTL primary and single-task
                # both score the primary head via metric_fn (predict_proba
                # returns the primary head for the MTL model).
                if is_mtl and mtl_select_on == "combined":
                    assert y_aux is not None
                    fold_metrics.append(
                        _compute_mtl_loss(model, X_test, y_test, y_aux[test_idx])
                    )
                else:
                    fold_metrics.append(metric_fn(y_test, y_prob))

            if not fold_metrics:
                # Every fold's test slice was empty under eval_filters.
                return float("inf")
            return float(np.mean(fold_metrics))

        return scorer

    def resample_folds(
        self, row_mask: np.ndarray, min_fold_rows: int,
    ) -> tuple[list[tuple[np.ndarray, np.ndarray]], list[np.ndarray], int]:
        """Assign a resample's rows to the frozen fold windows.

        For each frozen fold window, selects the masked rows whose date falls in
        the window's train / test range. Fold geometry and medians come from the
        full unmasked frame, so the only thing that varies across resamples is
        which rows populate each fixed fold — selection frequency therefore
        reflects feature reproducibility, not a shifting evaluation period.

        Folds where either side falls below ``min_fold_rows`` are skipped (a
        degenerate fold would inject noise into the selection-frequency count).

        Args:
            row_mask: Boolean mask over X_wide rows (the resample subset).
            min_fold_rows: Minimum train AND test rows for a fold to be kept.

        Returns:
            (folds, fold_medians, n_skipped) where folds/fold_medians are aligned
            and restricted to surviving windows, ready to pass to create_scorer.
        """
        if not self.fold_windows:
            raise ValueError(
                "Frozen fold windows unavailable — stability selection requires "
                "a date splitter (date_sliding / date_expanding)."
            )
        if len(self.fold_windows) != len(self.fold_medians):
            raise RuntimeError(
                "fold_windows / fold_medians length mismatch "
                f"({len(self.fold_windows)} vs {len(self.fold_medians)}); "
                "frozen geometry is inconsistent."
            )

        dates = self.row_dates
        folds: list[tuple[np.ndarray, np.ndarray]] = []
        medians: list[np.ndarray] = []
        skipped = 0
        for fold_idx, (tr_s, tr_e, te_s, te_e) in enumerate(self.fold_windows):
            tr_s64, tr_e64 = np.datetime64(tr_s), np.datetime64(tr_e)
            te_s64, te_e64 = np.datetime64(te_s), np.datetime64(te_e)
            train_idx = np.nonzero(
                row_mask & (dates >= tr_s64) & (dates < tr_e64)
            )[0]
            test_idx = np.nonzero(
                row_mask & (dates >= te_s64) & (dates < te_e64)
            )[0]
            if len(train_idx) < min_fold_rows or len(test_idx) < min_fold_rows:
                skipped += 1
                continue
            folds.append((train_idx, test_idx))
            medians.append(self.fold_medians[fold_idx])
        return folds, medians, skipped
