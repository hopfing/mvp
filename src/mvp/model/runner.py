"""Experiment runner for training and evaluating models."""

import logging
import time
import warnings
from pathlib import Path

warnings.filterwarnings("ignore", message="All-NaN slice encountered")
warnings.filterwarnings("ignore", message="sklearn.utils.parallel.delayed")
from typing import Any

import numpy as np
import polars as pl
from sklearn.metrics import r2_score

run_logger = logging.getLogger(__name__)

from mvp.model.completeness import is_incomplete_match
from mvp.model.calibration import (
    AsymmIsotonicCalibrator,
    IsotonicCalibrator,
    PlattCalibrator,
    SegmentedIsotonicCalibrator,
    SegmentedPlattCalibrator,
    fit_calibrator_with_nested_cv,
    make_calibrator,
)
from mvp.model.config import (
    DateRange,
    EnsembleParams,
    ExperimentConfig,
    apply_filters,
    get_filter_feature_specs,
)
from mvp.model.diagnostics import Diagnostics, EnsembleDiagnostics
from mvp.model.discovery.importance import gain_importance
from mvp.model.early_stopping import two_stage_fit
from mvp.model.engine import check_memory, get_feature_columns, make_fs_engine
from mvp.model.features._score_helpers import (
    sets_lost as _sets_lost,
    sets_won as _sets_won,
    total_games_lost as _total_games_lost,
    total_games_won as _total_games_won,
)
from mvp.model.imputation import apply_imputation, build_imputation, fit_imputation
from mvp.model.metrics import compute_metrics
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.models import EnsembleModel, XGBoostMTLModel, get_model
from mvp.model.registry import get_registry
from mvp.model.splitters import BaseSplitter, make_splitter
from mvp.model.weighting import compute_sample_weights


def _reporting_calibrated_holdout(
    oof_y_true: np.ndarray,
    oof_y_prob: np.ndarray,
    holdout_predictions: list[dict],
    lambda_over: float | None,
) -> tuple[dict[str, float] | None, list[dict[str, float]] | None]:
    """Deployment-frame (global-Platt) metrics for the held-out outer block.

    Fit a global Platt on the tuning-fold OOF (which the held-out block was never
    part of — leak-free) and apply it to the RAW held-out preds. Platt is low-DoF
    (2 params), so no nested CV is needed here: nested CV only de-biases the
    in-sample tuning-fold diagnostics, not a block the calibrator never trained
    on. A fixed global Platt (rather than the config's calibration block) keeps
    the number comparable across trials/configs — but it is therefore a
    CONSERVATIVE (lower-bound) estimate of deployment quality for models whose
    real miscalibration is segment-structured and would be corrected by a
    segmented/isotonic deployment calibrator. Read it as a same-yardstick
    comparison number, not the literal deployment number.

    Reporting-only: never mutates its inputs, and must never abort a tuning run.
    Returns (None, None) if the calibrator can't be fit (e.g. single-class OOF).
    """
    reporting_cal = PlattCalibrator()
    try:
        reporting_cal.fit(oof_y_prob, oof_y_true)
    except ValueError:
        # e.g. single-class OOF → LogisticRegression can't fit. This is a
        # reporting extra; skip it rather than crash the whole tuning study.
        return None, None
    holdout_y_true = np.concatenate([p["y_true"] for p in holdout_predictions])
    holdout_y_prob = np.concatenate([p["y_prob"] for p in holdout_predictions])
    overall = compute_metrics(
        holdout_y_true, reporting_cal.transform(holdout_y_prob), lambda_over=lambda_over
    )
    per_fold = [
        compute_metrics(
            p["y_true"], reporting_cal.transform(p["y_prob"]), lambda_over=lambda_over
        )
        for p in holdout_predictions
    ]
    return overall, per_fold


class ExperimentRunner:
    """Runner for executing experiments."""

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
        workflow: str = "training",
        run_name: str | None = None,
        log_to_mlflow: bool = True,
        holdout_folds: int = 0,
        inner_cv_folds: int = 0,
        calibrate: bool = True,
        report_calibrated_holdout: bool = False,
    ) -> None:
        """Initialize runner.

        Args:
            config_path: Path to experiment config YAML.
            matches_path: Path to matches.parquet.
            cache_dir: Optional cache directory for features.
            mlflow_dir: Optional MLflow tracking directory.
            workflow: MLflow experiment name ("training" or "discovery").
            run_name: Override for MLflow run name. Defaults to filename.
            log_to_mlflow: Whether to log to MLflow. Set False for intermediate runs.
            holdout_folds: Number of trailing CV folds to hold out from the
                calibrator fit, diagnostics, and headline `metrics`. The held-out
                folds still get calibrated probabilities (using the
                tuning-fold-only calibrator) and are reported separately as
                `holdout_metrics` / `holdout_fold_metrics`. Tuning sets this to 1.
            inner_cv_folds: When > 0, each non-holdout outer fold replaces its
                single outer-test prediction with k inner expanding-window CV
                splits on the training portion. Optuna then sees the mean of
                inner LL across outer folds (less noisy than a single per-fold
                point estimate). Requires holdout_folds >= 1. Tuning sets this
                to 4; normal model runs default to 0 (unchanged).
            calibrate: When True (default), fit a Platt calibrator on tuning
                OOF and apply it to all fold predictions before computing
                metrics. When False, skip Platt entirely — `avg_metrics` and
                `holdout_metrics` reflect raw predictor quality. Used by
                `HyperparamTuner` so HP search optimizes raw discrimination,
                not a calibrated objective (calibration scaffolding is a
                deployment concern, not an HP search concern). Default True
                preserves existing behavior for `mvp run` and `mvp model`.
            report_calibrated_holdout: Reporting-only. When True (and
                `calibrate=False`, i.e. the raw-search tuning path) also emit
                deployment-frame (global-Platt) metrics for the held-out block as
                `holdout_metrics_calibrated` / `holdout_fold_metrics_calibrated`,
                so probability-scale metrics are comparable across trials/configs
                without changing the raw search objective. See
                `_reporting_calibrated_holdout`. Default False.
        """
        if holdout_folds < 0:
            raise ValueError(f"holdout_folds must be >= 0, got {holdout_folds}")
        if inner_cv_folds < 0:
            raise ValueError(f"inner_cv_folds must be >= 0, got {inner_cv_folds}")
        if inner_cv_folds > 0 and holdout_folds < 1:
            raise ValueError(
                f"inner_cv_folds={inner_cv_folds} requires holdout_folds >= 1 "
                "(inner CV gives a noise-resistant tuning signal, but the "
                "honest selection check still depends on the held-out fold)"
            )
        self.config_path = Path(config_path)
        self.config = ExperimentConfig.from_file(str(config_path))
        from mvp.common.base_job import get_data_root, get_local_data_root

        self.matches_path = Path(matches_path) if matches_path else (
            get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        )
        self.cache_dir = Path(cache_dir) if cache_dir else (
            get_local_data_root() / "features" / "cache"
        )
        self.mlflow_dir = Path(mlflow_dir) if mlflow_dir else None
        self.workflow = workflow
        self.run_name = run_name or self.config_path.stem
        self.log_to_mlflow = log_to_mlflow
        self.holdout_folds = holdout_folds
        self.inner_cv_folds = inner_cv_folds
        self.calibrate = calibrate
        # Reporting-only: when set (raw-search tuning path, calibrate=False), also
        # emit deployment-frame (global-Platt) metrics for the held-out block, so
        # tune-review can show calibrated numbers without touching the raw search
        # objective. See holdout_metrics_calibrated in run()'s result.
        self.report_calibrated_holdout = report_calibrated_holdout

        self.engine = make_fs_engine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )

    def _resolve_target(self, df: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
        """Add the target column(s) to df and return (df, target_cols).

        Primary target is always at index 0 (today: `won` or `deciding_set`).
        When `config.mtl` is set, auxiliary regression targets are derived and
        appended. Backward compat: callers that only need the primary target
        use `target_cols[0]`.

        Walkovers are always excluded — they're voided bets with no on-court
        signal. When MTL is active, additionally excludes RET / DEF / UNP
        because aux targets are undefined for incomplete matches.

        For 'won': uses existing column as-is.
        For 'deciding_set': derives target from sets_played == best_of,
            excludes incomplete matches where outcome is uncertain.
        For MTL: derives game_margin / set_margin / set_count auxiliaries
            (per `config.mtl.auxiliary_targets`) and applies a secondary
            `drop_nulls` gate to catch edge cases where `reason` is null or
            unrecognized but set/game columns are still partially missing.
        """
        target = self.config.target
        mtl_cfg = self.config.mtl
        # The completeness gate is active when MTL is on OR when the user
        # opts in via data.exclude_incomplete. MTL needs it because aux
        # targets are undefined for partial scores; non-MTL configs use the
        # flag to match the MTL path's row set so MTL-vs-baseline comparisons
        # are apples-to-apples.
        # Primary completeness filter: walkovers (voided, no on-court play) are
        # always excluded — not gradeable/bettable matches. RET/DEF/UNP are NOT
        # excluded here: they are real, graded matches that must stay in the test
        # fold and at prediction time. The RET/DEF/UNP exclusion, the sets_played
        # requirement, and the aux drop_nulls exist only because aux LABELS are
        # undefined for partial scores — a TRAINING concern — so they're applied
        # to the training slice in the fold loop, not globally pre-split.
        df = df.filter(~is_incomplete_match(df.columns, False))

        # Resolve primary target column
        if target == "won":
            primary_col = "won"
        elif target == "deciding_set":
            primary_col = "_target_deciding_set"
            df = df.filter(pl.col("sets_played").is_not_null())
            # Retirements where sets_played == best_of are settled (deciding set
            # started, over graded a winner). Retirements before that point are
            # voided — outcome uncertain. DEF/UNP always excluded. Always runs:
            # RET/DEF/UNP are no longer dropped globally, so the deciding-set
            # primary target must handle them here for both train and test.
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
                .alias(primary_col)
            )
        else:
            raise ValueError(f"Unknown target: {target}")

        target_cols = [primary_col]

        # MTL: derive auxiliary target columns + secondary completeness gate
        if mtl_cfg is not None:
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
                    (pl.col(primary_col).cast(pl.Float64) * 2.0 - 1.0),
                ),
                # Placebo controls (see MTLConfig.auxiliary_targets). Seeded for
                # reproducibility. placebo_gaussian is pure N(0,1) noise; the
                # shuffled variant keeps set_margin's marginal but destroys its
                # per-match link to the outcome via a seeded column shuffle.
                "placebo_gaussian": (
                    "_aux_placebo_gaussian",
                    pl.Series(np.random.default_rng(42).standard_normal(df.height)),
                ),
                "placebo_shuffled_set_margin": (
                    "_aux_placebo_shuffled_set_margin",
                    (_sets_won() - _sets_lost()).shuffle(seed=42),
                ),
            }
            aux_cols: list[str] = []
            for aux_name in mtl_cfg.auxiliary_targets:
                col_name, expr = aux_exprs[aux_name]
                df = df.with_columns(expr.alias(col_name))
                aux_cols.append(col_name)
            # Aux columns are left nullable here (retirements/partial scores
            # have undefined aux labels). They are NOT dropped globally: such
            # rows stay in the test fold (primary eval doesn't need aux) and at
            # predict time. The training slice drops them in the fold loop,
            # where the aux labels must be valid for the fit.
            target_cols.extend(aux_cols)

        return df, target_cols

    def _filter_training_completeness(
        self, train_df: pl.DataFrame, target_cols: list[str]
    ) -> pl.DataFrame:
        """Drop incomplete matches from the TRAINING slice only.

        Aux regression labels are undefined for incomplete matches (RET/DEF/UNP)
        and partial scores, so the fit must exclude them. The test fold keeps
        them — they are real graded matches and belong in eval/backtest. Called
        per-fold on the training rows, never globally pre-split. No-op unless MTL
        or data.exclude_incomplete is set.
        """
        if not (self.config.mtl is not None or self.config.data.exclude_incomplete):
            return train_df
        train_df = train_df.filter(
            ~is_incomplete_match(train_df.columns, True)
        ).filter(pl.col("sets_played").is_not_null())
        if self.config.mtl is not None:
            train_df = train_df.drop_nulls(subset=target_cols[1:])
        return train_df

    def _get_splitter(self) -> BaseSplitter:
        """Get the appropriate splitter for validation strategy."""
        val = self.config.validation
        return make_splitter(
            val_type=val.type,
            n_splits=val.n_splits,
            min_train_size=val.min_train_size,
            test_size=val.test_size,
            initial_train_size=val.initial_train_size,
            step_size=val.step_size,
            train_size=val.train_size,
            test_start=val.test_start,
            train_months=val.train_months,
            initial_train_months=val.initial_train_months,
            test_months=val.test_months,
        )

    def _resolve_ensemble(
        self,
    ) -> tuple[list[str], list[dict[str, Any]], list["DateRange"], list[int], list[dict[str, Any] | None], list[Any]]:
        """Resolve ensemble config into union features and base model specs.

        Returns:
            (union_feature_specs, base_model_specs, model_date_ranges,
             meta_feature_indices, model_filters, model_sample_weights)
             where each spec has type, params, weight, feature_indices,
             model_date_ranges[i] is the DateRange from base config i,
             meta_feature_indices maps meta-features into the union column
             list, model_filters[i] is the filter dict from base config i
             (or None if matching ensemble), and model_sample_weights[i]
             is the SampleWeightConfig from base config i (or None).
        """
        from mvp.model.config import DateRange, SampleWeightConfig

        ensemble_params = EnsembleParams.model_validate(self.config.model.params)
        ensemble_strategy = (self.config.model.params or {}).get("strategy", "average")

        all_feature_specs: list[str] = []
        base_model_specs: list[dict[str, Any]] = []
        model_date_ranges: list[DateRange] = []
        model_filters: list[dict[str, Any] | None] = []
        model_sample_weights: list[SampleWeightConfig | None] = []

        for ref in ensemble_params.base_models:
            base_config = ExperimentConfig.from_file(ref.config)
            if base_config.features is None:
                raise ValueError(f"Base model {ref.config} has no features section")

            sub_cal_cfg = base_config.calibration

            # v1: reject stacking + sub-cal. EnsembleModel.predict_proba_per_model
            # routes through _predict_all, which would apply sub cals to stacking
            # meta-features once any sub has cal. That changes meta-model input
            # distribution asymmetrically. Deferred to a follow-up PR.
            if ensemble_strategy == "stacking" and sub_cal_cfg is not None:
                raise ValueError(
                    f"Base model {ref.config} has a calibration block, but "
                    f"ensemble strategy is 'stacking'. Stacking + per-sub "
                    f"calibration is not supported in v1 (would change stacking "
                    f"meta-feature distribution). Use strategy='average' or "
                    f"remove sub-cal blocks."
                )

            for spec in base_config.features.include:
                if spec not in all_feature_specs:
                    all_feature_specs.append(spec)
            base_model_specs.append({
                "type": base_config.model.type,
                "params": base_config.model.params or {},
                "weight": ref.weight,
                "feature_specs": base_config.features.include,
                "calibration": sub_cal_cfg,
            })
            model_date_ranges.append(base_config.data.date_range)
            if base_config.data.filters != self.config.data.filters:
                model_filters.append(base_config.data.filters)
            else:
                model_filters.append(None)
            model_sample_weights.append(base_config.sample_weight)

        for spec in ensemble_params.meta_features:
            if spec not in all_feature_specs:
                all_feature_specs.append(spec)

        union_cols = get_feature_columns(all_feature_specs)
        for spec in base_model_specs:
            base_cols = get_feature_columns(spec["feature_specs"])
            spec["feature_indices"] = [union_cols.index(c) for c in base_cols]
            del spec["feature_specs"]

        meta_feature_cols = get_feature_columns(ensemble_params.meta_features)
        meta_feature_indices = [union_cols.index(c) for c in meta_feature_cols]

        return all_feature_specs, base_model_specs, model_date_ranges, meta_feature_indices, model_filters, model_sample_weights

    def run(self, trial: Any = None) -> dict[str, Any]:
        """Execute the experiment.

        Args:
            trial: Optional Optuna Trial. When provided, the runner reports
                each tuning outer-fold's objective metric (metrics.objective)
                to the trial via
                trial.report(step=outer_idx) and consults trial.should_prune()
                at each outer-fold boundary. If pruning fires, raises
                optuna.TrialPruned. Only the tuning folds (0..n_tuning-1)
                are reported — the holdout fold(s) never feed the pruner.

        Returns:
            Dictionary with metrics and metadata.
        """
        import mlflow

        if self.log_to_mlflow:
            if self.mlflow_dir:
                mlflow_uri = f"file:///{str(self.mlflow_dir).replace(chr(92), '/')}"
                mlflow.set_tracking_uri(mlflow_uri)
            logger = ExperimentLogger(experiment_name=self.workflow)
        else:
            logger = None

        # Resolve ensemble or standard features
        is_ensemble = self.config.model.type == "ensemble"
        is_mtl = self.config.mtl is not None

        # When the model is trained with asymmetric_logloss, mirror its
        # lambda_over into compute_metrics so the tune metric evaluates the
        # same loss surface the model was fit against. Ensemble top-level
        # params don't carry lambda_over directly; fall back to default.
        lambda_over_eval: float | None = None
        if not is_ensemble and self.config.model.params:
            lo = self.config.model.params.get("lambda_over")
            if lo is not None:
                lambda_over_eval = float(lo)
        base_model_specs: list[dict[str, Any]] | None = None
        model_date_ranges: list | None = None
        model_filters: list[dict[str, Any] | None] | None = None
        model_sample_weights: list | None = None
        meta_feature_indices: list[int] = []
        if is_ensemble:
            feature_specs, base_model_specs, model_date_ranges, meta_feature_indices, model_filters, model_sample_weights = (
                self._resolve_ensemble()
            )
        else:
            assert self.config.features is not None
            feature_specs = self.config.features.include

        # Compute features (include compute_only and filter-referenced features)
        compute_only = (
            self.config.features.compute_only
            if self.config.features and self.config.features.compute_only
            else []
        )
        filter_specs = get_filter_feature_specs(self.config.data.filters)
        filter_specs.extend(get_filter_feature_specs(self.config.data.train_filters))
        filter_specs.extend(get_filter_feature_specs(self.config.data.eval_filters))
        if is_ensemble and model_filters:
            for filt in model_filters:
                if filt is not None:
                    filter_specs.extend(get_filter_feature_specs(filt))
        extra = compute_only + filter_specs
        all_specs = feature_specs + [s for s in extra if s not in feature_specs]
        t_run = time.perf_counter()

        # Columns the runner needs beyond what features reference:
        # - target resolution: won, reason, sets_played, best_of
        # - date filtering: effective_match_date (already structural)
        # - diagnostics: circuit, surface, round
        # - per-fold prediction persistence: match_uid, player_id, opp_id
        # - raw filter columns (draw_type, etc.) that aren't computed features
        runner_columns = [
            "won", "reason", "result_type", "sets_played", "best_of",
            "circuit", "surface", "round",
            "match_uid", "player_id", "opp_id",
        ]
        # Sequence model needs raw history columns from matches.parquet
        # so they're available for the per-player history dict.
        if self.config.model.type == "sequence":
            from mvp.model.sequence_model import HISTORY_RAW_COLUMNS
            for col in HISTORY_RAW_COLUMNS:
                if col not in runner_columns:
                    runner_columns.append(col)
        # MTL aux target derivation reads raw matches.parquet columns directly;
        # _resolve_target raises ColumnNotFoundError if they aren't projected.
        if is_mtl:
            for i in range(1, 6):
                for prefix in ("player", "opp"):
                    col = f"{prefix}_set{i}_games"
                    if col not in runner_columns:
                        runner_columns.append(col)
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
            if self.config.mtl is not None:
                for aux_name in self.config.mtl.auxiliary_targets:
                    for col in aux_required.get(aux_name, []):
                        if col not in runner_columns:
                            runner_columns.append(col)
        for _scope_filt in (
            self.config.data.filters,
            self.config.data.train_filters,
            self.config.data.eval_filters,
        ):
            if _scope_filt:
                for col in _scope_filt:
                    if col not in runner_columns:
                        runner_columns.append(col)
        if is_ensemble and model_filters:
            for filt in model_filters:
                if filt:
                    for col in filt:
                        if col not in runner_columns:
                            runner_columns.append(col)

        df = self.engine.compute(all_specs, extra_columns=runner_columns)

        # Apply additional filters (e.g., draw_type: "singles")
        # These are applied AFTER feature computation so workload features
        # include doubles appearances before filtering to singles-only
        if self.config.data.filters:
            df = apply_filters(df, self.config.data.filters)

        # Determine if per-model custom training is needed
        needs_per_model = False
        if is_ensemble and model_date_ranges and model_filters:
            for dr, filt in zip(model_date_ranges, model_filters):
                if dr.start < self.config.data.date_range.start:
                    needs_per_model = True
                if filt is not None:
                    needs_per_model = True
        if is_ensemble and model_sample_weights:
            if any(sw is not None for sw in model_sample_weights):
                needs_per_model = True

        # Resolve target column(s). Primary is always at index 0. When
        # config.mtl is set, aux targets are appended to target_cols and the
        # aux columns are materialized on df. Step A leaves downstream y
        # construction on the primary target only (target_col); Step C will
        # wire the multi-target y path for the MTL model.
        df, target_cols = self._resolve_target(df)
        target_col = target_cols[0]

        # Build wide date range df for per-model training (ensemble only)
        df_wide = None
        if is_ensemble and model_date_ranges:
            earliest = min(dr.start for dr in model_date_ranges)
            if earliest < self.config.data.date_range.start:
                df_wide = df.filter(
                    (pl.col("effective_match_date") >= earliest)
                    & (pl.col("effective_match_date") <= self.config.data.date_range.end)
                    & (pl.col(target_col).is_not_null())
                )

        # Capture the pre-date-filter df for sequence-model history seeding
        # (includes pre-config.date_range.start matches that won't be in
        # train/test but serve as anti-cold-start context).
        df_history_seed = None
        if self.config.model.type == "sequence":
            df_history_seed = df.filter(
                pl.col("effective_match_date") < self.config.data.date_range.start
            )

        # Filter by ensemble's date range (evaluation window)
        df = df.filter(
            (pl.col("effective_match_date") >= self.config.data.date_range.start)
            & (pl.col("effective_match_date") <= self.config.data.date_range.end)
        )

        # Drop rows with no outcome (e.g., future/unfinished matches)
        df = df.filter(pl.col(target_col).is_not_null())

        # Get feature columns from config
        feature_cols = get_feature_columns(feature_specs)

        if not feature_cols:
            raise ValueError("No feature columns found after computing features")

        # Build per-feature imputation specs from registry declarations
        build_result = build_imputation(feature_specs, get_registry())
        augmented_cols = feature_cols + build_result.aux_base_col_names
        n_model = build_result.n_model_features

        # Validate that any passthrough (NaN) features are only routed to
        # NaN-tolerant models. base_model_specs is resolved earlier in run()
        # for ensembles, so we can validate both cases here uniformly.
        from mvp.model.imputation import validate_impute_compat
        validate_impute_compat(
            build_result.specs,
            feature_cols,
            self.config.model.type,
            base_model_specs=base_model_specs,
        )

        # Embedding configuration
        embedding_col = None
        opp_embedding_col = None
        min_player_matches = 10
        if self.config.model.params:
            embedding_col = self.config.model.params.get("embedding_col")
            opp_embedding_col = self.config.model.params.get("opp_embedding_col")
            min_player_matches = self.config.model.params.get(
                "min_player_matches", 10
            )

        # Get splitter
        splitter = self._get_splitter()
        run_logger.info(
            "Training %s model with %d features on %d rows",
            self.config.model.type, len(feature_cols), len(df),
        )

        # Per-fold pruning requires a single-objective study: Optuna's
        # trial.report()/should_prune() raise on multi-objective studies (there's
        # no single value to prune on). When tuning multiple metrics, skip pruning
        # so trials run to completion and still record their metrics, rather than
        # crashing at the first outer-fold boundary.
        pruning_enabled = trial is not None and len(trial.study.directions) == 1

        # Train and evaluate
        check_memory("before training loop")
        all_metrics: list[dict[str, float]] = []
        all_train_metrics: list[dict[str, float]] = []
        all_predictions: list[dict[str, Any]] = []
        all_fold_meta: list[dict[str, Any]] = []
        # Per-fold aux head R² captured only when MTL is active. Empty list
        # under non-MTL configs. Friendly aux names (without "_aux_" prefix).
        all_aux_r2: list[dict[str, float]] = []
        all_fold_importances: list[dict[str, float]] | None = (
            None if is_ensemble else []
        )
        all_per_model_predictions: list[list[np.ndarray]] = [] if is_ensemble else []

        run_context = logger.start_run(run_name=self.run_name) if logger else None
        if run_context:
            run_context.__enter__()
            logger.log_params({
                "model_type": self.config.model.type,
                "target": self.config.target,
                "validation_type": self.config.validation.type,
                "n_features": len(feature_cols),
                "n_splits": self.config.validation.n_splits,
                "date_range_start": str(self.config.data.date_range.start),
                "date_range_end": str(self.config.data.date_range.end),
                "n_rows": len(df),
            })
            if self.config.model.params:
                for k, v in self.config.model.params.items():
                    if k == "base_models":
                        continue
                    logger.log_params({f"model_{k}": v})
            if self.config.data.filters:
                for k, v in self.config.data.filters.items():
                    if isinstance(v, dict):
                        if "min" in v:
                            logger.log_params({f"filter_{k}_min": v["min"]})
                        if "max" in v:
                            logger.log_params({f"filter_{k}_max": v["max"]})
                    else:
                        logger.log_params({f"filter_{k}": v})
            # Log feature list (MLflow truncates long param values, so use one per feature)
            for i, feat in enumerate(feature_cols):
                logger.log_params({f"feature_{i}": feat})
            # Log config YAML as artifact
            logger.log_artifact(str(self.config_path))
            # Log ensemble base model configs as artifacts
            if is_ensemble and self.config.model.params:
                ens = EnsembleParams.model_validate(self.config.model.params)
                for i, bm in enumerate(ens.base_models):
                    bm_path = self.config_path.parent / bm.config
                    if bm_path.exists():
                        logger.log_artifact(str(bm_path))

        # Build the iteration plan. With inner_cv_folds=0 this is just the
        # outer splits; with inner_cv_folds>0 each non-holdout outer fold is
        # expanded into k inner expanding-window splits on its training
        # portion. The loop body below doesn't care which kind it's processing
        # — we regroup after the loop using `iteration_to_outer`.
        outer_splits = list(splitter.split(df))
        n_outer = len(outer_splits)
        if self.holdout_folds > 0:
            n_tuning = n_outer - self.holdout_folds
        else:
            n_tuning = n_outer

        # Precompute outer fold meta (used post-regroup so we report on outer
        # windows, not inner split windows).
        outer_fold_meta: list[dict[str, Any]] = []
        for ofi, (otr_train_idx, otr_test_idx) in enumerate(outer_splits):
            otr_test_df = df[otr_test_idx]
            t_dates = otr_test_df["effective_match_date"]
            t_min = t_dates.min()
            t_max = t_dates.max()
            outer_fold_meta.append({
                "fold_idx": ofi + 1,
                "test_start": t_min.date() if hasattr(t_min, "date") else t_min,
                "test_end": t_max.date() if hasattr(t_max, "date") else t_max,
                "n_train": len(otr_train_idx),
                "n_test": len(otr_test_idx),
            })

        iteration_splits: list[tuple[list[int], list[int]]] = []
        iteration_to_outer: list[int] = []
        inner_fold_count_per_outer: list[int] = []
        for ofi, (otr_train_idx, otr_test_idx) in enumerate(outer_splits):
            is_tuning_fold = ofi < n_tuning
            if self.inner_cv_folds > 0 and is_tuning_fold:
                outer_train_df = df[otr_train_idx]
                n_otr = len(outer_train_df)
                # Heuristic split sizes that give roughly k expanding-window
                # inner folds: min_train covers the first ~half of the data,
                # test windows split the second half evenly.
                min_train = max(n_otr // 2, 1000)
                test_size_inner = max(
                    (n_otr - min_train) // self.inner_cv_folds, 100
                )
                try:
                    from mvp.model.splitters import ExpandingWindowSplitter
                    inner_splitter = ExpandingWindowSplitter(
                        n_splits=self.inner_cv_folds,
                        min_train_size=min_train,
                        test_size=test_size_inner,
                    )
                    inner_pairs = list(inner_splitter.split(outer_train_df))
                except Exception as e:
                    run_logger.warning(
                        "Outer fold %d: inner CV unavailable (n_train=%d): %s. "
                        "Falling back to single outer-test prediction.",
                        ofi + 1, n_otr, e,
                    )
                    inner_pairs = []

                if inner_pairs:
                    # Map inner indices (positions within outer_train_df) back
                    # to original df indices.
                    for in_tr, in_te in inner_pairs:
                        iteration_splits.append((
                            [otr_train_idx[i] for i in in_tr],
                            [otr_train_idx[i] for i in in_te],
                        ))
                        iteration_to_outer.append(ofi)
                    inner_fold_count_per_outer.append(len(inner_pairs))
                else:
                    # Fallback: outer behavior for this fold
                    iteration_splits.append((otr_train_idx, otr_test_idx))
                    iteration_to_outer.append(ofi)
                    inner_fold_count_per_outer.append(1)
            else:
                iteration_splits.append((otr_train_idx, otr_test_idx))
                iteration_to_outer.append(ofi)
                inner_fold_count_per_outer.append(1)

        if self.inner_cv_folds > 0:
            run_logger.info(
                "Inner CV active: %d outer folds expanded to %d total fits "
                "(inner_fold_count_per_outer=%s, holdout_folds=%d)",
                n_outer, len(iteration_splits),
                inner_fold_count_per_outer, self.holdout_folds,
            )

        try:
            for fold_idx, (train_idx, test_idx) in enumerate(iteration_splits):
                outer_fold_id = iteration_to_outer[fold_idx]
                check_memory(f"iter {fold_idx + 1} start (outer fold {outer_fold_id + 1})")
                t_fold = time.perf_counter()
                train_df = df[train_idx]
                test_df = df[test_idx]
                if self.config.data.train_filters:
                    train_df = apply_filters(train_df, self.config.data.train_filters)
                if self.config.data.eval_filters:
                    test_df = apply_filters(test_df, self.config.data.eval_filters)
                # Completeness gate is TRAINING-ONLY: incomplete matches
                # (RET/DEF/UNP, partial scores) are dropped from the fit but kept
                # in the test fold — they're real graded matches for eval/backtest.
                train_df = self._filter_training_completeness(train_df, target_cols)
                run_logger.info(
                    "Iter %d/%d (outer fold %d): train=%d, test=%d",
                    fold_idx + 1, len(iteration_splits),
                    outer_fold_id + 1, len(train_df), len(test_df),
                )

                X_train = train_df.select(
                    pl.col(c).cast(pl.Float64) for c in augmented_cols
                ).to_numpy()
                y_train = train_df[target_col].to_numpy().astype(int)
                # MTL path: assemble 2D y for the multi-target fit. Primary
                # column (target_cols[0]) is the same as 1D y_train; aux
                # columns are appended in the order target_cols specifies. The
                # XGBoostMTLModel handles aux standardization internally.
                # Non-MTL path: y_train_for_fit aliases y_train (1D).
                y_train_for_fit = (
                    train_df.select(target_cols).to_numpy()
                    if is_mtl
                    else y_train
                )
                X_test = test_df.select(
                    pl.col(c).cast(pl.Float64) for c in augmented_cols
                ).to_numpy()
                y_test = test_df[target_col].to_numpy().astype(int)

                # Impute NaN using per-feature strategy with circuit-stratified medians
                circuit_train = train_df["circuit"].to_numpy()
                circuit_test = test_df["circuit"].to_numpy()
                impute_state = fit_imputation(X_train, circuit_train, build_result.specs)

                # Compute scaling stats from real data (before imputation), model cols only
                import warnings as _w
                with _w.catch_warnings():
                    _w.simplefilter("ignore", RuntimeWarning)
                    train_mean = np.nanmean(X_train[:, :n_model], axis=0)
                    train_std = np.nanstd(X_train[:, :n_model], axis=0)
                train_mean = np.where(np.isnan(train_mean), 0.0, train_mean)
                train_std = np.where(np.isnan(train_std), 1.0, train_std)
                train_std[train_std == 0] = 1.0

                # Impute (augmented), strip aux columns, then scale
                X_train = apply_imputation(X_train, circuit_train, impute_state)
                X_test = apply_imputation(X_test, circuit_test, impute_state)
                X_train = X_train[:, :n_model]
                X_test = X_test[:, :n_model]
                X_train = (X_train - train_mean) / train_std
                X_test = (X_test - train_mean) / train_std

                # Append embedding column (integer-encoded, not scaled)
                vocab: dict[Any, int] | None = None
                if embedding_col and embedding_col in train_df.columns:
                    # Build vocab from player column (and opp column if dual)
                    player_ids = train_df[embedding_col].to_list()
                    if opp_embedding_col and opp_embedding_col in train_df.columns:
                        opp_ids = train_df[opp_embedding_col].to_list()
                        all_ids = player_ids + opp_ids
                    else:
                        all_ids = player_ids
                    id_counts: dict[str, int] = {}
                    for pid in all_ids:
                        id_counts[pid] = id_counts.get(pid, 0) + 1
                    eligible_ids = [
                        pid for pid, count in id_counts.items()
                        if count >= min_player_matches
                    ]
                    vocab = {pid: idx + 1 for idx, pid in enumerate(eligible_ids)}

                    emb_train = np.array(
                        [vocab.get(p, 0) for p in train_df[embedding_col].to_list()]
                    ).reshape(-1, 1)
                    emb_test = np.array(
                        [vocab.get(p, 0) for p in test_df[embedding_col].to_list()]
                    ).reshape(-1, 1)
                    X_train = np.hstack([X_train, emb_train.astype(np.float64)])
                    X_test = np.hstack([X_test, emb_test.astype(np.float64)])
                    self.config.model.params["embedding_col_idx"] = X_train.shape[1] - 1

                    if opp_embedding_col and opp_embedding_col in train_df.columns:
                        opp_emb_train = np.array(
                            [vocab.get(p, 0) for p in train_df[opp_embedding_col].to_list()]
                        ).reshape(-1, 1)
                        opp_emb_test = np.array(
                            [vocab.get(p, 0) for p in test_df[opp_embedding_col].to_list()]
                        ).reshape(-1, 1)
                        X_train = np.hstack([X_train, opp_emb_train.astype(np.float64)])
                        X_test = np.hstack([X_test, opp_emb_test.astype(np.float64)])
                        self.config.model.params["opp_embedding_col_idx"] = X_train.shape[1] - 1

                    self.config.model.params["n_players"] = len(vocab)

                # Sequence model: append match_date column and resolve identifier
                # indices. Requires embedding_col + opp_embedding_col in config so
                # vocab is built above.
                if self.config.model.type == "sequence":
                    if vocab is None:
                        raise ValueError(
                            "SequenceModel requires embedding_col and opp_embedding_col "
                            "in model.params so player_id / opp_id can be vocab-encoded "
                            "(history dict keys must match X column values)"
                        )
                    train_dates_int = (
                        train_df["effective_match_date"].cast(pl.Date).cast(pl.Int64)
                        .to_numpy().reshape(-1, 1)
                    )
                    test_dates_int = (
                        test_df["effective_match_date"].cast(pl.Date).cast(pl.Int64)
                        .to_numpy().reshape(-1, 1)
                    )
                    X_train = np.hstack([X_train, train_dates_int.astype(np.float64)])
                    X_test = np.hstack([X_test, test_dates_int.astype(np.float64)])
                    self.config.model.params["match_date_col_idx"] = X_train.shape[1] - 1
                    # Alias embedding indices to sequence model's expected names
                    self.config.model.params["player_id_col_idx"] = (
                        self.config.model.params["embedding_col_idx"]
                    )
                    self.config.model.params["opp_id_col_idx"] = (
                        self.config.model.params["opp_embedding_col_idx"]
                    )

                # Compute sample weights if configured
                train_weights = None
                if self.config.sample_weight is not None:
                    train_dates = train_df["effective_match_date"].to_numpy()
                    train_weights = compute_sample_weights(
                        train_dates, self.config.sample_weight
                    )

                # Build per-model training data for ensemble date/filter/weight differences
                per_model_data = None
                if is_ensemble and needs_per_model and model_date_ranges and model_filters:
                    _sw_list = model_sample_weights or [None] * len(model_date_ranges)
                    test_start_date = test_df["effective_match_date"].min()
                    per_model_data = []
                    for dr, filt, sw_cfg in zip(model_date_ranges, model_filters, _sw_list):
                        has_wider_dates = dr.start < self.config.data.date_range.start
                        has_custom_filters = filt is not None
                        has_custom_weights = sw_cfg is not None
                        if has_wider_dates or has_custom_filters or has_custom_weights:
                            if has_wider_dates and df_wide is not None:
                                model_train_df = df_wide.filter(
                                    (pl.col("effective_match_date") >= dr.start)
                                    & (pl.col("effective_match_date") < test_start_date)
                                )
                            else:
                                model_train_df = train_df
                            if has_custom_filters:
                                model_train_df = apply_filters(model_train_df, filt)
                            X_m = model_train_df.select(
                                pl.col(c).cast(pl.Float64) for c in augmented_cols
                            ).to_numpy()
                            y_m = model_train_df[target_col].to_numpy().astype(int)
                            circuit_m = model_train_df["circuit"].to_numpy()
                            X_m = apply_imputation(X_m, circuit_m, impute_state)
                            X_m = X_m[:, :n_model]
                            X_m = (X_m - train_mean) / train_std
                            # Use base model's sample_weight config, fall back to ensemble's
                            w_cfg = sw_cfg or self.config.sample_weight
                            w_m = None
                            if w_cfg is not None:
                                model_dates = model_train_df["effective_match_date"].to_numpy()
                                w_m = compute_sample_weights(model_dates, w_cfg)
                            per_model_data.append((X_m, y_m, w_m))
                        else:
                            per_model_data.append(None)

                # Train model. MTL dispatches to XGBoostMTLModel directly:
                # model.type stays "xgboost" in config, but the runner routes
                # to the multi-task wrapper because MTL is a runner-level
                # decision (vector-leaf + custom heterogeneous objective).
                # target_names uses the user-friendly aux names from the MTL
                # config (e.g. "game_margin"), not the derived column names
                # (e.g. "_aux_game_margin"), so per-target loss-weight params
                # (weight_game_margin, ...) match up with the model's
                # extraction logic.
                if is_mtl:
                    assert self.config.mtl is not None
                    target_names_mtl = (
                        [self.config.target]
                        + list(self.config.mtl.auxiliary_targets)
                    )
                    model = XGBoostMTLModel(
                        params=self.config.model.params or {},
                        target_names=target_names_mtl,
                        feature_names=feature_cols,
                    )
                else:
                    model = get_model(
                        self.config.model.type,
                        self.config.model.params or {},
                        feature_names=feature_cols,
                    )
                if self.config.model.type == "sequence":
                    # Build per-fold history DataFrame: pre-start seed (anti-cold-start)
                    # plus this fold's training rows. Encode player_id with the same
                    # vocab used for X columns so history dict keys align.
                    from mvp.model.sequence_model import (
                        HISTORY_RAW_COLUMNS,
                        SequenceModel,
                    )
                    assert isinstance(model, SequenceModel)
                    assert vocab is not None  # enforced above
                    history_parts = [train_df.select(HISTORY_RAW_COLUMNS)]
                    if df_history_seed is not None and df_history_seed.height > 0:
                        # The seed df may not have all HISTORY_RAW_COLUMNS if some
                        # were not part of runner_columns at compute time; but we
                        # added them above so this should be a no-op.
                        seed_cols_present = [
                            c for c in HISTORY_RAW_COLUMNS
                            if c in df_history_seed.columns
                        ]
                        if seed_cols_present == list(HISTORY_RAW_COLUMNS):
                            history_parts.insert(0, df_history_seed.select(HISTORY_RAW_COLUMNS))
                    history_df = pl.concat(history_parts)
                    # Encode player_id via vocab; unknown players → 0 (cold-start at lookup)
                    encoded = np.array(
                        [vocab.get(p, 0) for p in history_df["player_id"].to_list()],
                        dtype=np.int64,
                    )
                    history_df = history_df.with_columns(
                        pl.Series("player_id", encoded)
                    )
                    model.set_history_features(history_df)

                if is_ensemble and base_model_specs is not None:
                    assert isinstance(model, EnsembleModel)
                    model.configure(base_model_specs)
                    model.fit(
                        X_train, y_train,
                        sample_weight=train_weights,
                        per_model_data=per_model_data,
                    )
                else:
                    es = self.config.early_stopping
                    if (
                        es is not None and es.enabled
                        and self.config.model.type == "xgboost"
                    ):
                        # Two-stage early stopping (sklearn or MTL xgb.train path).
                        # Stops on metrics.objective[0] (the run's objective),
                        # refits on full train at best_iteration. Not the FS path.
                        params = self.config.model.params or {}

                        def _es_factory(n_rounds: int):
                            p = {**params, "n_estimators": n_rounds}
                            if is_mtl:
                                tn = [self.config.target] + list(
                                    self.config.mtl.auxiliary_targets
                                )
                                return XGBoostMTLModel(
                                    params=p, target_names=tn,
                                    feature_names=feature_cols,
                                )
                            return get_model("xgboost", p, feature_names=feature_cols)

                        _ts = test_df["effective_match_date"].min()
                        model, _best_it = two_stage_fit(
                            _es_factory, X_train, y_train_for_fit, train_weights,
                            train_df["effective_match_date"].to_numpy(),
                            _ts.date() if hasattr(_ts, "date") else _ts,
                            es, metric=self.config.metrics.objective[0],
                            lambda_over=params.get("lambda_over"), is_mtl=is_mtl,
                        )
                    else:
                        model.fit(X_train, y_train_for_fit, sample_weight=train_weights)

                # Predict and evaluate on test
                is_stacking = (
                    is_ensemble
                    and self.config.model.params.get("strategy") == "stacking"
                )
                if is_stacking:
                    assert isinstance(model, EnsembleModel)
                    per_model = model.predict_proba_per_model(X_test)
                    y_prob = np.mean(per_model, axis=0)
                    per_model_train = model.predict_proba_per_model(X_train)
                    y_prob_train = np.mean(per_model_train, axis=0)
                else:
                    y_prob = model.predict_proba(X_test)
                    y_prob_train = model.predict_proba(X_train)
                metrics = compute_metrics(y_test, y_prob, lambda_over=lambda_over_eval)
                all_metrics.append(metrics)

                # Predict and evaluate on train (for overfitting detection)
                train_metrics = compute_metrics(y_train, y_prob_train, lambda_over=lambda_over_eval)
                all_train_metrics.append(train_metrics)

                # MTL: per-fold aux head R² on the test fold. H38 design uses
                # aux R² as the sanity-check gate — if the aux heads collapsed
                # to no signal, MTL was effectively single-task. R² computed on
                # ORIGINAL (un-standardized) scale because predict_aux returns
                # inverse-transformed predictions.
                if is_mtl and hasattr(model, "predict_aux"):
                    aux_pred_test = model.predict_aux(X_test)
                    aux_col_names = target_cols[1:]  # exclude primary
                    y_test_aux = test_df.select(aux_col_names).to_numpy()
                    fold_aux_r2: dict[str, float] = {}
                    for i, aux_col in enumerate(aux_col_names):
                        friendly = aux_col.removeprefix("_aux_")
                        # Retirements/incompletes are kept in the test fold but
                        # carry null aux labels — score R² only on rows where the
                        # aux target is defined.
                        col = y_test_aux[:, i]
                        valid = ~np.isnan(col)
                        try:
                            if int(valid.sum()) < 2:
                                raise ValueError("insufficient non-null aux")
                            fold_aux_r2[friendly] = float(
                                r2_score(col[valid], aux_pred_test[valid, i])
                            )
                        except (ValueError, RuntimeWarning):
                            fold_aux_r2[friendly] = float("nan")
                    all_aux_r2.append(fold_aux_r2)

                # Capture per-fold gain importance (tree, non-ensemble only)
                if all_fold_importances is not None:
                    try:
                        all_fold_importances.append(
                            gain_importance(model, feature_cols)
                        )
                    except ValueError:
                        all_fold_importances = None

                # Collect predictions for diagnostics
                all_predictions.append({
                    "df": test_df,
                    "y_true": y_test,
                    "y_prob": y_prob,
                })

                # Capture fold metadata for the per-fold report
                _test_dates = test_df["effective_match_date"]
                _test_min = _test_dates.min()
                _test_max = _test_dates.max()
                all_fold_meta.append({
                    "fold_idx": fold_idx + 1,
                    "test_start": _test_min.date() if hasattr(_test_min, "date") else _test_min,
                    "test_end": _test_max.date() if hasattr(_test_max, "date") else _test_max,
                    "n_train": len(train_df),
                    "n_test": len(test_df),
                })

                if is_ensemble and isinstance(model, EnsembleModel):
                    all_per_model_predictions.append(
                        model.predict_proba_per_model(X_test)
                    )

                run_logger.info(
                    "Fold %d: acc=%.3f, auc=%.3f, ll=%.4f (%.1fs)",
                    fold_idx + 1, metrics.get("accuracy", 0),
                    metrics.get("roc_auc", 0), metrics.get("log_loss", 0),
                    time.perf_counter() - t_fold,
                )

                # Log fold metrics
                if logger:
                    logger.log_metrics(
                        {f"fold_{fold_idx}_{k}": v for k, v in metrics.items()}
                    )

                # Pruning check at outer-fold boundaries only. Aggregate the
                # current outer fold's per-iteration predictions (only one
                # entry when inner_cv_folds=0; up to inner_cv_folds entries
                # otherwise), compute the outer-fold log_loss, report to
                # the trial, and consult the pruner. Holdout folds are
                # excluded (only the tuning folds 0..n_tuning-1 are reported).
                if pruning_enabled:
                    current_outer = iteration_to_outer[fold_idx]
                    next_outer = (
                        iteration_to_outer[fold_idx + 1]
                        if fold_idx + 1 < len(iteration_to_outer)
                        else -1
                    )
                    is_last_iter_of_outer = next_outer != current_outer
                    is_tuning_outer = current_outer < n_tuning
                    if is_last_iter_of_outer and is_tuning_outer:
                        outer_iter_idxs = [
                            i for i, o in enumerate(iteration_to_outer)
                            if o == current_outer and i <= fold_idx
                        ]
                        outer_y_true = np.concatenate(
                            [all_predictions[i]["y_true"] for i in outer_iter_idxs]
                        )
                        outer_y_prob = np.concatenate(
                            [all_predictions[i]["y_prob"] for i in outer_iter_idxs]
                        )
                        # Prune on the tuning OBJECTIVE, not a hardcoded log_loss:
                        # a single-objective study ranks/stops on metrics.objective,
                        # so the pruner must too (else it kills trials by a metric
                        # the study isn't optimizing). objective is single here (the
                        # ES validator / single-objective pruning both guarantee it).
                        obj = self.config.metrics.objective
                        prune_metric = obj[0] if obj else "log_loss"
                        outer_val = float(compute_metrics(
                            outer_y_true, outer_y_prob, lambda_over=lambda_over_eval,
                        ).get(prune_metric, float("nan")))
                        if np.isnan(outer_val):
                            # The objective should be in the computed metrics (the
                            # study optimizes it). If not, skip pruning rather than
                            # prune on a direction-mismatched log_loss fallback.
                            run_logger.warning(
                                "pruner: objective %r missing from computed metrics; "
                                "skipping prune at outer fold %d",
                                prune_metric, current_outer,
                            )
                        else:
                            trial.report(outer_val, step=current_outer)
                            if trial.should_prune():
                                # Inline import: runner is also called from
                                # `mvp model` (no optuna dep). Importing at the
                                # top would force optuna onto every code path.
                                import optuna
                                raise optuna.TrialPruned()

            # If inner CV was active, the per-iteration lists above hold one
            # entry per inner split. Regroup them by outer fold so downstream
            # code (holdout split, calibration, diagnostics, reporting) keeps
            # operating in terms of outer folds.
            if self.inner_cv_folds > 0 and len(iteration_splits) > n_outer:
                regrouped_predictions: list[dict[str, Any]] = []
                regrouped_metrics: list[dict[str, float]] = []
                regrouped_train_metrics: list[dict[str, float]] = []
                regrouped_per_model: list[list[np.ndarray]] = []
                regrouped_importances: list[dict[str, float]] | None = (
                    [] if all_fold_importances is not None else None
                )

                for outer_idx in range(n_outer):
                    iter_idxs = [
                        i for i, oid in enumerate(iteration_to_outer)
                        if oid == outer_idx
                    ]
                    if not iter_idxs:
                        continue

                    c_y_true = np.concatenate(
                        [all_predictions[i]["y_true"] for i in iter_idxs]
                    )
                    c_y_prob = np.concatenate(
                        [all_predictions[i]["y_prob"] for i in iter_idxs]
                    )
                    c_df = pl.concat(
                        [all_predictions[i]["df"] for i in iter_idxs],
                        how="diagonal_relaxed",
                    )
                    regrouped_predictions.append({
                        "y_true": c_y_true,
                        "y_prob": c_y_prob,
                        "df": c_df,
                    })
                    regrouped_metrics.append(compute_metrics(c_y_true, c_y_prob, lambda_over=lambda_over_eval))

                    train_keys = list(all_train_metrics[iter_idxs[0]].keys())
                    regrouped_train_metrics.append({
                        k: float(np.mean(
                            [all_train_metrics[i][k] for i in iter_idxs]
                        ))
                        for k in train_keys
                    })

                    if is_ensemble and all_per_model_predictions:
                        n_base = len(all_per_model_predictions[iter_idxs[0]])
                        regrouped_per_model.append([
                            np.concatenate(
                                [all_per_model_predictions[i][b] for i in iter_idxs]
                            )
                            for b in range(n_base)
                        ])

                    if (
                        regrouped_importances is not None
                        and all_fold_importances
                    ):
                        # Use the largest inner split's importance (most data)
                        regrouped_importances.append(
                            all_fold_importances[iter_idxs[-1]]
                        )

                all_predictions = regrouped_predictions
                all_metrics = regrouped_metrics
                all_train_metrics = regrouped_train_metrics
                all_fold_meta = outer_fold_meta
                if is_ensemble:
                    all_per_model_predictions = regrouped_per_model
                if regrouped_importances is not None:
                    all_fold_importances = regrouped_importances

            # Build per-fold predictions parquet content. Used by the
            # feature-error analysis pipeline (mvp-docs/experiments/
            # 2026-06-03-feature-error-analysis-plan.md) to join predictions
            # back to features. Captured here after any inner-CV regrouping so
            # fold_idx corresponds to outer folds the user sees.
            _fold_pred_cols = [
                "match_uid", "player_id", "opp_id",
                "effective_match_date", "circuit", "surface", "round",
            ]
            fold_pred_frames: list[pl.DataFrame] = []
            for fold_idx, pred in enumerate(all_predictions):
                fold_df = pred["df"]
                available = [c for c in _fold_pred_cols if c in fold_df.columns]
                fold_pred_frames.append(
                    fold_df.select(available).with_columns(
                        pl.lit(fold_idx + 1).cast(pl.Int32).alias("fold_idx"),
                        pl.Series("y_test", pred["y_true"]).cast(pl.Int64),
                        pl.Series("y_prob", pred["y_prob"]).cast(pl.Float64),
                    )
                )
            fold_predictions_df = (
                pl.concat(fold_pred_frames, how="diagonal_relaxed")
                if fold_pred_frames else None
            )

            # Per-sub OOF transpose. Reshape all_per_model_predictions
            # (list[fold][sub] → ndarray) into per_sub_predictions
            # (list[sub][fold] → dict shaped for fit_calibrator_with_nested_cv).
            # Built unconditionally for ensembles so the downstream cal logic
            # can slice it by tuning/holdout without re-collection. The y_prob
            # arrays are SHARED references with all_per_model_predictions;
            # fit_calibrator_with_nested_cv mutates by dict-key reassignment
            # (pred["y_prob"] = ...), not in-place array mutation, so the
            # originals in all_per_model_predictions remain intact for any
            # other downstream consumer that might want raw per-sub preds.
            per_sub_predictions: list[list[dict[str, Any]]] = []
            if is_ensemble and all_per_model_predictions:
                n_subs = len(all_per_model_predictions[0])
                for sub_idx in range(n_subs):
                    sub_fold_preds = []
                    for fold_idx, fold_pred_dict in enumerate(all_predictions):
                        sub_fold_preds.append({
                            "y_prob": all_per_model_predictions[fold_idx][sub_idx],
                            "y_true": fold_pred_dict["y_true"],
                            "df": fold_pred_dict["df"],
                        })
                    per_sub_predictions.append(sub_fold_preds)

            # Split predictions into tuning vs holdout. The trailing folds
            # become the holdout: they get calibrated probabilities using a
            # calibrator fit only on tuning preds, but they don't influence
            # the reported `metrics` / diagnostics / objective. Tuning sets
            # holdout_folds=1; normal runs default to 0.
            n_folds_total = len(all_predictions)
            if self.holdout_folds >= n_folds_total:
                raise ValueError(
                    f"holdout_folds ({self.holdout_folds}) must be < n_folds "
                    f"({n_folds_total}). Tuning a single-fold setup with holdout "
                    "isn't supported."
                )
            if self.holdout_folds > 0:
                tuning_predictions = all_predictions[:-self.holdout_folds]
                holdout_predictions = all_predictions[-self.holdout_folds:]
                tuning_fold_meta = all_fold_meta[:-self.holdout_folds]
                holdout_fold_meta = all_fold_meta[-self.holdout_folds:]
                tuning_fold_indices = list(range(n_folds_total - self.holdout_folds))
                holdout_fold_indices = list(
                    range(n_folds_total - self.holdout_folds, n_folds_total)
                )
                per_sub_tuning_predictions = [
                    s[:-self.holdout_folds] for s in per_sub_predictions
                ]
                per_sub_holdout_predictions = [
                    s[-self.holdout_folds:] for s in per_sub_predictions
                ]
            else:
                tuning_predictions = all_predictions
                holdout_predictions = []
                tuning_fold_meta = all_fold_meta
                holdout_fold_meta = []
                tuning_fold_indices = list(range(n_folds_total))
                holdout_fold_indices = []
                per_sub_tuning_predictions = per_sub_predictions
                per_sub_holdout_predictions = [[] for _ in per_sub_predictions]

            # Average metrics across folds
            avg_metrics = {
                k: float(np.mean([m[k] for m in all_metrics]))
                for k in all_metrics[0].keys()
            }
            avg_train_metrics = {
                k: float(np.mean([m[k] for m in all_train_metrics]))
                for k in all_train_metrics[0].keys()
            }

            # Fit stacking meta-model on concatenated OOF predictions.
            # When holdout is on, fit on tuning preds only (scaler stats too) so
            # holdout labels can't leak into stacking weights. Predict on full
            # set so holdout still gets a stacked probability.
            if is_ensemble and self.config.model.params.get("strategy") == "stacking":
                assert isinstance(model, EnsembleModel)
                n_base = len(all_per_model_predictions[0])

                X_meta = np.column_stack([
                    np.concatenate([fold[i] for fold in all_per_model_predictions])
                    for i in range(n_base)
                ])
                y_meta = np.concatenate([p["y_true"] for p in all_predictions])

                n_tuning_samples = sum(
                    len(p["y_true"]) for p in tuning_predictions
                )

                ensemble_params = EnsembleParams.model_validate(self.config.model.params)
                base_names = [ref.config for ref in ensemble_params.base_models]
                meta_feature_col_names: list[str] = []

                if meta_feature_indices:
                    meta_feature_col_names = get_feature_columns(
                        ensemble_params.meta_features
                    )
                    combined_X = np.concatenate([
                        p["df"].select(
                            pl.col(c).cast(pl.Float64) for c in feature_cols
                        ).to_numpy()
                        for p in all_predictions
                    ])
                    X_meta_raw = combined_X[:, meta_feature_indices]

                    # Scaler stats from tuning slice only (prevents holdout leak)
                    X_meta_raw_tuning = X_meta_raw[:n_tuning_samples]
                    medians_meta = np.nanmedian(X_meta_raw_tuning, axis=0)
                    medians_meta = np.where(np.isnan(medians_meta), 0.0, medians_meta)
                    X_meta_raw = np.where(np.isnan(X_meta_raw), medians_meta, X_meta_raw)

                    X_meta_raw_tuning = X_meta_raw[:n_tuning_samples]
                    meta_mean = X_meta_raw_tuning.mean(axis=0)
                    meta_std = X_meta_raw_tuning.std(axis=0)
                    meta_std[meta_std == 0] = 1.0
                    X_meta_std = (X_meta_raw - meta_mean) / meta_std

                    X_meta = np.hstack([X_meta, X_meta_std])
                    model._meta_scaler = (meta_mean, meta_std)

                model.set_meta_feature_indices(meta_feature_indices)
                model.set_meta_feature_names(base_names + meta_feature_col_names)
                # Fit on tuning slice only
                model.fit_meta(X_meta[:n_tuning_samples], y_meta[:n_tuning_samples])

                # Predict on full set so holdout preds also get stacked probs
                y_prob_stacked = model._meta_model.predict_proba(X_meta)[:, 1]
                avg_metrics = compute_metrics(
                    y_meta[:n_tuning_samples],
                    y_prob_stacked[:n_tuning_samples],
                    lambda_over=lambda_over_eval,
                )

                offset = 0
                for pred_dict in all_predictions:
                    n = len(pred_dict["y_true"])
                    pred_dict["y_prob"] = y_prob_stacked[offset:offset + n]
                    offset += n

            # Concat tuning-fold OOF preds for calibrator fitting and/or
            # raw-metric computation.
            combined_y_true_oof = np.concatenate(
                [p["y_true"] for p in tuning_predictions]
            )
            combined_y_prob_oof = np.concatenate(
                [p["y_prob"] for p in tuning_predictions]
            )

            calibrator: (
                PlattCalibrator
                | SegmentedPlattCalibrator
                | IsotonicCalibrator
                | SegmentedIsotonicCalibrator
                | AsymmIsotonicCalibrator
                | None
            ) = None
            if self.calibrate:
                # Calibration. Fit on tuning OOF only so the holdout is never
                # seen by the calibrator. Apply the resulting calibrator to BOTH
                # tuning and holdout preds so every fold gets a calibrated
                # probability (the holdout's just hasn't seen its own labels).
                # raw_metrics are computed pre-calibration for diagnostic visibility.
                raw_metrics = compute_metrics(
                    combined_y_true_oof, combined_y_prob_oof, lambda_over=lambda_over_eval
                )

                cal_cfg = self.config.calibration

                # Per-sub calibration (ensemble-only). Fit each sub's own
                # calibrator on its own OOF preds BEFORE the top-level cal
                # so the top-level cal fits on sub-cal-applied averages, which
                # mirrors deployment flow: raw sub → sub cal → average → top cal.
                has_any_sub_cal = is_ensemble and any(
                    cfg is not None for cfg in getattr(model, "_sub_cal_configs", [])
                )
                if has_any_sub_cal:
                    assert isinstance(model, EnsembleModel)
                    n_subs = len(model._sub_cal_configs)
                    for sub_idx in range(n_subs):
                        sub_cal_cfg = model._sub_cal_configs[sub_idx]
                        if sub_cal_cfg is None:
                            continue
                        # fit_calibrator_with_nested_cv mutates
                        # per_sub_tuning_predictions[sub_idx] in place: each
                        # fold dict's y_prob key is REASSIGNED to nested-CV-
                        # calibrated values. Dict-key reassignment overwrites
                        # the reference (doesn't mutate the array), so original
                        # raw arrays in all_per_model_predictions stay intact.
                        # The returned deployed cal is what we attach for
                        # inference.
                        sub_deployed_cal = fit_calibrator_with_nested_cv(
                            per_sub_tuning_predictions[sub_idx], sub_cal_cfg
                        )
                        model.set_sub_calibrator(sub_idx, sub_deployed_cal)

                    # Re-average tuning ensemble preds to reflect the now-
                    # nested-cal'd per-sub outputs. Subs without cal contribute
                    # raw preds. Strategy is honored (stacking is rejected at
                    # spec-build when sub-cal is present, so only average and
                    # weighted_average can reach here).
                    strategy = self.config.model.params.get("strategy", "average")
                    for fold_idx, ensemble_pred_dict in enumerate(tuning_predictions):
                        sub_outs = np.array([
                            per_sub_tuning_predictions[s][fold_idx]["y_prob"]
                            for s in range(n_subs)
                        ])
                        if strategy == "weighted_average":
                            ensemble_pred_dict["y_prob"] = np.average(
                                sub_outs, axis=0, weights=model._weights
                            )
                        else:
                            ensemble_pred_dict["y_prob"] = np.mean(sub_outs, axis=0)

                    # Re-average holdout ensemble preds. Holdout per-sub preds
                    # weren't passed to fit_calibrator_with_nested_cv, so they
                    # are still raw — apply each sub's DEPLOYED cal here to
                    # get the deployment-flow holdout output. Segmented sub
                    # cals get the per-fold df.
                    for fold_idx, ensemble_pred_dict in enumerate(holdout_predictions):
                        fold_df = ensemble_pred_dict["df"]
                        sub_outs_list = []
                        for s in range(n_subs):
                            raw_holdout = per_sub_holdout_predictions[s][fold_idx]["y_prob"]
                            cal = model._sub_calibrators[s]
                            if cal is None:
                                sub_outs_list.append(raw_holdout)
                            elif isinstance(
                                cal,
                                (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
                            ):
                                sub_outs_list.append(cal.transform(raw_holdout, fold_df))
                            else:
                                sub_outs_list.append(cal.transform(raw_holdout))
                        sub_outs = np.array(sub_outs_list)
                        if strategy == "weighted_average":
                            ensemble_pred_dict["y_prob"] = np.average(
                                sub_outs, axis=0, weights=model._weights
                            )
                        else:
                            ensemble_pred_dict["y_prob"] = np.mean(sub_outs, axis=0)

                # Nested CV for honest diagnostics: each tuning fold's preds
                # get calibrated by a fitter that didn't see them. Helper
                # returns the deployed calibrator (fit on all tuning OOF), which
                # is what we apply to holdout preds and what gets saved.
                calibrator = fit_calibrator_with_nested_cv(
                    tuning_predictions, cal_cfg
                )
                is_segmented = isinstance(
                    calibrator,
                    (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
                )
                if is_segmented:
                    run_logger.info(
                        "Segmented %s: %d per-segment fits + global fallback "
                        "(deployed); diagnostics use nested fold-out fits",
                        type(calibrator).__name__,
                        calibrator.n_segments,
                    )
                # Holdout: deployed calibrator (never saw holdout data)
                for pred_dict in holdout_predictions:
                    if is_segmented:
                        pred_dict["y_prob"] = calibrator.transform(
                            pred_dict["y_prob"], pred_dict["df"]
                        )
                    else:
                        pred_dict["y_prob"] = calibrator.transform(
                            pred_dict["y_prob"]
                        )

                # Recompute per-fold and avg metrics on calibrated predictions
                # so the per-fold report matches the headline (both reflect the
                # single calibrator that gets deployed).
                all_metrics = [
                    compute_metrics(p["y_true"], p["y_prob"], lambda_over=lambda_over_eval)
                    for p in tuning_predictions
                ]
                calibrated_y_prob = np.concatenate(
                    [p["y_prob"] for p in tuning_predictions]
                )
                avg_metrics = compute_metrics(combined_y_true_oof, calibrated_y_prob, lambda_over=lambda_over_eval)
                for k, v in raw_metrics.items():
                    avg_metrics[f"raw_{k}"] = v
            else:
                # No calibration: metrics ARE raw. Tuning passes calibrate=False
                # so HP search optimizes raw discrimination. The `calibration:`
                # block in config is ignored here; it's a deployment concern
                # honored by `mvp model` (ProductionPredictor) not by tuning.
                run_logger.info("Calibration disabled (calibrate=False)")
                avg_metrics = compute_metrics(
                    combined_y_true_oof, combined_y_prob_oof, lambda_over=lambda_over_eval
                )
                # all_metrics already contains per-fold raw metrics from earlier
                # in run(); no recomputation needed since y_prob was never mutated.

            holdout_metrics: dict[str, float] | None = None
            holdout_fold_metrics: list[dict[str, float]] | None = None
            if holdout_predictions:
                holdout_y_true = np.concatenate(
                    [p["y_true"] for p in holdout_predictions]
                )
                holdout_y_prob = np.concatenate(
                    [p["y_prob"] for p in holdout_predictions]
                )
                holdout_metrics = compute_metrics(holdout_y_true, holdout_y_prob, lambda_over=lambda_over_eval)
                holdout_fold_metrics = [
                    compute_metrics(p["y_true"], p["y_prob"], lambda_over=lambda_over_eval)
                    for p in holdout_predictions
                ]

            holdout_metrics_calibrated: dict[str, float] | None = None
            holdout_fold_metrics_calibrated: list[dict[str, float]] | None = None
            if holdout_predictions and self.report_calibrated_holdout and not self.calibrate:
                # Deployment-frame outer-block metrics for reporting/comparison
                # (see _reporting_calibrated_holdout). Reporting-only: leaves the
                # raw objective and stored preds untouched, and never aborts the run.
                holdout_metrics_calibrated, holdout_fold_metrics_calibrated = (
                    _reporting_calibrated_holdout(
                        combined_y_true_oof,
                        combined_y_prob_oof,
                        holdout_predictions,
                        lambda_over_eval,
                    )
                )

            # Compute diagnostics on tuning preds only. Tuning preds were
            # calibrated by nested-CV (fold-i-out) calibrators, so segment cal /
            # global cal bins / error conditions are unbiased: every pred was
            # transformed by a calibrator that hasn't seen it. The deployed
            # calibrator (fit on all tuning OOF) is what gets logged and used at
            # prediction time; the nested ones exist only to produce fair
            # diagnostic numbers.
            run_logger.info("Computing diagnostics...")
            diagnostics = Diagnostics()
            cal_segments = (
                self.config.calibration.segments
                if self.config.calibration is not None
                else None
            )
            diagnostic_results = diagnostics.compute_all(
                tuning_predictions, calibration_segments=cal_segments
            )

            # Persist per-feature gain importance (mean/std across folds, full
            # list sorted by mean gain) into the diagnostics JSON so it survives
            # past stdout — same aggregation as cli._print_feature_importance.
            if all_fold_importances:
                importance_summary: list[dict[str, Any]] = []
                for feat in feature_cols:
                    vals = [fi.get(feat, 0.0) for fi in all_fold_importances]
                    mean_val = sum(vals) / len(vals)
                    var = sum((v - mean_val) ** 2 for v in vals) / len(vals)
                    importance_summary.append({
                        "feature": feat,
                        "mean_gain": mean_val,
                        "std_gain": var ** 0.5,
                    })
                importance_summary.sort(key=lambda r: r["mean_gain"], reverse=True)
                diagnostic_results.feature_importance = importance_summary

            # Compute ensemble-specific diagnostics (tuning preds only)
            ensemble_diagnostic_results = None
            if is_ensemble and all_per_model_predictions:
                # Slice per-model preds to tuning folds
                tuning_per_model_predictions = (
                    all_per_model_predictions[:-self.holdout_folds]
                    if self.holdout_folds > 0
                    else all_per_model_predictions
                )
                n_base = len(tuning_per_model_predictions[0])
                per_model_preds = [
                    np.concatenate(
                        [fold[i] for fold in tuning_per_model_predictions]
                    )
                    for i in range(n_base)
                ]
                combined_y_true = np.concatenate(
                    [p["y_true"] for p in tuning_predictions]
                )
                combined_y_prob = np.concatenate(
                    [p["y_prob"] for p in tuning_predictions]
                )
                # If per-sub calibrators are attached, replace per_model_preds
                # with sub-cal-applied outputs so the Per-Model Comparison
                # reflects what each sub actually contributes to the ensemble.
                # Use per_sub_tuning_predictions which holds NESTED-CV-cal'd
                # values (fit_calibrator_with_nested_cv mutated each fold's
                # y_prob in place with fold-i-out calibration). Avoids the
                # in-sample overfit that would result from applying the
                # deployed cal (fit on all OOF) to its own training data.
                # Subs with no cal block contribute raw values (no mutation
                # happened) — same as the pre-PR per-model report.
                if isinstance(model, EnsembleModel) and any(
                    c is not None for c in getattr(model, "_sub_calibrators", [])
                ):
                    per_model_preds = [
                        np.concatenate(
                            [fold["y_prob"] for fold in per_sub_tuning_predictions[i]]
                        )
                        for i in range(n_base)
                    ]
                assert base_model_specs is not None
                ensemble_params = EnsembleParams.model_validate(
                    self.config.model.params
                )
                weights = np.array([s["weight"] for s in base_model_specs])
                weights = weights / weights.sum()
                base_names = [
                    ref.config for ref in ensemble_params.base_models
                ]
                meta_intercept = None
                meta_coefficients = None
                if (
                    ensemble_params.strategy == "stacking"
                    and isinstance(model, EnsembleModel)
                    and model._meta_model is not None
                ):
                    meta_intercept, meta_coefficients = model.get_meta_coefficients()

                combined_df = pl.concat([p["df"] for p in tuning_predictions])
                ediag = EnsembleDiagnostics()
                ensemble_diagnostic_results = ediag.compute(
                    combined_y_true,
                    combined_y_prob,
                    per_model_preds,
                    weights,
                    base_names,
                    strategy=ensemble_params.strategy,
                    meta_intercept=meta_intercept,
                    meta_coefficients=meta_coefficients,
                    combined_df=combined_df,
                )
                diagnostic_results.ensemble = ensemble_diagnostic_results

                # Error conditions for primary model (first base model)
                primary_diag = Diagnostics()
                diagnostic_results.error_conditions = primary_diag._error_conditions(
                    combined_df, combined_y_true, per_model_preds[0]
                )

            # Merge diagnostic metrics (calibration_error, etc.) into avg_metrics
            avg_metrics.update(diagnostic_results.metrics)

            run_id = None
            if logger:
                logger.log_metrics(avg_metrics)
                logger.log_metrics({f"train_{k}": v for k, v in avg_train_metrics.items()})
                logger.log_metrics(diagnostic_results.metrics)
                if holdout_metrics is not None:
                    logger.log_metrics(
                        {f"holdout_{k}": v for k, v in holdout_metrics.items()}
                    )
                if holdout_metrics_calibrated is not None:
                    logger.log_metrics(
                        {f"holdout_cal_{k}": v for k, v in holdout_metrics_calibrated.items()}
                    )
                if calibrator is not None and calibrator.is_fitted:
                    if isinstance(calibrator, SegmentedPlattCalibrator):
                        logger.log_params({
                            "cal_method": "platt",
                            "cal_segmented": "true",
                            "cal_n_segments": str(calibrator.n_segments),
                            "cal_global_slope": f"{calibrator._global.slope:.6f}",
                            "cal_global_intercept": f"{calibrator._global.intercept:.6f}",
                        })
                    elif isinstance(calibrator, PlattCalibrator):
                        logger.log_params({
                            "cal_method": "platt",
                            "cal_segmented": "false",
                            "cal_slope": f"{calibrator.slope:.6f}",
                            "cal_intercept": f"{calibrator.intercept:.6f}",
                        })
                    elif isinstance(calibrator, SegmentedIsotonicCalibrator):
                        g = calibrator._global
                        logger.log_params({
                            "cal_method": "isotonic",
                            "cal_segmented": "true",
                            "cal_n_segments": str(calibrator.n_segments),
                            "cal_global_n_thresholds": str(g.n_thresholds),
                            "cal_global_y_min": f"{g.y_min:.6f}",
                            "cal_global_y_max": f"{g.y_max:.6f}",
                            "cal_global_grid": ",".join(
                                f"{v:.4f}" for v in g.grid_sample()
                            ),
                            "cal_mean_n_thresholds": f"{calibrator.mean_n_thresholds():.1f}",
                            "cal_max_n_thresholds": str(calibrator.max_n_thresholds()),
                        })
                    elif isinstance(calibrator, AsymmIsotonicCalibrator):
                        logger.log_params({
                            "cal_method": "asymm_isotonic",
                            "cal_segmented": "false",
                            "cal_lambda_over": f"{calibrator.lambda_over:.4f}",
                            "cal_n_thresholds": str(calibrator.n_thresholds),
                            "cal_y_min": f"{calibrator.y_min:.6f}",
                            "cal_y_max": f"{calibrator.y_max:.6f}",
                            "cal_grid": ",".join(
                                f"{v:.4f}" for v in calibrator.grid_sample()
                            ),
                        })
                    elif isinstance(calibrator, IsotonicCalibrator):
                        logger.log_params({
                            "cal_method": "isotonic",
                            "cal_segmented": "false",
                            "cal_n_thresholds": str(calibrator.n_thresholds),
                            "cal_y_min": f"{calibrator.y_min:.6f}",
                            "cal_y_max": f"{calibrator.y_max:.6f}",
                            "cal_grid": ",".join(
                                f"{v:.4f}" for v in calibrator.grid_sample()
                            ),
                        })

                # Log diagnostic JSON artifact
                import tempfile
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as f:
                    f.write(diagnostic_results.to_json())
                    temp_path = f.name
                logger.log_artifact(temp_path)
                run_id = logger.run_id

                # Mirror diagnostics + config snapshot to the fingerprint dir
                # so evaluation artifacts can be looked up by content hash.
                try:
                    import shutil

                    from mvp.common.config_hash import (
                        append_source,
                        compute_fingerprint,
                        fingerprint_dir,
                        write_config_snapshot,
                    )

                    fp = compute_fingerprint(
                        self.config, config_path=self.config_path
                    )
                    fp_dir = fingerprint_dir(fp)
                    fp_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(temp_path, fp_dir / "diagnostics.json")
                    write_config_snapshot(
                        self.config, fp, config_path=self.config_path
                    )
                    append_source(fp, self.config_path.stem, run_id)
                    if fold_predictions_df is not None:
                        fold_predictions_df.write_parquet(
                            fp_dir / "fold_predictions.parquet"
                        )
                except Exception:
                    run_logger.exception(
                        "Failed to write fingerprint artifacts; mlflow "
                        "diagnostics already logged"
                    )

        finally:
            if run_context:
                run_context.__exit__(None, None, None)

        run_logger.info("Run complete in %.1fs", time.perf_counter() - t_run)

        # MTL: aggregate per-fold aux head R² to fold-mean (only when MTL was
        # active). Empty dict if no aux R² captured.
        aux_r2_summary: dict[str, float] = {}
        if all_aux_r2:
            aux_names = list(all_aux_r2[0].keys())
            for name in aux_names:
                vals = [f.get(name, float("nan")) for f in all_aux_r2]
                aux_r2_summary[name] = float(np.nanmean(vals))

        return {
            "metrics": avg_metrics,
            "train_metrics": avg_train_metrics,
            "fold_metrics": all_metrics,
            "fold_meta": tuning_fold_meta,
            "fold_feature_importances": all_fold_importances,
            "n_folds": n_folds_total,
            "feature_columns": feature_cols,
            "run_id": run_id,
            "diagnostics": diagnostic_results,
            "calibrator": calibrator,
            "last_fold_model": model,
            "last_fold_X_test": X_test,
            "last_fold_y_test": y_test,
            "all_predictions": all_predictions,
            "per_model_oof": all_per_model_predictions if is_ensemble else [],
            "holdout_metrics": holdout_metrics,
            "holdout_fold_metrics": holdout_fold_metrics,
            "holdout_metrics_calibrated": holdout_metrics_calibrated,
            "holdout_fold_metrics_calibrated": holdout_fold_metrics_calibrated,
            "holdout_fold_meta": holdout_fold_meta if holdout_predictions else None,
            "tuning_fold_indices": tuning_fold_indices,
            "holdout_fold_indices": holdout_fold_indices,
            "inner_cv_folds": self.inner_cv_folds,
            "inner_fold_count_per_outer": (
                inner_fold_count_per_outer if self.inner_cv_folds > 0 else None
            ),
            "aux_r2_test": aux_r2_summary or None,
            "aux_r2_per_fold": all_aux_r2 or None,
        }
