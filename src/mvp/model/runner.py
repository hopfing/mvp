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

run_logger = logging.getLogger(__name__)

from mvp.model.calibration import PlattCalibrator
from mvp.model.config import (
    DateRange,
    EnsembleParams,
    ExperimentConfig,
    apply_filters,
    get_filter_feature_specs,
)
from mvp.model.diagnostics import Diagnostics, EnsembleDiagnostics
from mvp.model.engine import FeatureEngine, check_memory, get_feature_columns
from mvp.model.imputation import apply_imputation, build_imputation, fit_imputation
from mvp.model.metrics import compute_metrics
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.models import EnsembleModel, get_model
from mvp.model.registry import get_registry
from mvp.model.splitters import BaseSplitter, make_splitter
from mvp.model.weighting import compute_sample_weights


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
        """
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

        self.engine = FeatureEngine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )

    def _resolve_target(self, df: pl.DataFrame) -> tuple[pl.DataFrame, str]:
        """Add the target column to df and return (df, target_col_name).

        Walkovers are always excluded — they're voided bets with no on-court signal.

        For 'won': uses existing column as-is, excludes walkovers.
        For 'deciding_set': derives target from sets_played == best_of,
            excludes incomplete matches where outcome is uncertain.
        """
        target = self.config.target
        # Walkovers are voided bets — never valid training data for any target
        if "reason" in df.columns:
            df = df.filter(pl.col("reason").fill_null("").ne("W/O"))
        if target == "won":
            return df, "won"
        if target == "deciding_set":
            target_col = "_target_deciding_set"
            df = df.filter(pl.col("sets_played").is_not_null())
            # Retirements where sets_played == best_of are settled:
            # the deciding set started, so over is graded a winner.
            # Retirements before that point are voided — outcome uncertain.
            # DEF/UNP are always excluded (no meaningful play).
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
            return df, target_col
        raise ValueError(f"Unknown target: {target}")

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

        all_feature_specs: list[str] = []
        base_model_specs: list[dict[str, Any]] = []
        model_date_ranges: list[DateRange] = []
        model_filters: list[dict[str, Any] | None] = []
        model_sample_weights: list[SampleWeightConfig | None] = []

        for ref in ensemble_params.base_models:
            base_config = ExperimentConfig.from_file(ref.config)
            if base_config.features is None:
                raise ValueError(f"Base model {ref.config} has no features section")
            for spec in base_config.features.include:
                if spec not in all_feature_specs:
                    all_feature_specs.append(spec)
            base_model_specs.append({
                "type": base_config.model.type,
                "params": base_config.model.params or {},
                "weight": ref.weight,
                "feature_specs": base_config.features.include,
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

    def run(self) -> dict[str, Any]:
        """Execute the experiment.

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
        # - raw filter columns (draw_type, etc.) that aren't computed features
        runner_columns = [
            "won", "reason", "sets_played", "best_of",
            "circuit", "surface", "round",
        ]
        if self.config.data.filters:
            for col in self.config.data.filters:
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

        # Resolve target column (adds derived column if needed, filters incomplete matches)
        df, target_col = self._resolve_target(df)

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

        # Embedding configuration
        embedding_col = None
        min_player_matches = 10
        if self.config.model.params:
            embedding_col = self.config.model.params.get("embedding_col")
            min_player_matches = self.config.model.params.get(
                "min_player_matches", 10
            )

        # Get splitter
        splitter = self._get_splitter()
        run_logger.info(
            "Training %s model with %d features on %d rows",
            self.config.model.type, len(feature_cols), len(df),
        )

        # Train and evaluate
        check_memory("before training loop")
        all_metrics: list[dict[str, float]] = []
        all_train_metrics: list[dict[str, float]] = []
        all_predictions: list[dict[str, Any]] = []
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

        try:
            for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(df)):
                check_memory(f"fold {fold_idx + 1} start")
                t_fold = time.perf_counter()
                train_df = df[train_idx]
                test_df = df[test_idx]
                run_logger.info(
                    "Fold %d: train=%d, test=%d",
                    fold_idx + 1, len(train_df), len(test_df),
                )

                X_train = train_df.select(
                    pl.col(c).cast(pl.Float64) for c in augmented_cols
                ).to_numpy()
                y_train = train_df[target_col].to_numpy().astype(int)
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
                if embedding_col and embedding_col in train_df.columns:
                    player_counts = train_df[embedding_col].value_counts()
                    eligible = player_counts.filter(
                        pl.col("count") >= min_player_matches
                    )
                    vocab = {
                        pid: idx + 1
                        for idx, pid in enumerate(
                            eligible[embedding_col].to_list()
                        )
                    }
                    emb_train = np.array(
                        [vocab.get(p, 0) for p in train_df[embedding_col].to_list()]
                    ).reshape(-1, 1)
                    emb_test = np.array(
                        [vocab.get(p, 0) for p in test_df[embedding_col].to_list()]
                    ).reshape(-1, 1)
                    X_train = np.hstack([X_train, emb_train.astype(np.float64)])
                    X_test = np.hstack([X_test, emb_test.astype(np.float64)])
                    self.config.model.params["embedding_col_idx"] = X_train.shape[1] - 1
                    self.config.model.params["n_players"] = len(vocab)

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

                # Train model
                model = get_model(
                    self.config.model.type,
                    self.config.model.params or {},
                )
                if is_ensemble and base_model_specs is not None:
                    assert isinstance(model, EnsembleModel)
                    model.configure(base_model_specs)
                    model.fit(
                        X_train, y_train,
                        sample_weight=train_weights,
                        per_model_data=per_model_data,
                    )
                else:
                    model.fit(X_train, y_train, sample_weight=train_weights)

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
                metrics = compute_metrics(y_test, y_prob)
                all_metrics.append(metrics)

                # Predict and evaluate on train (for overfitting detection)
                train_metrics = compute_metrics(y_train, y_prob_train)
                all_train_metrics.append(train_metrics)

                # Collect predictions for diagnostics
                all_predictions.append({
                    "df": test_df,
                    "y_true": y_test,
                    "y_prob": y_prob,
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

            # Average metrics across folds
            avg_metrics = {
                k: float(np.mean([m[k] for m in all_metrics]))
                for k in all_metrics[0].keys()
            }
            avg_train_metrics = {
                k: float(np.mean([m[k] for m in all_train_metrics]))
                for k in all_train_metrics[0].keys()
            }

            # Fit stacking meta-model on concatenated OOF predictions
            if is_ensemble and self.config.model.params.get("strategy") == "stacking":
                assert isinstance(model, EnsembleModel)
                n_base = len(all_per_model_predictions[0])

                X_meta = np.column_stack([
                    np.concatenate([fold[i] for fold in all_per_model_predictions])
                    for i in range(n_base)
                ])
                y_meta = np.concatenate([p["y_true"] for p in all_predictions])

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

                    medians_meta = np.nanmedian(X_meta_raw, axis=0)
                    medians_meta = np.where(np.isnan(medians_meta), 0.0, medians_meta)
                    X_meta_raw = np.where(np.isnan(X_meta_raw), medians_meta, X_meta_raw)

                    meta_mean = X_meta_raw.mean(axis=0)
                    meta_std = X_meta_raw.std(axis=0)
                    meta_std[meta_std == 0] = 1.0
                    X_meta_std = (X_meta_raw - meta_mean) / meta_std

                    X_meta = np.hstack([X_meta, X_meta_std])
                    model._meta_scaler = (meta_mean, meta_std)

                model.set_meta_feature_indices(meta_feature_indices)
                model.set_meta_feature_names(base_names + meta_feature_col_names)
                model.fit_meta(X_meta, y_meta)

                y_prob_stacked = model._meta_model.predict_proba(X_meta)[:, 1]
                avg_metrics = compute_metrics(y_meta, y_prob_stacked)

                offset = 0
                for pred_dict in all_predictions:
                    n = len(pred_dict["y_true"])
                    pred_dict["y_prob"] = y_prob_stacked[offset:offset + n]
                    offset += n

            # Platt scaling calibration on concatenated OOF predictions
            combined_y_true_oof = np.concatenate(
                [p["y_true"] for p in all_predictions]
            )
            combined_y_prob_oof = np.concatenate(
                [p["y_prob"] for p in all_predictions]
            )
            raw_metrics = compute_metrics(combined_y_true_oof, combined_y_prob_oof)

            calibrator = PlattCalibrator()
            calibrator.fit(combined_y_prob_oof, combined_y_true_oof)

            # Apply calibration to each fold's predictions
            for pred_dict in all_predictions:
                pred_dict["y_prob"] = calibrator.transform(pred_dict["y_prob"])

            # Recompute avg_metrics on calibrated predictions
            calibrated_y_prob = np.concatenate(
                [p["y_prob"] for p in all_predictions]
            )
            avg_metrics = compute_metrics(combined_y_true_oof, calibrated_y_prob)

            # Merge raw (pre-calibration) metrics with raw_ prefix
            for k, v in raw_metrics.items():
                avg_metrics[f"raw_{k}"] = v

            # Compute diagnostics
            run_logger.info("Computing diagnostics...")
            diagnostics = Diagnostics()
            diagnostic_results = diagnostics.compute_all(all_predictions)

            # Compute ensemble-specific diagnostics
            ensemble_diagnostic_results = None
            if is_ensemble and all_per_model_predictions:
                n_base = len(all_per_model_predictions[0])
                per_model_preds = [
                    np.concatenate([fold[i] for fold in all_per_model_predictions])
                    for i in range(n_base)
                ]
                combined_y_true = np.concatenate(
                    [p["y_true"] for p in all_predictions]
                )
                combined_y_prob = np.concatenate(
                    [p["y_prob"] for p in all_predictions]
                )
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

                combined_df = pl.concat([p["df"] for p in all_predictions])
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
                if calibrator.is_fitted:
                    logger.log_params({
                        "platt_slope": f"{calibrator.slope:.6f}",
                        "platt_intercept": f"{calibrator.intercept:.6f}",
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

        finally:
            if run_context:
                run_context.__exit__(None, None, None)

        run_logger.info("Run complete in %.1fs", time.perf_counter() - t_run)

        return {
            "metrics": avg_metrics,
            "train_metrics": avg_train_metrics,
            "fold_metrics": all_metrics,
            "n_folds": len(all_metrics),
            "feature_columns": feature_cols,
            "run_id": run_id,
            "diagnostics": diagnostic_results,
            "calibrator": calibrator,
            "last_fold_model": model,
            "last_fold_X_test": X_test,
            "last_fold_y_test": y_test,
            "all_predictions": all_predictions,
            "per_model_oof": all_per_model_predictions if is_ensemble else [],
        }
