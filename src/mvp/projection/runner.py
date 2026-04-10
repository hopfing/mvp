"""Projection runner for training and evaluating regression models."""

import logging
import time
import warnings
from pathlib import Path

warnings.filterwarnings("ignore", message="All-NaN slice encountered")
from typing import Any

import numpy as np
import polars as pl

from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.engine import FeatureEngine, check_memory, get_feature_columns
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.model.imputation import apply_imputation, build_imputation, fit_imputation
from mvp.model.registry import get_registry
from mvp.model.splitters import make_splitter
from mvp.projection.config import ProjectionConfig
from mvp.projection.diagnostics import ProjectionDiagnostics
from mvp.projection.metrics import compute_regression_metrics
from mvp.projection.models import get_regression_model

run_logger = logging.getLogger(__name__)


class ProjectionRunner:
    """Runner for executing game projection experiments."""

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
        workflow: str = "projection",
        run_name: str | None = None,
        log_to_mlflow: bool = True,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = ProjectionConfig.from_file(str(config_path))
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
        """Add target column and filter invalid matches.

        Excludes walkovers, retirements, defaults, unplayed, and rows
        missing set score data. For match_games target, collapses to one
        row per match after filtering.
        """
        target_mode = self.config.model.target

        # Require at least set 1 and set 2 scores
        df = df.filter(
            pl.col("player_set1_games").is_not_null()
            & pl.col("player_set2_games").is_not_null()
        )

        # Exclude incomplete matches
        if "reason" in df.columns:
            df = df.filter(
                pl.col("reason").fill_null("").is_in(["W/O", "RET", "DEF", "UNP"]).not_()
            )

        if target_mode == "match_games":
            target_col = "_target_match_games"
            df = df.with_columns(
                (total_games_won() + total_games_lost())
                .cast(pl.Float64)
                .alias(target_col)
            )
            # Collapse to one row per match
            df = df.sort(["match_uid", "player_id"]).unique(
                subset=["match_uid"], keep="first", maintain_order=True,
            )
        else:
            target_col = "_target_total_games"
            df = df.with_columns(
                total_games_won().cast(pl.Float64).alias(target_col)
            )

        df = df.filter(pl.col(target_col).is_not_null())
        return df, target_col

    def run(self) -> dict[str, Any]:
        """Execute the projection experiment.

        Returns:
            Dictionary with metrics and metadata.
        """
        import mlflow

        from mvp.model.mlflow_logger import ExperimentLogger

        if self.log_to_mlflow:
            if self.mlflow_dir:
                mlflow_uri = f"file:///{str(self.mlflow_dir).replace(chr(92), '/')}"
                mlflow.set_tracking_uri(mlflow_uri)
            logger = ExperimentLogger(experiment_name=self.workflow)
        else:
            logger = None

        feature_specs = self.config.features.include

        # Include compute_only and filter-referenced features
        compute_only = self.config.features.compute_only or []
        filter_specs = get_filter_feature_specs(self.config.data.filters)
        extra = compute_only + filter_specs
        all_specs = feature_specs + [s for s in extra if s not in feature_specs]
        t_run = time.perf_counter()

        # Columns needed for target resolution, diagnostics, and filtering
        runner_columns = [
            "won", "reason", "sets_played", "best_of",
            "circuit", "surface", "round", "match_uid", "player_id",
            "player_set1_games", "player_set2_games",
            "player_set3_games", "player_set4_games", "player_set5_games",
            "opp_set1_games", "opp_set2_games",
            "opp_set3_games", "opp_set4_games", "opp_set5_games",
        ]
        if self.config.data.filters:
            for col in self.config.data.filters:
                if col not in runner_columns:
                    runner_columns.append(col)

        df = self.engine.compute(all_specs, extra_columns=runner_columns)

        # Apply filters
        if self.config.data.filters:
            df = apply_filters(df, self.config.data.filters)

        # Resolve target (adds column, filters incomplete matches)
        df, target_col = self._resolve_target(df)

        # Filter by date range
        df = df.filter(
            (pl.col("effective_match_date") >= self.config.data.date_range.start)
            & (pl.col("effective_match_date") <= self.config.data.date_range.end)
        )

        # Get feature columns
        feature_cols = get_feature_columns(feature_specs)
        if not feature_cols:
            raise ValueError("No feature columns found after computing features")

        # Build imputation specs
        build_result = build_imputation(feature_specs, get_registry())
        augmented_cols = feature_cols + build_result.aux_base_col_names
        n_model = build_result.n_model_features

        # Get splitter
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
        run_logger.info(
            "Training %s projection with %d features on %d rows",
            self.config.model.type, len(feature_cols), len(df),
        )

        # Train and evaluate
        check_memory("before projection training loop")
        all_metrics: list[dict[str, float]] = []
        all_train_metrics: list[dict[str, float]] = []
        all_predictions: list[dict[str, Any]] = []

        run_context = logger.start_run(run_name=self.run_name) if logger else None
        if run_context:
            run_context.__enter__()
            logger.log_params({
                "model_type": self.config.model.type,
                "task": "projection",
                "validation_type": self.config.validation.type,
                "n_features": len(feature_cols),
                "n_splits": self.config.validation.n_splits,
                "date_range_start": str(self.config.data.date_range.start),
                "date_range_end": str(self.config.data.date_range.end),
                "n_rows": len(df),
            })
            if self.config.model.params:
                for k, v in self.config.model.params.items():
                    logger.log_params({f"model_{k}": v})
            logger.log_artifact(str(self.config_path))

        try:
            for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(df)):
                check_memory(f"projection fold {fold_idx + 1} start")
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
                y_train = train_df[target_col].to_numpy().astype(float)
                X_test = test_df.select(
                    pl.col(c).cast(pl.Float64) for c in augmented_cols
                ).to_numpy()
                y_test = test_df[target_col].to_numpy().astype(float)

                # Impute
                circuit_train = train_df["circuit"].to_numpy()
                circuit_test = test_df["circuit"].to_numpy()
                impute_state = fit_imputation(X_train, circuit_train, build_result.specs)

                # Scale
                import warnings as _w
                with _w.catch_warnings():
                    _w.simplefilter("ignore", RuntimeWarning)
                    train_mean = np.nanmean(X_train[:, :n_model], axis=0)
                    train_std = np.nanstd(X_train[:, :n_model], axis=0)
                train_mean = np.where(np.isnan(train_mean), 0.0, train_mean)
                train_std = np.where(np.isnan(train_std), 1.0, train_std)
                train_std[train_std == 0] = 1.0

                X_train = apply_imputation(X_train, circuit_train, impute_state)
                X_test = apply_imputation(X_test, circuit_test, impute_state)
                X_train = X_train[:, :n_model]
                X_test = X_test[:, :n_model]
                X_train = (X_train - train_mean) / train_std
                X_test = (X_test - train_mean) / train_std

                # Train
                model = get_regression_model(
                    self.config.model.type,
                    self.config.model.params or {},
                )
                model.fit(X_train, y_train)

                # Predict
                y_pred_raw = model.predict(X_test)
                y_pred_train_raw = model.predict(X_train)

                # Multi-quantile: predict() returns (n, n_quantiles).
                # Use median (middle column) for standard metrics.
                if y_pred_raw.ndim == 2:
                    mid = y_pred_raw.shape[1] // 2
                    y_pred = y_pred_raw[:, mid]
                    y_pred_train = y_pred_train_raw[:, mid]
                else:
                    y_pred = y_pred_raw
                    y_pred_train = y_pred_train_raw

                metrics = compute_regression_metrics(y_test, y_pred)
                all_metrics.append(metrics)

                train_metrics = compute_regression_metrics(y_train, y_pred_train)
                all_train_metrics.append(train_metrics)

                pred_entry: dict[str, Any] = {
                    "df": test_df,
                    "y_true": y_test,
                    "y_pred": y_pred,
                }
                if y_pred_raw.ndim == 2:
                    pred_entry["y_pred_quantiles"] = y_pred_raw
                all_predictions.append(pred_entry)

                run_logger.info(
                    "Fold %d: mae=%.3f, rmse=%.3f, r2=%.3f (%.1fs)",
                    fold_idx + 1, metrics["mae"], metrics["rmse"],
                    metrics["r_squared"], time.perf_counter() - t_fold,
                )

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

            # Compute diagnostics
            run_logger.info("Computing projection diagnostics...")
            diagnostics = ProjectionDiagnostics()
            diagnostic_results = diagnostics.compute_all(all_predictions)

            avg_metrics.update(diagnostic_results.metrics)

            # Quantile calibration: check coverage for each quantile
            if all_predictions and "y_pred_quantiles" in all_predictions[0]:
                quantile_alphas = (self.config.model.params or {}).get(
                    "quantile_alpha", []
                )
                if isinstance(quantile_alphas, list) and len(quantile_alphas) > 0:
                    all_y_true = np.concatenate(
                        [p["y_true"] for p in all_predictions]
                    )
                    all_q_preds = np.vstack(
                        [p["y_pred_quantiles"] for p in all_predictions]
                    )
                    run_logger.info("Quantile calibration:")
                    for i, alpha in enumerate(quantile_alphas):
                        coverage = float(
                            np.mean(all_y_true <= all_q_preds[:, i])
                        )
                        avg_metrics[f"quantile_{alpha}_coverage"] = coverage
                        run_logger.info(
                            "  q%.2f: target=%.0f%%, actual=%.1f%%",
                            alpha, alpha * 100, coverage * 100,
                        )

            run_id = None
            if logger:
                logger.log_metrics(avg_metrics)
                logger.log_metrics({f"train_{k}": v for k, v in avg_train_metrics.items()})
                run_id = logger.run_id

        finally:
            if run_context:
                run_context.__exit__(None, None, None)

        run_logger.info("Projection run complete in %.1fs", time.perf_counter() - t_run)

        return {
            "metrics": avg_metrics,
            "train_metrics": avg_train_metrics,
            "fold_metrics": all_metrics,
            "n_folds": len(all_metrics),
            "feature_columns": feature_cols,
            "run_id": run_id,
            "diagnostics": diagnostic_results,
            "all_predictions": all_predictions,
            "_config": self.config,
        }
