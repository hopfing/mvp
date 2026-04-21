"""Experiment runner for the IID/Markov tennis projector.

Mirrors the shell of `src/mvp/projection/runner.py` (config → FeatureEngine →
date filter → splitter → fold loop → mlflow logging) but produces distributions
instead of point estimates and logs three metric families per fold:

    - classification (log_loss/brier/...) via mvp.model.metrics.compute_metrics
    - regression (mae/rmse/...) via mvp.projection.metrics.compute_regression_metrics
    - distributional (CRPS, line calibration) via mvp.projection.iid.metrics
"""

import logging
import time
import warnings
from pathlib import Path
from typing import Any

warnings.filterwarnings("ignore", message="All-NaN slice encountered")

import mlflow
import numpy as np
import polars as pl

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.engine import FeatureEngine, check_memory
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.splitters import make_splitter
from mvp.projection.iid.config import IIDProjectionConfig, ServeModelConfig
from mvp.projection.iid.diagnostics import IIDProjectionDiagnostics
from mvp.projection.iid.metrics import (
    compute_hold_diagnostics,
    compute_iid_metrics,
    compute_serve_diagnostics,
    compute_set_score_diagnostics,
    compute_tiebreak_diagnostics,
)
from mvp.projection.iid.projector import TennisProjector
from mvp.projection.iid.serve_model import (
    IdentityServeModel,
    MatchupServeModel,
    ScoreStateChainServeModel,
    ServeWinProbEstimator,
)

run_logger = logging.getLogger(__name__)


def _build_serve_model(cfg: ServeModelConfig) -> ServeWinProbEstimator:
    if cfg.type == "identity":
        return IdentityServeModel(
            window=cfg.window,
            clip_min=cfg.clip_min,
            clip_max=cfg.clip_max,
        )
    if cfg.type == "matchup":
        if not cfg.feature_columns:
            raise ValueError(
                "serve_model.feature_columns must be non-empty for type=matchup"
            )
        return MatchupServeModel(
            feature_columns=cfg.feature_columns,
            match_level_columns=cfg.match_level_columns,
            regressor_type=cfg.regressor.type,
            regressor_params=dict(cfg.regressor.params),
            clip_min=cfg.clip_min,
            clip_max=cfg.clip_max,
        )
    if cfg.type == "score_state":
        if not cfg.match_level_features and not cfg.point_level_features:
            raise ValueError(
                "serve_model.match_level_features and/or point_level_features "
                "must be non-empty for type=score_state"
            )
        return ScoreStateChainServeModel(
            model_type=cfg.model_type,
            match_level_features=cfg.match_level_features,
            point_level_features=cfg.point_level_features,
            params=dict(cfg.params),
            clip_min=cfg.clip_min,
            clip_max=cfg.clip_max,
        )
    raise ValueError(f"Unknown serve model type: {cfg.type}")


class IIDProjectionRunner:
    """Runner for executing IID projection experiments."""

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
        workflow: str = "iid_projection",
        run_name: str | None = None,
        log_to_mlflow: bool = True,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = IIDProjectionConfig.from_file(str(config_path))

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

    def _resolve_targets(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add per-row targets and filter invalid matches.

        Excludes walkovers/retirements/defaults/unplayed and rows missing
        first two set scores. Adds `_target_games_a` (the row's player) and
        `_target_games_b` (the row's opponent).
        """
        df = df.filter(
            pl.col("player_set1_games").is_not_null()
            & pl.col("player_set2_games").is_not_null()
        )
        if "reason" in df.columns:
            df = df.filter(
                pl.col("reason").fill_null("").is_in(["W/O", "RET", "DEF", "UNP"]).not_()
            )

        df = df.with_columns(
            total_games_won().cast(pl.Float64).alias("_target_games_a"),
            total_games_lost().cast(pl.Float64).alias("_target_games_b"),
        )
        df = df.filter(
            pl.col("_target_games_a").is_not_null()
            & pl.col("_target_games_b").is_not_null()
        )
        return df

    def _collapse_to_match_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """Collapse mirrored player rows to one row per `match_uid`.

        Picks the row whose `player_id` sorts first within each match. This
        deterministically orients the projection: the lower-id player becomes
        "A" in the resulting `MatchDistribution`.
        """
        return df.sort(["match_uid", "player_id"]).unique(
            subset=["match_uid"], keep="first", maintain_order=True,
        )

    def run(self) -> dict[str, Any]:
        """Execute the IID projection experiment."""
        if self.log_to_mlflow:
            if self.mlflow_dir:
                mlflow_uri = f"file:///{str(self.mlflow_dir).replace(chr(92), '/')}"
                mlflow.set_tracking_uri(mlflow_uri)
            logger = ExperimentLogger(experiment_name=self.workflow)
        else:
            logger = None

        feature_specs = self.config.features.include
        compute_only = self.config.features.compute_only or []
        filter_specs = get_filter_feature_specs(self.config.data.filters)
        extra = compute_only + filter_specs
        all_specs = feature_specs + [s for s in extra if s not in feature_specs]
        t_run = time.perf_counter()

        runner_columns = [
            "match_uid", "player_id", "won", "reason", "best_of",
            "circuit", "surface", "round",
            "player_set1_games", "player_set2_games",
            "player_set3_games", "player_set4_games", "player_set5_games",
            "opp_set1_games", "opp_set2_games",
            "opp_set3_games", "opp_set4_games", "opp_set5_games",
            # Raw per-match service stats — needed by MatchupServeModel's
            # training target (both perspectives). Player perspective is the
            # unprefixed parquet column; opp perspective has the opp_ prefix.
            "pts_service_pts_won", "pts_service_pts_played",
            "opp_pts_service_pts_won", "opp_pts_service_pts_played",
            # Service game stats — for hold rate diagnostics.
            "svc_games_played", "svc_bp_saved", "svc_bp_faced",
            "opp_svc_games_played", "opp_svc_bp_saved", "opp_svc_bp_faced",
            # Tiebreak scores — for tiebreak frequency diagnostics.
            "player_set1_tiebreak", "player_set2_tiebreak",
            "player_set3_tiebreak", "player_set4_tiebreak", "player_set5_tiebreak",
            "opp_set1_tiebreak", "opp_set2_tiebreak",
            "opp_set3_tiebreak", "opp_set4_tiebreak", "opp_set5_tiebreak",
        ]
        if self.config.data.filters:
            for col in self.config.data.filters:
                if col not in runner_columns:
                    runner_columns.append(col)

        df = self.engine.compute(all_specs, extra_columns=runner_columns)

        if self.config.data.filters:
            df = apply_filters(df, self.config.data.filters)

        df = self._resolve_targets(df)

        df = df.filter(
            (pl.col("effective_match_date") >= self.config.data.date_range.start)
            & (pl.col("effective_match_date") <= self.config.data.date_range.end)
        )

        df = df.filter(pl.col("best_of").is_in([3, 5]))

        df = self._collapse_to_match_rows(df)

        n_total = len(df)
        if n_total == 0:
            raise ValueError("No matches remain after filtering and target resolution")

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
            "IID projection on %d matches (after collapse), serve_model=%s",
            n_total, self.config.serve_model.type,
        )

        check_memory("before iid projection fold loop")
        all_metrics: list[dict[str, float]] = []
        all_predictions: list[dict[str, Any]] = []

        run_context = logger.start_run(run_name=self.run_name) if logger else None
        if run_context:
            run_context.__enter__()
            logger.log_params({
                "serve_model_type": self.config.serve_model.type,
                "serve_window": self.config.serve_model.window,
                "task": "iid_projection",
                "validation_type": self.config.validation.type,
                "n_splits": self.config.validation.n_splits,
                "date_range_start": str(self.config.data.date_range.start),
                "date_range_end": str(self.config.data.date_range.end),
                "n_matches": n_total,
            })
            logger.log_artifact(str(self.config_path))

        try:
            for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(df)):
                check_memory(f"iid projection fold {fold_idx + 1} start")
                t_fold = time.perf_counter()
                train_df = df[train_idx]
                test_df = df[test_idx]
                run_logger.info(
                    "Fold %d: train=%d, test=%d",
                    fold_idx + 1, len(train_df), len(test_df),
                )

                serve_model = _build_serve_model(self.config.serve_model)
                projector = TennisProjector(serve_model)
                projector.fit(train_df)
                out = projector.project(test_df)

                y_won = test_df["won"].to_numpy().astype(np.int64)
                y_games_a = test_df["_target_games_a"].to_numpy().astype(np.float64)
                y_games_b = test_df["_target_games_b"].to_numpy().astype(np.float64)

                metrics = compute_iid_metrics(
                    out,
                    y_won,
                    y_games_a,
                    y_games_b,
                    total_lines=self.config.metrics.total_lines,
                    spread_lines=self.config.metrics.spread_lines,
                    include_classification=self.config.metrics.include_classification,
                    include_regression=self.config.metrics.include_regression,
                )
                metrics.update(compute_serve_diagnostics(out, test_df))
                metrics.update(compute_hold_diagnostics(out, test_df))
                metrics.update(compute_set_score_diagnostics(out, test_df))
                metrics.update(compute_tiebreak_diagnostics(out, test_df))
                all_metrics.append(metrics)
                all_predictions.append({
                    "df": test_df.select(["match_uid", "circuit", "surface", "round", "best_of"]),
                    "out": out,
                    "y_won": y_won,
                    "y_games_a": y_games_a,
                    "y_games_b": y_games_b,
                })

                run_logger.info(
                    "Fold %d: log_loss=%.4f mae=%.3f crps_total=%.3f (%.1fs)",
                    fold_idx + 1,
                    metrics.get("log_loss", float("nan")),
                    metrics.get("mae", float("nan")),
                    metrics.get("iid_crps_total_games", float("nan")),
                    time.perf_counter() - t_fold,
                )

                if logger:
                    logger.log_metrics(
                        {f"fold_{fold_idx}_{k}": v for k, v in metrics.items()}
                    )

            if not all_metrics:
                raise ValueError(
                    f"Splitter produced 0 folds for {n_total} matches with "
                    f"initial_train_size={val.initial_train_size}, "
                    f"step_size={val.step_size}. Lower these in the config."
                )
            avg_metrics = {
                k: float(np.mean([m[k] for m in all_metrics]))
                for k in all_metrics[0].keys()
            }

            run_logger.info("Computing IID projection diagnostics...")
            diagnostics = IIDProjectionDiagnostics()
            diagnostic_results = diagnostics.compute_all(
                all_predictions,
                total_lines=self.config.metrics.total_lines,
                spread_lines=self.config.metrics.spread_lines,
            )
            avg_metrics.update(diagnostic_results.metrics)

            run_id = None
            if logger:
                logger.log_metrics(avg_metrics)
                run_id = logger.run_id

        finally:
            if run_context:
                run_context.__exit__(None, None, None)

        run_logger.info(
            "IID projection run complete in %.1fs", time.perf_counter() - t_run,
        )

        return {
            "metrics": avg_metrics,
            "fold_metrics": all_metrics,
            "n_folds": len(all_metrics),
            "n_matches": n_total,
            "run_id": run_id,
            "diagnostics": diagnostic_results,
            "_config": self.config,
        }
