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
from mvp.model.engine import check_memory, make_fs_engine
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.splitters import BaseSplitter, make_splitter
from mvp.projection.iid.artifacts import (
    record_run,
    shape_scalars,
    write_projection_json,
)
from mvp.projection.iid.config import IIDProjectionConfig
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
    ScoreStateChainServeModel,
    build_serve_model,
)

run_logger = logging.getLogger(__name__)


def preload_match_specs(serve_model_config) -> list[str]:
    """Match-level specs to materialize once for the whole run.

    For `two_level` this is the deduped UNION of the three components', not
    `match_level_features` — that field is the single-level one and is empty on
    a two-level config, so reading it would preload nothing and silently leave
    every branch recomputing per fold.

    Deliberately WITHOUT swap-side partners. `engine.compute` returns two
    mirrored rows per match, one per player perspective, and fit joins at
    (match_uid, player_id -> server_id), so the returner's values arrive in the
    ROW rather than in an `opp_` column. Partners are a predict-time need, met
    separately by the config's `features.include`.

    Order-preserving so the preloaded frame's column order is stable across runs
    of the same config.
    """
    if serve_model_config.type == "two_level":
        specs = (
            list(serve_model_config.first_in_match_features)
            + list(serve_model_config.win_first_match_features)
            + list(serve_model_config.win_second_match_features)
        )
    else:
        specs = list(serve_model_config.match_level_features)
    seen: set[str] = set()
    return [s for s in specs if not (s in seen or seen.add(s))]


def build_fold_match_frame(
    test_df: pl.DataFrame, out: Any, fold_idx: int, y_won: np.ndarray
) -> pl.DataFrame:
    """One fold's rows for the fold_match_win artifact.

    Same alignment guard `build_pmf_frame` carries: the distribution indexes
    by row order, and a silent reorder here would hand every match another
    match's probability in the OOF store models train against. `won_a` (the
    A row's outcome, the same array the fold metrics score against) rides
    along so a consumer can calibrate the raw chain probability without
    re-deriving outcomes.
    """
    if not (test_df["match_uid"].to_numpy() == out.match_uid).all():
        raise ValueError(
            "fold_match_win: test_df and ProjectionOutput are not row-aligned"
        )
    return pl.DataFrame({
        "match_uid": test_df["match_uid"],
        "player_id": test_df["player_id"],
        "opp_id": test_df["opp_id"],
        "effective_match_date": test_df["effective_match_date"],
        "fold_idx": pl.Series([fold_idx] * len(test_df), dtype=pl.Int32),
        "p_match_win_a": out.distribution.p_match_win_a,
        "won_a": pl.Series(np.asarray(y_won).astype(np.int8)),
        # Shape scalars ride the same aligned `out` — the winner-side
        # chain_shape transform consumes them the way the prior consumes
        # p_match_win_a.
        **shape_scalars(out),
    })


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
        source: str | None = None,
        persist: bool = True,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = IIDProjectionConfig.from_file(str(config_path))
        # Groups this run under a parent config in the fingerprint dir's
        # source.txt — a sweep passes its parent stem so iid-rank can show the
        # hyperparameter variants of one config together.
        self.source = source
        # Write evaluation artifacts to the fingerprint dir. The tuner sets this
        # False: it runs this runner once per trial against a temp config, so
        # persisting would leave one junk dir per trial — all labelled
        # `tune_<stem>` with a `tmpXXXX` source — carrying nothing the study DB
        # doesn't already hold (the tuner runs no backtest).
        self.persist = persist

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

        self.engine = make_fs_engine(
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

    def _make_splitter(self) -> BaseSplitter:
        """Build the fold splitter from `config.validation`.

        Every validation mode's parameters are forwarded, including the
        calendar-month ones (`date_expanding` / `date_sliding`) — a serve-FS
        config promoted by `experiment ... --output` inherits its match-grain
        validation block verbatim, so a date-based mode arrives here intact.
        """
        val = self.config.validation
        return make_splitter(
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

        splitter = self._make_splitter()
        run_logger.info(
            "IID projection on %d matches (after collapse), serve_model=%s",
            n_total, self.config.serve_model.type,
        )

        check_memory("before iid projection fold loop")
        all_metrics: list[dict[str, float]] = []
        all_predictions: list[dict[str, Any]] = []
        # Per-fold match-win rows for the fold_match_win artifact: six skinny
        # columns per test row, accumulated here and written once after the
        # loop (the winner-side prior's OOF store).
        fold_match_rows: list[pl.DataFrame] = []

        # Materialize points and match-level features once and reuse across
        # folds. This avoids re-reading match_beats_points.parquet on every fold
        # (in fit) and again per fold for score_test_points; the engine.compute
        # call is also collapsed to a single call.
        #
        # TWO_LEVEL IS INCLUDED. It was not, and the omission was invisible: a
        # two-level model is three branch fits, so it paid the per-fold reload
        # THREE times per fold rather than once, and `TwoLevelServeModel.fit`
        # forwards both preloads to every branch already. Under `mvp tune`, which
        # builds a fresh runner per trial, that repeats for every trial.
        #
        # The spec list is the union of the three components', NOT the
        # single-level `match_level_features` (which is inert and empty on a
        # two-level config, so gating alone would have preloaded nothing).
        # Deliberately WITHOUT swap-side partners: `engine.compute` returns two
        # mirrored rows per match, one per player perspective, and fit joins at
        # (match_uid, player_id -> server_id), so the returner's values arrive in
        # the ROW. Partners are a predict-time need, met separately by
        # `features.include` (config.py's two-level block builder). Requesting
        # them here would only trigger mirror self-joins nothing at fit time
        # selects.
        sm = self.config.serve_model
        preloaded_points_full: pl.DataFrame | None = None
        preloaded_match_features: pl.DataFrame | None = None
        if sm.type in ("score_state", "two_level"):
            points_path = (
                get_data_root() / "aggregate" / "atptour"
                / "match_beats_points.parquet"
            )
            run_logger.info("Preloading points parquet from %s", points_path)
            preloaded_points_full = pl.read_parquet(points_path)
            mlf = preload_match_specs(sm)
            if mlf:
                run_logger.info(
                    "Preloading %d match-level features for %s",
                    len(mlf), sm.type,
                )
                preloaded_match_features = self.engine.compute(
                    feature_specs=mlf,
                    extra_columns=["player_id", "opp_id", "match_uid"],
                )

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

                serve_model = build_serve_model(
                    self.config.serve_model, engine=self.engine,
                )
                projector = TennisProjector(serve_model)

                fold_train_points: pl.DataFrame | None = None
                fold_test_points: pl.DataFrame | None = None
                if preloaded_points_full is not None:
                    train_uids = train_df["match_uid"].unique().to_list()
                    test_uids = test_df["match_uid"].unique().to_list()
                    fold_train_points = preloaded_points_full.filter(
                        pl.col("match_uid").is_in(train_uids)
                    )
                    fold_test_points = preloaded_points_full.filter(
                        pl.col("match_uid").is_in(test_uids)
                    )
                    serve_model.fit(
                        train_df,
                        preloaded_points=fold_train_points,
                        preloaded_match_features=preloaded_match_features,
                    )
                else:
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
                metrics.update(compute_serve_diagnostics(
                    out, test_df,
                    clip_min=self.config.serve_model.clip_min,
                    clip_max=self.config.serve_model.clip_max,
                ))
                metrics.update(compute_hold_diagnostics(out, test_df))
                metrics.update(compute_set_score_diagnostics(out, test_df))
                metrics.update(compute_tiebreak_diagnostics(out, test_df))
                if isinstance(serve_model, ScoreStateChainServeModel):
                    metrics.update(serve_model.score_test_points(
                        test_df,
                        preloaded_points=fold_test_points,
                        preloaded_match_features=preloaded_match_features,
                    ))
                all_metrics.append(metrics)
                pred_cols = [
                    "match_uid", "circuit", "surface", "round", "best_of",
                    "pts_service_pts_won", "pts_service_pts_played",
                    "opp_pts_service_pts_won", "opp_pts_service_pts_played",
                    "svc_games_played", "svc_bp_saved", "svc_bp_faced",
                    "opp_svc_games_played", "opp_svc_bp_saved", "opp_svc_bp_faced",
                    "player_set1_games", "player_set2_games",
                    "player_set3_games", "player_set4_games", "player_set5_games",
                    "opp_set1_games", "opp_set2_games",
                    "opp_set3_games", "opp_set4_games", "opp_set5_games",
                    "player_set1_tiebreak", "player_set2_tiebreak",
                    "player_set3_tiebreak", "player_set4_tiebreak", "player_set5_tiebreak",
                    "opp_set1_tiebreak", "opp_set2_tiebreak",
                    "opp_set3_tiebreak", "opp_set4_tiebreak", "opp_set5_tiebreak",
                ]
                all_predictions.append({
                    "df": test_df.select(
                        [c for c in pred_cols if c in test_df.columns]
                    ),
                    "out": out,
                    "y_won": y_won,
                    "y_games_a": y_games_a,
                    "y_games_b": y_games_b,
                })
                fold_match_rows.append(
                    build_fold_match_frame(test_df, out, fold_idx + 1, y_won)
                )

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
                clip_min=self.config.serve_model.clip_min,
                clip_max=self.config.serve_model.clip_max,
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

        result = {
            "metrics": avg_metrics,
            "fold_metrics": all_metrics,
            "n_folds": len(all_metrics),
            "n_matches": n_total,
            "run_id": run_id,
            "diagnostics": diagnostic_results,
            "_config": self.config,
        }

        # Persist to the config's fingerprint dir so runs are comparable after
        # the fact. Previously the only durable output was MLflow and everything
        # else was stdout, so comparing two configs meant re-running both and
        # reading the scrollback.
        if self.persist:
            fp_dir = record_run(
                self.config, self.config_path,
                source=self.source, run_id=self.run_name,
            )
            write_projection_json(fp_dir, result)
            if fold_match_rows:
                from mvp.projection.iid.artifacts import write_fold_match_win

                write_fold_match_win(fp_dir, pl.concat(fold_match_rows))
            run_logger.info("Wrote projection metrics -> %s", fp_dir)

        return result
