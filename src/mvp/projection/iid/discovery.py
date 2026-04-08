"""Forward-selection discovery for the IID matchup serve model.

Mirrors the shape of `src/mvp/projection/discovery.py` (which wraps the
per-player regression) and `src/mvp/projection/fast_selection.py` (which
precomputes a wide feature matrix once and slices per candidate). The IID
variant adds two pieces of inlined logic:

1. The matchup serve model's two-perspective fit (stack player + opp rows
   targeting the actual per-row serve win pct from raw `pts_service_pts_*`).
2. The chain step: convert predicted serve pcts to hold/tiebreak probs and
   call `match_distribution` to derive `expected_games_a`, then MAE.

Per-candidate work is pure numpy. The wide matrix and folds are computed
once via `FastIIDDiscoverySelector.precompute()`.
"""

import logging
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import mlflow
import numpy as np
import polars as pl

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.discovery.discover import get_all_feature_specs
from mvp.model.discovery.selection import FeatureSelector, SelectionResult
from mvp.model.engine import (
    FeatureEngine,
    build_column_name,
    check_memory,
    parse_feature_spec,
)
from mvp.model.features._score_helpers import total_games_lost, total_games_won
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.splitters import make_splitter
from mvp.projection.iid.chain import (
    match_distribution,
    p_service_game_win,
    p_tiebreak_game_win,
)
from mvp.projection.iid.config import IIDDiscoveryConfig
from mvp.projection.models import get_regression_model

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message="All-NaN slice encountered")
warnings.filterwarnings("ignore", message="invalid value encountered", category=RuntimeWarning)


# League-mean fallback for missing serve win pct values, mirrors serve_model.py.
LEAGUE_MEAN_SERVE_PROB: float = 0.62
SERVE_PROB_MIN: float = 0.30
SERVE_PROB_MAX: float = 0.90


def _spec_to_column(spec: str) -> str:
    """Resolve a feature spec to its concrete column name."""
    _prefix, _base, full_name, params = parse_feature_spec(spec)
    return build_column_name(full_name, params)


def _swap_perspective(col: str) -> str:
    """Swap player_↔opp_ prefix on a column name. Returns col unchanged if no prefix."""
    if col.startswith("player_"):
        return "opp_" + col[len("player_"):]
    if col.startswith("opp_"):
        return "player_" + col[len("opp_"):]
    return col


@dataclass
class IIDProjectionDiscoveryResult:
    """Result of one IID forward-selection run."""

    selected_features: list[str]
    selection_result: SelectionResult | None
    final_metric: float
    n_experiments: int


class FastIIDDiscoverySelector:
    """Precomputes the wide feature matrix and target arrays for fast IID FS.

    `precompute()` is the one-time expensive work: cache features, load the
    collapsed (one-row-per-match) DataFrame, extract everything to numpy.
    `create_scorer()` returns a closure that scores a candidate feature set
    using only numpy slices, ridge fits, and chain calls — no polars, no
    MLflow, no I/O.
    """

    def __init__(
        self,
        config: IIDDiscoveryConfig,
        all_feature_specs: list[str],
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.config = config
        self.all_feature_specs = list(all_feature_specs)
        self.matches_path = Path(matches_path) if matches_path else (
            get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        )
        self.cache_dir = Path(cache_dir) if cache_dir else (
            get_local_data_root() / "features" / "cache"
        )

        # Populated by precompute()
        self.X_wide: np.ndarray | None = None
        self.col_to_idx: dict[str, int] = {}
        self.y_games_a: np.ndarray | None = None
        self.y_games_b: np.ndarray | None = None
        self.y_won: np.ndarray | None = None
        self.best_of: np.ndarray | None = None
        self.actual_serve_rate_a: np.ndarray | None = None
        self.actual_serve_rate_b: np.ndarray | None = None
        self.folds: list[tuple[np.ndarray, np.ndarray]] = []

    def precompute(self) -> None:
        """Run the one-time expensive work: cache, load, collapse, extract."""
        engine = FeatureEngine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )

        filter_specs = get_filter_feature_specs(self.config.data.filters)
        all_specs = self.all_feature_specs + [
            s for s in filter_specs if s not in self.all_feature_specs
        ]

        extra_columns = [
            "match_uid", "player_id", "opp_id", "won", "reason", "best_of",
            "circuit", "surface", "round",
            "player_set1_games", "player_set2_games",
            "player_set3_games", "player_set4_games", "player_set5_games",
            "opp_set1_games", "opp_set2_games",
            "opp_set3_games", "opp_set4_games", "opp_set5_games",
            "pts_service_pts_won", "pts_service_pts_played",
            "opp_pts_service_pts_won", "opp_pts_service_pts_played",
        ]
        if self.config.data.filters:
            for col in self.config.data.filters:
                if col not in extra_columns:
                    extra_columns.append(col)

        t0 = time.perf_counter()
        df = engine.compute(all_specs, extra_columns=extra_columns)
        logger.info("Phase A: feature engine compute in %.1fs", time.perf_counter() - t0)

        if self.config.data.filters:
            df = apply_filters(df, self.config.data.filters)

        # Resolve targets and filter incomplete matches (mirrors IIDProjectionRunner)
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
        df = df.filter(
            (pl.col("effective_match_date") >= self.config.data.date_range.start)
            & (pl.col("effective_match_date") <= self.config.data.date_range.end)
        )
        df = df.filter(pl.col("best_of").is_in([3, 5]))

        # Collapse mirrored rows: one row per match, lower player_id wins the "A" slot
        df = df.sort(["match_uid", "player_id"]).unique(
            subset=["match_uid"], keep="first", maintain_order=True,
        )

        n_matches = len(df)
        if n_matches == 0:
            raise ValueError("No matches remain after filtering")
        logger.info("Phase B: collapsed to %d matches", n_matches)
        check_memory("iid discovery: after collapse")

        # Build the wide matrix from the candidate column names. Each candidate
        # spec resolves to one column; the swap-counterpart column is also
        # included so the per-candidate scorer can build both perspectives.
        candidate_cols: set[str] = set()
        for spec in self.all_feature_specs:
            col = _spec_to_column(spec)
            candidate_cols.add(col)
            candidate_cols.add(_swap_perspective(col))

        available_cols = set(df.columns)
        missing = candidate_cols - available_cols
        if missing:
            logger.warning(
                "iid discovery: %d candidate columns not in DataFrame, dropping: %s",
                len(missing), sorted(missing)[:5],
            )
        candidate_cols = sorted(candidate_cols & available_cols)

        self.col_to_idx = {c: i for i, c in enumerate(candidate_cols)}

        t0 = time.perf_counter()
        self.X_wide = (
            df.select(pl.col(c).cast(pl.Float64) for c in candidate_cols)
            .to_numpy()
        )
        logger.info(
            "Phase C: extracted X_wide shape=%s in %.1fs",
            self.X_wide.shape, time.perf_counter() - t0,
        )

        self.y_games_a = df["_target_games_a"].to_numpy().astype(np.float64)
        self.y_games_b = df["_target_games_b"].to_numpy().astype(np.float64)
        self.y_won = df["won"].to_numpy().astype(np.int64)
        self.best_of = df["best_of"].to_numpy().astype(np.int64)

        # Per-row actual serve rates (training target for the matchup model).
        # Player perspective: unprefixed parquet columns. Opp: opp_ prefix.
        won_a = df["pts_service_pts_won"].to_numpy().astype(np.float64)
        played_a = df["pts_service_pts_played"].to_numpy().astype(np.float64)
        won_b = df["opp_pts_service_pts_won"].to_numpy().astype(np.float64)
        played_b = df["opp_pts_service_pts_played"].to_numpy().astype(np.float64)
        with np.errstate(divide="ignore", invalid="ignore"):
            self.actual_serve_rate_a = np.where(
                played_a > 0, won_a / played_a, np.nan,
            )
            self.actual_serve_rate_b = np.where(
                played_b > 0, won_b / played_b, np.nan,
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
        )
        self.folds = [
            (np.array(train_idx), np.array(test_idx))
            for train_idx, test_idx in splitter.split(df)
        ]
        logger.info("Phase D: built %d expanding-window folds", len(self.folds))

    def create_scorer(self) -> Callable[[list[str]], float]:
        """Return a closure scoring candidate feature subsets via inlined matchup+chain."""
        if self.X_wide is None:
            raise RuntimeError("precompute() must be called before create_scorer()")

        X_wide = self.X_wide
        col_to_idx = self.col_to_idx
        y_games_a = self.y_games_a
        best_of = self.best_of
        actual_a = self.actual_serve_rate_a
        actual_b = self.actual_serve_rate_b
        folds = self.folds

        regressor_type = self.config.serve_model.regressor.type
        regressor_params = dict(self.config.serve_model.regressor.params)
        clip_min = self.config.serve_model.clip_min
        clip_max = self.config.serve_model.clip_max

        def scorer(specs: list[str]) -> float:
            if not specs:
                return float("inf")

            # Resolve candidate specs to column indices in BOTH perspectives.
            # The matchup model uses each spec's column as the row-player view,
            # and the swap counterpart for the row-opp view.
            try:
                player_cols = [_spec_to_column(s) for s in specs]
                opp_cols = [_swap_perspective(c) for c in player_cols]
                player_idx = np.array([col_to_idx[c] for c in player_cols])
                opp_idx = np.array([col_to_idx[c] for c in opp_cols])
            except KeyError:
                return float("inf")

            fold_maes: list[float] = []
            for train_idx, test_idx in folds:
                # ---- Build training matrix from BOTH perspectives ----
                X_train_player = X_wide[np.ix_(train_idx, player_idx)]
                X_train_opp = X_wide[np.ix_(train_idx, opp_idx)]
                y_train_player = actual_a[train_idx]
                y_train_opp = actual_b[train_idx]

                X_train_full = np.vstack([X_train_player, X_train_opp])
                y_train_full = np.concatenate([y_train_player, y_train_opp])

                valid = (
                    np.isfinite(y_train_full)
                    & np.isfinite(X_train_full).all(axis=1)
                )
                X_train_valid = X_train_full[valid]
                y_train_valid = y_train_full[valid]
                if len(X_train_valid) == 0:
                    return float("inf")

                # Standardize on the stacked train rows.
                mean = X_train_valid.mean(axis=0)
                std = X_train_valid.std(axis=0)
                std = np.where(std == 0, 1.0, std)
                X_train_scaled = (X_train_valid - mean) / std

                # Fit ridge / linear via the existing factory.
                model = get_regression_model(regressor_type, dict(regressor_params))
                try:
                    model.fit(X_train_scaled, y_train_valid)
                except Exception:
                    return float("inf")

                # ---- Predict serve win pcts for the test set, both perspectives ----
                X_test_a = X_wide[np.ix_(test_idx, player_idx)]
                X_test_b = X_wide[np.ix_(test_idx, opp_idx)]
                # Impute missing with train mean (post-scale = 0)
                X_test_a = np.where(np.isnan(X_test_a), mean, X_test_a)
                X_test_b = np.where(np.isnan(X_test_b), mean, X_test_b)
                X_test_a_scaled = (X_test_a - mean) / std
                X_test_b_scaled = (X_test_b - mean) / std

                p_a = model.predict(X_test_a_scaled).astype(np.float64)
                p_b = model.predict(X_test_b_scaled).astype(np.float64)
                p_a = np.clip(p_a, clip_min, clip_max)
                p_b = np.clip(p_b, clip_min, clip_max)

                # ---- Run chain → expected_games_a → MAE ----
                h_a = p_service_game_win(p_a)
                h_b = p_service_game_win(p_b)
                t_ab = p_tiebreak_game_win(p_a, p_b)
                bo_test = best_of[test_idx]
                dist = match_distribution(h_a, h_b, t_ab, bo_test)

                fold_maes.append(
                    float(np.mean(np.abs(y_games_a[test_idx] - dist.expected_games_a)))
                )

            return float(np.mean(fold_maes))

        return scorer


class IIDProjectionDiscovery:
    """Orchestrates forward selection over the matchup serve model.

    Mirrors the shell of `mvp.projection.discovery.ProjectionDiscovery`:
    enumerate candidates → precompute fast scorer → forward_selection →
    log a single wrapper MLflow run with selected features and final metric.
    """

    def __init__(
        self,
        config_path: Path | str,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
        mlflow_dir: Path | str | None = None,
        verbose: bool = False,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = IIDDiscoveryConfig.from_file(self.config_path)
        self.matches_path = matches_path
        self.cache_dir = cache_dir
        self.mlflow_dir = mlflow_dir
        self.verbose = verbose
        self._experiment_count = 0

    def _log(self, msg: str) -> None:
        logger.info(msg)

    def run(self) -> IIDProjectionDiscoveryResult:
        self._log(f"IID Projection Discovery: {self.config_path.stem}")
        self._log("=" * 60)

        feat_cfg = self.config.features
        all_features = get_all_feature_specs(window_sizes=feat_cfg.window_sizes)

        if feat_cfg.include:
            included = set(feat_cfg.include)
            all_features = [f for f in all_features if f in included]
            self._log(f"Restricted to {len(all_features)} features via include")

        if feat_cfg.exclude:
            excluded = set(feat_cfg.exclude)
            all_features = [f for f in all_features if f not in excluded]
            self._log(f"Excluding {len(excluded)} features")

        # Drop any candidates whose swap-counterpart isn't a known column on the
        # source schema (mirrors the FastIIDDiscoverySelector loose-end check).
        # We can't know without precomputing, so the selector handles missing
        # columns by returning float("inf"). Just log the candidate count here.
        self._log(f"Candidate pool: {len(all_features)} feature specs")

        # Precompute the fast scorer
        fast = FastIIDDiscoverySelector(
            config=self.config,
            all_feature_specs=all_features,
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )
        self._log("Precomputing wide feature matrix...")
        fast.precompute()

        scorer = fast.create_scorer()

        selector = FeatureSelector(
            scorer=scorer,
            all_features=all_features,
            method="forward",
            direction="minimize",
            min_features=1,
            max_features=feat_cfg.max_features,
            base_features=feat_cfg.base,
        )

        # Open one wrapper MLflow run for the whole discovery
        if self.mlflow_dir:
            mlflow_uri = f"file:///{str(self.mlflow_dir).replace(chr(92), '/')}"
            mlflow.set_tracking_uri(mlflow_uri)
        ml_logger = ExperimentLogger(experiment_name="iid_projection_discovery")

        with ml_logger.start_run(run_name=self.config_path.stem):
            ml_logger.log_params({
                "metric": self.config.metric,
                "selection_method": self.config.selection_method,
                "candidate_pool_size": len(all_features),
                "max_features": feat_cfg.max_features or 0,
                "window_sizes": str(feat_cfg.window_sizes),
                "regressor_type": self.config.serve_model.regressor.type,
                "regressor_alpha": self.config.serve_model.regressor.params.get(
                    "alpha", "n/a",
                ),
                "date_range_start": str(self.config.data.date_range.start),
                "date_range_end": str(self.config.data.date_range.end),
                "n_folds": len(fast.folds),
            })
            ml_logger.log_artifact(str(self.config_path))

            self._log("Running forward selection...")
            t0 = time.perf_counter()
            selection_result = selector.forward_selection(verbose=self.verbose)
            selected = selection_result.selected_features
            self._experiment_count = len(selection_result.history)

            elapsed = time.perf_counter() - t0
            self._log(f"Forward selection complete in {elapsed:.1f}s")

            final_metric = selection_result.final_metric
            ml_logger.log_metrics({
                "final_mae": final_metric,
                "n_selected_features": len(selected),
                "n_iterations": len(selection_result.history),
                "wall_seconds": elapsed,
            })
            for i, feat in enumerate(selected):
                ml_logger.log_params({f"selected_feature_{i:02d}": feat})

        self._log("")
        self._log("RESULTS")
        self._log("-" * 30)
        self._log(f"Selected ({len(selected)} features):")
        for f in selected:
            self._log(f"  - {f}")
        self._log(f"Final {self.config.metric}: {final_metric:.4f}")

        return IIDProjectionDiscoveryResult(
            selected_features=selected,
            selection_result=selection_result,
            final_metric=final_metric,
            n_experiments=self._experiment_count,
        )
