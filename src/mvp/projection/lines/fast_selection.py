"""Fast forward selection using a precomputed feature matrix for line markets.

Mirrors `FastProjectionSelector`'s precompute pattern: cache all candidate
features once, materialize a wide numpy matrix, then per-candidate just slice
columns and refit. The scorer fits K binary classifiers (one per line) on the
selected feature subset, predicts on each fold's test set, and aggregates a
lines metric across lines and folds.

Target handling:
    - `total`  : dedup to one row per match (outcome is symmetric).
    - `spread` : dedup to one row per match (signed diff vs. canonical perspective).
    - `player_games`: NO dedup (matches.parquet stores both perspectives;
                      keeping both gives doubled-data symmetric training).
"""

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
from mvp.projection.lines.config import LinesDiscoveryConfig
from mvp.projection.lines.metric import score as score_lines
from mvp.projection.lines.model import LineModel
from mvp.projection.lines.targets import derive_labels


logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message="All-NaN slice encountered")
warnings.filterwarnings("ignore", message="invalid value encountered", category=RuntimeWarning)


class FastLinesSelector:
    """Precomputes the candidate feature matrix once for fast lines FS scoring."""

    def __init__(
        self,
        config: LinesDiscoveryConfig,
        all_feature_specs: list[str],
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        from mvp.common.base_job import get_data_root, get_local_data_root

        self.config = config
        self.all_feature_specs = all_feature_specs
        self.matches_path = Path(matches_path) if matches_path else (
            get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        )
        self.cache_dir = Path(cache_dir) if cache_dir else (
            get_local_data_root() / "features" / "cache"
        )

        self.X_wide: np.ndarray | None = None
        self.y_a: np.ndarray | None = None
        self.y_b: np.ndarray | None = None
        self.col_to_idx: dict[str, int] = {}
        self.folds: list[tuple[np.ndarray, np.ndarray]] = []
        self.fold_medians: list[np.ndarray] = []

    def precompute(self) -> None:
        """Run the one-time feature/target/fold materialization."""
        engine = make_fs_engine(
            matches_path=self.matches_path,
            cache_dir=self.cache_dir,
        )

        extra_columns = [
            "won", "reason", "sets_played", "best_of",
            "circuit", "surface", "round", "match_uid", "player_id", "opp_id",
            "player_set1_games", "player_set2_games",
            "player_set3_games", "player_set4_games", "player_set5_games",
            "opp_set1_games", "opp_set2_games",
            "opp_set3_games", "opp_set4_games", "opp_set5_games",
        ]
        if self.config.data.filters:
            for col in self.config.data.filters:
                if col not in extra_columns:
                    extra_columns.append(col)

        cache_key = engine.ensure_cached(
            self.all_feature_specs, extra_columns=extra_columns,
        )

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

        df = df.filter(
            pl.col("player_set1_games").is_not_null()
            & pl.col("player_set2_games").is_not_null()
        )
        if "reason" in df.columns:
            df = df.filter(
                pl.col("reason").fill_null("").is_in(["W/O", "RET", "DEF", "UNP"]).not_()
            )

        df = df.with_columns(
            total_games_won().cast(pl.Int64).alias("_y_a"),
            total_games_lost().cast(pl.Int64).alias("_y_b"),
        )
        df = df.filter(
            pl.col("_y_a").is_not_null()
            & pl.col("_y_b").is_not_null()
            & pl.col("best_of").is_in([3, 5])
        )

        # Dedup for symmetric / canonical-perspective targets; keep both
        # perspectives for player_games (doubled-data symmetric training).
        target = self.config.discovery.target
        if target in ("total", "spread"):
            df = df.sort(["match_uid", "player_id"]).unique(
                subset=["match_uid"], keep="first", maintain_order=True,
            )

        logger.info(
            "Filtered to %d rows (target=%s); loading %d feature specs from cache",
            len(df), target, len(self.all_feature_specs),
        )
        check_memory("precompute: after filtering")

        df = engine.load_features_numpy(self.all_feature_specs, df, cache_key)
        if self.config.data.filters:
            df = apply_filters(df, self.config.data.filters)

        all_col_names = get_feature_columns(self.all_feature_specs)
        self._build_result = build_imputation(self.all_feature_specs, get_registry())
        augmented = all_col_names + self._build_result.aux_base_col_names
        self.col_to_idx = {c: i for i, c in enumerate(augmented)}

        logger.info(
            "Extracting %d features (%d aux) x %d rows to numpy",
            len(all_col_names), len(self._build_result.aux_base_col_names), len(df),
        )
        t0 = time.perf_counter()
        self.X_wide = (
            df.select(pl.col(c).cast(pl.Float64) for c in augmented)
            .to_numpy()
        )
        self.y_a = df["_y_a"].to_numpy().astype(np.int64)
        self.y_b = df["_y_b"].to_numpy().astype(np.int64)
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
        for train_idx, _ in self.folds:
            medians = np.nanmedian(self.X_wide[train_idx], axis=0)
            medians = np.where(np.isnan(medians), 0.0, medians)
            self.fold_medians.append(medians)
        logger.info("Per-fold medians computed in %.1fs", time.perf_counter() - t0)

    def create_scorer(self) -> Callable[[list[str]], float]:
        """Return a fast scorer that evaluates a feature subset across folds."""
        if self.X_wide is None:
            raise RuntimeError("FastLinesSelector.create_scorer called before precompute()")

        X_wide = self.X_wide
        y_a = self.y_a
        y_b = self.y_b
        col_to_idx = self.col_to_idx
        folds = self.folds
        fold_medians = self.fold_medians
        target = self.config.discovery.target
        lines = self.config.discovery.active_lines
        metric_name = self.config.discovery.metric
        model_type = self.config.model.type
        model_params = self.config.model.params or {}

        def scorer(features: list[str]) -> float:
            if not features:
                return float("inf")
            try:
                col_names = get_feature_columns(features)
                col_indices = np.array([col_to_idx[c] for c in col_names])
            except KeyError:
                return float("inf")

            fold_scores = []
            for fold_idx, (train_idx, test_idx) in enumerate(folds):
                X_train = X_wide[np.ix_(train_idx, col_indices)].copy()
                X_test = X_wide[np.ix_(test_idx, col_indices)].copy()

                medians = fold_medians[fold_idx][col_indices]
                X_train = np.where(np.isnan(X_train), medians, X_train)
                X_test = np.where(np.isnan(X_test), medians, X_test)

                train_labels = derive_labels(target, y_a[train_idx], y_b[train_idx], lines)
                test_labels = derive_labels(target, y_a[test_idx], y_b[test_idx], lines)

                model = LineModel(model_type=model_type, lines=lines, params=model_params)
                model.fit(X_train, train_labels)
                preds = model.predict_line_probs(X_test)
                fold_scores.append(score_lines(metric_name, preds, test_labels))

            return float(np.mean(fold_scores))

        return scorer
