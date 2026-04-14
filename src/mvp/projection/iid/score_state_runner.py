"""Training runner for the score-state-dependent serve model.

Trains a point-grain classifier (one row per point) predicting
`point_won_by_server` from a mixture of match-level features (broadcast via
the server's row in matches.parquet) and point-level features (score state,
serve_num, flags) from match_beats_points.parquet.

Evaluation: expanding-window CV on `effective_match_date`. Per-fold metrics
include per-point log-loss, Brier, accuracy, AUC, and calibration_error.

Chain integration (Phase 3) will adapt this model to the callable interface
consumed by `chain.py`. For now, the runner is a standalone pipeline whose
purpose is to establish the data flow and produce a baseline.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model.config import apply_filters, get_filter_feature_specs
from mvp.model.engine import FeatureEngine
from mvp.model.metrics import compute_metrics
from mvp.model.splitters import make_splitter
from mvp.projection.iid.config import ScoreStateConfig
from mvp.projection.iid.score_state_features import add_derived_point_features
from mvp.projection.iid.score_state_model import build_score_state_model

logger = logging.getLogger(__name__)


@dataclass
class FoldResult:
    fold_idx: int
    train_n: int
    test_n: int
    metrics: dict[str, float]


class ScoreStateRunner:
    """Executes a standalone training run for a ScoreStateServeModel."""

    def __init__(
        self,
        config_path: Path | str,
        *,
        points_path: Path | str | None = None,
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.config = ScoreStateConfig.from_file(config_path)
        self.points_path = Path(points_path) if points_path else get_data_root() / "aggregate" / "atptour" / "match_beats_points.parquet"
        self.matches_path = Path(matches_path) if matches_path else get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        self.cache_dir = Path(cache_dir) if cache_dir else get_local_data_root() / "features" / "cache"

    def run(self) -> dict[str, Any]:
        df = self._build_training_dataset()

        # Date filter from config.data.date_range
        start = self.config.data.date_range.start if self.config.data.date_range else None
        end = self.config.data.date_range.end if self.config.data.date_range else None
        if start is not None:
            df = df.filter(pl.col("effective_match_date") >= start)
        if end is not None:
            df = df.filter(pl.col("effective_match_date") <= end)

        # match_beats_points.parquet is singles-only by construction (is_doubles filtered out
        # in the aggregator). Add draw_type as a literal for filter compatibility with the
        # shared apply_filters utility.
        if "draw_type" not in df.columns:
            df = df.with_columns(pl.lit("singles").alias("draw_type"))
        df = apply_filters(df, self.config.data.filters)

        # Drop rows with missing target (shouldn't happen but belt-and-suspenders).
        df = df.filter(pl.col("point_won_by_server").is_not_null())

        if len(df) == 0:
            raise ValueError("Training dataset is empty after filters")

        logger.info("Training dataset: %d points across %d matches", len(df), df["match_uid"].n_unique())

        # Assemble feature matrix columns
        feature_cols = self._resolve_feature_columns()
        logger.info("Feature columns (%d): %s", len(feature_cols), feature_cols)

        # CV
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
        fold_results: list[FoldResult] = []
        for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(df)):
            train_df = df[train_idx]
            test_df = df[test_idx]
            X_train = train_df.select(feature_cols).to_numpy()
            y_train = train_df["point_won_by_server"].cast(pl.Int64).to_numpy()
            X_test = test_df.select(feature_cols).to_numpy()
            y_test = test_df["point_won_by_server"].cast(pl.Int64).to_numpy()

            model = build_score_state_model(
                type_=self.config.model.type,
                feature_names=feature_cols,
                params=dict(self.config.model.params),
            )
            model.fit(X_train, y_train)
            y_prob = model.predict_proba(X_test)

            metrics = compute_metrics(y_test, y_prob)
            fold_results.append(
                FoldResult(fold_idx=fold_idx, train_n=len(train_df), test_n=len(test_df), metrics=metrics)
            )
            logger.info(
                "Fold %d: train=%d, test=%d, log_loss=%.4f, accuracy=%.4f, auc=%.4f",
                fold_idx, len(train_df), len(test_df),
                metrics.get("log_loss", float("nan")),
                metrics.get("accuracy", float("nan")),
                metrics.get("roc_auc", float("nan")),
            )

        # Aggregate
        agg_metrics: dict[str, float] = {}
        if fold_results:
            metric_keys = fold_results[0].metrics.keys()
            for k in metric_keys:
                vals = [fr.metrics[k] for fr in fold_results if k in fr.metrics]
                if vals:
                    agg_metrics[k] = sum(vals) / len(vals)

        # Final-fit coefficient summary (fit on full training data for interpretability)
        X_full = df.select(feature_cols).to_numpy()
        y_full = df["point_won_by_server"].cast(pl.Int64).to_numpy()
        final_model = build_score_state_model(
            type_=self.config.model.type,
            feature_names=feature_cols,
            params=dict(self.config.model.params),
        )
        final_model.fit(X_full, y_full)
        coefs = final_model.coef_summary()

        return {
            "fold_results": fold_results,
            "aggregate_metrics": agg_metrics,
            "coef_summary": coefs,
            "n_train_rows": len(df),
            "feature_cols": feature_cols,
        }

    def _build_training_dataset(self) -> pl.DataFrame:
        """Load points + join server-perspective match-level features."""
        points = pl.read_parquet(self.points_path)
        logger.info("Loaded %d point rows from %s", len(points), self.points_path)

        # Compute match-level features via FeatureEngine
        specs = self._match_level_specs_with_filters()
        engine = FeatureEngine(matches_path=self.matches_path, cache_dir=self.cache_dir)
        matches_features = engine.compute(feature_specs=specs, extra_columns=["player_id", "opp_id", "match_uid"])
        # matches_features: match-grain (2 rows per match: one per player perspective).

        # Join on (match_uid, server_id=player_id). player_* cols become server's stats;
        # opp_* cols become returner's stats.
        joined = points.join(
            matches_features.rename({"player_id": "server_id", "opp_id": "returner_id"}),
            on=["match_uid", "server_id", "returner_id"],
            how="inner",
        )

        # Rename match-level feature columns from player_/opp_ to server_/returner_
        renames: dict[str, str] = {}
        for col in joined.columns:
            if col.startswith("player_") and col not in ("player_id",):
                renames[col] = "server_" + col[len("player_"):]
            elif col.startswith("opp_") and col not in ("opp_id",):
                renames[col] = "returner_" + col[len("opp_"):]
        if renames:
            joined = joined.rename(renames)

        # Add derived point-level columns requested by the config.
        joined = add_derived_point_features(joined, self.config.model.point_level_features)
        return joined

    def _match_level_specs_with_filters(self) -> list[str]:
        """Union of configured match-level feature specs and filter-referenced specs."""
        specs = list(self.config.model.match_level_features)
        # Filter specs may reference additional features; include them to let the engine compute.
        filter_specs = get_filter_feature_specs(self.config.data.filters)
        for s in filter_specs:
            if s not in specs:
                specs.append(s)
        return specs

    def _resolve_feature_columns(self) -> list[str]:
        """Concrete column names for the model's feature matrix, in order."""
        from mvp.model.engine import build_column_name, parse_feature_spec

        cols: list[str] = []
        # Match-level: convert specs to concrete columns, then apply server_/returner_ rename.
        for spec in self.config.model.match_level_features:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            col = build_column_name(full_name, params)
            # Map player_/opp_ → server_/returner_
            if col.startswith("player_"):
                col = "server_" + col[len("player_"):]
            elif col.startswith("opp_"):
                col = "returner_" + col[len("opp_"):]
            cols.append(col)
        # Point-level: pass-through column names.
        cols.extend(self.config.model.point_level_features)
        return cols
