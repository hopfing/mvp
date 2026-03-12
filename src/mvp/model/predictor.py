"""Production model: train, save, load, and predict."""


import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import polars as pl
import yaml

from mvp.model.calibration import PlattCalibrator
from mvp.model.confidence.dimensions import MODIFIERS
from mvp.model.config import EnsembleParams, ExperimentConfig, apply_filters, get_filter_feature_specs
from mvp.model.engine import FeatureEngine, get_feature_columns
from mvp.model.features.elo import surface_elo_expr
from mvp.model.models import EnsembleModel, get_model

logger = logging.getLogger(__name__)

MATCHES_PATH = Path("data/aggregate/atptour/matches.parquet")
CACHE_DIR = Path("data/features/cache")
PREDICTIONS_PATH = Path("data/predictions/predictions.parquet")
PRODUCTION_CONFIG_PATH = Path("production.yaml")

# Tolerance for prediction consistency checks
PREDICTION_TOLERANCE = 1e-4
# Threshold for drift alerts (5% probability swing)
DRIFT_THRESHOLD = 0.05


class ProductionPredictor:
    """Train, save, load, and predict with the production model."""

    def __init__(
        self,
        production_config_path: Path | str = PRODUCTION_CONFIG_PATH,
        matches_path: Path | str = MATCHES_PATH,
        cache_dir: Path | str = CACHE_DIR,
        predictions_path: Path | str = PREDICTIONS_PATH,
    ) -> None:
        self.production_config_path = Path(production_config_path)
        self.matches_path = Path(matches_path)
        self.cache_dir = Path(cache_dir)
        self.predictions_path = Path(predictions_path)

        with open(self.production_config_path) as f:
            self.config: dict[str, Any] = yaml.safe_load(f)

        self._experiment_config = ExperimentConfig.from_file(
            self.config["active"]["config"]
        )

    def _resolve_ensemble_features(self) -> tuple[list[str], list[dict]]:
        """Resolve ensemble config to union features and base model specs."""
        _, feature_specs, base_model_specs = self._resolve_entry_features(
            self.config["active"]
        )
        assert base_model_specs is not None
        return feature_specs, base_model_specs

    def _resolve_entry_features(
        self, entry: dict
    ) -> tuple[ExperimentConfig, list[str], list[dict] | None]:
        """Load experiment config for an entry and resolve its features.

        Returns:
            Tuple of (experiment_config, feature_specs, base_model_specs_or_None).
        """
        config = ExperimentConfig.from_file(entry["config"])
        is_ensemble = config.model.type == "ensemble"

        if is_ensemble:
            ensemble_params = EnsembleParams.model_validate(config.model.params)
            all_feature_specs: list[str] = []
            base_model_specs: list[dict] = []

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

            for spec in ensemble_params.meta_features:
                if spec not in all_feature_specs:
                    all_feature_specs.append(spec)

            union_cols = get_feature_columns(all_feature_specs)
            for spec in base_model_specs:
                base_cols = get_feature_columns(spec["feature_specs"])
                spec["feature_indices"] = [union_cols.index(c) for c in base_cols]
                del spec["feature_specs"]

            return config, all_feature_specs, base_model_specs
        else:
            assert config.features is not None
            return config, config.features.include, None

    def _train_single(self, entry: dict) -> None:
        """Train a single model from an entry dict and save its artifact.

        Args:
            entry: Dict with keys config, artifact, train_date_range, filters.
        """
        config, feature_specs, base_model_specs = self._resolve_entry_features(entry)
        is_ensemble = config.model.type == "ensemble"
        engine = FeatureEngine(
            matches_path=self.matches_path, cache_dir=self.cache_dir
        )

        # Compute features (include compute_only and filter-referenced features)
        compute_only = (
            config.features.compute_only
            if config.features and config.features.compute_only
            else []
        )
        filter_specs = get_filter_feature_specs(entry.get("filters"))
        extra = compute_only + filter_specs
        all_specs = feature_specs + [s for s in extra if s not in feature_specs]
        df = engine.compute(all_specs)

        # Apply training filters
        train_range = entry["train_date_range"]
        start = datetime.fromisoformat(train_range["start"])
        end = datetime.fromisoformat(train_range["end"])
        df = df.filter(
            (pl.col("effective_match_date") >= start)
            & (pl.col("effective_match_date") <= end)
        )

        if entry.get("filters"):
            df = apply_filters(df, entry["filters"])

        # Drop rows without outcomes
        df = df.filter(pl.col("won").is_not_null())

        feature_cols = get_feature_columns(feature_specs)
        X = df.select(pl.col(c).cast(pl.Float64) for c in feature_cols).to_numpy()
        y = df["won"].to_numpy().astype(int)

        logger.info("Training on %d rows with %d features", len(y), len(feature_cols))

        # Median imputation (fallback to 0 for all-NaN columns)
        medians = np.nanmedian(X, axis=0)
        medians = np.where(np.isnan(medians), 0.0, medians)
        X = np.where(np.isnan(X), medians, X)

        # Train
        model = get_model(config.model.type, config.model.params or {})
        if is_ensemble and base_model_specs is not None:
            assert isinstance(model, EnsembleModel)
            model.configure(base_model_specs)
        model.fit(X, y)

        # Fit Platt calibrator via 5-fold CV on OOF predictions
        from sklearn.model_selection import StratifiedKFold

        oof_probs = np.zeros(len(y))
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        for train_idx, val_idx in skf.split(X, y):
            fold_model = get_model(config.model.type, config.model.params or {})
            if is_ensemble and base_model_specs is not None:
                assert isinstance(fold_model, EnsembleModel)
                fold_model.configure(base_model_specs)
            fold_model.fit(X[train_idx], y[train_idx])
            oof_probs[val_idx] = fold_model.predict_proba(X[val_idx])
        calibrator = PlattCalibrator()
        calibrator.fit(oof_probs, y)
        logger.info(
            "Platt calibrator: slope=%.4f, intercept=%.4f",
            calibrator.slope,
            calibrator.intercept,
        )

        # Save artifact
        artifact_path = Path(entry["artifact"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": model,
                "medians": medians,
                "feature_cols": feature_cols,
                "calibrator": calibrator,
            },
            artifact_path,
        )

        # Update trained_at in config
        entry["trained_at"] = datetime.now(timezone.utc).isoformat()
        with open(self.production_config_path, "w") as f:
            yaml.dump(self.config, f, default_flow_style=False)

        logger.info("Model saved to %s", artifact_path)

    def train(self) -> None:
        """Train production model on all matching data and save artifact."""
        self._train_single(self.config["active"])

    def load(self) -> tuple[Any, np.ndarray, list[str], PlattCalibrator | None]:
        """Load the trained production model.

        Returns:
            Tuple of (model, medians, feature_cols, calibrator).

        Raises:
            FileNotFoundError: If no trained artifact exists.
        """
        artifact_path = Path(self.config["active"]["artifact"])
        if not artifact_path.exists():
            raise FileNotFoundError(
                f"No trained model at {artifact_path}. Run with --train first."
            )

        artifact = joblib.load(artifact_path)
        calibrator = artifact.get("calibrator")
        if calibrator is None:
            logger.warning("No calibrator in artifact — predictions will be uncalibrated")
        return artifact["model"], artifact["medians"], artifact["feature_cols"], calibrator

    def _load_single(
        self, entry: dict
    ) -> tuple[Any, np.ndarray, list[str], PlattCalibrator | None]:
        """Load a trained model artifact from an entry dict."""
        artifact_path = Path(entry["artifact"])
        if not artifact_path.exists():
            raise FileNotFoundError(f"No trained model at {artifact_path}")
        artifact = joblib.load(artifact_path)
        calibrator = artifact.get("calibrator")
        return artifact["model"], artifact["medians"], artifact["feature_cols"], calibrator

    def _predict_raw(
        self,
        entry: dict,
        tournament_keys: list[tuple[str, int]] | None,
        match_uids: set[str],
    ) -> dict[str, float]:
        """Generate raw uid->prob predictions for a single model entry.

        Only predicts matches whose uid is in match_uids (intersection with
        production predictions). Returns {match_uid: p1_win_prob}.
        """
        model, medians, feature_cols, calibrator = self._load_single(entry)
        config, feature_specs, _ = self._resolve_entry_features(entry)
        engine = FeatureEngine(
            matches_path=self.matches_path, cache_dir=self.cache_dir
        )

        # Compute features
        compute_only = (
            config.features.compute_only
            if config.features and config.features.compute_only
            else []
        )
        filter_feature_specs = get_filter_feature_specs(entry.get("filters"))
        extra = compute_only + filter_feature_specs
        all_specs = feature_specs + [s for s in extra if s not in feature_specs]
        df = engine.compute(all_specs)

        # Apply non-date filters
        if entry.get("filters"):
            df = apply_filters(df, entry["filters"])

        # Scope to tournaments
        if tournament_keys is not None:
            keys_df = pl.DataFrame(
                {"tournament_id": [t for t, _ in tournament_keys],
                 "_year": [y for _, y in tournament_keys]},
            ).with_columns(pl.col("_year").cast(pl.Int32))
            df = df.with_columns(
                pl.col("effective_match_date").dt.year().alias("_year")
            ).join(keys_df, on=["tournament_id", "_year"], how="semi").drop("_year")

        # Keep only pending matches in the production set
        pending = df.filter(
            pl.col("won").is_null() & pl.col("match_uid").is_in(list(match_uids))
        )

        if len(pending) == 0:
            return {}

        # Deduplicate to canonical row per match
        if "draw_p1_id" in pending.columns:
            pending = pending.with_columns(
                pl.when(pl.col("draw_p1_id").is_not_null())
                .then(pl.col("player_id") == pl.col("draw_p1_id"))
                .otherwise(pl.col("player_id") < pl.col("opp_id"))
                .alias("_is_canonical")
            )
        else:
            pending = pending.with_columns(
                (pl.col("player_id") < pl.col("opp_id")).alias("_is_canonical")
            )
        canonical = pending.filter(pl.col("_is_canonical"))
        non_canonical = pending.filter(~pl.col("_is_canonical"))

        if len(non_canonical) > 0:
            seen_uids = set(canonical["match_uid"].to_list())
            missing = non_canonical.filter(
                ~pl.col("match_uid").is_in(list(seen_uids))
            )
            if len(missing) > 0:
                canonical = pl.concat([canonical, missing], how="diagonal_relaxed")

        # Predict
        X = canonical.select(
            pl.col(c).cast(pl.Float64) for c in feature_cols
        ).to_numpy()
        X = np.where(np.isnan(X), medians, X)
        probs = model.predict_proba(X)
        if calibrator is not None:
            probs = calibrator.transform(probs)

        # For non-canonical rows that were included, flip the prob
        result: dict[str, float] = {}
        for i, row in enumerate(canonical.iter_rows(named=True)):
            uid = row["match_uid"]
            p = float(probs[i])
            is_canonical = row["_is_canonical"]
            if not is_canonical:
                p = 1.0 - p
            result[uid] = p

        return result

    def train_voters(self) -> int:
        """Train all voter models. Returns count of voters trained."""
        voters = self.config.get("voters", [])
        for voter in voters:
            name = voter.get("name", "unnamed")
            logger.info("Training voter: %s", name)
            self._train_single(voter)
        return len(voters)

    def predict_voters(
        self,
        tournament_keys: list[tuple[str, int]] | None,
        predictions: pl.DataFrame,
    ) -> pl.DataFrame:
        """Add consensus column from voter models.

        Args:
            tournament_keys: Tournament scope (same as predict()).
            predictions: DataFrame from predict() with p1_win_prob.

        Returns:
            predictions with added 'consensus' column.
        """
        voters = self.config.get("voters", [])
        if not voters:
            return predictions.with_columns(
                pl.lit(None).cast(pl.Float64).alias("consensus")
            )

        match_uids = set(predictions["match_uid"].to_list())

        # Production binary pick per match
        prod_picks: dict[str, bool] = {}
        for row in predictions.iter_rows(named=True):
            prod_picks[row["match_uid"]] = row["p1_win_prob"] >= 0.5

        # Collect voter picks
        voter_picks: list[dict[str, bool]] = []
        for voter in voters:
            raw = self._predict_raw(voter, tournament_keys, match_uids)
            picks = {uid: prob >= 0.5 for uid, prob in raw.items()}
            voter_picks.append(picks)

        # Build consensus as decimal (agree / total)
        consensus_values: list[float] = []
        for row in predictions.iter_rows(named=True):
            uid = row["match_uid"]
            prod_pick = prod_picks[uid]
            total = 1  # production always counts
            agree = 1  # production always agrees with itself
            for picks in voter_picks:
                if uid in picks:
                    total += 1
                    if picks[uid] == prod_pick:
                        agree += 1
            consensus_values.append(round(agree / total, 2))

        return predictions.with_columns(
            pl.Series("consensus", consensus_values)
        )

    def predict(
        self, tournament_keys: list[tuple[str, int]] | None = None
    ) -> pl.DataFrame:
        """Generate predictions for pending matches (won is null).

        Args:
            tournament_keys: If provided, only predict for these (tid, year) pairs.

        Returns:
            DataFrame with one row per match, containing prediction columns.
        """
        model, medians, feature_cols, calibrator = self.load()
        config = self._experiment_config
        engine = FeatureEngine(
            matches_path=self.matches_path, cache_dir=self.cache_dir
        )

        # Compute features on all data (needed for temporal features)
        if config.model.type == "ensemble":
            feature_specs, _ = self._resolve_ensemble_features()
        else:
            assert config.features is not None
            feature_specs = config.features.include
        compute_only = (
            config.features.compute_only
            if config.features and config.features.compute_only
            else []
        )
        filter_specs = get_filter_feature_specs(self.config["active"].get("filters"))
        extra = compute_only + filter_specs
        all_specs = feature_specs + [s for s in extra if s not in feature_specs]
        df = engine.compute(all_specs)

        # Apply non-date filters (same as training, minus date range)
        if self.config["active"].get("filters"):
            df = apply_filters(df, self.config["active"]["filters"])

        # Scope to specific tournaments if requested
        if tournament_keys is not None:
            keys_df = pl.DataFrame(
                {"tournament_id": [t for t, _ in tournament_keys],
                 "_year": [y for _, y in tournament_keys]},
            ).with_columns(pl.col("_year").cast(pl.Int32))
            df = df.with_columns(
                pl.col("effective_match_date").dt.year().alias("_year")
            ).join(keys_df, on=["tournament_id", "_year"], how="semi").drop("_year")

        # Keep only pending matches
        pending = df.filter(pl.col("won").is_null())

        if len(pending) == 0:
            logger.warning("No pending matches to predict")
            return pl.DataFrame()

        # Extract features and predict
        X = pending.select(
            pl.col(c).cast(pl.Float64) for c in feature_cols
        ).to_numpy()
        X = np.where(np.isnan(X), medians, X)
        probs = model.predict_proba(X)
        if calibrator is not None:
            probs = calibrator.transform(probs)

        # Add probabilities to pending matches
        pending = pending.with_columns(pl.Series("_p1_win_prob", probs))

        # Compute surface-adjusted Elo before the canonical split
        pending = pending.with_columns(
            surface_elo_expr("player").alias("_player_surface_elo"),
            surface_elo_expr("opp").alias("_opp_surface_elo"),
        )

        # Deduplicate to one row per match using draw order (fall back to alphabetical)
        if "draw_p1_id" in pending.columns:
            pending = pending.with_columns(
                pl.when(pl.col("draw_p1_id").is_not_null())
                .then(pl.col("player_id") == pl.col("draw_p1_id"))
                .otherwise(pl.col("player_id") < pl.col("opp_id"))
                .alias("_is_canonical")
            )
        else:
            pending = pending.with_columns(
                (pl.col("player_id") < pl.col("opp_id")).alias("_is_canonical")
            )
        canonical = pending.filter(pl.col("_is_canonical"))
        non_canonical = pending.filter(~pl.col("_is_canonical"))

        # For matches where we only have the non-canonical row, flip the prob
        if len(non_canonical) > 0:
            seen_uids = set(canonical["match_uid"].to_list())
            missing = non_canonical.filter(
                ~pl.col("match_uid").is_in(list(seen_uids))
            )
            if len(missing) > 0:
                # Flip: this row's player is actually p2
                missing = missing.with_columns(
                    (1.0 - pl.col("_p1_win_prob")).alias("_p1_win_prob"),
                    pl.col("opp_id").alias("_tmp_player_id"),
                    pl.col("player_id").alias("_tmp_opp_id"),
                    pl.col("opp_first_name").alias("_tmp_pfn"),
                    pl.col("opp_last_name").alias("_tmp_pln"),
                    pl.col("player_first_name").alias("_tmp_ofn"),
                    pl.col("player_last_name").alias("_tmp_oln"),
                    pl.col("_opp_surface_elo").alias("_tmp_player_elo"),
                    pl.col("_player_surface_elo").alias("_tmp_opp_elo"),
                ).with_columns(
                    pl.col("_tmp_player_id").alias("player_id"),
                    pl.col("_tmp_opp_id").alias("opp_id"),
                    pl.col("_tmp_pfn").alias("player_first_name"),
                    pl.col("_tmp_pln").alias("player_last_name"),
                    pl.col("_tmp_ofn").alias("opp_first_name"),
                    pl.col("_tmp_oln").alias("opp_last_name"),
                    pl.col("_tmp_player_elo").alias("_player_surface_elo"),
                    pl.col("_tmp_opp_elo").alias("_opp_surface_elo"),
                )
                canonical = pl.concat(
                    [canonical, missing], how="diagonal_relaxed"
                )

        # Compute confidence modifier values for Layer 2 enrichment
        for mod in MODIFIERS:
            if all(c in canonical.columns for c in mod.required_columns):
                try:
                    canonical = canonical.with_columns(
                        mod.compute_value(canonical).alias(f"conf_{mod.name}")
                    )
                except Exception:
                    logger.debug("Modifier %s failed, skipping", mod.name)

        # Build output
        model_version = Path(self.config["active"]["config"]).stem
        now = datetime.now(timezone.utc)

        select_exprs = [
            pl.col("match_uid"),
            pl.col("player_id").alias("p1_id"),
            pl.col("opp_id").alias("p2_id"),
            (pl.col("player_first_name") + pl.lit(" ") + pl.col("player_last_name")).alias("p1_name"),
            (pl.col("opp_first_name") + pl.lit(" ") + pl.col("opp_last_name")).alias("p2_name"),
            pl.col("_p1_win_prob").alias("p1_win_prob"),
            (1.0 - pl.col("_p1_win_prob")).alias("p2_win_prob"),
            pl.col("_player_surface_elo").alias("p1_elo"),
            pl.col("_opp_surface_elo").alias("p2_elo"),
            pl.col("tournament_id"),
            pl.col("tournament_name"),
            pl.col("circuit"),
            pl.col("surface"),
            pl.col("round"),
            pl.col("effective_match_date"),
            pl.lit(model_version).alias("model_version"),
            pl.lit(now).alias("predicted_at"),
        ]
        if "scheduled_datetime" in canonical.columns:
            select_exprs.append(pl.col("scheduled_datetime"))
        if "match_date" in canonical.columns:
            select_exprs.append(pl.col("match_date"))
        for col in canonical.columns:
            if col.startswith("conf_"):
                select_exprs.append(pl.col(col))
        result = canonical.select(select_exprs)

        logger.info("Generated %d predictions", len(result))
        return result

    def save_predictions(self, predictions: pl.DataFrame) -> pl.DataFrame:
        """Save predictions to parquet, appending new and validating existing.

        Args:
            predictions: DataFrame from predict().

        Returns:
            DataFrame of newly added predictions.
        """
        self.predictions_path.parent.mkdir(parents=True, exist_ok=True)

        if self.predictions_path.exists():
            existing = pl.read_parquet(self.predictions_path)
            existing_uids = set(existing["match_uid"].to_list())

            # Split into new and overlapping
            new = predictions.filter(
                ~pl.col("match_uid").is_in(list(existing_uids))
            )
            overlap = predictions.filter(
                pl.col("match_uid").is_in(list(existing_uids))
            )

            # Consistency validation on overlapping predictions
            updated_uids: set[str] = set()
            if len(overlap) > 0:
                merged = overlap.select("match_uid", "p1_win_prob", "predicted_at").join(
                    existing.select(
                        "match_uid",
                        pl.col("p1_win_prob").alias("prev_p1_win_prob"),
                        pl.col("predicted_at").alias("prev_predicted_at"),
                    ),
                    on="match_uid",
                    how="inner",
                )
                diffs = (merged["p1_win_prob"] - merged["prev_p1_win_prob"]).abs()
                mismatched = merged.filter(diffs > PREDICTION_TOLERANCE)
                if len(mismatched) > 0:
                    updated_uids = set(mismatched["match_uid"].to_list())
                    logger.info(
                        "Updating %d predictions with changed probabilities (max diff: %.6f)",
                        len(updated_uids),
                        diffs.filter(diffs > PREDICTION_TOLERANCE).max(),
                    )
                    self._log_prediction_changes(mismatched)

            # Replace updated predictions in existing, then append new
            if updated_uids:
                existing = existing.filter(~pl.col("match_uid").is_in(list(updated_uids)))
                to_add = pl.concat([new, overlap.filter(pl.col("match_uid").is_in(list(updated_uids)))], how="diagonal_relaxed")
            else:
                to_add = new

            if len(to_add) > 0:
                combined = pl.concat(
                    [existing, to_add], how="diagonal_relaxed"
                )
                combined.write_parquet(self.predictions_path)
                logger.info("Saved predictions: %d new, %d updated (%d total)", len(new), len(updated_uids), len(combined))
            else:
                logger.info("No new predictions to save (%d already stored)", len(existing))

            return new
        else:
            predictions.write_parquet(self.predictions_path)
            logger.info("Saved %d predictions", len(predictions))
            return predictions

    def _log_prediction_changes(self, mismatched: pl.DataFrame) -> None:
        """Append changed predictions to the prediction drift log and emit alerts."""
        log_path = self.predictions_path.parent / "prediction_drift.parquet"

        diff_abs = (mismatched["p1_win_prob"] - mismatched["prev_p1_win_prob"]).abs()
        # Detect winner flip: previous and current on opposite sides of 0.5
        flipped = (
            (mismatched["prev_p1_win_prob"] > 0.5) & (mismatched["p1_win_prob"] < 0.5)
        ) | (
            (mismatched["prev_p1_win_prob"] < 0.5) & (mismatched["p1_win_prob"] > 0.5)
        )

        n_flips = flipped.sum()
        n_drifts = (diff_abs >= DRIFT_THRESHOLD).sum() - n_flips

        if n_flips > 0:
            flip_rows = mismatched.filter(flipped)
            for row in flip_rows.iter_rows(named=True):
                logger.warning(
                    "FLIP %s: %.1f%% -> %.1f%%",
                    row["match_uid"],
                    row["prev_p1_win_prob"] * 100,
                    row["p1_win_prob"] * 100,
                )

        if n_drifts > 0:
            drift_rows = mismatched.filter((diff_abs >= DRIFT_THRESHOLD) & ~flipped)
            for row in drift_rows.iter_rows(named=True):
                logger.info(
                    "DRIFT %s: %.1f%% -> %.1f%%",
                    row["match_uid"],
                    row["prev_p1_win_prob"] * 100,
                    row["p1_win_prob"] * 100,
                )

        log_entry = mismatched.select(
            "match_uid",
            pl.col("p1_win_prob"),
            (1 - pl.col("p1_win_prob")).alias("p2_win_prob"),
            pl.col("prev_p1_win_prob"),
            (1 - pl.col("prev_p1_win_prob")).alias("prev_p2_win_prob"),
            "prev_predicted_at",
            pl.col("predicted_at").alias("updated_at"),
        )
        if log_path.exists():
            existing_log = pl.read_parquet(log_path)
            log_entry = pl.concat([existing_log, log_entry], how="diagonal_relaxed")
        log_entry.write_parquet(log_path)
        logger.info("Logged %d prediction changes (%d flips, %d drifts) to %s",
                     len(mismatched), n_flips, n_drifts, log_path.name)
