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

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model.calibration import PlattCalibrator
from mvp.model.confidence.dimensions import MODIFIERS
from mvp.model.config import (
    EnsembleParams,
    ExperimentConfig,
    apply_filters,
    get_filter_feature_specs,
)
from mvp.model.engine import FeatureEngine, get_feature_columns
from mvp.model.features.elo import surface_elo_expr
from mvp.model.imputation import apply_imputation, build_imputation, fit_imputation
from mvp.model.models import EnsembleModel, get_model
from mvp.model.registry import get_registry
from mvp.model.weighting import compute_sample_weights

logger = logging.getLogger(__name__)

MATCHES_PATH = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
CACHE_DIR = get_local_data_root() / "features" / "cache"
PREDICTIONS_PATH = get_data_root() / "predictions" / "predictions.parquet"
PRODUCTION_CONFIG_PATH = Path("production.yaml")

# Columns the predictor needs beyond what features reference
_PREDICTOR_EXTRA_COLS = [
    "won", "reason", "sets_played", "best_of",
    "circuit", "surface", "round", "draw_type",
    "tournament_id", "tournament_name",
    "player_first_name", "player_last_name",
    "opp_first_name", "opp_last_name",
    "player_display_name", "opp_display_name",
    "draw_p1_id", "scheduled_datetime", "match_date",
    "player_elo", "opp_elo",
    "player_hard_adj", "player_clay_adj", "player_grass_adj",
    "opp_hard_adj", "opp_clay_adj", "opp_grass_adj",
]

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
        *,
        target_section: str = "winner",
    ) -> None:
        self.production_config_path = Path(production_config_path)
        self.matches_path = Path(matches_path)
        self.cache_dir = Path(cache_dir)
        self.predictions_path = Path(predictions_path)
        self.target_section = target_section

        with open(self.production_config_path) as f:
            raw_config: dict[str, Any] = yaml.safe_load(f)

        # Support both flat (legacy) and sectioned (multi-target) config formats.
        # Flat: {active: ..., voters: [...]}
        # Sectioned: {winner: {active: ..., voters: [...]}, deciding_set: {...}}
        if "active" in raw_config:
            # Legacy flat format — treat as "winner" section
            self.full_config = raw_config
            self.config = raw_config
        else:
            self.full_config = raw_config
            if target_section not in raw_config:
                raise ValueError(
                    f"Target section '{target_section}' not found "
                    f"in {self.production_config_path}"
                )
            self.config = raw_config[target_section]

        self._experiment_config = ExperimentConfig.from_file(
            self.config["active"]["config"]
        )

    @property
    def target(self) -> str:
        """The target this predictor is configured for ('won' or 'deciding_set')."""
        return self._experiment_config.target

    def _resolve_target(self, df: pl.DataFrame) -> tuple[pl.DataFrame, str]:
        """Add the target column and filter invalid rows.

        Mirrors ExperimentRunner._resolve_target().
        """
        target = self.target
        if "reason" in df.columns:
            df = df.filter(pl.col("reason").fill_null("").ne("W/O"))
        if target == "won":
            return df, "won"
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
            return df, target_col
        raise ValueError(f"Unknown target: {target}")

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
        df = engine.compute(all_specs, extra_columns=_PREDICTOR_EXTRA_COLS)

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

        # Resolve target column and filter invalid rows
        df, target_col = self._resolve_target(df)

        # Drop rows without outcomes
        df = df.filter(pl.col(target_col).is_not_null())

        feature_cols = get_feature_columns(feature_specs)
        build_result = build_imputation(feature_specs, get_registry())
        augmented_cols = feature_cols + build_result.aux_base_col_names
        n_model = build_result.n_model_features

        X = df.select(pl.col(c).cast(pl.Float64) for c in augmented_cols).to_numpy()
        y = df[target_col].to_numpy().astype(int)

        logger.info("Training on %d rows with %d features", len(y), len(feature_cols))

        # Impute using per-feature strategy with circuit-stratified medians
        circuit = df["circuit"].to_numpy()
        impute_state = fit_imputation(X, circuit, build_result.specs)

        # Scaling stats from real data (before imputation), model cols only
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            scaler_mean = np.nanmean(X[:, :n_model], axis=0)
            scaler_std = np.nanstd(X[:, :n_model], axis=0)
        scaler_mean = np.where(np.isnan(scaler_mean), 0.0, scaler_mean)
        scaler_std = np.where(np.isnan(scaler_std), 1.0, scaler_std)
        scaler_std[scaler_std == 0] = 1.0

        X = apply_imputation(X, circuit, impute_state)
        X = X[:, :n_model]
        X = (X - scaler_mean) / scaler_std

        # Compute sample weights if configured
        sample_weights = None
        if config.sample_weight is not None:
            train_dates = df["effective_match_date"].to_numpy()
            sample_weights = compute_sample_weights(
                train_dates, config.sample_weight
            )

        # Train
        model = get_model(config.model.type, config.model.params or {})
        if is_ensemble and base_model_specs is not None:
            assert isinstance(model, EnsembleModel)
            model.configure(base_model_specs)
        model.fit(X, y, sample_weight=sample_weights)

        # Fit Platt calibrator via 5-fold CV on OOF predictions
        from sklearn.model_selection import StratifiedKFold

        oof_probs = np.zeros(len(y))
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        for train_idx, val_idx in skf.split(X, y):
            fold_model = get_model(config.model.type, config.model.params or {})
            if is_ensemble and base_model_specs is not None:
                assert isinstance(fold_model, EnsembleModel)
                fold_model.configure(base_model_specs)
            fold_weights = sample_weights[train_idx] if sample_weights is not None else None
            fold_model.fit(X[train_idx], y[train_idx], sample_weight=fold_weights)
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
                "impute_state": impute_state,
                "scaler": {"mean": scaler_mean, "std": scaler_std},
                "feature_cols": feature_cols,
                "calibrator": calibrator,
                "aux_base_col_names": build_result.aux_base_col_names,
                "target": self.target,
                # Backward compat: keep medians for old code paths
                "medians": impute_state.global_medians[:n_model],
            },
            artifact_path,
        )

        # Update trained_at in config
        entry["trained_at"] = datetime.now(timezone.utc).isoformat()
        with open(self.production_config_path, "w") as f:
            yaml.dump(self.full_config, f, default_flow_style=False)

        logger.info("Model saved to %s", artifact_path)

    def train(self) -> None:
        """Train production model on all matching data and save artifact."""
        self._train_single(self.config["active"])

    def load(self) -> dict[str, Any]:
        """Load the trained production model.

        Returns:
            Dict with model artifact contents.

        Raises:
            FileNotFoundError: If no trained artifact exists.
        """
        artifact_path = Path(self.config["active"]["artifact"])
        if not artifact_path.exists():
            raise FileNotFoundError(
                f"No trained model at {artifact_path}. Run with --train first."
            )

        artifact = joblib.load(artifact_path)
        if artifact.get("calibrator") is None:
            logger.warning("No calibrator in artifact — predictions will be uncalibrated")
        return artifact

    def _load_single(self, entry: dict) -> dict[str, Any]:
        """Load a trained model artifact from an entry dict."""
        artifact_path = Path(entry["artifact"])
        if not artifact_path.exists():
            raise FileNotFoundError(f"No trained model at {artifact_path}")
        return joblib.load(artifact_path)

    def _predict_raw(
        self,
        entry: dict,
        tournament_keys: list[tuple[str, int]] | None,
        match_uids: set[str],
        scoped: bool = False,
    ) -> dict[str, float]:
        """Generate raw uid->prob predictions for a single model entry.

        Only predicts matches whose uid is in match_uids (intersection with
        production predictions). When scoped=True, additionally excludes
        matches that don't pass the entry's config filters (the voter only
        votes on matches within its training domain).

        Returns {match_uid: p1_win_prob}.
        """
        artifact = self._load_single(entry)
        model = artifact["model"]
        feature_cols = artifact["feature_cols"]
        calibrator = artifact.get("calibrator")
        config, feature_specs, _ = self._resolve_entry_features(entry)
        engine = FeatureEngine(
            matches_path=self.matches_path, cache_dir=self.cache_dir
        )

        # Compute features — include filter-referenced features so scoping
        # can evaluate filter columns (e.g., player_elo_surface_diff)
        compute_only = (
            config.features.compute_only
            if config.features and config.features.compute_only
            else []
        )
        filter_specs = get_filter_feature_specs(config.data.filters) if scoped else []
        extra = compute_only + filter_specs
        all_specs = feature_specs + [s for s in extra if s not in feature_specs]
        df = engine.compute(all_specs, extra_columns=_PREDICTOR_EXTRA_COLS)

        # Determine in-scope match UIDs for scoped voters
        in_scope_uids: set[str] | None = None
        if scoped and config.data.filters:
            scoped_df = apply_filters(df, config.data.filters)
            in_scope_uids = set(scoped_df["match_uid"].unique().to_list())

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
        aux_cols = artifact.get("aux_base_col_names", [])
        augmented_cols = list(feature_cols) + aux_cols
        X = canonical.select(
            pl.col(c).cast(pl.Float64) for c in augmented_cols
        ).to_numpy()
        circuit_arr = canonical["circuit"].to_numpy()
        if "impute_state" in artifact:
            X = apply_imputation(X, circuit_arr, artifact["impute_state"])
            X = X[:, :len(feature_cols)]
            scaler = artifact["scaler"]
            X = (X - scaler["mean"]) / scaler["std"]
        else:
            # Backward compat: old artifact without impute_state
            medians = artifact["medians"]
            X = np.where(np.isnan(X), medians, X)
        probs = model.predict_proba(X)
        if calibrator is not None:
            probs = calibrator.transform(probs)

        # For non-canonical rows that were included, flip the prob (winner only;
        # deciding_set prob is symmetric — no flip needed)
        is_deciding_set = self.target == "deciding_set"
        result: dict[str, float] = {}
        for i, row in enumerate(canonical.iter_rows(named=True)):
            uid = row["match_uid"]
            # Scoped voters skip matches outside their training domain
            if in_scope_uids is not None and uid not in in_scope_uids:
                continue
            p = float(probs[i])
            is_canonical = row["_is_canonical"]
            if not is_canonical and not is_deciding_set:
                p = 1.0 - p
            result[uid] = p

        return result

    def train_voters(self) -> int:
        """Train all voter models using each config's native filters."""
        voters = self.config.get("voters", [])
        for voter in voters:
            name = voter.get("name", "unnamed")
            logger.info("Training voter: %s", name)
            # Build training entry: config's own data.filters + voter's date range/artifact
            voter_config = ExperimentConfig.from_file(voter["config"])
            train_entry = {
                "config": voter["config"],
                "artifact": voter["artifact"],
                "train_date_range": {
                    "start": voter_config.data.date_range.start.isoformat(),
                    "end": voter_config.data.date_range.end.isoformat(),
                },
                "filters": voter_config.data.filters,
            }
            self._train_single(train_entry)
        return len(voters)

    def predict_voters(
        self,
        tournament_keys: list[tuple[str, int]] | None,
        predictions: pl.DataFrame,
    ) -> pl.DataFrame:
        """Add consensus and voter_count columns from voter models.

        Scoped voters (scoped: true in config) only vote on matches that
        pass their training filters. Unscoped voters vote on everything.

        Args:
            tournament_keys: Tournament scope (same as predict()).
            predictions: DataFrame from predict() with p1_win_prob.

        Returns:
            predictions with added 'consensus' and 'voter_count' columns.
        """
        voters = self.config.get("voters", [])
        if not voters:
            return predictions.with_columns(
                pl.lit(None).cast(pl.Float64).alias("consensus"),
                pl.lit(None).cast(pl.Int64).alias("voter_count"),
            )

        match_uids = set(predictions["match_uid"].to_list())

        # Production binary pick per match
        is_ds = self.target == "deciding_set"
        prob_col = "deciding_set_prob" if is_ds else "p1_win_prob"
        prod_picks: dict[str, bool] = {}
        for row in predictions.iter_rows(named=True):
            prod_picks[row["match_uid"]] = row[prob_col] >= 0.5

        # Collect voter picks (scoped voters will have fewer UIDs)
        voter_picks: list[dict[str, bool]] = []
        for voter in voters:
            is_scoped = voter.get("scoped", False)
            raw = self._predict_raw(
                voter, tournament_keys, match_uids, scoped=is_scoped
            )
            picks = {uid: prob >= 0.5 for uid, prob in raw.items()}
            voter_picks.append(picks)

        # Build consensus (decimal) and voter_count
        consensus_values: list[float] = []
        voter_count_values: list[int] = []
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
            voter_count_values.append(total)

        return predictions.with_columns(
            pl.Series("consensus", consensus_values),
            pl.Series("voter_count", voter_count_values),
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
        artifact = self.load()
        model = artifact["model"]
        feature_cols = artifact["feature_cols"]
        calibrator = artifact.get("calibrator")
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
        df = engine.compute(all_specs, extra_columns=_PREDICTOR_EXTRA_COLS)

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
        aux_cols = artifact.get("aux_base_col_names", [])
        augmented_cols = list(feature_cols) + aux_cols
        X = pending.select(
            pl.col(c).cast(pl.Float64) for c in augmented_cols
        ).to_numpy()
        circuit_arr = pending["circuit"].to_numpy()
        if "impute_state" in artifact:
            X = apply_imputation(X, circuit_arr, artifact["impute_state"])
            X = X[:, :len(feature_cols)]
            scaler = artifact["scaler"]
            X = (X - scaler["mean"]) / scaler["std"]
        else:
            # Backward compat: old artifact without impute_state
            medians = artifact["medians"]
            X = np.where(np.isnan(X), medians, X)
        probs = model.predict_proba(X)
        if calibrator is not None:
            probs = calibrator.transform(probs)

        # Add probabilities to pending matches
        pending = pending.with_columns(pl.Series("_prob", probs))

        is_deciding_set = self.target == "deciding_set"

        # Compute surface-adjusted Elo before the canonical split
        if not is_deciding_set:
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

        # For matches where we only have the non-canonical row
        if len(non_canonical) > 0:
            seen_uids = set(canonical["match_uid"].to_list())
            missing = non_canonical.filter(
                ~pl.col("match_uid").is_in(list(seen_uids))
            )
            if len(missing) > 0:
                if is_deciding_set:
                    # Deciding set prob is symmetric — just swap player/opp identity
                    missing = missing.with_columns(
                        pl.col("opp_id").alias("_tmp_player_id"),
                        pl.col("player_id").alias("_tmp_opp_id"),
                        pl.col("opp_first_name").alias("_tmp_pfn"),
                        pl.col("opp_last_name").alias("_tmp_pln"),
                        pl.col("player_first_name").alias("_tmp_ofn"),
                        pl.col("player_last_name").alias("_tmp_oln"),
                    ).with_columns(
                        pl.col("_tmp_player_id").alias("player_id"),
                        pl.col("_tmp_opp_id").alias("opp_id"),
                        pl.col("_tmp_pfn").alias("player_first_name"),
                        pl.col("_tmp_pln").alias("player_last_name"),
                        pl.col("_tmp_ofn").alias("opp_first_name"),
                        pl.col("_tmp_oln").alias("opp_last_name"),
                    )
                else:
                    # Winner prob is directional — flip prob and swap identities
                    missing = missing.with_columns(
                        (1.0 - pl.col("_prob")).alias("_prob"),
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

        # Compute confidence modifier values for Layer 2 enrichment (winner only)
        if not is_deciding_set:
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

        # Bio name (first + last) preferred; schedule/results display_name as fallback
        _p1_name_expr = pl.coalesce(
            pl.col("player_first_name") + pl.lit(" ") + pl.col("player_last_name"),
            pl.col("player_display_name"),
        ).alias("p1_name")
        _p2_name_expr = pl.coalesce(
            pl.col("opp_first_name") + pl.lit(" ") + pl.col("opp_last_name"),
            pl.col("opp_display_name"),
        ).alias("p2_name")

        if is_deciding_set:
            select_exprs = [
                pl.col("match_uid"),
                pl.col("player_id").alias("p1_id"),
                pl.col("opp_id").alias("p2_id"),
                _p1_name_expr,
                _p2_name_expr,
                pl.col("_prob").alias("deciding_set_prob"),
                pl.col("tournament_id"),
                pl.col("tournament_name"),
                pl.col("circuit"),
                pl.col("surface"),
                pl.col("round"),
                pl.col("effective_match_date"),
                pl.lit(model_version).alias("model_version"),
                pl.lit(now).alias("predicted_at"),
            ]
        else:
            select_exprs = [
                pl.col("match_uid"),
                pl.col("player_id").alias("p1_id"),
                pl.col("opp_id").alias("p2_id"),
                _p1_name_expr,
                _p2_name_expr,
                pl.col("_prob").alias("p1_win_prob"),
                (1.0 - pl.col("_prob")).alias("p2_win_prob"),
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
        if not is_deciding_set:
            for col in canonical.columns:
                if col.startswith("conf_"):
                    select_exprs.append(pl.col(col))
        result = canonical.select(select_exprs)

        logger.info("Generated %d %s predictions", len(result), self.target)
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
            is_ds = self.target == "deciding_set"
            prob_col = "deciding_set_prob" if is_ds else "p1_win_prob"
            prev_prob_col = f"prev_{prob_col}"
            updated_uids: set[str] = set()
            if len(overlap) > 0 and prob_col in existing.columns:
                merged = overlap.select("match_uid", prob_col, "predicted_at").join(
                    existing.select(
                        "match_uid",
                        pl.col(prob_col).alias(prev_prob_col),
                        pl.col("predicted_at").alias("prev_predicted_at"),
                    ),
                    on="match_uid",
                    how="inner",
                )
                diffs = (merged[prob_col] - merged[prev_prob_col]).abs()
                mismatched = merged.filter(diffs > PREDICTION_TOLERANCE)
                if len(mismatched) > 0:
                    updated_uids = set(mismatched["match_uid"].to_list())
                    logger.info(
                        "Updating %d predictions with changed probabilities (max diff: %.6f)",
                        len(updated_uids),
                        diffs.filter(diffs > PREDICTION_TOLERANCE).max(),
                    )
                    self._log_prediction_changes(mismatched, prob_col=prob_col)

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

    def _log_prediction_changes(
        self, mismatched: pl.DataFrame, prob_col: str = "p1_win_prob"
    ) -> None:
        """Append changed predictions to the prediction drift log and emit alerts."""
        log_path = self.predictions_path.parent / "prediction_drift.parquet"
        prev_col = f"prev_{prob_col}"

        diff_abs = (mismatched[prob_col] - mismatched[prev_col]).abs()
        # Detect flip: previous and current on opposite sides of 0.5
        flipped = (
            (mismatched[prev_col] > 0.5) & (mismatched[prob_col] < 0.5)
        ) | (
            (mismatched[prev_col] < 0.5) & (mismatched[prob_col] > 0.5)
        )

        n_flips = flipped.sum()
        n_drifts = (diff_abs >= DRIFT_THRESHOLD).sum() - n_flips

        if n_flips > 0:
            flip_rows = mismatched.filter(flipped)
            for row in flip_rows.iter_rows(named=True):
                logger.warning(
                    "FLIP %s: %.1f%% -> %.1f%%",
                    row["match_uid"],
                    row[prev_col] * 100,
                    row[prob_col] * 100,
                )

        if n_drifts > 0:
            drift_rows = mismatched.filter((diff_abs >= DRIFT_THRESHOLD) & ~flipped)
            for row in drift_rows.iter_rows(named=True):
                logger.info(
                    "DRIFT %s: %.1f%% -> %.1f%%",
                    row["match_uid"],
                    row[prev_col] * 100,
                    row[prob_col] * 100,
                )

        log_cols = [
            "match_uid",
            pl.col(prob_col),
            pl.col(prev_col),
            "prev_predicted_at",
            pl.col("predicted_at").alias("updated_at"),
        ]
        if prob_col == "p1_win_prob":
            log_cols.insert(2, (1 - pl.col(prob_col)).alias("p2_win_prob"))
            log_cols.insert(4, (1 - pl.col(prev_col)).alias("prev_p2_win_prob"))
        log_entry = mismatched.select(log_cols)
        if log_path.exists():
            existing_log = pl.read_parquet(log_path)
            log_entry = pl.concat([existing_log, log_entry], how="diagonal_relaxed")
        log_entry.write_parquet(log_path)
        logger.info("Logged %d prediction changes (%d flips, %d drifts) to %s",
                     len(mismatched), n_flips, n_drifts, log_path.name)
