"""Production model: train, save, load, and predict."""


import json
import logging
import warnings
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import polars as pl
import yaml
from dateutil.relativedelta import relativedelta

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    SegmentedIsotonicCalibrator,
    SegmentedPlattCalibrator,
    fit_calibrator_with_nested_cv,
    make_calibrator,
)
from mvp.model.completeness import is_incomplete_match
from mvp.model.confidence.dimensions import MODIFIERS
from mvp.model.config import (
    CalibrationConfig,
    EnsembleParams,
    ExperimentConfig,
    apply_filters,
    get_filter_feature_specs,
)
from mvp.model.diagnostics import Diagnostics
from mvp.model.engine import FeatureEngine, get_feature_columns
from mvp.model.features._score_helpers import (
    sets_lost as _sets_lost,
    sets_won as _sets_won,
    total_games_lost as _total_games_lost,
    total_games_won as _total_games_won,
)
from mvp.model.features.elo import surface_elo_expr
from mvp.model.imputation import apply_imputation, build_imputation, fit_imputation
from mvp.model.models import EnsembleModel, XGBoostMTLModel, get_model
from mvp.model.registry import get_registry
from mvp.model.splitters import make_splitter
from mvp.model.weighting import compute_sample_weights

logger = logging.getLogger(__name__)

MATCHES_PATH = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
CACHE_DIR = get_local_data_root() / "features" / "cache"
PREDICTIONS_PATH = get_data_root() / "predictions" / "predictions.parquet"
PRODUCTION_CONFIG_PATH = Path("production.yaml")

# Columns the predictor needs beyond what features reference
_PREDICTOR_EXTRA_COLS = [
    "won", "reason", "result_type", "sets_played", "best_of",
    "circuit", "surface", "round", "draw_type",
    "tournament_id", "tournament_name",
    "player_first_name", "player_last_name",
    "opp_first_name", "opp_last_name",
    "player_display_name", "opp_display_name",
    "draw_p1_id", "scheduled_datetime", "match_date", "schedule_day",
    "player_elo", "opp_elo",
    "player_hard_adj", "player_clay_adj", "player_grass_adj",
    "opp_hard_adj", "opp_clay_adj", "opp_grass_adj",
]

# Tolerance for prediction consistency checks
PREDICTION_TOLERANCE = 1e-4
# Threshold for drift alerts (5% probability swing)
DRIFT_THRESHOLD = 0.05


def _resolve_artifact_path(raw: str | Path) -> Path:
    """Resolve an artifact path so `data/...` always means the data root.

    production.yaml carries paths like `data/models/X.joblib`. On boxes where
    MVP_DATA_ROOT points elsewhere (e.g., B:/), a literal `data/` would write
    to the repo's local dir and miss the shared location the live pipeline
    reads from.
    """
    p = Path(raw)
    if p.is_absolute():
        return p
    parts = p.parts
    if parts and parts[0] == "data":
        return get_data_root().joinpath(*parts[1:])
    return p


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

    def _resolve_target(
        self, df: pl.DataFrame, config: ExperimentConfig | None = None,
    ) -> tuple[pl.DataFrame, list[str]]:
        """Add target column(s) and filter invalid rows.

        Mirrors ExperimentRunner._resolve_target() including MTL parity:
        primary target at index 0; auxiliary regression targets appended when
        `config.mtl` is set. Backward compat: callers that only need the
        primary use `target_cols[0]`.

        Walkovers always excluded. When MTL is active, additionally exclude
        RET/DEF/UNP and require sets_played not null because aux targets
        require completed match scores.

        Args:
            df: input dataframe.
            config: experiment config to read target / mtl from. Falls back to
                `self._experiment_config` for backward compat with callers
                that didn't pass one (e.g., production-prediction paths that
                don't load per-entry configs).
        """
        cfg = config if config is not None else self._experiment_config
        target = cfg.target
        mtl_cfg = getattr(cfg, "mtl", None)

        # Primary completeness filter: walkovers always; RET/DEF/UNP when MTL.
        # This path resolves TRAINING data (deploy fit + calibration CV), so the
        # MTL completeness gate is correct here — the fit needs valid aux labels.
        df = df.filter(~is_incomplete_match(df.columns, mtl_cfg is not None))

        # When MTL is active, also require sets_played not null (necessary for
        # any aux target derivation; the dropna gate below catches per-aux
        # edge cases).
        if mtl_cfg is not None:
            df = df.filter(pl.col("sets_played").is_not_null())

        # Resolve primary target column
        if target == "won":
            primary_col = "won"
        elif target == "deciding_set":
            primary_col = "_target_deciding_set"
            df = df.filter(pl.col("sets_played").is_not_null())
            # When MTL is active, RET/DEF/UNP are already filtered above —
            # this branch becomes a no-op then.
            if "reason" in df.columns and mtl_cfg is None:
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
            df = df.drop_nulls(subset=aux_cols)
            target_cols.extend(aux_cols)

        return df, target_cols

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
        # MTL: aux target derivation reads raw matches.parquet columns directly.
        # Extend the base projection list when the entry's config has an mtl block.
        extra_cols_eff = list(_PREDICTOR_EXTRA_COLS)
        if config.mtl is not None:
            for i in range(1, 6):
                for prefix in ("player", "opp"):
                    col = f"{prefix}_set{i}_games"
                    if col not in extra_cols_eff:
                        extra_cols_eff.append(col)
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
            for aux_name in config.mtl.auxiliary_targets:
                for col in aux_required.get(aux_name, []):
                    if col not in extra_cols_eff:
                        extra_cols_eff.append(col)
        df = engine.compute(all_specs, extra_columns=extra_cols_eff)

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

        # Resolve target column(s). Primary at index 0; aux targets appended
        # when config.mtl is set. Single-task path: target_cols has length 1
        # and target_col aliases it (unchanged behavior).
        df, target_cols = self._resolve_target(df, config=config)
        target_col = target_cols[0]
        is_mtl = config.mtl is not None

        # Drop rows without outcomes
        df = df.filter(pl.col(target_col).is_not_null())

        # For date_sliding validation, the deployed model fits on the last
        # train_months window — same shape as one sliding fold — while
        # temporal CV continues to span the full pool. Matches live
        # deployment: a re-trained sliding-window artifact in production
        # only ever sees the last train_months of data, but the calibrator
        # gets richer signal by fitting on OOF preds from many such folds.
        val_cfg = config.validation
        if (
            val_cfg is not None
            and val_cfg.type == "date_sliding"
            and val_cfg.train_months
        ):
            deploy_start = (end + timedelta(days=1)) - relativedelta(
                months=val_cfg.train_months
            )
            if deploy_start < start:
                deploy_start = start
            df_deploy = df.filter(pl.col("effective_match_date") >= deploy_start)
            logger.info(
                "Sliding-window deploy fit: %s..%s (%d rows); "
                "full pool (%d rows) used for temporal CV calibration",
                deploy_start.date(), end.date(), len(df_deploy), len(df),
            )
        else:
            df_deploy = df

        feature_cols = get_feature_columns(feature_specs)
        build_result = build_imputation(feature_specs, get_registry())
        augmented_cols = feature_cols + build_result.aux_base_col_names
        n_model = build_result.n_model_features

        # Validate impute=None features only reach NaN-tolerant models.
        # No-op when no feature declares impute=None.
        from mvp.model.imputation import validate_impute_compat
        validate_impute_compat(
            build_result.specs,
            feature_cols,
            config.model.type,
            base_model_specs=base_model_specs,
        )

        X = df_deploy.select(pl.col(c).cast(pl.Float64) for c in augmented_cols).to_numpy()
        y = df_deploy[target_col].to_numpy().astype(int)
        # MTL: 2D y for the multi-target fit (primary + aux columns). Single-
        # task path: y_for_fit aliases y (1D). The 1D `y` stays the canonical
        # primary label for calibrator fits and OOF buffers downstream.
        y_for_fit = (
            df_deploy.select(target_cols).to_numpy() if is_mtl else y
        )

        logger.info("Training on %d rows with %d features", len(y), len(feature_cols))

        # Impute using per-feature strategy with circuit-stratified medians
        circuit = df_deploy["circuit"].to_numpy()
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

        # Append embedding column (integer-encoded, not scaled)
        embedding_col = None
        embedding_vocab = None
        model_params = config.model.params or {}
        if model_params.get("embedding_col"):
            embedding_col = model_params["embedding_col"]
            min_matches = model_params.get("min_player_matches", 10)
            player_counts = df_deploy[embedding_col].value_counts()
            eligible = player_counts.filter(pl.col("count") >= min_matches)
            embedding_vocab = {
                pid: idx + 1
                for idx, pid in enumerate(eligible[embedding_col].to_list())
            }
            emb_ids = np.array(
                [embedding_vocab.get(p, 0) for p in df_deploy[embedding_col].to_list()]
            ).reshape(-1, 1)
            X = np.hstack([X, emb_ids.astype(np.float64)])
            model_params["embedding_col_idx"] = X.shape[1] - 1
            model_params["n_players"] = len(embedding_vocab)

        # Compute sample weights if configured
        sample_weights = None
        if config.sample_weight is not None:
            train_dates = df_deploy["effective_match_date"].to_numpy()
            sample_weights = compute_sample_weights(
                train_dates, config.sample_weight
            )

        # Train. MTL: dispatch to XGBoostMTLModel directly (model.type stays
        # "xgboost" in config; routing is a training-path decision). Aligns
        # with ExperimentRunner's MTL branch — same model class, same 2D y,
        # same weight_{target_name} extraction.
        if is_mtl:
            assert config.mtl is not None
            target_names_mtl = [config.target, *config.mtl.auxiliary_targets]
            model = XGBoostMTLModel(
                config.model.params or {},
                target_names=target_names_mtl,
                feature_names=feature_cols,
            )
        else:
            model = get_model(
                config.model.type,
                config.model.params or {},
                feature_names=feature_cols,
            )
        if is_ensemble and base_model_specs is not None:
            assert isinstance(model, EnsembleModel)
            model.configure(base_model_specs)
        model.fit(X, y_for_fit, sample_weight=sample_weights)

        # Fit Platt calibrator on OOF predictions. Prefer the same temporal CV
        # that `mvp model` uses (per the validation block in the model YAML)
        # so the calibrator and the cal_tiers sidecar align with what
        # diagnostics evaluate. Fall back to random K-fold for configs
        # (embedding-using) where per-fold prep would need extra wiring.
        cal_cfg = config.calibration
        can_temporal_cv = (
            val_cfg is not None
            and val_cfg.type
            in {"expanding_window", "sliding_window", "date_sliding", "date_expanding"}
            and embedding_col is None
        )

        calibrated_preds: list[dict[str, Any]] | None = None

        if can_temporal_cv:
            splitter = make_splitter(
                val_type=val_cfg.type,
                n_splits=val_cfg.n_splits,
                min_train_size=val_cfg.min_train_size,
                test_size=val_cfg.test_size,
                initial_train_size=val_cfg.initial_train_size,
                step_size=val_cfg.step_size,
                train_size=val_cfg.train_size,
                test_start=val_cfg.test_start,
                train_months=val_cfg.train_months,
                initial_train_months=val_cfg.initial_train_months,
                test_months=val_cfg.test_months,
            )
            fold_predictions: list[dict[str, Any]] = []
            # Per-sub fold predictions (ensemble only). Outer index is sub
            # idx; each entry is a list of fold dicts shaped for
            # fit_calibrator_with_nested_cv. Populated in lockstep with
            # fold_predictions so indexes align by fold position.
            n_subs_ens = (
                len(base_model_specs)
                if (is_ensemble and base_model_specs is not None)
                else 0
            )
            per_sub_fold_predictions: list[list[dict[str, Any]]] = [
                [] for _ in range(n_subs_ens)
            ]
            for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(df)):
                train_df = df[train_idx]
                test_df = df[test_idx]

                X_train_raw = train_df.select(
                    pl.col(c).cast(pl.Float64) for c in augmented_cols
                ).to_numpy()
                y_train_fold = train_df[target_col].to_numpy().astype(int)
                # MTL: 2D y for the fold model fit. Single-task: alias to the
                # 1D primary. y_test_fold and y_train_fold (1D) remain the
                # canonical labels for calibrator fitting and metrics.
                y_train_fold_for_fit = (
                    train_df.select(target_cols).to_numpy()
                    if is_mtl else y_train_fold
                )
                X_test_raw = test_df.select(
                    pl.col(c).cast(pl.Float64) for c in augmented_cols
                ).to_numpy()
                y_test_fold = test_df[target_col].to_numpy().astype(int)

                circuit_train_fold = train_df["circuit"].to_numpy()
                circuit_test_fold = test_df["circuit"].to_numpy()
                impute_fold = fit_imputation(
                    X_train_raw, circuit_train_fold, build_result.specs
                )

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", RuntimeWarning)
                    mean_fold = np.nanmean(X_train_raw[:, :n_model], axis=0)
                    std_fold = np.nanstd(X_train_raw[:, :n_model], axis=0)
                mean_fold = np.where(np.isnan(mean_fold), 0.0, mean_fold)
                std_fold = np.where(np.isnan(std_fold), 1.0, std_fold)
                std_fold[std_fold == 0] = 1.0

                X_train_fold = apply_imputation(
                    X_train_raw, circuit_train_fold, impute_fold
                )
                X_test_fold = apply_imputation(
                    X_test_raw, circuit_test_fold, impute_fold
                )
                X_train_fold = X_train_fold[:, :n_model]
                X_test_fold = X_test_fold[:, :n_model]
                X_train_fold = (X_train_fold - mean_fold) / std_fold
                X_test_fold = (X_test_fold - mean_fold) / std_fold

                fold_weights = None
                if config.sample_weight is not None:
                    train_dates_fold = train_df["effective_match_date"].to_numpy()
                    fold_weights = compute_sample_weights(
                        train_dates_fold, config.sample_weight
                    )

                if is_mtl:
                    assert config.mtl is not None
                    fold_model = XGBoostMTLModel(
                        config.model.params or {},
                        target_names=[config.target, *config.mtl.auxiliary_targets],
                        feature_names=feature_cols,
                    )
                else:
                    fold_model = get_model(
                        config.model.type,
                        config.model.params or {},
                        feature_names=feature_cols,
                    )
                if is_ensemble and base_model_specs is not None:
                    assert isinstance(fold_model, EnsembleModel)
                    fold_model.configure(base_model_specs)
                fold_model.fit(
                    X_train_fold, y_train_fold_for_fit, sample_weight=fold_weights
                )
                if is_ensemble:
                    assert isinstance(fold_model, EnsembleModel)
                    y_prob_test = fold_model.predict_proba(
                        X_test_fold, df=test_df
                    )
                    sub_preds = fold_model.predict_proba_per_model(
                        X_test_fold, df=test_df
                    )
                    for s, sp in enumerate(sub_preds):
                        per_sub_fold_predictions[s].append({
                            "df": test_df,
                            "y_true": y_test_fold,
                            "y_prob": sp,
                        })
                else:
                    y_prob_test = fold_model.predict_proba(X_test_fold)

                fold_predictions.append({
                    "df": test_df,
                    "y_true": y_test_fold,
                    "y_prob": y_prob_test,
                })
                logger.info(
                    "Temporal CV fold %d: train=%d, test=%d",
                    fold_idx + 1, len(train_df), len(test_df),
                )

            if not fold_predictions:
                raise RuntimeError(
                    f"Temporal CV ({val_cfg.type}) produced no folds"
                )

            # Per-sub calibration (ensemble only). For each sub with a
            # configured cal, run nested CV over its OOF: fit the deployed
            # cal on all folds (attached for inference) AND mutate each
            # fold's per-sub y_prob to a nested-CV-cal'd value. Then re-
            # average ensemble fold y_prob from the now-cal'd per-sub
            # outputs so the top-level nested CV below fits on deployment-
            # shape values (raw sub → sub cal → average → top cal). This
            # mirrors what the `mvp model` runner does for honest cal_tiers.
            has_any_sub_cal = is_ensemble and any(
                cfg is not None for cfg in getattr(model, "_sub_cal_configs", [])
            )
            if has_any_sub_cal:
                assert isinstance(model, EnsembleModel)
                for sub_idx in range(n_subs_ens):
                    sub_cal_cfg = model._sub_cal_configs[sub_idx]
                    if sub_cal_cfg is None:
                        continue
                    sub_deployed_cal = fit_calibrator_with_nested_cv(
                        per_sub_fold_predictions[sub_idx], sub_cal_cfg
                    )
                    model.set_sub_calibrator(sub_idx, sub_deployed_cal)

                strategy = (config.model.params or {}).get("strategy", "average")
                for fold_idx, ensemble_pred_dict in enumerate(fold_predictions):
                    sub_outs = np.array([
                        per_sub_fold_predictions[s][fold_idx]["y_prob"]
                        for s in range(n_subs_ens)
                    ])
                    if strategy == "weighted_average":
                        ensemble_pred_dict["y_prob"] = np.average(
                            sub_outs, axis=0, weights=model._weights
                        )
                    else:
                        ensemble_pred_dict["y_prob"] = np.mean(sub_outs, axis=0)

            # Nested-CV calibration for honest diagnostics. Each fold's
            # preds get transformed by a calibrator fit on the OTHER folds —
            # the cal_tiers sidecar emitted below uses these honest preds
            # rather than in-sample-fit ones. Critical for high-DoF
            # calibrators (isotonic) which would otherwise trivially
            # produce near-zero per-cell residuals via overfit. The
            # returned deployed calibrator (fit on all OOF) is saved on
            # the artifact for inference-time use.
            effective_cal_cfg = cal_cfg if cal_cfg is not None else CalibrationConfig()
            calibrator = fit_calibrator_with_nested_cv(
                fold_predictions, effective_cal_cfg
            )
            if isinstance(
                calibrator,
                (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
            ):
                logger.info(
                    "Segmented %s (temporal CV, nested): "
                    "%d per-segment fits + global fallback",
                    type(calibrator).__name__, calibrator.n_segments,
                )
            elif isinstance(calibrator, PlattCalibrator):
                logger.info(
                    "Platt calibrator (temporal CV, nested): "
                    "slope=%.4f, intercept=%.4f",
                    calibrator.slope, calibrator.intercept,
                )
            elif isinstance(calibrator, IsotonicCalibrator):
                logger.info(
                    "Isotonic calibrator (temporal CV, nested): "
                    "n_thresholds=%d, y range=[%.4f, %.4f]",
                    calibrator.n_thresholds, calibrator.y_min, calibrator.y_max,
                )

            calibrated_preds = fold_predictions
        else:
            fallback_reason = (
                "no validation block" if val_cfg is None
                else "embedding configured" if embedding_col is not None
                else f"validation type '{val_cfg.type}' unsupported here"
            )
            logger.warning(
                "Falling back to random K-fold for Platt fit (reason: %s); "
                "cal_tiers sidecar will not be written",
                fallback_reason,
            )
            from sklearn.model_selection import StratifiedKFold

            n_subs = (
                len(base_model_specs)
                if (is_ensemble and base_model_specs is not None)
                else 0
            )
            oof_probs = np.zeros(len(y))
            # Per-sub OOF buffers (ensemble only). Each entry accumulates raw
            # sub probabilities at val_idx positions across folds. After the
            # loop, we fit each sub's deployed calibrator on its full OOF
            # vector (in-sample fit — fallback path has no nested CV).
            per_sub_oof_probs: list[np.ndarray] = (
                [np.zeros(len(y)) for _ in range(n_subs)] if is_ensemble else []
            )
            skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
            for train_idx, val_idx in skf.split(X, y):
                if is_mtl:
                    assert config.mtl is not None
                    fold_model = XGBoostMTLModel(
                        config.model.params or {},
                        target_names=[config.target, *config.mtl.auxiliary_targets],
                        feature_names=feature_cols,
                    )
                else:
                    fold_model = get_model(
                        config.model.type,
                        config.model.params or {},
                        feature_names=feature_cols,
                    )
                if is_ensemble and base_model_specs is not None:
                    assert isinstance(fold_model, EnsembleModel)
                    fold_model.configure(base_model_specs)
                fold_weights = sample_weights[train_idx] if sample_weights is not None else None
                # MTL: y_for_fit is 2D; slicing by train_idx preserves shape.
                # Single-task: y_for_fit is 1D primary (aliased to y).
                fold_model.fit(
                    X[train_idx], y_for_fit[train_idx], sample_weight=fold_weights,
                )
                if is_ensemble and n_subs > 0:
                    assert isinstance(fold_model, EnsembleModel)
                    sub_preds = fold_model.predict_proba_per_model(X[val_idx])
                    for s, sp in enumerate(sub_preds):
                        per_sub_oof_probs[s][val_idx] = sp
                    # Ensemble OOF computed after per-sub cals are fit (below).
                    # For now stash raw average; will be overwritten if sub-cal
                    # is configured.
                    oof_probs[val_idx] = np.mean(sub_preds, axis=0)
                else:
                    oof_probs[val_idx] = fold_model.predict_proba(X[val_idx])

            # Per-sub calibration (ensemble-only). Fit each sub's deployed
            # calibrator on its OOF and attach to model. Then recompute
            # ensemble OOF = mean of sub-cal'd outputs so top-level cal fits
            # on deployment-shape values (raw sub → sub cal → average → top cal).
            # Fallback path uses in-sample fits (no nested CV) — matches the
            # existing fallback semantics for top-level cal at this site.
            has_any_sub_cal = is_ensemble and any(
                cfg is not None for cfg in getattr(model, "_sub_cal_configs", [])
            )
            if has_any_sub_cal:
                assert isinstance(model, EnsembleModel)
                for sub_idx in range(n_subs):
                    sub_cal_cfg = model._sub_cal_configs[sub_idx]
                    if sub_cal_cfg is None:
                        continue
                    sub_cal = make_calibrator(sub_cal_cfg)
                    if isinstance(
                        sub_cal,
                        (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
                    ):
                        # df_deploy is aligned with per_sub_oof_probs (both
                        # index by val_idx positions in df_deploy across folds)
                        sub_cal.fit(per_sub_oof_probs[sub_idx], y, df_deploy)
                    else:
                        sub_cal.fit(per_sub_oof_probs[sub_idx], y)
                    model.set_sub_calibrator(sub_idx, sub_cal)
                # Recompute ensemble OOF with sub cals applied
                strategy = (config.model.params or {}).get("strategy", "average")
                calibrated_per_sub = []
                for s in range(n_subs):
                    cal = model._sub_calibrators[s]
                    if cal is None:
                        calibrated_per_sub.append(per_sub_oof_probs[s])
                    elif isinstance(
                        cal,
                        (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
                    ):
                        calibrated_per_sub.append(cal.transform(per_sub_oof_probs[s], df_deploy))
                    else:
                        calibrated_per_sub.append(cal.transform(per_sub_oof_probs[s]))
                stacked = np.array(calibrated_per_sub)
                if strategy == "weighted_average":
                    oof_probs = np.average(stacked, axis=0, weights=model._weights)
                else:
                    oof_probs = np.mean(stacked, axis=0)

            calibrator = make_calibrator(cal_cfg) if cal_cfg else PlattCalibrator()
            if isinstance(
                calibrator,
                (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
            ):
                calibrator.fit(oof_probs, y, df_deploy)
                logger.info(
                    "Segmented %s: %d per-segment fits + global fallback",
                    type(calibrator).__name__, calibrator.n_segments,
                )
            else:
                calibrator.fit(oof_probs, y)
                if isinstance(calibrator, PlattCalibrator):
                    logger.info(
                        "Platt calibrator: slope=%.4f, intercept=%.4f",
                        calibrator.slope, calibrator.intercept,
                    )
                else:
                    logger.info(
                        "Isotonic calibrator: n_thresholds=%d, y range=[%.4f, %.4f]",
                        calibrator.n_thresholds, calibrator.y_min, calibrator.y_max,
                    )

        # Save artifact
        artifact_path = _resolve_artifact_path(entry["artifact"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_data = {
            "model": model,
            "impute_state": impute_state,
            "scaler": {"mean": scaler_mean, "std": scaler_std},
            "feature_cols": feature_cols,
            "calibrator": calibrator,
            "aux_base_col_names": build_result.aux_base_col_names,
            "target": self.target,
            # Backward compat: keep medians for old code paths
            "medians": impute_state.global_medians[:n_model],
        }
        if embedding_vocab is not None:
            artifact_data["embedding_vocab"] = embedding_vocab
            artifact_data["embedding_col"] = embedding_col
        joblib.dump(artifact_data, artifact_path)

        # Write cal_tiers sidecar from temporal CV predictions (when available)
        sidecar_path = artifact_path.with_name(
            f"{artifact_path.stem}_cal_tiers.json"
        )
        if calibrated_preds is not None:
            diag = Diagnostics()
            diag_results = diag.compute_all(
                calibrated_preds, calibration_segments=["circuit", "round"]
            )
            sidecar_data = {
                "segments": diag_results.segments,
                "calibration_by_segment": diag_results.calibration_by_segment,
                "config_stem": artifact_path.stem,
                "trained_at": datetime.now(UTC).isoformat(),
            }
            with open(sidecar_path, "w") as f:
                json.dump(sidecar_data, f, indent=2, default=str)
            logger.info("Wrote cal_tiers sidecar to %s", sidecar_path)
        elif sidecar_path.exists():
            sidecar_path.unlink()
            logger.info(
                "Removed stale cal_tiers sidecar at %s (no temporal CV this run)",
                sidecar_path,
            )

        # Update trained_at in config
        entry["trained_at"] = datetime.now(UTC).isoformat()
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
        artifact_path = _resolve_artifact_path(self.config["active"]["artifact"])
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
        artifact_path = _resolve_artifact_path(entry["artifact"])
        if not artifact_path.exists():
            raise FileNotFoundError(f"No trained model at {artifact_path}")
        return joblib.load(artifact_path)

    def _predict_raw(
        self,
        entry: dict,
        tournament_keys: list[tuple[str, int]] | None,
        match_uids: set[str],
        scoped: bool = False,
        *,
        include_settled: bool = False,
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

        # Keep matches in the production set (pending only, unless include_settled)
        if include_settled:
            pending = df.filter(pl.col("match_uid").is_in(list(match_uids)))
        else:
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

        # Append embedding column if model uses embeddings
        emb_vocab = artifact.get("embedding_vocab")
        if emb_vocab is not None:
            emb_col_name = artifact.get("embedding_col", "player_id")
            emb_ids = np.array(
                [emb_vocab.get(p, 0) for p in canonical[emb_col_name].to_list()]
            ).reshape(-1, 1)
            X = np.hstack([X, emb_ids.astype(np.float64)])

        # df is passed for ensemble models with segmented sub calibrators
        # (ignored by single models and ensembles with no/non-segmented sub cals)
        probs = model.predict_proba(X, df=canonical) if isinstance(model, EnsembleModel) else model.predict_proba(X)
        if calibrator is not None:
            if isinstance(
                calibrator,
                (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
            ):
                probs = calibrator.transform(probs, canonical)
            else:
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
        *,
        include_settled: bool = False,
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
                voter,
                tournament_keys,
                match_uids,
                scoped=is_scoped,
                include_settled=include_settled,
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
        self,
        tournament_keys: list[tuple[str, int]] | None = None,
        *,
        include_settled: bool = False,
        date_window: tuple[Any, Any] | None = None,
        include_features: bool = False,
    ) -> pl.DataFrame:
        """Generate predictions for pending matches (won is null).

        Args:
            tournament_keys: If provided, only predict for these (tid, year) pairs.
            include_settled: If True, also score matches with known outcomes
                (used by lead backtest). Defaults to False so the live pipeline
                continues to score only pending matches.
            date_window: Optional (start_date, end_date) tuple restricting
                predictions to matches whose effective_match_date falls in
                [start, end] inclusive. Used by the lead backtest.
            include_features: If True, stash the per-(match_uid, player_id)
                feature values (each side in its OWN orientation) on
                ``self._feature_frame`` before the canonical dedup collapses to
                one row/match. The lead backtest joins that onto its per-side bet
                rows so the CSV carries the exact values the model saw for each
                side. Off for live serving — the returned frame is unchanged.

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
        if include_features and config.data.eval_filters:
            # Backtest wants eval_filters columns available (to carry into the CSV
            # and restrict the bet set) even when they aren't model features.
            extra = extra + get_filter_feature_specs(config.data.eval_filters)
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

        # Keep pending matches (unless caller asked for settled too — backtest path)
        if include_settled:
            pending = df
        else:
            pending = df.filter(pl.col("won").is_null())

        if date_window is not None:
            start, end = date_window
            pending = pending.filter(
                (pl.col("effective_match_date") >= start)
                & (pl.col("effective_match_date") <= end)
            )

        if len(pending) == 0:
            logger.warning("No matches to predict")
            return pl.DataFrame()

        # Backtest hook: capture per-(match_uid, player_id) feature values in each
        # side's OWN orientation, before the canonical dedup below collapses to
        # one row/match. The lead backtest joins this onto its per-side bet rows,
        # so the CSV carries the exact values the model saw for that side — no
        # synthesized/negated values, and correct for any feature type. Live
        # serving never sets the flag, so this is skipped there.
        self._feature_frame = None
        if include_features:
            feat_cols = [c for c in feature_cols if c in pending.columns]
            filt_cols = [
                c for c in (config.data.eval_filters or {})
                if c in pending.columns and c not in feat_cols
            ]
            # Carry active data.filters feature columns too (e.g. the
            # anti-symmetric diff feature player_age_diff) so the backtest can
            # re-apply data.filters on the per-side bet rows. The predict-time
            # filter above runs on the pre-expansion frame; for a diff feature
            # the two-sided bet expansion re-adds the filtered-out orientation,
            # so the filter must also bite post-expansion where each side holds
            # its own orientation. filter_specs excludes raw match-level cols
            # (circuit/draw_type) that are already present on the bet rows.
            data_filt_cols = [
                c for c in filter_specs
                if c in pending.columns
                and c not in feat_cols
                and c not in filt_cols
            ]
            self._feature_frame = pending.select(
                ["match_uid", "player_id", *feat_cols, *filt_cols, *data_filt_cols]
            ).unique(subset=["match_uid", "player_id"], keep="first")

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

        # Append embedding column if model uses embeddings
        emb_vocab = artifact.get("embedding_vocab")
        if emb_vocab is not None:
            emb_col_name = artifact.get("embedding_col", "player_id")
            emb_ids = np.array(
                [emb_vocab.get(p, 0) for p in pending[emb_col_name].to_list()]
            ).reshape(-1, 1)
            X = np.hstack([X, emb_ids.astype(np.float64)])

        # For ensemble lead models, also capture per-sub probs so consumers
        # (backtest, analysis) can compute consensus stats. per_sub_probs are
        # in the same orientation as `probs` (canonical-side, before flip).
        per_sub_probs: list[np.ndarray] | None = None
        if isinstance(model, EnsembleModel):
            probs = model.predict_proba(X, df=pending)
            per_sub_probs = model.predict_proba_per_model(X, df=pending)
        else:
            probs = model.predict_proba(X)
        if calibrator is not None:
            if isinstance(
                calibrator,
                (SegmentedPlattCalibrator, SegmentedIsotonicCalibrator),
            ):
                probs = calibrator.transform(probs, pending)
            else:
                probs = calibrator.transform(probs)

        # Add probabilities to pending matches
        pending = pending.with_columns(pl.Series("_prob", probs))

        # Add per-sub probs and consensus count for ensembles. n_agree is the
        # count of subs picking the same side as the ensemble (>=0.5 vs <0.5).
        # Symmetric under canonical flip, so computed once here.
        if per_sub_probs is not None:
            ensemble_pick = (probs >= 0.5).astype(int)
            sub_picks = np.array([(p >= 0.5).astype(int) for p in per_sub_probs])
            n_agree = (sub_picks == ensemble_pick).sum(axis=0).astype(int)
            # per_sub_probs: list of arrays → list of lists per row
            per_sub_probs_rows = np.stack(per_sub_probs, axis=1).tolist()
            pending = pending.with_columns(
                pl.Series("_n_agree", n_agree),
                pl.Series("_per_sub_probs", per_sub_probs_rows),
            )

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
                    # Winner prob is directional — flip prob and swap identities.
                    # If per-sub probs are present (ensemble lead), flip each
                    # element too so they remain aligned with _prob (winner-side).
                    flip_cols = [
                        (1.0 - pl.col("_prob")).alias("_prob"),
                        pl.col("opp_id").alias("_tmp_player_id"),
                        pl.col("player_id").alias("_tmp_opp_id"),
                        pl.col("opp_first_name").alias("_tmp_pfn"),
                        pl.col("opp_last_name").alias("_tmp_pln"),
                        pl.col("player_first_name").alias("_tmp_ofn"),
                        pl.col("player_last_name").alias("_tmp_oln"),
                        pl.col("_opp_surface_elo").alias("_tmp_player_elo"),
                        pl.col("_player_surface_elo").alias("_tmp_opp_elo"),
                    ]
                    if "_per_sub_probs" in missing.columns:
                        flip_cols.append(
                            pl.col("_per_sub_probs")
                              .list.eval(1.0 - pl.element())
                              .alias("_per_sub_probs")
                        )
                    missing = missing.with_columns(flip_cols).with_columns(
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
        now = datetime.now(UTC)

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
        if "schedule_day" in canonical.columns:
            select_exprs.append(pl.col("schedule_day"))
        # Per-ensemble-sub consensus signals (winner target only; ensemble lead)
        if "_n_agree" in canonical.columns:
            select_exprs.append(pl.col("_n_agree").alias("n_agree"))
        if "_per_sub_probs" in canonical.columns:
            select_exprs.append(pl.col("_per_sub_probs").alias("per_sub_probs"))
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
