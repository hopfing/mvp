"""Configuration schema for the IID projection runner."""

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel

from mvp.model.config import DataConfig, FeaturesConfig, ValidationConfig


class MatchupServeRegressorConfig(BaseModel):
    """Underlying regressor for the MatchupServeModel."""

    type: Literal["ridge", "linear"] = "ridge"
    params: dict[str, Any] = {}


class ServeModelConfig(BaseModel):
    """Serve win prob estimator configuration."""

    type: Literal["identity", "matchup", "score_state"] = "identity"
    window: int | None = 90
    clip_min: float = 0.30
    clip_max: float = 0.90
    # Used only when type == "matchup"
    feature_columns: list[str] = []
    match_level_columns: list[str] = []
    regressor: MatchupServeRegressorConfig = MatchupServeRegressorConfig()
    # Used only when type == "score_state"
    model_type: Literal["logistic", "xgboost"] = "logistic"
    match_level_features: list[str] = []
    point_level_features: list[str] = []
    params: dict[str, Any] = {}


class IIDMetricsConfig(BaseModel):
    """Metric reporting configuration for the IID projector."""

    include_classification: bool = True
    include_regression: bool = True
    total_lines: list[float] = [18.5, 19.5, 20.5, 21.5, 22.5, 23.5, 24.5, 25.5]
    spread_lines: list[float] = [-5.5, -4.5, -3.5, -2.5, -1.5, 1.5, 2.5, 3.5, 4.5, 5.5]


class IIDProjectionConfig(BaseModel):
    """Complete IID projection configuration."""

    description: str | None = None
    data: DataConfig
    features: FeaturesConfig
    serve_model: ServeModelConfig = ServeModelConfig()
    validation: ValidationConfig = ValidationConfig()
    metrics: IIDMetricsConfig = IIDMetricsConfig()

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "IIDProjectionConfig":
        data: dict[str, Any] = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "IIDProjectionConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())


class ScoreStateModelConfig(BaseModel):
    """Score-state-dependent serve model configuration.

    Operates at point grain rather than match grain. Feature inputs mix
    match-level (broadcast to every point via server/returner perspective)
    and point-level (varying per point). Output: P(point_won_by_server | features).
    """

    type: Literal["logistic", "xgboost"] = "logistic"
    match_level_features: list[str] = []  # FeatureEngine specs, server-perspective
    point_level_features: list[str] = []  # columns from match_beats_points.parquet
    params: dict[str, Any] = {}


class ServeDiscoveryFeaturesConfig(BaseModel):
    """Candidate pool + base set for score-state serve forward selection.

    Empty candidate lists default to the full pool — match-level candidates
    come from the registered feature engine (matching classification /
    projection / IID FS behavior); point-level candidates come from the
    `match_beats_points.parquet` raw columns plus the registered derived
    features in `score_state_features.DERIVED_POINT_FEATURES`.
    """

    # Base sets: always included in every candidate model
    base_match_level_features: list[str] = []
    base_point_level_features: list[str] = []
    # Candidate pools: FS iterates over these looking for additions that improve the score.
    # Empty list → expand to full pool.
    candidate_match_level_features: list[str] = []
    candidate_point_level_features: list[str] = []
    # Window sizes passed through to get_all_feature_specs when expanding the
    # default match-level pool. None = use the shared DEFAULT_day_windows
    # ([0, 7, 14, 30, 60, 90, 180, 365]) from model.discovery.discover.
    window_sizes: list[int] | None = None
    max_features: int | None = None  # cap on total selected features (base + FS additions)


class ServeDiscoveryConfig(BaseModel):
    """Forward-selection discovery for the score-state serve model.

    Scores candidates using a single model form (default: logistic for speed).
    Optionally re-trains all `model_forms` on the selected feature set at the
    end to compare forms.
    """

    description: str | None = None
    data: DataConfig
    # FS runs at point-grain; sizes here are row counts in
    # match_beats_points.parquet (millions of rows).
    point_validation: ValidationConfig = ValidationConfig()
    # Match-grain validation, emitted verbatim into the promoted IID projection
    # config. The projection runner operates on one row per match.
    validation: ValidationConfig = ValidationConfig()
    features: ServeDiscoveryFeaturesConfig = ServeDiscoveryFeaturesConfig()
    # Model form used to score candidates during FS (kept simple/fast).
    scoring_model: ScoreStateModelConfig = ScoreStateModelConfig(type="logistic")
    # All forms to compare on the final selected feature set (inherits params from model_params
    # if present, else library defaults).
    model_forms: list[Literal["logistic", "xgboost"]] = ["logistic", "xgboost"]
    model_params: dict[str, dict[str, Any]] = {}  # per-form params overrides
    metric: Literal[
        "log_loss",
        "brier_score",
        "roc_auc",
        "calibration_error",
        "iid_crps_total_games",
        "iid_crps_spread",
        "mae",
        "rmse",
    ] = "log_loss"
    selection_method: Literal["forward"] = "forward"
    min_delta: float = 0.0001  # minimum fractional improvement to accept a candidate
    # Cap on training rows per fold during candidate scoring. None = use full training slice.
    # Final-form re-eval always runs on the full slice so reported metrics are honest.
    fs_train_subsample: int | None = None
    fs_subsample_seed: int = 42

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "ServeDiscoveryConfig":
        data: dict[str, Any] = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "ServeDiscoveryConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())

    def to_iid_projection_config_dict(
        self,
        selected_match_level: list[str],
        selected_point_level: list[str],
        model_type: str = "logistic",
        model_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Emit a runnable IIDProjectionConfig-compatible dict from FS output.

        `features.include` gets both player_/opp_ versions of each selected
        match-level spec — match-constant point features are pulled directly
        from `match_beats_points.parquet` at fit time and don't need to be
        engine-computed.
        """
        from mvp.model.engine import parse_feature_spec

        include_specs: list[str] = []
        seen: set[str] = set()

        for spec in selected_match_level:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            if prefix == "player":
                swap_full = f"opp_{base_name}"
            elif prefix == "opp":
                swap_full = f"player_{base_name}"
            else:
                swap_full = full_name

            if params:
                param_str = ", ".join(f"{k}={v}" for k, v in params.items())
                own_spec = f"{full_name}({param_str})"
                swap_spec = f"{swap_full}({param_str})"
            else:
                own_spec = full_name
                swap_spec = swap_full

            for s in (own_spec, swap_spec):
                if s not in seen:
                    include_specs.append(s)
                    seen.add(s)

        return {
            "description": (
                self.description
                or "IID score-state projection from forward-selected features"
            ),
            "data": self.data.model_dump(),
            "features": {"include": include_specs},
            "serve_model": {
                "type": "score_state",
                "model_type": model_type,
                "match_level_features": selected_match_level,
                "point_level_features": selected_point_level,
                "params": model_params or {},
            },
            "validation": self.validation.model_dump(),
        }


class IIDDiscoveryFeaturesConfig(BaseModel):
    """Forward-selection candidate pool configuration."""

    include: list[str] = []           # optional allowlist of candidate specs
    exclude: list[str] = []           # optional blocklist of candidate specs
    base: list[str] = []              # features always kept (FS starts from these)
    window_sizes: list[int] = [60, 90]
    max_features: int | None = None   # cap on FS depth


class IIDDiscoveryConfig(BaseModel):
    """Forward-selection discovery configuration for the IID matchup serve model.

    Drives the IIDProjectionDiscovery orchestrator: defines the data slice,
    candidate pool, validation folds, regressor, and target metric.
    """

    description: str | None = None
    data: DataConfig
    validation: ValidationConfig = ValidationConfig()
    serve_model: ServeModelConfig = ServeModelConfig(type="matchup")
    metrics: IIDMetricsConfig = IIDMetricsConfig()
    features: IIDDiscoveryFeaturesConfig = IIDDiscoveryFeaturesConfig()
    metric: Literal["mae", "rmse", "log_loss", "iid_crps_total_games"] = "mae"
    selection_method: Literal["forward"] = "forward"

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "IIDDiscoveryConfig":
        data: dict[str, Any] = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "IIDDiscoveryConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())

    def to_iid_config_dict(self, selected_specs: list[str]) -> dict[str, Any]:
        """Emit a runnable `IIDProjectionConfig`-compatible dict.

        `features.include` gets BOTH the player_* and opp_* versions of each
        selected spec (the matchup serve model's swap mechanism requires both
        perspectives loaded at fit and predict time). `serve_model.feature_columns`
        gets the resolved column name for each selected spec (row-player
        perspective, whatever prefix the FS picked).
        """
        from mvp.model.engine import build_column_name, parse_feature_spec

        include_specs: list[str] = []
        feature_columns: list[str] = []
        seen_specs: set[str] = set()

        for spec in selected_specs:
            prefix, base_name, full_name, params = parse_feature_spec(spec)
            col_name = build_column_name(full_name, params)
            feature_columns.append(col_name)

            # Derive the swapped-perspective spec so the engine loads both
            # versions (needed by MatchupServeModel's two-perspective fit).
            if prefix == "player":
                swap_full = f"opp_{base_name}"
            elif prefix == "opp":
                swap_full = f"player_{base_name}"
            else:
                swap_full = full_name  # match-level, no swap

            if params:
                param_str = ", ".join(f"{k}={v}" for k, v in params.items())
                own_spec = f"{full_name}({param_str})"
                swap_spec = f"{swap_full}({param_str})"
            else:
                own_spec = full_name
                swap_spec = swap_full

            for s in (own_spec, swap_spec):
                if s not in seen_specs:
                    include_specs.append(s)
                    seen_specs.add(s)

        return {
            "description": (
                self.description or "IID matchup projection from forward-selected features"
            ),
            "data": self.data.model_dump(),
            "features": {"include": include_specs},
            "serve_model": {
                "type": "matchup",
                "window": self.serve_model.window,
                "clip_min": self.serve_model.clip_min,
                "clip_max": self.serve_model.clip_max,
                "feature_columns": feature_columns,
                "match_level_columns": list(self.serve_model.match_level_columns),
                "regressor": self.serve_model.regressor.model_dump(),
            },
            "validation": self.validation.model_dump(),
            "metrics": self.metrics.model_dump(),
        }
