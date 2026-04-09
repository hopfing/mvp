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

    type: Literal["identity", "matchup"] = "identity"
    window: int | None = 90
    clip_min: float = 0.30
    clip_max: float = 0.90
    # Used only when type == "matchup"
    feature_columns: list[str] = []
    match_level_columns: list[str] = []
    regressor: MatchupServeRegressorConfig = MatchupServeRegressorConfig()


class IIDMetricsConfig(BaseModel):
    """Metric reporting configuration for the IID projector."""

    include_classification: bool = True
    include_regression: bool = True
    total_lines: list[float] = [20.5, 21.5, 22.5, 23.5]
    spread_lines: list[float] = [-3.5, -2.5, -1.5, 1.5, 2.5, 3.5]


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
