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

    primary: str = "log_loss"
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
