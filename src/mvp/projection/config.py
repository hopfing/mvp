"""Configuration schema for game projection pipeline."""


from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel

from mvp.model.config import DataConfig, FeaturesConfig, ValidationConfig


class ProjectionModelConfig(BaseModel):
    """Regression model configuration."""

    type: Literal["xgb_regressor", "linear", "ridge"] = "xgb_regressor"
    params: dict[str, Any] | None = None
    target: Literal["player_games", "match_games"] = "player_games"


class ProjectionMetricsConfig(BaseModel):
    """Metrics configuration for projection."""

    primary: str = "mae"
    secondary: list[str] = ["rmse", "r_squared"]


class ProjectionConfig(BaseModel):
    """Complete projection model configuration."""

    description: str | None = None
    data: DataConfig
    features: FeaturesConfig
    model: ProjectionModelConfig = ProjectionModelConfig()
    validation: ValidationConfig = ValidationConfig()
    metrics: ProjectionMetricsConfig = ProjectionMetricsConfig()

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "ProjectionConfig":
        data = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "ProjectionConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())


class DiscoveryFeaturesConfig(BaseModel):
    """Feature configuration for projection discovery."""

    include: list[str] = []
    exclude: list[str] = []
    compute_only: list[str] = []
    base: list[str] = []
    min: int = 3
    max: int | None = None
    window_sizes: list[int] | None = None


class ProjectionDiscoveryOptions(BaseModel):
    """Discovery-specific options for projection."""

    selection_method: Literal["forward", "recursive", "threshold"] = "forward"
    metric: str = "mae"
    direction: Literal["minimize", "maximize"] = "minimize"
    importance_threshold: float = 0.05
    features: DiscoveryFeaturesConfig = DiscoveryFeaturesConfig()


class ProjectionDiscoveryConfig(BaseModel):
    """Complete discovery configuration for projection."""

    description: str | None = None
    data: DataConfig
    discovery: ProjectionDiscoveryOptions = ProjectionDiscoveryOptions()
    model: ProjectionModelConfig = ProjectionModelConfig()
    validation: ValidationConfig = ValidationConfig()

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "ProjectionDiscoveryConfig":
        data = yaml.safe_load(yaml_str)
        data.pop("name", None)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "ProjectionDiscoveryConfig":
        with open(path) as f:
            return cls.from_yaml(f.read())

    def to_projection_config_dict(self, features: list[str]) -> dict[str, Any]:
        """Convert to projection config dict with given features."""
        features_dict: dict[str, Any] = {"include": features}
        if self.discovery.features.compute_only:
            features_dict["compute_only"] = self.discovery.features.compute_only
        result: dict[str, Any] = {
            "description": self.description,
            "data": self.data.model_dump(),
            "features": features_dict,
            "model": self.model.model_dump(),
            "validation": self.validation.model_dump(),
            "metrics": {"primary": self.discovery.metric},
        }
        return result
