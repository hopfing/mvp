"""Configuration schema for feature discovery."""


from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, field_validator


class DateRange(BaseModel):
    """Date range for data selection."""

    start: date
    end: date

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> date:
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            return date.fromisoformat(v)
        raise ValueError(f"Cannot parse date: {v}")


class DataConfig(BaseModel):
    """Data selection configuration."""

    date_range: DateRange
    filters: dict[str, Any] | None = None


class MetaDiscoveryConfig(BaseModel):
    """Configuration for meta-feature discovery via model disagreement."""

    ensemble_config: str
    weighting: Literal["binary", "magnitude"] = "magnitude"


class DiscoveryFeaturesConfig(BaseModel):
    """Feature configuration for discovery."""

    compute_only: list[str] = []


class DiscoveryOptions(BaseModel):
    """Discovery-specific options."""

    importance_method: Literal["gain", "permutation", "shap"] = "permutation"
    selection_method: Literal["forward", "recursive", "threshold"] = "forward"
    sweep_params: bool = True
    segment_analysis: bool = True
    metric: str = "calibration_error"
    direction: Literal["minimize", "maximize"] = "minimize"
    min_features: int = 5
    max_features: int | None = None
    importance_threshold: float = 0.05
    include_features: list[str] = []
    exclude_features: list[str] = []
    base_features: list[str] = []
    window_sizes: list[int] | None = None  # None = all defaults, 0 = alltime variant
    meta_discovery: MetaDiscoveryConfig | None = None
    features: DiscoveryFeaturesConfig = DiscoveryFeaturesConfig()


class ModelConfig(BaseModel):
    """Model configuration."""

    type: Literal["xgboost", "logistic", "random_forest", "ensemble"] = "xgboost"
    params: dict[str, Any] | None = None


class ValidationConfig(BaseModel):
    """Validation strategy configuration."""

    type: Literal["walk_forward", "expanding_window", "sliding_window"] = "walk_forward"
    n_splits: int = 5
    min_train_size: int = 50000
    test_size: int = 10000
    initial_train_size: int | None = None
    step_size: int | None = None
    train_size: int | None = None


class DiscoveryConfig(BaseModel):
    """Complete discovery configuration."""

    description: str | None = None
    data: DataConfig
    discovery: DiscoveryOptions = DiscoveryOptions()
    model: ModelConfig = ModelConfig()
    validation: ValidationConfig = ValidationConfig()

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "DiscoveryConfig":
        """Parse config from YAML string."""
        data = yaml.safe_load(yaml_str)
        data.pop("name", None)  # Ignore legacy name field
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "DiscoveryConfig":
        """Load config from YAML file."""
        with open(path) as f:
            return cls.from_yaml(f.read())

    def to_experiment_config_dict(self, features: list[str]) -> dict[str, Any]:
        """Convert to experiment config dict with given features.

        Args:
            features: List of feature specs to include.

        Returns:
            Dict suitable for ExperimentConfig.model_validate().
        """
        features_dict: dict[str, Any] = {"include": features}
        if self.discovery.features.compute_only:
            features_dict["compute_only"] = self.discovery.features.compute_only
        return {
            "description": self.description,
            "data": self.data.model_dump(),
            "features": features_dict,
            "model": self.model.model_dump(),
            "validation": self.validation.model_dump(),
            "metrics": {"primary": self.discovery.metric},
        }
