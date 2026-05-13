"""Configuration schema for feature discovery."""


from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, field_validator, model_validator

from mvp.model.config import SampleWeightConfig


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

    include: list[str] = []
    exclude: list[str] = []
    compute_only: list[str] = []
    base: list[str] = []
    min: int = 5
    max: int | None = None
    window_sizes: list[int] | None = None  # None = all defaults, 0 = alltime variant


class DiscoveryOptions(BaseModel):
    """Discovery-specific options."""

    importance_method: Literal["gain", "permutation", "shap"] = "permutation"
    selection_method: Literal["forward", "recursive", "threshold"] = "forward"
    sweep_params: bool = False
    segment_analysis: bool = False
    metric: str = "calibration_error"
    direction: Literal["minimize", "maximize"] = "minimize"
    importance_threshold: float = 0.05
    min_delta: float = 0.0  # forward selection: minimum absolute improvement to accept a candidate
    meta_discovery: MetaDiscoveryConfig | None = None
    features: DiscoveryFeaturesConfig = DiscoveryFeaturesConfig()


class ModelConfig(BaseModel):
    """Model configuration."""

    type: Literal["xgboost", "logistic", "random_forest", "ensemble", "neural_net"] = "xgboost"
    params: dict[str, Any] | None = None


class ValidationConfig(BaseModel):
    """Validation strategy configuration."""

    type: Literal[
        "walk_forward",
        "expanding_window",
        "sliding_window",
        "date_window",
        "date_sliding",
        "date_expanding",
    ] = "walk_forward"
    n_splits: int = 5
    min_train_size: int = 50000
    test_size: int = 10000
    initial_train_size: int | None = None
    step_size: int | None = None
    train_size: int | None = None
    test_start: date | None = None
    train_months: int | None = None
    initial_train_months: int | None = None
    test_months: int | None = None

    @model_validator(mode="after")
    def _validate_date_splitter_params(self) -> "ValidationConfig":
        if self.type == "date_sliding":
            if self.initial_train_months is not None:
                raise ValueError(
                    "initial_train_months is for date_expanding; use train_months with date_sliding"
                )
            if self.train_months is None or self.test_months is None:
                raise ValueError(
                    "date_sliding requires train_months and test_months"
                )
        elif self.type == "date_expanding":
            if self.train_months is not None:
                raise ValueError(
                    "train_months is for date_sliding; use initial_train_months with date_expanding"
                )
            if self.initial_train_months is None or self.test_months is None:
                raise ValueError(
                    "date_expanding requires initial_train_months and test_months"
                )
        else:
            if (
                self.train_months is not None
                or self.initial_train_months is not None
                or self.test_months is not None
            ):
                raise ValueError(
                    f"train_months / initial_train_months / test_months are only valid "
                    f"with date_sliding or date_expanding, not {self.type}"
                )
        return self


class DiscoveryConfig(BaseModel):
    """Complete discovery configuration."""

    description: str | None = None
    target: Literal["won", "deciding_set"] = "won"
    data: DataConfig
    discovery: DiscoveryOptions = DiscoveryOptions()
    model: ModelConfig = ModelConfig()
    validation: ValidationConfig = ValidationConfig()
    sample_weight: SampleWeightConfig | None = None

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

    def _ordered_validation_dump(self) -> dict[str, Any]:
        """Dump only explicitly-set validation fields."""
        all_fields = self.validation.model_dump()
        set_fields = self.validation.model_fields_set
        return {k: all_fields[k] for k in all_fields if k in set_fields}

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
        model_dump = self.model.model_dump(exclude_none=True)
        result: dict[str, Any] = {
            "data": self.data.model_dump(),
            "features": features_dict,
            "model": model_dump,
            "validation": self._ordered_validation_dump(),
            "metrics": {"primary": self.discovery.metric},
        }
        if self.description:
            result = {"description": self.description, **result}
        if self.target != "won":
            result["target"] = self.target
        if self.sample_weight is not None:
            result["sample_weight"] = self.sample_weight.model_dump()
        return result
