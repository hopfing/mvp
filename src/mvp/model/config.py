"""Experiment configuration schema."""


from datetime import date
from typing import Any, Literal

import yaml
from pydantic import BaseModel, field_validator, model_validator


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
    filters: dict[str, Any] | None = None  # Applied in Phase 3 (Diagnostics)


class FeaturesConfig(BaseModel):
    """Feature selection configuration."""

    include: list[str]


class EnsembleBaseModelRef(BaseModel):
    """Reference to a base model in an ensemble."""

    config: str
    weight: float = 1.0


class EnsembleParams(BaseModel):
    """Ensemble-specific parameters."""

    strategy: Literal["average", "weighted_average", "stacking"] = "average"
    base_models: list[EnsembleBaseModelRef]
    meta_features: list[str] = []

    @model_validator(mode="after")
    def validate_stacking_no_weights(self) -> "EnsembleParams":
        if self.strategy == "stacking":
            for ref in self.base_models:
                if ref.weight != 1.0:
                    raise ValueError(
                        "weight is not allowed with strategy='stacking' "
                        "(meta-model learns coefficients)"
                    )
        if self.meta_features and self.strategy != "stacking":
            raise ValueError(
                "meta_features is only allowed with strategy='stacking'"
            )
        return self


class ModelConfig(BaseModel):
    """Model configuration."""

    type: Literal["xgboost", "logistic", "random_forest", "ensemble"]
    params: dict[str, Any] | None = None


class ValidationConfig(BaseModel):
    """Validation strategy configuration."""

    type: Literal["walk_forward", "expanding_window", "sliding_window"] = "walk_forward"
    # For walk_forward (n_splits mode)
    n_splits: int = 5
    min_train_size: int = 50000
    test_size: int = 10000
    # For expanding_window (step_size mode)
    initial_train_size: int | None = None
    step_size: int | None = None
    # For sliding_window
    train_size: int | None = None


class MetricsConfig(BaseModel):
    """Metrics configuration."""

    primary: str = "log_loss"
    secondary: list[str] = ["accuracy", "brier_score", "roc_auc"]


class ExperimentConfig(BaseModel):
    """Complete experiment configuration."""

    description: str | None = None
    data: DataConfig
    features: FeaturesConfig | None = None
    model: ModelConfig
    validation: ValidationConfig = ValidationConfig()
    metrics: MetricsConfig = MetricsConfig()

    @model_validator(mode="after")
    def validate_features_required(self) -> "ExperimentConfig":
        if self.model.type != "ensemble" and self.features is None:
            raise ValueError("features is required for non-ensemble models")
        return self

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "ExperimentConfig":
        """Parse config from YAML string."""
        data = yaml.safe_load(yaml_str)
        data.pop("name", None)  # Ignore legacy name field
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str) -> "ExperimentConfig":
        """Load config from YAML file."""
        with open(path) as f:
            return cls.from_yaml(f.read())
