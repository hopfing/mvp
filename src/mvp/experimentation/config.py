"""Experiment configuration schema."""

from __future__ import annotations

from datetime import date
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


class FeaturesConfig(BaseModel):
    """Feature selection configuration."""

    include: list[str]


class ModelConfig(BaseModel):
    """Model configuration."""

    type: Literal["xgboost", "logistic"]
    params: dict[str, Any] | None = None


class ExperimentConfig(BaseModel):
    """Complete experiment configuration."""

    name: str
    description: str | None = None
    data: DataConfig
    features: FeaturesConfig
    model: ModelConfig

    @classmethod
    def from_yaml(cls, yaml_str: str) -> ExperimentConfig:
        """Parse config from YAML string."""
        data = yaml.safe_load(yaml_str)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str) -> ExperimentConfig:
        """Load config from YAML file."""
        with open(path) as f:
            return cls.from_yaml(f.read())
