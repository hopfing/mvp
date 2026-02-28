"""Experimentation platform for feature engineering and model training."""

from mvp.experimentation.config import ExperimentConfig
from mvp.experimentation.context import FeatureContext
from mvp.experimentation.diagnostics import DiagnosticResults, Diagnostics
from mvp.experimentation.engine import FeatureEngine
from mvp.experimentation.metrics import compute_metrics
from mvp.experimentation.mlflow_logger import ExperimentLogger
from mvp.experimentation.models import BaseModel, LogisticModel, XGBoostModel, get_model
from mvp.experimentation.registry import feature, get_registry
from mvp.experimentation.runner import ExperimentRunner
from mvp.experimentation.splitters import (
    BaseSplitter,
    ExpandingWindowSplitter,
    WalkForwardSplitter,
)

__all__ = [
    "BaseSplitter",
    "BaseModel",
    "compute_metrics",
    "DiagnosticResults",
    "Diagnostics",
    "ExpandingWindowSplitter",
    "ExperimentConfig",
    "ExperimentLogger",
    "ExperimentRunner",
    "feature",
    "FeatureContext",
    "FeatureEngine",
    "get_model",
    "get_registry",
    "LogisticModel",
    "WalkForwardSplitter",
    "XGBoostModel",
]
