"""Experimentation platform for feature engineering and model training."""

from mvp.model.config import ExperimentConfig
from mvp.model.context import FeatureContext
from mvp.model.diagnostics import DiagnosticResults, Diagnostics
from mvp.model.engine import FeatureEngine
from mvp.model.metrics import compute_metrics
from mvp.model.mlflow_logger import ExperimentLogger
from mvp.model.models import BaseModel, LogisticModel, XGBoostModel, get_model
from mvp.model.registry import feature, get_registry
from mvp.model.runner import ExperimentRunner
from mvp.model.splitters import (
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
