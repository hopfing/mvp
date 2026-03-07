"""Feature discovery tools for automated feature selection and tuning."""

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.discover import (
    DiscoveryResult,
    FeatureDiscovery,
    get_all_feature_specs,
)
from mvp.model.discovery.fast_selection import FastForwardSelector
from mvp.model.discovery.importance import (
    compute_importance,
    gain_importance,
    permutation_importance,
    shap_importance,
)
from mvp.model.discovery.segments import (
    SegmentAnalyzer,
    SegmentImportanceResult,
    SplitComparisonResult,
    compute_segment_importance,
)
from mvp.model.discovery.selection import (
    FeatureSelector,
    SelectionResult,
    create_scorer,
)
from mvp.model.discovery.sweeps import (
    DEFAULT_SWEEP_RANGES,
    ParameterSweep,
    SweepResult,
    build_feature_spec,
    parse_feature_spec,
)

__all__ = [
    "build_feature_spec",
    "compute_importance",
    "compute_segment_importance",
    "create_scorer",
    "DEFAULT_SWEEP_RANGES",
    "DiscoveryConfig",
    "DiscoveryResult",
    "FastForwardSelector",
    "FeatureDiscovery",
    "FeatureSelector",
    "gain_importance",
    "get_all_feature_specs",
    "ParameterSweep",
    "parse_feature_spec",
    "permutation_importance",
    "SegmentAnalyzer",
    "SegmentImportanceResult",
    "SelectionResult",
    "shap_importance",
    "SplitComparisonResult",
    "SweepResult",
]
