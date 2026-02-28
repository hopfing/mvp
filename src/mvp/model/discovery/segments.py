"""Segment-level analysis for feature discovery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import polars as pl

from mvp.model.discovery.importance import compute_importance
from mvp.model.models import BaseModel


@dataclass
class SegmentImportanceResult:
    """Feature importance broken down by segment."""

    segment_column: str
    segment_values: list[str]
    importance_by_segment: dict[str, dict[str, float]]
    overall_importance: dict[str, float]


@dataclass
class SplitComparisonResult:
    """Comparison of single model vs per-segment models."""

    segment_column: str
    single_model_metrics: dict[str, float]
    split_model_metrics: dict[str, float]
    per_segment_metrics: dict[str, dict[str, float]]
    recommendation: str


def compute_segment_importance(
    model: BaseModel,
    X: np.ndarray,
    y: np.ndarray,
    df: pl.DataFrame,
    feature_names: list[str],
    segment_column: str,
    method: str = "permutation",
    **kwargs: Any,
) -> SegmentImportanceResult:
    """Compute feature importance for each segment.

    Args:
        model: Trained model.
        X: Feature matrix.
        y: Target array.
        df: DataFrame with segment column.
        feature_names: Feature names matching X columns.
        segment_column: Column to segment by (e.g., "circuit").
        method: Importance method (gain, permutation, shap).
        **kwargs: Additional args for importance computation.

    Returns:
        SegmentImportanceResult with per-segment importance.
    """
    if segment_column not in df.columns:
        raise ValueError(f"Segment column '{segment_column}' not in DataFrame")

    # Overall importance
    overall = compute_importance(model, X, y, feature_names, method, **kwargs)

    # Per-segment importance
    segment_values = df[segment_column].unique().to_list()
    importance_by_segment: dict[str, dict[str, float]] = {}

    for segment in segment_values:
        if segment is None:
            continue

        mask = (df[segment_column] == segment).to_numpy()
        if mask.sum() < 100:  # Skip very small segments
            continue

        X_seg = X[mask]
        y_seg = y[mask]

        try:
            seg_importance = compute_importance(
                model, X_seg, y_seg, feature_names, method, **kwargs
            )
            importance_by_segment[str(segment)] = seg_importance
        except Exception:
            # Skip segments that fail (e.g., all same class)
            continue

    return SegmentImportanceResult(
        segment_column=segment_column,
        segment_values=[str(s) for s in segment_values if s is not None],
        importance_by_segment=importance_by_segment,
        overall_importance=overall,
    )


def compare_single_vs_split(
    config_path: Path | str,
    segment_column: str = "circuit",
    metric: str = "calibration_error",
    matches_path: Path | str | None = None,
    cache_dir: Path | str | None = None,
) -> SplitComparisonResult:
    """Compare single model vs per-segment models.

    Trains a single model on all data, then trains separate models
    for each segment value. Compares performance.

    Args:
        config_path: Path to experiment config.
        segment_column: Column to split by.
        metric: Primary metric for comparison.
        matches_path: Path to matches.parquet.
        cache_dir: Path to feature cache.

    Returns:
        SplitComparisonResult with comparison.
    """
    import tempfile
    import yaml

    from mvp.model.config import ExperimentConfig
    from mvp.model.runner import ExperimentRunner

    config = ExperimentConfig.from_file(str(config_path))

    # Run single model
    runner = ExperimentRunner(
        config_path=config_path,
        matches_path=matches_path,
        cache_dir=cache_dir,
    )
    single_result = runner.run()
    single_metrics = single_result["metrics"]

    # Get segment values from diagnostics
    diagnostics = single_result.get("diagnostics")
    if diagnostics is None or segment_column not in diagnostics.segments:
        raise ValueError(f"No segment data for '{segment_column}'")

    segment_data = diagnostics.segments[segment_column]
    segment_values = list(segment_data.keys())

    # Run per-segment models
    per_segment_metrics: dict[str, dict[str, float]] = {}
    weighted_metrics: dict[str, list[tuple[float, int]]] = {}

    for segment in segment_values:
        # Create filtered config
        config_dict = config.model_dump()
        if config_dict.get("data", {}).get("filters") is None:
            config_dict["data"]["filters"] = {}
        config_dict["data"]["filters"][segment_column] = [segment]
        config_dict["name"] = f"{config.name}_{segment}"

        seg_config = ExperimentConfig.model_validate(config_dict)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(seg_config.model_dump(), f, default_flow_style=False)
            temp_path = f.name

        try:
            seg_runner = ExperimentRunner(
                config_path=temp_path,
                matches_path=matches_path,
                cache_dir=cache_dir,
            )
            seg_result = seg_runner.run()
            seg_metrics = seg_result["metrics"]
            per_segment_metrics[segment] = seg_metrics

            # Track weighted average
            n_matches = int(segment_data[segment].get("n_matches", 0))
            for metric_name, value in seg_metrics.items():
                if metric_name not in weighted_metrics:
                    weighted_metrics[metric_name] = []
                weighted_metrics[metric_name].append((value, n_matches))

        except Exception as e:
            per_segment_metrics[segment] = {"error": str(e)}

        finally:
            Path(temp_path).unlink(missing_ok=True)

    # Compute weighted average for split models
    split_metrics: dict[str, float] = {}
    for metric_name, values in weighted_metrics.items():
        total_weight = sum(w for _, w in values)
        if total_weight > 0:
            weighted_sum = sum(v * w for v, w in values)
            split_metrics[metric_name] = weighted_sum / total_weight

    # Generate recommendation
    single_val = single_metrics.get(metric, float("inf"))
    split_val = split_metrics.get(metric, float("inf"))
    diff = single_val - split_val

    if abs(diff) < 0.005:
        recommendation = (
            f"No meaningful difference ({metric}: single={single_val:.4f}, "
            f"split={split_val:.4f}). Use single model for simplicity."
        )
    elif diff > 0:
        recommendation = (
            f"Split models improve {metric} by {diff:.4f} "
            f"(single={single_val:.4f}, split={split_val:.4f}). "
            "Consider separate models if precision is critical."
        )
    else:
        recommendation = (
            f"Single model outperforms split by {-diff:.4f} "
            f"(single={single_val:.4f}, split={split_val:.4f}). "
            "Use single model."
        )

    return SplitComparisonResult(
        segment_column=segment_column,
        single_model_metrics=single_metrics,
        split_model_metrics=split_metrics,
        per_segment_metrics=per_segment_metrics,
        recommendation=recommendation,
    )


class SegmentAnalyzer:
    """Analyzes feature importance and model performance across segments."""

    def __init__(
        self,
        config_path: Path | str,
        segment_columns: list[str] | None = None,
        importance_method: str = "permutation",
        metric: str = "calibration_error",
        matches_path: Path | str | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        """Initialize segment analyzer.

        Args:
            config_path: Path to experiment config.
            segment_columns: Columns to analyze (default: ["circuit"]).
            importance_method: Method for importance computation.
            metric: Metric for single vs split comparison.
            matches_path: Path to matches.parquet.
            cache_dir: Path to feature cache.
        """
        self.config_path = Path(config_path)
        self.segment_columns = segment_columns or ["circuit"]
        self.importance_method = importance_method
        self.metric = metric
        self.matches_path = matches_path
        self.cache_dir = cache_dir

    def analyze_importance(
        self,
        model: BaseModel,
        X: np.ndarray,
        y: np.ndarray,
        df: pl.DataFrame,
        feature_names: list[str],
    ) -> dict[str, SegmentImportanceResult]:
        """Compute importance for all configured segments.

        Args:
            model: Trained model.
            X: Feature matrix.
            y: Target array.
            df: DataFrame with segment columns.
            feature_names: Feature names.

        Returns:
            Dict mapping segment column to importance result.
        """
        results = {}
        for col in self.segment_columns:
            if col in df.columns:
                try:
                    results[col] = compute_segment_importance(
                        model=model,
                        X=X,
                        y=y,
                        df=df,
                        feature_names=feature_names,
                        segment_column=col,
                        method=self.importance_method,
                    )
                except Exception:
                    continue
        return results

    def compare_splits(self) -> dict[str, SplitComparisonResult]:
        """Compare single vs split models for all segments.

        Returns:
            Dict mapping segment column to comparison result.
        """
        results = {}
        for col in self.segment_columns:
            try:
                results[col] = compare_single_vs_split(
                    config_path=self.config_path,
                    segment_column=col,
                    metric=self.metric,
                    matches_path=self.matches_path,
                    cache_dir=self.cache_dir,
                )
            except Exception:
                continue
        return results
