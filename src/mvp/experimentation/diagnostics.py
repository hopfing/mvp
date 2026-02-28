"""Diagnostics for experiment analysis."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class DiagnosticResults:
    """Container for all diagnostic results."""

    segments: dict[str, dict[str, dict[str, float]]]
    calibration: dict[str, Any]
    errors: dict[str, Any]
    temporal: dict[str, Any]

    @property
    def metrics(self) -> dict[str, float]:
        """Flatten results to MLflow-loggable metrics."""
        result: dict[str, float] = {}

        # Flatten segment metrics
        for segment_type, segments in self.segments.items():
            for segment_value, metrics in segments.items():
                for metric_name, value in metrics.items():
                    if metric_name != "n_matches":
                        key = f"segment_{segment_type}_{segment_value}_{metric_name}"
                        result[key] = value

        # Add calibration metrics
        for key in ["calibration_error", "calibration_max_error"]:
            if key in self.calibration:
                result[key] = self.calibration[key]

        # Add error metrics
        for key, value in self.errors.items():
            if key.startswith("error_rate_") or key.startswith("error_count_"):
                result[key] = value

        # Add temporal metrics
        if "temporal_drift" in self.temporal:
            result["temporal_drift"] = self.temporal["temporal_drift"]

        return result

    def to_json(self) -> str:
        """Serialize full results to JSON."""
        return json.dumps(
            {
                "segments": self.segments,
                "calibration": self.calibration,
                "errors": self.errors,
                "temporal": self.temporal,
            },
            indent=2,
            default=str,
        )
