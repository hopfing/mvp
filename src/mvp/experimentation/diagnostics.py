"""Diagnostics for experiment analysis."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score

# Round group mappings
ROUND_GROUPS: dict[str, list[str]] = {
    "Qualifying": ["Q1", "Q2", "Q3"],
    "Early": ["R128", "R64", "R32"],
    "Late": ["R16", "QF", "SF", "F", "RR"],
}

# Ranking bucket boundaries
RANKING_BUCKETS: list[tuple[str, int, int | None]] = [
    ("1-20", 1, 20),
    ("21-50", 21, 50),
    ("51-100", 51, 100),
    ("101-200", 101, 200),
    ("201+", 201, None),
]


def _compute_metrics_for_segment(
    y_true: np.ndarray, y_prob: np.ndarray
) -> dict[str, float]:
    """Compute standard metrics for a segment."""
    if len(y_true) == 0:
        return {
            "accuracy": 0.0,
            "log_loss": 0.0,
            "brier_score": 0.0,
            "roc_auc": 0.0,
            "n_matches": 0,
        }

    y_pred = (y_prob >= 0.5).astype(int)
    y_prob_clipped = np.clip(y_prob, 1e-15, 1 - 1e-15)

    metrics: dict[str, float] = {"n_matches": len(y_true)}

    metrics["accuracy"] = float(accuracy_score(y_true, y_pred))
    metrics["brier_score"] = float(brier_score_loss(y_true, y_prob))

    # log_loss and roc_auc need both classes present
    if len(np.unique(y_true)) > 1:
        metrics["log_loss"] = float(log_loss(y_true, y_prob_clipped))
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_prob))
    else:
        metrics["log_loss"] = 0.0
        metrics["roc_auc"] = 0.0

    return metrics


class Diagnostics:
    """Compute diagnostics for experiment analysis."""

    def _get_round_group(self, round_val: str) -> str:
        """Map round to group."""
        for group, rounds in ROUND_GROUPS.items():
            if round_val in rounds:
                return group
        return "Other"

    def _get_ranking_bucket(self, ranking: int | None) -> str:
        """Map ranking to bucket."""
        if ranking is None:
            return "Unranked"
        for name, low, high in RANKING_BUCKETS:
            if high is None:
                if ranking >= low:
                    return name
            elif low <= ranking <= high:
                return name
        return "Unranked"

    def _segment_metrics(
        self, df: pl.DataFrame, y_true: np.ndarray, y_prob: np.ndarray
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Compute metrics for each segment."""
        result: dict[str, dict[str, dict[str, float]]] = {}

        # Circuit segment
        result["circuit"] = {}
        for circuit in ["tour", "chal"]:
            mask = (df["circuit"] == circuit).to_numpy()
            if mask.any():
                result["circuit"][circuit] = _compute_metrics_for_segment(
                    y_true[mask], y_prob[mask]
                )

        # Surface segment
        result["surface"] = {}
        for surface in ["Hard", "Clay", "Grass", "Carpet"]:
            mask = (df["surface"] == surface).to_numpy()
            if mask.any():
                result["surface"][surface] = _compute_metrics_for_segment(
                    y_true[mask], y_prob[mask]
                )

        # Round group segment
        round_groups = df["round"].map_elements(
            self._get_round_group, return_dtype=pl.Utf8
        ).to_numpy()
        result["round_group"] = {}
        for group in ["Qualifying", "Early", "Late", "Other"]:
            mask = round_groups == group
            if mask.any():
                result["round_group"][group] = _compute_metrics_for_segment(
                    y_true[mask], y_prob[mask]
                )

        # Raw rounds (JSON only, not flattened to metrics)
        result["round_raw"] = {}
        for round_val in df["round"].unique().to_list():
            mask = (df["round"] == round_val).to_numpy()
            if mask.any():
                result["round_raw"][round_val] = _compute_metrics_for_segment(
                    y_true[mask], y_prob[mask]
                )

        # Ranking bucket segment
        ranking_buckets = np.array([
            self._get_ranking_bucket(r) for r in df["player_ranking"].to_list()
        ])
        result["ranking_bucket"] = {}
        for bucket, _, _ in RANKING_BUCKETS:
            mask = ranking_buckets == bucket
            if mask.any():
                result["ranking_bucket"][bucket] = _compute_metrics_for_segment(
                    y_true[mask], y_prob[mask]
                )

        return result

    def _calibration(
        self, y_true: np.ndarray, y_prob: np.ndarray
    ) -> dict[str, Any]:
        """Compute calibration analysis with 5% buckets."""
        # Only analyze probabilities >= 0.50
        mask = y_prob >= 0.50
        y_true_filtered = y_true[mask]
        y_prob_filtered = y_prob[mask]

        if len(y_true_filtered) == 0:
            return {
                "buckets": [],
                "calibration_error": 0.0,
                "calibration_max_error": 0.0,
            }

        # 5% buckets from 0.50 to 1.00
        bucket_edges = [
            0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00
        ]
        buckets = []
        errors = []
        weights = []

        for i in range(len(bucket_edges) - 1):
            low, high = bucket_edges[i], bucket_edges[i + 1]

            # Include upper bound only for last bucket
            if i == len(bucket_edges) - 2:
                bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered <= high)
            else:
                bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered < high)

            if not bucket_mask.any():
                continue

            bucket_probs = y_prob_filtered[bucket_mask]
            bucket_true = y_true_filtered[bucket_mask]

            predicted_mean = float(np.mean(bucket_probs))
            actual = float(np.mean(bucket_true))
            n = int(bucket_mask.sum())
            error = abs(predicted_mean - actual)

            buckets.append({
                "range": [low, high],
                "predicted_mean": predicted_mean,
                "actual": actual,
                "n": n,
                "error": error,
            })
            errors.append(error)
            weights.append(n)

        # Weighted mean calibration error
        if weights:
            calibration_error = float(np.average(errors, weights=weights))
            calibration_max_error = float(max(errors))
        else:
            calibration_error = 0.0
            calibration_max_error = 0.0

        return {
            "buckets": buckets,
            "calibration_error": calibration_error,
            "calibration_max_error": calibration_max_error,
        }


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
