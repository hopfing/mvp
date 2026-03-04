"""Diagnostics for experiment analysis."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score

# Ordered rounds for per-round diagnostics
ROUND_ORDER: list[str] = ["Q1", "Q2", "Q3", "RR", "R128", "R64", "R32", "R16", "QF", "SF", "F"]

# Performance-based betting groups (circuit-aware)
# Tour: flat performance R128-SF, only F/RR separate
# Chal: Q1 stands alone (.813 AUC), Q2-QF moderate, SF/F weak
BETTING_GROUPS: dict[str, dict[str, list[str]]] = {
    "tour": {
        "Qualifying": ["Q1", "Q2", "Q3"],
        "Main Draw": ["R128", "R64", "R32", "R16", "QF", "SF"],
        "Final": ["F", "RR"],
    },
    "chal": {
        "Strong": ["Q1"],
        "Mid": ["Q2", "R32", "R16", "QF"],
        "Tight": ["SF", "F"],
    },
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
    y_true: np.ndarray, y_prob: np.ndarray, include_calibration: bool = False
) -> dict[str, float]:
    """Compute standard metrics for a segment.

    Args:
        y_true: True labels.
        y_prob: Predicted probabilities.
        include_calibration: If True, include calibration_error and error_rate_80plus.
    """
    if len(y_true) == 0:
        result = {
            "accuracy": 0.0,
            "log_loss": 0.0,
            "brier_score": 0.0,
            "roc_auc": 0.0,
            "n_matches": 0,
        }
        if include_calibration:
            result["calibration_error"] = 0.0
            result["error_rate_80plus"] = 0.0
        return result

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

    if include_calibration:
        metrics["calibration_error"] = _compute_calibration_error(y_true, y_prob)
        metrics["error_rate_80plus"] = _compute_error_rate_80plus(y_true, y_prob)

    return metrics


def _compute_calibration_error(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute weighted mean calibration error for probabilities >= 0.50."""
    mask = y_prob >= 0.50
    y_true_filtered = y_true[mask]
    y_prob_filtered = y_prob[mask]

    if len(y_true_filtered) == 0:
        return 0.0

    bucket_edges = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00]
    errors = []
    weights = []

    for i in range(len(bucket_edges) - 1):
        low, high = bucket_edges[i], bucket_edges[i + 1]
        if i == len(bucket_edges) - 2:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered <= high)
        else:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered < high)

        if not bucket_mask.any():
            continue

        predicted_mean = float(np.mean(y_prob_filtered[bucket_mask]))
        actual = float(np.mean(y_true_filtered[bucket_mask]))
        n = int(bucket_mask.sum())
        error = abs(predicted_mean - actual)

        errors.append(error)
        weights.append(n)

    if weights:
        return float(np.average(errors, weights=weights))
    return 0.0


def _compute_error_rate_80plus(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute error rate for predictions at 80%+ confidence."""
    y_pred = (y_prob >= 0.5).astype(int)
    is_error = y_pred != y_true
    tier_mask = y_prob >= 0.80
    tier_total = int(tier_mask.sum())
    if tier_total == 0:
        return 0.0
    tier_errors = int((tier_mask & is_error).sum())
    return tier_errors / tier_total


class Diagnostics:
    """Compute diagnostics for experiment analysis."""

    def _get_betting_group(self, round_val: str, circuit: str) -> str:
        """Map round to betting group based on circuit."""
        circuit_groups = BETTING_GROUPS.get(circuit, {})
        for group, rounds in circuit_groups.items():
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
    ) -> dict[str, Any]:
        """Compute metrics with circuit as primary segment.

        Returns structure:
        {
            "by_circuit": {
                "chal": {
                    "overall": {metrics with calibration},
                    "surface": {"Clay": {...}, "Hard": {...}},
                    "round": {"Q1": {...}, "R32": {...}, ...},
                    "betting_group": {"Strong": {...}, "Mid": {...}, "Tight": {...}},
                },
                "tour": {...},
            },
            "overall": {
                "surface": {"Clay": {...}, ...},
                "round": {"Q1": {...}, ...},
            }
        }
        """
        result: dict[str, Any] = {"by_circuit": {}, "overall": {}}

        # Precompute round values
        rounds_arr = None
        if "round" in df.columns:
            rounds_arr = df["round"].fill_null("").to_numpy()

        # Get list of circuits
        circuits = []
        if "circuit" in df.columns:
            circuits = df["circuit"].drop_nulls().unique().sort().to_list()

        # Process each circuit
        for circuit in circuits:
            circuit_mask = (df["circuit"] == circuit).fill_null(False).to_numpy()
            if not circuit_mask.any():
                continue

            circuit_y_true = y_true[circuit_mask]
            circuit_y_prob = y_prob[circuit_mask]
            circuit_df = df.filter(pl.col("circuit") == circuit)

            circuit_data: dict[str, Any] = {}

            # Overall metrics for this circuit
            circuit_data["overall"] = _compute_metrics_for_segment(
                circuit_y_true, circuit_y_prob, include_calibration=True
            )

            # Surface subsegments within this circuit
            circuit_data["surface"] = {}
            if "surface" in df.columns:
                for surface in circuit_df["surface"].drop_nulls().unique().sort().to_list():
                    surface_mask = (circuit_df["surface"] == surface).fill_null(False).to_numpy()
                    if surface_mask.any():
                        circuit_data["surface"][surface] = _compute_metrics_for_segment(
                            circuit_y_true[surface_mask],
                            circuit_y_prob[surface_mask],
                            include_calibration=True,
                        )

            # Per-round metrics within this circuit
            circuit_data["round"] = {}
            if rounds_arr is not None:
                circuit_rounds = rounds_arr[circuit_mask]
                for rnd in ROUND_ORDER:
                    rnd_mask = circuit_rounds == rnd
                    if rnd_mask.any():
                        circuit_data["round"][rnd] = _compute_metrics_for_segment(
                            circuit_y_true[rnd_mask],
                            circuit_y_prob[rnd_mask],
                            include_calibration=True,
                        )

            # Betting group subsegments (circuit-aware)
            circuit_data["betting_group"] = {}
            circuit_groups = BETTING_GROUPS.get(circuit, {})
            if rounds_arr is not None and circuit_groups:
                circuit_rounds = rounds_arr[circuit_mask]
                for group, group_rounds in circuit_groups.items():
                    group_mask = np.isin(circuit_rounds, group_rounds)
                    if group_mask.any():
                        circuit_data["betting_group"][group] = _compute_metrics_for_segment(
                            circuit_y_true[group_mask],
                            circuit_y_prob[group_mask],
                            include_calibration=True,
                        )

            result["by_circuit"][circuit] = circuit_data

        # Overall (non-circuit-specific) segments
        result["overall"]["surface"] = {}
        if "surface" in df.columns:
            for surface in df["surface"].drop_nulls().unique().sort().to_list():
                mask = (df["surface"] == surface).fill_null(False).to_numpy()
                if mask.any():
                    result["overall"]["surface"][surface] = _compute_metrics_for_segment(
                        y_true[mask], y_prob[mask], include_calibration=True
                    )

        result["overall"]["round"] = {}
        if rounds_arr is not None:
            for rnd in ROUND_ORDER:
                mask = rounds_arr == rnd
                if mask.any():
                    result["overall"]["round"][rnd] = _compute_metrics_for_segment(
                        y_true[mask], y_prob[mask], include_calibration=True
                    )

        return result

    def _error_analysis(
        self, df: pl.DataFrame, y_true: np.ndarray, y_prob: np.ndarray
    ) -> dict[str, Any]:
        """Analyze high-confidence errors."""
        y_pred = (y_prob >= 0.5).astype(int)
        is_error = y_pred != y_true

        # Confidence tiers
        tiers = [
            ("60plus", 0.60),
            ("70plus", 0.70),
            ("80plus", 0.80),
            ("90plus", 0.90),
        ]

        summary = {}
        for name, threshold in tiers:
            tier_mask = y_prob >= threshold
            tier_total = int(tier_mask.sum())
            tier_errors = int((tier_mask & is_error).sum())
            error_rate = tier_errors / tier_total if tier_total > 0 else 0.0

            summary[name] = {
                "total": tier_total,
                "errors": tier_errors,
                "error_rate": error_rate,
            }

        # High-confidence errors (80%+) with match details
        high_conf_error_mask = (y_prob >= 0.80) & is_error
        high_conf_errors = []

        if high_conf_error_mask.any():
            error_indices = np.where(high_conf_error_mask)[0]
            error_df = df[error_indices.tolist()]

            for i, idx in enumerate(error_indices):
                row = error_df.row(i, named=True)
                high_conf_errors.append({
                    "match_uid": row.get("match_uid", ""),
                    "tournament_name": row.get("tournament_name", ""),
                    "round": row.get("round", ""),
                    "player_name": row.get("player_name", ""),
                    "opp_name": row.get("opp_name", ""),
                    "predicted_prob": float(y_prob[idx]),
                    "effective_match_date": str(row.get("effective_match_date", "")),
                })

        return {
            "summary": summary,
            "high_confidence_errors": high_conf_errors,
            "error_rate_60plus": summary["60plus"]["error_rate"],
            "error_rate_70plus": summary["70plus"]["error_rate"],
            "error_rate_80plus": summary["80plus"]["error_rate"],
            "error_rate_90plus": summary["90plus"]["error_rate"],
            "error_count_80plus": summary["80plus"]["errors"],
        }

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

    def _temporal_stability(
        self, df: pl.DataFrame, y_true: np.ndarray, y_prob: np.ndarray
    ) -> dict[str, Any]:
        """Analyze performance stability across time periods."""
        # Extract year from effective_match_date
        if "effective_match_date" not in df.columns:
            return {
                "periods": [],
                "overall_accuracy": 0.0,
                "temporal_drift": 0.0,
            }

        dates = df["effective_match_date"].to_list()
        years = np.array([str(d)[:4] for d in dates])

        # Overall accuracy
        y_pred = (y_prob >= 0.5).astype(int)
        overall_accuracy = float(accuracy_score(y_true, y_pred))

        # Metrics per year
        periods = []
        drifts = []

        for year in sorted(set(years)):
            mask = years == year
            if not mask.any():
                continue

            year_metrics = _compute_metrics_for_segment(y_true[mask], y_prob[mask])
            year_metrics["period"] = year
            periods.append(year_metrics)

            drift = abs(year_metrics["accuracy"] - overall_accuracy)
            drifts.append(drift)

        temporal_drift = max(drifts) if drifts else 0.0

        return {
            "periods": periods,
            "overall_accuracy": overall_accuracy,
            "temporal_drift": temporal_drift,
        }

    def compute_all(
        self, predictions: list[dict[str, Any]]
    ) -> DiagnosticResults:
        """Compute all diagnostics on aggregated predictions.

        Args:
            predictions: List of dicts with keys "df", "y_true", "y_prob"
                for each fold.

        Returns:
            DiagnosticResults with all computed diagnostics.
        """
        if not predictions:
            return DiagnosticResults(
                segments={},
                calibration=self._calibration(np.array([]), np.array([])),
                errors=self._error_analysis(
                    pl.DataFrame(), np.array([]), np.array([])
                ),
                temporal=self._temporal_stability(
                    pl.DataFrame(), np.array([]), np.array([])
                ),
            )

        dfs = [p["df"] for p in predictions]
        y_trues = [p["y_true"] for p in predictions]
        y_probs = [p["y_prob"] for p in predictions]

        combined_df = pl.concat(dfs)
        combined_y_true = np.concatenate(y_trues)
        combined_y_prob = np.concatenate(y_probs)

        segments = self._segment_metrics(combined_df, combined_y_true, combined_y_prob)
        calibration = self._calibration(combined_y_true, combined_y_prob)
        errors = self._error_analysis(combined_df, combined_y_true, combined_y_prob)
        temporal = self._temporal_stability(
            combined_df, combined_y_true, combined_y_prob
        )

        return DiagnosticResults(
            segments=segments,
            calibration=calibration,
            errors=errors,
            temporal=temporal,
        )


@dataclass
class DiagnosticResults:
    """Container for all diagnostic results."""

    segments: dict[str, Any]
    calibration: dict[str, Any]
    errors: dict[str, Any]
    temporal: dict[str, Any]

    @property
    def metrics(self) -> dict[str, float]:
        """Flatten results to MLflow-loggable metrics."""
        result: dict[str, float] = {}

        # Flatten circuit-based segment metrics
        by_circuit = self.segments.get("by_circuit", {})
        for circuit, circuit_data in by_circuit.items():
            # Circuit overall
            if "overall" in circuit_data:
                for metric_name, value in circuit_data["overall"].items():
                    if metric_name != "n_matches" and isinstance(value, (int, float)):
                        key = f"segment_{circuit}_{metric_name}"
                        result[key] = value

            # Circuit subsegments
            for subseg_type in ["surface", "round", "betting_group"]:
                if subseg_type in circuit_data:
                    for subseg_value, metrics in circuit_data[subseg_type].items():
                        for metric_name, value in metrics.items():
                            if metric_name != "n_matches" and isinstance(value, (int, float)):
                                key = f"segment_{circuit}_{subseg_type}_{subseg_value}_{metric_name}"
                                result[key] = value

        # Flatten overall segment metrics (non-circuit-specific)
        overall = self.segments.get("overall", {})
        for seg_type, segments in overall.items():
            for seg_value, metrics in segments.items():
                for metric_name, value in metrics.items():
                    if metric_name != "n_matches" and isinstance(value, (int, float)):
                        key = f"segment_overall_{seg_type}_{seg_value}_{metric_name}"
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
