"""Diagnostics for experiment analysis."""


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

# Fixed-threshold conditions (physically meaningful cutoffs)
FIXED_CONDITIONS: list[tuple[str, str, Any]] = [
    ("Large Elo gap (>150)", "player_elo_surface_diff", lambda a: np.abs(a) > 150),
    ("Medium Elo gap (75-150)", "player_elo_surface_diff", lambda a: (np.abs(a) >= 75) & (np.abs(a) <= 150)),
    ("Small Elo gap (<75)", "player_elo_surface_diff", lambda a: np.abs(a) < 75),
    ("Age gap >6 years", "player_age_diff", lambda a: np.abs(a) > 6),
    ("High uncertainty (RD>150)", "elo_rd_sum", lambda a: a > 150),
]

# Extreme magnitude conditions — |feature| at p90/p95/p99
MAGNITUDE_FEATURES: list[tuple[str, str]] = [
    ("Svc pts matchup", "svc_pts_won_pct_matchup"),
    ("Ret pts matchup", "ret_pts_won_pct_matchup"),
    ("Svc 1st serve matchup", "svc_first_serve_win_pct_matchup"),
    ("Ret 2nd serve matchup", "ret_second_serve_win_pct_matchup"),
]

# Signed quintile bucket features (directional analysis)
SIGNED_BUCKET_FEATURES: list[tuple[str, str]] = [
    ("Svc Elo matchup", "player_svc_elo_matchup"),
    ("Ret Elo matchup", "player_ret_elo_matchup"),
]

# Cross-conditions: small Elo gap + extreme factor (ensemble drilldown)
CROSS_CONDITIONS: list[tuple[str, str, str]] = [
    ("+ extreme svc matchup", "svc_pts_won_pct_matchup", "percentile"),
    ("+ extreme ret matchup", "ret_pts_won_pct_matchup", "percentile"),
    ("+ extreme BP matchup", "ret_bp_pct_matchup", "percentile"),
    ("+ age gap >6", "player_age_diff", "abs_threshold_6"),
    ("+ high uncertainty", "elo_rd_sum", "threshold_150"),
]


def _resolve_column(df: pl.DataFrame, prefix: str) -> str | None:
    """Find the best matching column for a feature prefix.

    Checks exact match first, then looks for parameterized variants
    (e.g., prefix_365d, prefix_180d) and picks the longest horizon.
    """
    if prefix in df.columns:
        return prefix

    best_col = None
    best_horizon = -1
    suffix_start = len(prefix) + 1
    for col in df.columns:
        if not col.startswith(prefix + "_"):
            continue
        suffix = col[suffix_start:]
        if suffix.endswith("d") and suffix[:-1].isdigit():
            horizon = int(suffix[:-1])
            if horizon > best_horizon:
                best_horizon = horizon
                best_col = col

    return best_col


def _get_column_values(df: pl.DataFrame, prefix: str) -> tuple[str, np.ndarray] | None:
    """Resolve column and extract values. Returns (col_name, values) or None."""
    col = _resolve_column(df, prefix)
    if col is None:
        return None
    return col, df[col].fill_null(0).to_numpy().astype(float)


def _build_conditions(df: pl.DataFrame) -> list[tuple[str, np.ndarray]]:
    """Build all diagnostic condition masks from the data."""
    conditions: list[tuple[str, np.ndarray]] = []

    # Fixed threshold conditions
    for label, col_prefix, predicate_fn in FIXED_CONDITIONS:
        result = _get_column_values(df, col_prefix)
        if result is None:
            continue
        conditions.append((label, predicate_fn(result[1])))

    # Magnitude conditions: |feature| ≥ p90/p95/p99
    for feat_label, col_prefix in MAGNITUDE_FEATURES:
        result = _get_column_values(df, col_prefix)
        if result is None:
            continue
        abs_vals = np.abs(result[1])
        for pct_label, pct in [("≥p90", 90), ("≥p95", 95), ("≥p99", 99)]:
            threshold = float(np.percentile(abs_vals, pct))
            if threshold == 0:
                continue
            conditions.append((f"{feat_label} {pct_label}", abs_vals >= threshold))

    # Signed quintile buckets
    for feat_label, col_prefix in SIGNED_BUCKET_FEATURES:
        result = _get_column_values(df, col_prefix)
        if result is None:
            continue
        vals = result[1]
        p10, p25, p75, p90 = np.percentile(vals, [10, 25, 75, 90])
        for bucket_label, mask in [
            ("≤p10", vals <= p10),
            ("p10-p25", (vals > p10) & (vals <= p25)),
            ("p25-p75", (vals > p25) & (vals < p75)),
            ("p75-p90", (vals >= p75) & (vals < p90)),
            ("≥p90", vals >= p90),
        ]:
            conditions.append((f"{feat_label} {bucket_label}", mask))

    return conditions


def _build_cross_conditions(df: pl.DataFrame) -> list[tuple[str, np.ndarray]]:
    """Build small-Elo-gap cross-condition masks for ensemble drilldown."""
    conditions: list[tuple[str, np.ndarray]] = []

    elo_result = _get_column_values(df, "player_elo_surface_diff")
    if elo_result is None:
        return conditions
    small_gap = np.abs(elo_result[1]) < 75

    for cross_label, col_prefix, mode in CROSS_CONDITIONS:
        result = _get_column_values(df, col_prefix)
        if result is None:
            continue
        vals = result[1]
        if mode == "percentile":
            abs_vals = np.abs(vals)
            threshold = float(np.percentile(abs_vals, 90))
            if threshold == 0:
                continue
            extreme = abs_vals >= threshold
        elif mode == "abs_threshold_6":
            extreme = np.abs(vals) > 6
        elif mode == "threshold_150":
            extreme = vals > 150
        else:
            continue
        mask = small_gap & extreme
        conditions.append((f"Small gap {cross_label}", mask))

    return conditions


# Correction analysis breakdown dimensions
CORRECTION_BREAKDOWNS: list[tuple[str, list[tuple[str, str, Any]]]] = [
    ("By Surface", [
        ("Hard", "is_hard", lambda a: a > 0.5),
        ("Clay", "is_clay", lambda a: a > 0.5),
        ("Grass", "is_grass", lambda a: a > 0.5),
    ]),
    ("By Elo Gap", [
        ("Large (>150)", "player_elo_surface_diff", lambda a: np.abs(a) > 150),
        ("Medium (75-150)", "player_elo_surface_diff", lambda a: (np.abs(a) >= 75) & (np.abs(a) <= 150)),
        ("Small (<75)", "player_elo_surface_diff", lambda a: np.abs(a) < 75),
    ]),
    ("By Age Gap", [
        ("Small (<3)", "player_age_diff", lambda a: np.abs(a) < 3),
        ("Medium (3-6)", "player_age_diff", lambda a: (np.abs(a) >= 3) & (np.abs(a) <= 6)),
        ("Large (>6)", "player_age_diff", lambda a: np.abs(a) > 6),
    ]),
]

# Tertile breakdown dimensions (computed from data percentiles)
CORRECTION_TERTILE_FEATURES: list[tuple[str, str]] = [
    ("By Svc Elo Matchup", "player_svc_elo_matchup"),
    ("By Ret Elo Matchup", "player_ret_elo_matchup"),
]


def _build_correction_breakdowns(
    df: pl.DataFrame,
) -> list[tuple[str, list[tuple[str, np.ndarray]]]]:
    """Build grouped breakdown dimensions for correction analysis."""
    sections: list[tuple[str, list[tuple[str, np.ndarray]]]] = []

    for section_label, bucket_specs in CORRECTION_BREAKDOWNS:
        buckets: list[tuple[str, np.ndarray]] = []
        for bucket_label, col_prefix, predicate_fn in bucket_specs:
            result = _get_column_values(df, col_prefix)
            if result is None:
                continue
            buckets.append((bucket_label, predicate_fn(result[1])))
        if buckets:
            sections.append((section_label, buckets))

    for section_label, col_prefix in CORRECTION_TERTILE_FEATURES:
        result = _get_column_values(df, col_prefix)
        if result is None:
            continue
        vals = result[1]
        p33, p67 = np.percentile(vals, [33, 67])
        buckets = [
            (f"Low (≤p33: {p33:.0f})", vals <= p33),
            (f"Mid (p33-p67)", (vals > p33) & (vals < p67)),
            (f"High (≥p67: {p67:.0f})", vals >= p67),
        ]
        sections.append((section_label, buckets))

    return sections


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

    def _error_conditions(
        self, df: pl.DataFrame, y_true: np.ndarray, y_prob: np.ndarray
    ) -> dict[str, Any]:
        """Analyze model errors grouped by feature-based conditions."""
        y_pred = (y_prob >= 0.5).astype(int)
        is_error = y_pred != y_true
        total_errors = int(is_error.sum())

        conditions = []
        for label, mask in _build_conditions(df):
            n_matches = int(mask.sum())
            if n_matches == 0:
                continue
            n_errors = int((mask & is_error).sum())
            accuracy = 1.0 - (n_errors / n_matches)
            error_share = n_errors / total_errors if total_errors > 0 else 0.0
            conditions.append({
                "label": label,
                "n_matches": n_matches,
                "accuracy": accuracy,
                "n_errors": n_errors,
                "error_share": error_share,
            })

        return {"conditions": conditions, "total_errors": total_errors}

    def compute_all(
        self, predictions: list[dict[str, Any]]
    ) -> "DiagnosticResults":
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
        error_conditions = self._error_conditions(
            combined_df, combined_y_true, combined_y_prob
        )

        return DiagnosticResults(
            segments=segments,
            calibration=calibration,
            errors=errors,
            temporal=temporal,
            error_conditions=error_conditions,
        )


class EnsembleDiagnostics:
    """Compute ensemble-specific diagnostics."""

    def compute(
        self,
        y_true: np.ndarray,
        y_prob_ensemble: np.ndarray,
        per_model_preds: list[np.ndarray],
        weights: np.ndarray,
        base_names: list[str],
        strategy: str = "average",
        meta_intercept: float | None = None,
        meta_coefficients: dict[str, float] | None = None,
        combined_df: pl.DataFrame | None = None,
    ) -> dict[str, Any]:
        """Compute all ensemble diagnostics.

        Args:
            y_true: True labels.
            y_prob_ensemble: Combined ensemble predictions.
            per_model_preds: List of prediction arrays, one per base model.
            weights: Normalized weights for each base model.
            base_names: Names/paths of base model configs.
            strategy: Ensemble combining strategy.
            meta_intercept: Stacking meta-model intercept (stacking only).
            meta_coefficients: Stacking meta-model coefficients (stacking only).
            combined_df: Combined DataFrame for correction analysis.

        Returns:
            Dict with per_model_metrics, correlation, agreement, contribution.
        """
        result: dict[str, Any] = {}

        result["per_model_metrics"] = self._per_model_metrics(
            y_true, y_prob_ensemble, per_model_preds, base_names
        )
        result["correlation"] = self._prediction_correlation(
            per_model_preds, base_names
        )
        result["agreement"] = self._agreement_analysis(
            y_true, y_prob_ensemble, per_model_preds
        )
        result["consensus"] = self._consensus_analysis(
            y_true, y_prob_ensemble, per_model_preds
        )
        result["dissenter"] = self._dissenter_analysis(
            y_true, per_model_preds, base_names
        )
        result["contribution"] = self._contribution_analysis(
            y_true, per_model_preds, weights, base_names, strategy
        )

        if strategy == "stacking" and meta_coefficients is not None:
            result["meta_intercept"] = meta_intercept
            result["meta_coefficients"] = meta_coefficients
            result["meta_feature_names"] = list(meta_coefficients.keys())

        if combined_df is not None:
            result["correction_analysis"] = self._correction_analysis(
                y_true, per_model_preds[0], y_prob_ensemble, combined_df
            )

        return result

    def _correction_analysis(
        self,
        y_true: np.ndarray,
        primary_preds: np.ndarray,
        ensemble_preds: np.ndarray,
        df: pl.DataFrame,
    ) -> dict[str, Any]:
        """Compare primary model vs ensemble accuracy per breakdown dimension."""
        breakdowns = _build_correction_breakdowns(df)
        sections: list[dict[str, Any]] = []
        for section_label, buckets in breakdowns:
            rows = []
            for bucket_label, mask in buckets:
                n = int(mask.sum())
                if n == 0:
                    continue
                p_acc = float(((primary_preds[mask] >= 0.5).astype(int) == y_true[mask]).mean())
                e_acc = float(((ensemble_preds[mask] >= 0.5).astype(int) == y_true[mask]).mean())
                rows.append({
                    "label": bucket_label,
                    "n_matches": n,
                    "primary_accuracy": p_acc,
                    "ensemble_accuracy": e_acc,
                    "improvement": e_acc - p_acc,
                })
            if rows:
                sections.append({"section": section_label, "rows": rows})
        return {"sections": sections}

    def _per_model_metrics(
        self,
        y_true: np.ndarray,
        y_prob_ensemble: np.ndarray,
        per_model_preds: list[np.ndarray],
        base_names: list[str],
    ) -> dict[str, Any]:
        """Compute independent metrics for each base model and the ensemble."""
        results: dict[str, Any] = {}
        for name, preds in zip(base_names, per_model_preds):
            results[name] = _compute_metrics_for_segment(
                y_true, preds, include_calibration=True
            )
        results["ensemble"] = _compute_metrics_for_segment(
            y_true, y_prob_ensemble, include_calibration=True
        )
        return results

    def _prediction_correlation(
        self,
        per_model_preds: list[np.ndarray],
        base_names: list[str],
    ) -> dict[str, Any]:
        """Compute Pearson correlation matrix between base model predictions."""
        n = len(per_model_preds)
        matrix = np.zeros((n, n))
        for i in range(n):
            for j in range(n):
                if i == j:
                    matrix[i, j] = 1.0
                elif i < j:
                    corr = float(
                        np.corrcoef(per_model_preds[i], per_model_preds[j])[0, 1]
                    )
                    matrix[i, j] = corr
                    matrix[j, i] = corr
        return {
            "names": base_names,
            "matrix": matrix.tolist(),
        }

    def _agreement_analysis(
        self,
        y_true: np.ndarray,
        y_prob_ensemble: np.ndarray,
        per_model_preds: list[np.ndarray],
    ) -> dict[str, Any]:
        """Categorize matches by model agreement and ensemble correctness."""
        n_matches = len(y_true)
        ensemble_pred = (y_prob_ensemble >= 0.5).astype(int)
        ensemble_correct = ensemble_pred == y_true

        model_preds = np.array([(p >= 0.5).astype(int) for p in per_model_preds])
        all_agree = np.all(model_preds == model_preds[0], axis=0)

        categories = {
            "all_agree_correct": int((all_agree & ensemble_correct).sum()),
            "all_agree_wrong": int((all_agree & ~ensemble_correct).sum()),
            "disagree_ensemble_correct": int((~all_agree & ensemble_correct).sum()),
            "disagree_ensemble_wrong": int((~all_agree & ~ensemble_correct).sum()),
        }
        categories["total"] = n_matches
        for key in list(categories.keys()):
            if key != "total":
                categories[f"{key}_pct"] = (
                    categories[key] / n_matches if n_matches > 0 else 0.0
                )
        return categories

    def _consensus_analysis(
        self,
        y_true: np.ndarray,
        y_prob_ensemble: np.ndarray,
        per_model_preds: list[np.ndarray],
    ) -> dict[str, Any]:
        """Bucket predictions by consensus strength (N-0, N-1, ...) with accuracy."""
        n_models = len(per_model_preds)
        n_matches = len(y_true)
        if n_models < 2 or n_matches == 0:
            return {"buckets": []}

        model_preds = np.array([(p >= 0.5).astype(int) for p in per_model_preds])
        ensemble_pred = (y_prob_ensemble >= 0.5).astype(int)
        ensemble_correct = ensemble_pred == y_true

        # Count how many models agree with the ensemble's prediction per match
        agree_with_ensemble = np.sum(model_preds == ensemble_pred, axis=0)

        buckets = []
        for n_agree in range(n_models, 0, -1):
            n_disagree = n_models - n_agree
            mask = agree_with_ensemble == n_agree
            count = int(mask.sum())
            if count == 0:
                continue
            acc = float(ensemble_correct[mask].mean())
            buckets.append({
                "label": f"{n_agree}-{n_disagree}",
                "n_agree": n_agree,
                "n_disagree": n_disagree,
                "count": count,
                "pct": count / n_matches,
                "accuracy": acc,
            })

        return {"buckets": buckets}

    def _dissenter_analysis(
        self,
        y_true: np.ndarray,
        per_model_preds: list[np.ndarray],
        base_names: list[str],
    ) -> dict[str, Any]:
        """When each model is the lone dissenter from majority, how often is it right?"""
        n_models = len(per_model_preds)
        if n_models < 3:
            return {}

        model_preds = np.array([(p >= 0.5).astype(int) for p in per_model_preds])
        majority = (model_preds.mean(axis=0) >= 0.5).astype(int)

        results: dict[str, Any] = {}
        for i, name in enumerate(base_names):
            disagrees = model_preds[i] != majority
            others_agree = np.all(
                np.delete(model_preds, i, axis=0) == majority, axis=0
            )
            lone_dissenter = disagrees & others_agree
            count = int(lone_dissenter.sum())
            if count == 0:
                results[name] = {
                    "count": 0,
                    "dissenter_correct": 0.0,
                    "majority_correct": 0.0,
                }
                continue
            dissenter_pred = model_preds[i][lone_dissenter]
            actual = y_true[lone_dissenter]
            dissenter_correct = float((dissenter_pred == actual).mean())
            majority_correct = float((majority[lone_dissenter] == actual).mean())
            results[name] = {
                "count": count,
                "dissenter_correct": dissenter_correct,
                "majority_correct": majority_correct,
            }

        return results

    def _contribution_analysis(
        self,
        y_true: np.ndarray,
        per_model_preds: list[np.ndarray],
        weights: np.ndarray,
        base_names: list[str],
        strategy: str,
    ) -> dict[str, Any]:
        """Leave-one-out analysis: what happens if we remove each model."""
        results: dict[str, Any] = {}

        full_preds = np.array(per_model_preds)
        if strategy == "stacking":
            ensemble_prob = self._stacking_predict(full_preds, y_true)
        elif strategy == "weighted_average":
            ensemble_prob = np.average(full_preds, axis=0, weights=weights)
        else:
            ensemble_prob = np.mean(full_preds, axis=0)

        ensemble_prob_clipped = np.clip(ensemble_prob, 1e-15, 1 - 1e-15)
        if len(np.unique(y_true)) > 1:
            full_ll = float(log_loss(y_true, ensemble_prob_clipped))
        else:
            full_ll = 0.0
        full_cal = _compute_calibration_error(y_true, ensemble_prob)

        for i, name in enumerate(base_names):
            if len(per_model_preds) == 1:
                results[name] = {"log_loss_delta": 0.0, "calibration_delta": 0.0}
                continue

            remaining_idx = [j for j in range(len(per_model_preds)) if j != i]
            remaining_preds = full_preds[remaining_idx]
            if strategy == "stacking":
                loo_prob = self._stacking_predict(remaining_preds, y_true)
            elif strategy == "weighted_average":
                remaining_weights = weights[remaining_idx]
                remaining_weights = remaining_weights / remaining_weights.sum()
                loo_prob = np.average(
                    remaining_preds, axis=0, weights=remaining_weights
                )
            else:
                loo_prob = np.mean(remaining_preds, axis=0)

            loo_clipped = np.clip(loo_prob, 1e-15, 1 - 1e-15)
            if len(np.unique(y_true)) > 1:
                loo_ll = float(log_loss(y_true, loo_clipped))
            else:
                loo_ll = 0.0
            loo_cal = _compute_calibration_error(y_true, loo_prob)

            results[name] = {
                "log_loss_delta": loo_ll - full_ll,
                "calibration_delta": loo_cal - full_cal,
            }

        return results

    def _stacking_predict(
        self, preds: np.ndarray, y_true: np.ndarray
    ) -> np.ndarray:
        """Fit a fresh logistic regression on preds and return predictions."""
        from sklearn.linear_model import LogisticRegression

        X = preds.T  # (n_samples, n_models)
        lr = LogisticRegression(max_iter=1000, random_state=42)
        lr.fit(X, y_true)
        return lr.predict_proba(X)[:, 1]


@dataclass
class DiagnosticResults:
    """Container for all diagnostic results."""

    segments: dict[str, Any]
    calibration: dict[str, Any]
    errors: dict[str, Any]
    temporal: dict[str, Any]
    error_conditions: dict[str, Any] | None = None
    ensemble: dict[str, Any] | None = None

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

        # Add ensemble metrics
        if self.ensemble is not None:
            agreement = self.ensemble.get("agreement", {})
            for key in ["all_agree_correct_pct", "all_agree_wrong_pct",
                        "disagree_ensemble_correct_pct", "disagree_ensemble_wrong_pct"]:
                if key in agreement:
                    result[f"ensemble_{key}"] = agreement[key]
            correlation = self.ensemble.get("correlation", {})
            matrix = correlation.get("matrix", [])
            if len(matrix) >= 2:
                result["ensemble_pred_correlation"] = matrix[0][1]
            consensus = self.ensemble.get("consensus", {})
            for bucket in consensus.get("buckets", []):
                label = bucket["label"].replace("-", "v")
                result[f"ensemble_consensus_{label}_accuracy"] = bucket["accuracy"]
                result[f"ensemble_consensus_{label}_pct"] = bucket["pct"]

        return result

    def to_json(self) -> str:
        """Serialize full results to JSON."""
        data: dict[str, Any] = {
            "segments": self.segments,
            "calibration": self.calibration,
            "errors": self.errors,
            "temporal": self.temporal,
        }
        if self.error_conditions is not None:
            data["error_conditions"] = self.error_conditions
        if self.ensemble is not None:
            data["ensemble"] = self.ensemble
        return json.dumps(data, indent=2, default=str)
