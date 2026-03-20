"""Diagnostics for game projection analysis."""


from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl

from mvp.model.diagnostics import ROUND_ORDER
from mvp.projection.metrics import compute_regression_metrics


def _segment_regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    """Compute regression metrics for a segment."""
    if len(y_true) == 0:
        return {"mae": 0.0, "rmse": 0.0, "r_squared": 0.0, "n": 0}
    m = compute_regression_metrics(y_true, y_pred)
    m["n"] = len(y_true)
    return m


class ProjectionDiagnostics:
    """Compute diagnostics for projection analysis."""

    def compute_all(self, predictions: list[dict[str, Any]]) -> "ProjectionDiagnosticResults":
        """Compute all diagnostics on aggregated predictions.

        Args:
            predictions: List of dicts with keys "df", "y_true", "y_pred"
                for each fold.
        """
        if not predictions:
            return ProjectionDiagnosticResults(
                residuals={}, segments={}, match_level={},
            )

        combined_df = pl.concat([p["df"] for p in predictions])
        combined_y_true = np.concatenate([p["y_true"] for p in predictions])
        combined_y_pred = np.concatenate([p["y_pred"] for p in predictions])

        residuals = self._residual_analysis(combined_y_true, combined_y_pred)
        segments = self._segment_breakdowns(combined_df, combined_y_true, combined_y_pred)
        match_level = self._match_level_analysis(combined_df, combined_y_true, combined_y_pred)

        return ProjectionDiagnosticResults(
            residuals=residuals,
            segments=segments,
            match_level=match_level,
        )

    def _residual_analysis(
        self, y_true: np.ndarray, y_pred: np.ndarray
    ) -> dict[str, Any]:
        """Analyze residual distribution."""
        residuals = y_true - y_pred
        abs_residuals = np.abs(residuals)

        result: dict[str, Any] = {
            "mean_residual": float(np.mean(residuals)),
            "std_residual": float(np.std(residuals)),
            "skewness": float(_skewness(residuals)),
            "median_abs_residual": float(np.median(abs_residuals)),
        }

        # Residual by predicted-value bins
        bin_edges = np.percentile(y_pred, [0, 20, 40, 60, 80, 100])
        bins = []
        for i in range(len(bin_edges) - 1):
            if i == len(bin_edges) - 2:
                mask = (y_pred >= bin_edges[i]) & (y_pred <= bin_edges[i + 1])
            else:
                mask = (y_pred >= bin_edges[i]) & (y_pred < bin_edges[i + 1])
            if not mask.any():
                continue
            bins.append({
                "range": [float(bin_edges[i]), float(bin_edges[i + 1])],
                "mean_residual": float(np.mean(residuals[mask])),
                "mae": float(np.mean(abs_residuals[mask])),
                "n": int(mask.sum()),
            })
        result["by_predicted_bin"] = bins
        return result

    def _segment_breakdowns(
        self, df: pl.DataFrame, y_true: np.ndarray, y_pred: np.ndarray
    ) -> dict[str, Any]:
        """Compute MAE/RMSE per segment."""
        result: dict[str, Any] = {}

        # By circuit
        if "circuit" in df.columns:
            result["circuit"] = {}
            for circuit in df["circuit"].drop_nulls().unique().sort().to_list():
                mask = (df["circuit"] == circuit).fill_null(False).to_numpy()
                if mask.any():
                    result["circuit"][circuit] = _segment_regression_metrics(
                        y_true[mask], y_pred[mask]
                    )

        # By surface
        if "surface" in df.columns:
            result["surface"] = {}
            for surface in df["surface"].drop_nulls().unique().sort().to_list():
                mask = (df["surface"] == surface).fill_null(False).to_numpy()
                if mask.any():
                    result["surface"][surface] = _segment_regression_metrics(
                        y_true[mask], y_pred[mask]
                    )

        # By round
        if "round" in df.columns:
            result["round"] = {}
            rounds_arr = df["round"].fill_null("").to_numpy()
            for rnd in ROUND_ORDER:
                mask = rounds_arr == rnd
                if mask.any():
                    result["round"][rnd] = _segment_regression_metrics(
                        y_true[mask], y_pred[mask]
                    )

        # By best_of (Bo3 vs Bo5)
        if "best_of" in df.columns:
            result["best_of"] = {}
            for bo in df["best_of"].drop_nulls().unique().sort().to_list():
                mask = (df["best_of"] == bo).fill_null(False).to_numpy()
                if mask.any():
                    result["best_of"][str(bo)] = _segment_regression_metrics(
                        y_true[mask], y_pred[mask]
                    )

        return result

    def _match_level_analysis(
        self, df: pl.DataFrame, y_true: np.ndarray, y_pred: np.ndarray
    ) -> dict[str, Any]:
        """Match-level pairing analysis.

        Groups by match_uid, sums both players' predictions to get
        predicted total games, and computes spread accuracy.
        """
        if "match_uid" not in df.columns:
            return {}

        match_df = df.select("match_uid").with_columns(
            pl.Series("y_true", y_true),
            pl.Series("y_pred", y_pred),
        )

        # Group by match: sum predictions and actuals
        grouped = match_df.group_by("match_uid").agg(
            pl.col("y_true").sum().alias("actual_total"),
            pl.col("y_pred").sum().alias("pred_total"),
            pl.col("y_true").max().alias("winner_games"),
            pl.col("y_true").min().alias("loser_games"),
            pl.col("y_pred").max().alias("pred_winner_games"),
            pl.col("y_pred").min().alias("pred_loser_games"),
            pl.col("y_true").count().alias("n_rows"),
        ).filter(pl.col("n_rows") == 2)  # Only complete pairs

        if len(grouped) == 0:
            return {}

        actual_total = grouped["actual_total"].to_numpy().astype(float)
        pred_total = grouped["pred_total"].to_numpy().astype(float)
        total_mae = float(np.mean(np.abs(actual_total - pred_total)))

        # Spread: difference between players' games
        actual_spread = grouped["winner_games"].to_numpy().astype(float) - grouped["loser_games"].to_numpy().astype(float)
        pred_spread = grouped["pred_winner_games"].to_numpy().astype(float) - grouped["pred_loser_games"].to_numpy().astype(float)
        spread_mae = float(np.mean(np.abs(actual_spread - pred_spread)))

        # Directional accuracy: did predicted spread get the winner right?
        # Winner is the player with more actual games
        # If predicted spread sign matches actual, we got direction right
        directional_correct = float(np.mean((pred_spread > 0) == (actual_spread > 0)))

        return {
            "n_matches": len(grouped),
            "total_games_mae": total_mae,
            "total_games_mean_actual": float(np.mean(actual_total)),
            "total_games_mean_pred": float(np.mean(pred_total)),
            "spread_mae": spread_mae,
            "directional_accuracy": directional_correct,
        }


@dataclass
class ProjectionDiagnosticResults:
    """Container for projection diagnostic results."""

    residuals: dict[str, Any]
    segments: dict[str, Any]
    match_level: dict[str, Any]

    @property
    def metrics(self) -> dict[str, float]:
        """Flatten results to MLflow-loggable metrics."""
        result: dict[str, float] = {}

        # Residual summary
        for key in ["mean_residual", "std_residual", "skewness", "median_abs_residual"]:
            if key in self.residuals:
                result[key] = self.residuals[key]

        # Segment metrics
        for seg_type, segments in self.segments.items():
            for seg_value, m in segments.items():
                for metric_name in ["mae", "rmse", "r_squared"]:
                    if metric_name in m:
                        result[f"segment_{seg_type}_{seg_value}_{metric_name}"] = m[metric_name]

        # Match-level
        for key in ["total_games_mae", "spread_mae", "directional_accuracy"]:
            if key in self.match_level:
                result[f"match_{key}"] = self.match_level[key]

        return result


def _skewness(arr: np.ndarray) -> float:
    """Compute sample skewness."""
    n = len(arr)
    if n < 3:
        return 0.0
    mean = np.mean(arr)
    std = np.std(arr, ddof=1)
    if std == 0:
        return 0.0
    return float((n / ((n - 1) * (n - 2))) * np.sum(((arr - mean) / std) ** 3))
