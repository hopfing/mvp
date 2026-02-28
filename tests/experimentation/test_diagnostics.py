"""Tests for diagnostics module."""

import json

import numpy as np
import polars as pl

from mvp.experimentation.diagnostics import DiagnosticResults, Diagnostics


class TestDiagnosticResults:
    """Tests for DiagnosticResults dataclass."""

    def test_metrics_property_flattens_segments(self) -> None:
        """Metrics property returns flat dict for MLflow."""
        results = DiagnosticResults(
            segments={
                "circuit": {
                    "tour": {"accuracy": 0.67, "n_matches": 100},
                    "chal": {"accuracy": 0.63, "n_matches": 200},
                }
            },
            calibration={"calibration_error": 0.03, "calibration_max_error": 0.08},
            errors={"error_rate_80plus": 0.09, "error_count_80plus": 5},
            temporal={"temporal_drift": 0.02},
        )

        metrics = results.metrics

        assert metrics["segment_circuit_tour_accuracy"] == 0.67
        assert metrics["segment_circuit_chal_accuracy"] == 0.63
        assert metrics["calibration_error"] == 0.03
        assert metrics["error_rate_80plus"] == 0.09
        assert metrics["temporal_drift"] == 0.02

    def test_to_json_returns_valid_json(self) -> None:
        """to_json returns parseable JSON with all sections."""
        results = DiagnosticResults(
            segments={"circuit": {"tour": {"accuracy": 0.67}}},
            calibration={"calibration_error": 0.03},
            errors={"error_rate_80plus": 0.09},
            temporal={"temporal_drift": 0.02},
        )

        json_str = results.to_json()
        parsed = json.loads(json_str)

        assert "segments" in parsed
        assert "calibration" in parsed
        assert "errors" in parsed
        assert "temporal" in parsed


class TestDiagnosticsSegmentAnalysis:
    """Tests for segment analysis."""

    def test_circuit_segment_metrics(self) -> None:
        """Computes metrics separately for tour and chal."""
        df = pl.DataFrame({
            "circuit": ["tour", "tour", "chal", "chal"],
            "surface": ["Hard", "Hard", "Hard", "Hard"],
            "round": ["R32", "R32", "R32", "R32"],
            "player_ranking": [10, 20, 50, 100],
            "effective_match_date": ["2023-01-01"] * 4,
        })
        y_true = np.array([1, 0, 1, 1])
        y_prob = np.array([0.7, 0.6, 0.8, 0.7])

        diagnostics = Diagnostics()
        result = diagnostics._segment_metrics(df, y_true, y_prob)

        assert "circuit" in result
        assert "tour" in result["circuit"]
        assert "chal" in result["circuit"]
        assert result["circuit"]["tour"]["n_matches"] == 2
        assert result["circuit"]["chal"]["n_matches"] == 2

    def test_round_group_mapping(self) -> None:
        """Maps rounds to Qualifying/Early/Late groups."""
        df = pl.DataFrame({
            "circuit": ["tour"] * 6,
            "surface": ["Hard"] * 6,
            "round": ["Q1", "R64", "R32", "R16", "QF", "F"],
            "player_ranking": [100] * 6,
            "effective_match_date": ["2023-01-01"] * 6,
        })
        y_true = np.array([1, 1, 1, 1, 1, 1])
        y_prob = np.array([0.6, 0.6, 0.6, 0.6, 0.6, 0.6])

        diagnostics = Diagnostics()
        result = diagnostics._segment_metrics(df, y_true, y_prob)

        assert "round_group" in result
        assert result["round_group"]["Qualifying"]["n_matches"] == 1
        assert result["round_group"]["Early"]["n_matches"] == 2  # R64, R32
        assert result["round_group"]["Late"]["n_matches"] == 3   # R16, QF, F

    def test_ranking_bucket_assignment(self) -> None:
        """Assigns players to ranking buckets correctly."""
        df = pl.DataFrame({
            "circuit": ["tour"] * 5,
            "surface": ["Hard"] * 5,
            "round": ["R32"] * 5,
            "player_ranking": [10, 30, 75, 150, 300],
            "effective_match_date": ["2023-01-01"] * 5,
        })
        y_true = np.array([1, 1, 1, 1, 1])
        y_prob = np.array([0.6, 0.6, 0.6, 0.6, 0.6])

        diagnostics = Diagnostics()
        result = diagnostics._segment_metrics(df, y_true, y_prob)

        assert "ranking_bucket" in result
        assert result["ranking_bucket"]["1-20"]["n_matches"] == 1
        assert result["ranking_bucket"]["21-50"]["n_matches"] == 1
        assert result["ranking_bucket"]["51-100"]["n_matches"] == 1
        assert result["ranking_bucket"]["101-200"]["n_matches"] == 1
        assert result["ranking_bucket"]["201+"]["n_matches"] == 1


class TestDiagnosticsCalibration:
    """Tests for calibration analysis."""

    def test_calibration_buckets_use_actual_mean(self) -> None:
        """Calibration uses actual mean predicted prob, not midpoint."""
        # All predictions in 0.50-0.55 bucket, clustered at 0.51
        y_prob = np.array([0.51, 0.51, 0.52, 0.51, 0.52])
        y_true = np.array([1, 0, 1, 1, 0])  # 60% actual

        diagnostics = Diagnostics()
        result = diagnostics._calibration(y_true, y_prob)

        bucket = result["buckets"][0]
        assert bucket["range"] == [0.50, 0.55]
        assert 0.51 <= bucket["predicted_mean"] <= 0.52  # actual mean, not 0.525
        assert bucket["actual"] == 0.6
        assert bucket["n"] == 5

    def test_calibration_error_calculation(self) -> None:
        """Calibration error is weighted mean of bucket errors."""
        # Two buckets with known errors
        y_prob = np.array([0.52, 0.52, 0.72, 0.72])  # 2 in each bucket
        y_true = np.array([1, 0, 1, 1])  # 50% and 100% actual

        diagnostics = Diagnostics()
        result = diagnostics._calibration(y_true, y_prob)

        # Bucket 1: predicted ~0.52, actual 0.50, error ~0.02
        # Bucket 2: predicted ~0.72, actual 1.00, error ~0.28
        # Weighted mean: (2*0.02 + 2*0.28) / 4 = 0.15
        assert "calibration_error" in result
        assert 0.10 <= result["calibration_error"] <= 0.20

    def test_only_analyzes_probs_above_50(self) -> None:
        """Only probabilities >= 0.50 are analyzed."""
        y_prob = np.array([0.45, 0.55, 0.65])
        y_true = np.array([0, 1, 1])

        diagnostics = Diagnostics()
        result = diagnostics._calibration(y_true, y_prob)

        total_n = sum(b["n"] for b in result["buckets"])
        assert total_n == 2  # Only 0.55 and 0.65, not 0.45
