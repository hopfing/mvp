"""Tests for diagnostics module."""

import json

from mvp.experimentation.diagnostics import DiagnosticResults


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
