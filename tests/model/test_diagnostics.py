"""Tests for diagnostics module."""

import json

import numpy as np
import polars as pl

from mvp.model.diagnostics import DiagnosticResults, Diagnostics


class TestDiagnosticResults:
    """Tests for DiagnosticResults dataclass."""

    def test_metrics_property_flattens_segments(self) -> None:
        """Metrics property returns flat dict for MLflow."""
        results = DiagnosticResults(
            segments={
                "by_circuit": {
                    "tour": {
                        "overall": {"accuracy": 0.67, "n_matches": 100},
                        "surface": {"Hard": {"accuracy": 0.68, "n_matches": 80}},
                    },
                    "chal": {
                        "overall": {"accuracy": 0.63, "n_matches": 200},
                    },
                },
                "overall": {},
            },
            calibration={"calibration_error": 0.03, "calibration_max_error": 0.08},
            errors={"error_rate_80plus": 0.09, "error_count_80plus": 5},
            temporal={"temporal_drift": 0.02},
        )

        metrics = results.metrics

        assert metrics["segment_tour_accuracy"] == 0.67
        assert metrics["segment_chal_accuracy"] == 0.63
        assert metrics["segment_tour_surface_Hard_accuracy"] == 0.68
        assert metrics["calibration_error"] == 0.03
        assert metrics["error_rate_80plus"] == 0.09
        assert metrics["temporal_drift"] == 0.02

    def test_to_json_returns_valid_json(self) -> None:
        """to_json returns parseable JSON with all sections."""
        results = DiagnosticResults(
            segments={"by_circuit": {"tour": {"overall": {"accuracy": 0.67}}}, "overall": {}},
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
        """Computes metrics separately for tour and chal with subsegments."""
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

        assert "by_circuit" in result
        assert "tour" in result["by_circuit"]
        assert "chal" in result["by_circuit"]
        assert result["by_circuit"]["tour"]["overall"]["n_matches"] == 2
        assert result["by_circuit"]["chal"]["overall"]["n_matches"] == 2
        # Check subsegments exist
        assert "surface" in result["by_circuit"]["tour"]
        assert "Hard" in result["by_circuit"]["tour"]["surface"]

    def test_per_round_metrics(self) -> None:
        """Computes metrics for each individual round."""
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

        # Per-round within circuit
        tour_rounds = result["by_circuit"]["tour"]["round"]
        assert tour_rounds["Q1"]["n_matches"] == 1
        assert tour_rounds["R64"]["n_matches"] == 1
        assert tour_rounds["F"]["n_matches"] == 1

        # Overall per-round
        assert result["overall"]["round"]["Q1"]["n_matches"] == 1
        assert result["overall"]["round"]["R32"]["n_matches"] == 1

    def test_betting_group_mapping(self) -> None:
        """Maps rounds to circuit-aware betting groups."""
        df = pl.DataFrame({
            "circuit": ["tour"] * 4 + ["chal"] * 3,
            "surface": ["Hard"] * 7,
            "round": ["Q1", "R32", "SF", "F", "Q1", "R32", "SF"],
            "player_ranking": [100] * 7,
            "effective_match_date": ["2023-01-01"] * 7,
        })
        y_true = np.array([1, 1, 1, 1, 1, 1, 1])
        y_prob = np.array([0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6])

        diagnostics = Diagnostics()
        result = diagnostics._segment_metrics(df, y_true, y_prob)

        # Tour: Qualifying=[Q1], Main Draw=[R32,SF], Final=[F]
        tour_bg = result["by_circuit"]["tour"]["betting_group"]
        assert tour_bg["Qualifying"]["n_matches"] == 1
        assert tour_bg["Main Draw"]["n_matches"] == 2  # R32, SF
        assert tour_bg["Final"]["n_matches"] == 1       # F

        # Chal: Strong=[Q1], Mid=[R32], Tight=[SF]
        chal_bg = result["by_circuit"]["chal"]["betting_group"]
        assert chal_bg["Strong"]["n_matches"] == 1
        assert chal_bg["Mid"]["n_matches"] == 1
        assert chal_bg["Tight"]["n_matches"] == 1

    def test_surface_subsegment_within_circuit(self) -> None:
        """Surface metrics computed within each circuit."""
        df = pl.DataFrame({
            "circuit": ["tour", "tour", "chal", "chal", "chal"],
            "surface": ["Hard", "Clay", "Hard", "Hard", "Clay"],
            "round": ["R32"] * 5,
            "effective_match_date": ["2023-01-01"] * 5,
        })
        y_true = np.array([1, 1, 1, 1, 1])
        y_prob = np.array([0.6, 0.6, 0.6, 0.6, 0.6])

        diagnostics = Diagnostics()
        result = diagnostics._segment_metrics(df, y_true, y_prob)

        # Tour has 1 Hard, 1 Clay
        assert result["by_circuit"]["tour"]["surface"]["Hard"]["n_matches"] == 1
        assert result["by_circuit"]["tour"]["surface"]["Clay"]["n_matches"] == 1

        # Chal has 2 Hard, 1 Clay
        assert result["by_circuit"]["chal"]["surface"]["Hard"]["n_matches"] == 2
        assert result["by_circuit"]["chal"]["surface"]["Clay"]["n_matches"] == 1


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


class TestDiagnosticsErrorAnalysis:
    """Tests for error analysis."""

    def test_error_rate_by_confidence_tier(self) -> None:
        """Computes error rates for each confidence tier."""
        # 10 predictions at various confidence levels (all predict 1)
        y_prob = np.array([0.55, 0.62, 0.65, 0.72, 0.75, 0.78, 0.82, 0.85, 0.91, 0.95])
        y_true = np.array([1, 0, 1, 1, 0, 1, 0, 1, 1, 0])  # some wrong

        df = pl.DataFrame({
            "match_uid": [f"m{i}" for i in range(10)],
            "tournament_name": ["Test"] * 10,
            "round": ["R32"] * 10,
            "player_name": ["A"] * 10,
            "opp_name": ["B"] * 10,
            "effective_match_date": ["2023-01-01"] * 10,
        })

        diagnostics = Diagnostics()
        result = diagnostics._error_analysis(df, y_true, y_prob)

        assert "summary" in result
        assert "60plus" in result["summary"]
        assert "70plus" in result["summary"]
        assert "80plus" in result["summary"]
        assert "90plus" in result["summary"]

    def test_high_confidence_errors_includes_match_details(self) -> None:
        """80%+ errors include match-level details."""
        y_prob = np.array([0.85, 0.90])
        y_true = np.array([0, 0])  # both wrong

        df = pl.DataFrame({
            "match_uid": ["uid1", "uid2"],
            "tournament_name": ["Monte Carlo", "Rome"],
            "round": ["R32", "QF"],
            "player_name": ["Player A", "Player C"],
            "opp_name": ["Player B", "Player D"],
            "effective_match_date": ["2023-04-15", "2023-05-10"],
        })

        diagnostics = Diagnostics()
        result = diagnostics._error_analysis(df, y_true, y_prob)

        assert "high_confidence_errors" in result
        assert len(result["high_confidence_errors"]) == 2

        error = result["high_confidence_errors"][0]
        assert "match_uid" in error
        assert "tournament_name" in error
        assert "predicted_prob" in error


class TestDiagnosticsTemporalStability:
    """Tests for temporal stability analysis."""

    def test_metrics_by_year(self) -> None:
        """Computes metrics for each year."""
        df = pl.DataFrame({
            "effective_match_date": [
                "2022-03-15", "2022-06-20",
                "2023-02-10", "2023-08-15",
            ],
        })
        y_true = np.array([1, 0, 1, 1])
        y_prob = np.array([0.7, 0.6, 0.8, 0.7])

        diagnostics = Diagnostics()
        result = diagnostics._temporal_stability(df, y_true, y_prob)

        assert "periods" in result
        periods = {p["period"]: p for p in result["periods"]}
        assert "2022" in periods
        assert "2023" in periods
        assert periods["2022"]["n_matches"] == 2
        assert periods["2023"]["n_matches"] == 2

    def test_temporal_drift_calculation(self) -> None:
        """Temporal drift is max deviation from overall accuracy."""
        df = pl.DataFrame({
            "effective_match_date": [
                "2022-01-01", "2022-01-02",  # 2022: 100% accuracy
                "2023-01-01", "2023-01-02",  # 2023: 50% accuracy
            ],
        })
        y_true = np.array([1, 1, 1, 0])
        y_prob = np.array([0.8, 0.8, 0.8, 0.8])  # all predict 1

        diagnostics = Diagnostics()
        result = diagnostics._temporal_stability(df, y_true, y_prob)

        # Overall accuracy: 75%
        # 2022 accuracy: 100%, drift = 0.25
        # 2023 accuracy: 50%, drift = 0.25
        assert result["overall_accuracy"] == 0.75
        assert result["temporal_drift"] == 0.25


class TestDiagnosticsComputeAll:
    """Tests for compute_all orchestration."""

    def test_compute_all_combines_folds(self) -> None:
        """compute_all aggregates predictions from multiple folds."""
        fold1_df = pl.DataFrame({
            "circuit": ["tour", "tour"],
            "surface": ["Hard", "Clay"],
            "round": ["R32", "R16"],
            "player_ranking": [10, 50],
            "effective_match_date": ["2023-01-01", "2023-01-02"],
            "match_uid": ["m1", "m2"],
            "tournament_name": ["T1", "T2"],
            "player_name": ["A", "C"],
            "opp_name": ["B", "D"],
        })
        fold2_df = pl.DataFrame({
            "circuit": ["chal", "chal"],
            "surface": ["Hard", "Hard"],
            "round": ["R32", "QF"],
            "player_ranking": [100, 200],
            "effective_match_date": ["2023-02-01", "2023-02-02"],
            "match_uid": ["m3", "m4"],
            "tournament_name": ["T3", "T4"],
            "player_name": ["E", "G"],
            "opp_name": ["F", "H"],
        })

        predictions = [
            {
                "df": fold1_df,
                "y_true": np.array([1, 0]),
                "y_prob": np.array([0.7, 0.6]),
            },
            {
                "df": fold2_df,
                "y_true": np.array([1, 1]),
                "y_prob": np.array([0.8, 0.75]),
            },
        ]

        diagnostics = Diagnostics()
        result = diagnostics.compute_all(predictions)

        # Verify result is DiagnosticResults
        assert isinstance(result, DiagnosticResults)

        # Verify segments include data from both folds
        assert "by_circuit" in result.segments
        assert "tour" in result.segments["by_circuit"]
        assert "chal" in result.segments["by_circuit"]
        assert result.segments["by_circuit"]["tour"]["overall"]["n_matches"] == 2
        assert result.segments["by_circuit"]["chal"]["overall"]["n_matches"] == 2

    def test_compute_all_returns_complete_diagnostics(self) -> None:
        """compute_all returns all diagnostic components."""
        df = pl.DataFrame({
            "circuit": ["tour"] * 4,
            "surface": ["Hard"] * 4,
            "round": ["R32", "R16", "QF", "SF"],
            "player_ranking": [10, 20, 30, 40],
            "effective_match_date": [
                "2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"
            ],
            "match_uid": ["m1", "m2", "m3", "m4"],
            "tournament_name": ["T1", "T1", "T1", "T1"],
            "player_name": ["A", "B", "C", "D"],
            "opp_name": ["X", "Y", "Z", "W"],
        })

        predictions = [
            {
                "df": df,
                "y_true": np.array([1, 0, 1, 1]),
                "y_prob": np.array([0.7, 0.6, 0.8, 0.75]),
            },
        ]

        diagnostics = Diagnostics()
        result = diagnostics.compute_all(predictions)

        # All components should be present
        assert result.segments is not None
        assert result.calibration is not None
        assert result.errors is not None
        assert result.temporal is not None

        # Verify metrics can be generated
        metrics = result.metrics
        assert isinstance(metrics, dict)
        assert len(metrics) > 0

    def test_compute_all_empty_predictions_list(self) -> None:
        """compute_all handles empty predictions list gracefully."""
        diagnostics = Diagnostics()
        result = diagnostics.compute_all([])

        assert isinstance(result, DiagnosticResults)
        # Empty results should still have structure
        assert result.segments == {}
        assert result.calibration is not None
        assert result.errors is not None
        assert result.temporal is not None
