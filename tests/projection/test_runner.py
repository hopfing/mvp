"""Tests for projection runner with synthetic data."""

import numpy as np
import polars as pl
import pytest

from mvp.projection.diagnostics import ProjectionDiagnostics
from mvp.projection.metrics import compute_regression_metrics


class TestProjectionDiagnosticsIntegration:
    """Test diagnostics with synthetic fold data."""

    @pytest.fixture
    def synthetic_predictions(self):
        """Create synthetic fold predictions matching runner output format."""
        np.random.seed(42)
        n = 200

        y_true = np.random.uniform(8, 25, n).astype(float)
        y_pred = y_true + np.random.randn(n) * 2

        df = pl.DataFrame({
            "match_uid": [f"match_{i // 2}" for i in range(n)],
            "circuit": ["tour" if i < n // 2 else "chal" for i in range(n)],
            "surface": ["Hard" if i % 3 == 0 else "Clay" if i % 3 == 1 else "Grass" for i in range(n)],
            "round": ["R32" if i % 4 == 0 else "R16" if i % 4 == 1 else "QF" if i % 4 == 2 else "SF" for i in range(n)],
            "best_of": [3 if i < n * 3 // 4 else 5 for i in range(n)],
        })

        return [{"df": df, "y_true": y_true, "y_pred": y_pred}]

    def test_compute_all_returns_results(self, synthetic_predictions):
        """Diagnostics compute returns all sections."""
        diag = ProjectionDiagnostics()
        results = diag.compute_all(synthetic_predictions)

        assert results.residuals
        assert "mean_residual" in results.residuals
        assert "std_residual" in results.residuals
        assert "by_predicted_bin" in results.residuals

        assert results.segments
        assert "circuit" in results.segments
        assert "surface" in results.segments
        assert "round" in results.segments
        assert "best_of" in results.segments

    def test_residual_analysis(self, synthetic_predictions):
        """Residual analysis produces sensible values."""
        diag = ProjectionDiagnostics()
        results = diag.compute_all(synthetic_predictions)

        assert abs(results.residuals["mean_residual"]) < 1.0  # close to zero
        assert results.residuals["std_residual"] > 0

    def test_segment_breakdowns(self, synthetic_predictions):
        """Each segment has correct metrics."""
        diag = ProjectionDiagnostics()
        results = diag.compute_all(synthetic_predictions)

        for circuit_data in results.segments["circuit"].values():
            assert "mae" in circuit_data
            assert "rmse" in circuit_data
            assert "n" in circuit_data
            assert circuit_data["mae"] >= 0

    def test_match_level_analysis(self, synthetic_predictions):
        """Match-level pairing works when match_uid pairs exist."""
        diag = ProjectionDiagnostics()
        results = diag.compute_all(synthetic_predictions)

        assert results.match_level
        assert "n_matches" in results.match_level
        assert "total_games_mae" in results.match_level
        assert "spread_mae" in results.match_level
        assert "directional_accuracy" in results.match_level
        assert 0 <= results.match_level["directional_accuracy"] <= 1

    def test_metrics_property(self, synthetic_predictions):
        """metrics property produces flat dict for MLflow."""
        diag = ProjectionDiagnostics()
        results = diag.compute_all(synthetic_predictions)

        flat = results.metrics
        assert isinstance(flat, dict)
        assert all(isinstance(v, float) for v in flat.values())
        assert "mean_residual" in flat

    def test_empty_predictions(self):
        """Empty predictions return empty results."""
        diag = ProjectionDiagnostics()
        results = diag.compute_all([])
        assert results.residuals == {}
        assert results.segments == {}
        assert results.match_level == {}


class TestProjectionRunnerConfig:
    """Test runner config loading without running the full pipeline."""

    def test_runner_import(self):
        """ProjectionRunner can be imported."""
        from mvp.projection.runner import ProjectionRunner
        assert ProjectionRunner is not None

    def test_runner_config_loading(self, tmp_path):
        """Runner loads config correctly."""
        import yaml
        from mvp.projection.runner import ProjectionRunner

        config = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "features": {"include": ["player_elo_surface_diff"]},
            "model": {"type": "ridge"},
        }
        config_path = tmp_path / "test.yaml"
        config_path.write_text(yaml.dump(config))

        runner = ProjectionRunner(
            config_path=config_path,
            matches_path=tmp_path / "matches.parquet",
            log_to_mlflow=False,
        )
        assert runner.config.model.type == "ridge"
        assert runner.config.features.include == ["player_elo_surface_diff"]
