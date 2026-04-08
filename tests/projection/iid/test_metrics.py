"""Tests for IID projector metrics."""

import numpy as np
import polars as pl
import pytest

from mvp.model.metrics import compute_metrics
from mvp.projection.iid.metrics import compute_iid_metrics, crps_discrete_pmf
from mvp.projection.iid.projector import TennisProjector
from mvp.projection.iid.serve_model import IdentityServeModel


class TestCRPS:
    """Hand-computed reference values for the discrete CRPS."""

    def test_zero_crps_for_perfect_point_mass(self):
        # Single-point pmf at the observation → CRPS = 0
        pmf = np.zeros((1, 5))
        pmf[0, 3] = 1.0
        obs = np.array([3])
        assert crps_discrete_pmf(obs, pmf) == pytest.approx(0.0, abs=1e-12)

    def test_uniform_pmf_at_center(self):
        # Uniform pmf over {0, 1, 2, 3, 4}, observation at 2 (center)
        pmf = np.full((1, 5), 0.2)
        obs = np.array([2])
        # CDF = [0.2, 0.4, 0.6, 0.8, 1.0]
        # H(j) at obs=2: [0,0,1,1,1]
        # diff^2 sum = 0.04 + 0.16 + 0.16 + 0.04 + 0 = 0.4
        expected = 0.04 + 0.16 + 0.16 + 0.04 + 0.0
        assert crps_discrete_pmf(obs, pmf) == pytest.approx(expected, abs=1e-12)

    def test_uniform_pmf_at_left_edge(self):
        # Observation at left edge
        pmf = np.full((1, 5), 0.2)
        obs = np.array([0])
        # CDF = [0.2, 0.4, 0.6, 0.8, 1.0]
        # H(j) at obs=0: [1,1,1,1,1]
        # diff^2 sum = 0.64 + 0.36 + 0.16 + 0.04 + 0 = 1.20
        expected = (1 - 0.2) ** 2 + (1 - 0.4) ** 2 + (1 - 0.6) ** 2 + (1 - 0.8) ** 2 + 0.0
        assert crps_discrete_pmf(obs, pmf) == pytest.approx(expected, abs=1e-12)

    def test_averages_across_matches(self):
        pmf = np.zeros((2, 5))
        pmf[0, 2] = 1.0  # Match 0: perfect prediction
        pmf[1, 0] = 1.0  # Match 1: predicted 0
        obs = np.array([2, 4])  # Match 0: correct (CRPS=0). Match 1: off by 4.
        # Match 1 CRPS: CDF = [1,1,1,1,1], H = [0,0,0,0,1], (1-0)^2 * 4 + 0 = 4
        result = crps_discrete_pmf(obs, pmf)
        assert result == pytest.approx((0.0 + 4.0) / 2, abs=1e-12)

    def test_empty(self):
        pmf = np.zeros((0, 5))
        obs = np.zeros(0, dtype=np.int64)
        assert crps_discrete_pmf(obs, pmf) == 0.0


class TestComputeIIDMetrics:
    """End-to-end metric computation through compute_iid_metrics."""

    def _make_synthetic_projection(self, n=20):
        """Build a small projection over equal-strength players for testing."""
        df = pl.DataFrame(
            {
                "match_uid": [f"m{i}" for i in range(n)],
                "best_of": [3] * n,
                "player_pts_service_won_pct_90d": [0.62] * n,
                "opp_pts_service_won_pct_90d": [0.62] * n,
            }
        )
        projector = TennisProjector(IdentityServeModel(window=90))
        return projector.project(df)

    def test_returns_classification_keys(self):
        out = self._make_synthetic_projection(n=50)
        rng = np.random.default_rng(0)
        y_won = rng.integers(0, 2, 50)
        y_a = rng.integers(0, 25, 50).astype(np.float64)
        y_b = rng.integers(0, 25, 50).astype(np.float64)
        m = compute_iid_metrics(out, y_won, y_a, y_b)
        for k in ["log_loss", "brier_score", "accuracy", "roc_auc"]:
            assert k in m

    def test_returns_regression_keys(self):
        out = self._make_synthetic_projection(n=50)
        rng = np.random.default_rng(1)
        y_won = rng.integers(0, 2, 50)
        y_a = rng.integers(0, 25, 50).astype(np.float64)
        y_b = rng.integers(0, 25, 50).astype(np.float64)
        m = compute_iid_metrics(out, y_won, y_a, y_b)
        for k in ["mae", "rmse", "r_squared"]:
            assert k in m

    def test_returns_distributional_keys(self):
        out = self._make_synthetic_projection(n=50)
        rng = np.random.default_rng(2)
        y_won = rng.integers(0, 2, 50)
        y_a = rng.integers(0, 25, 50).astype(np.float64)
        y_b = rng.integers(0, 25, 50).astype(np.float64)
        m = compute_iid_metrics(out, y_won, y_a, y_b, total_lines=[20.5], spread_lines=[-2.5, 2.5])
        assert "iid_crps_total_games" in m
        assert "iid_crps_spread" in m
        assert "iid_line_total_20.5_pred" in m
        assert "iid_line_total_20.5_actual" in m
        assert "iid_line_total_20.5_err" in m
        assert "iid_line_spread_-2.5_err" in m
        assert "iid_line_spread_2.5_err" in m

    def test_classification_bridge_matches_direct(self):
        # The classification metrics from compute_iid_metrics should match
        # what compute_metrics returns directly on the same y/probs.
        out = self._make_synthetic_projection(n=50)
        rng = np.random.default_rng(3)
        y_won = rng.integers(0, 2, 50)
        y_a = rng.integers(0, 25, 50).astype(np.float64)
        y_b = rng.integers(0, 25, 50).astype(np.float64)
        bridge = compute_iid_metrics(out, y_won, y_a, y_b, include_regression=False)
        direct = compute_metrics(y_won.astype(np.int64), out.distribution.p_match_win_a)
        for k in direct:
            assert bridge[k] == pytest.approx(direct[k], abs=1e-12)

    def test_line_calibration_zero_error_when_perfect(self):
        # If predicted P(over line) exactly matches actual rate, error = 0
        # Build a synthetic case where all players are equal so the model
        # predicts ~50% over and we feed actuals that match.
        out = self._make_synthetic_projection(n=200)
        # Compute the model's predicted P(over 21.5)
        p_over = out.distribution.p_over_total(21.5)
        mean_p_over = float(p_over.mean())
        # Construct y_a and y_b so the actual over rate exactly matches
        n = len(p_over)
        n_over = int(round(mean_p_over * n))
        # First n_over matches: total = 22 (over). Rest: total = 20 (under).
        y_a = np.array([11.0] * n_over + [10.0] * (n - n_over))
        y_b = np.array([11.0] * n_over + [10.0] * (n - n_over))
        y_won = np.zeros(n, dtype=np.int64)
        # Skip classification metrics — y_won is single-class and would
        # break sklearn log_loss which requires both labels.
        m = compute_iid_metrics(
            out, y_won, y_a, y_b,
            total_lines=[21.5],
            include_classification=False,
        )
        # Predicted vs actual for the line should match closely
        assert m["iid_line_total_21.5_err"] < 0.01

    def test_disable_classification(self):
        out = self._make_synthetic_projection(n=20)
        rng = np.random.default_rng(4)
        y_won = rng.integers(0, 2, 20)
        y_a = rng.integers(0, 25, 20).astype(np.float64)
        y_b = rng.integers(0, 25, 20).astype(np.float64)
        m = compute_iid_metrics(out, y_won, y_a, y_b, include_classification=False)
        assert "log_loss" not in m
        # Regression and distributional are still present
        assert "mae" in m
        assert "iid_crps_total_games" in m
