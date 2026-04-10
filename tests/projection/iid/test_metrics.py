"""Tests for IID projector metrics."""

import numpy as np
import polars as pl
import pytest

from mvp.model.metrics import compute_metrics
from mvp.projection.iid.chain import p_service_game_win
from mvp.projection.iid.metrics import (
    compute_hold_diagnostics,
    compute_iid_metrics,
    compute_set_score_diagnostics,
    compute_tiebreak_diagnostics,
    crps_discrete_pmf,
)
from mvp.projection.iid.projector import ProjectionOutput, TennisProjector
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


def _make_projection_output(n, serve_prob=0.62):
    """Build a ProjectionOutput with known serve probs for diagnostic tests."""
    from mvp.projection.iid.chain import (
        match_distribution,
        p_tiebreak_game_win,
    )

    p_a = np.full(n, serve_prob)
    p_b = np.full(n, serve_prob)
    h_a = p_service_game_win(p_a)
    h_b = p_service_game_win(p_b)
    t_ab = p_tiebreak_game_win(p_a, p_b)
    best_of = np.full(n, 3, dtype=np.int64)
    dist = match_distribution(h_a, h_b, t_ab, best_of)
    return ProjectionOutput(
        distribution=dist,
        match_uid=np.array([f"m{i}" for i in range(n)]),
        best_of=best_of,
        p_a_serve_win=p_a,
        p_b_serve_win=p_b,
        h_a=h_a,
        h_b=h_b,
        t_ab=t_ab,
    )


class TestHoldDiagnostics:
    """Tests for compute_hold_diagnostics."""

    def test_zero_bias_when_actual_matches_predicted(self):
        n = 50
        out = _make_projection_output(n)
        # Construct actuals that match predicted hold rates.
        # h = p_service_game_win(0.62) ≈ 0.8267
        # If a player played 10 service games and held h*10, the actual rate ≈ h.
        h = float(out.h_a[0])
        games_played = 10
        holds = round(h * games_played)
        breaks = games_played - holds

        df = pl.DataFrame({
            "svc_games_played": [games_played] * n,
            "svc_bp_faced": [breaks] * n,
            "svc_bp_saved": [0] * n,
            "opp_svc_games_played": [games_played] * n,
            "opp_svc_bp_faced": [breaks] * n,
            "opp_svc_bp_saved": [0] * n,
        })
        m = compute_hold_diagnostics(out, df)
        assert "hold_bias" in m
        assert abs(m["hold_bias"]) < 0.05  # close to zero given rounding

    def test_positive_bias_when_predicted_exceeds_actual(self):
        n = 20
        out = _make_projection_output(n)
        # Actual hold rate = 0.70, predicted ≈ 0.827 → positive bias.
        df = pl.DataFrame({
            "svc_games_played": [10] * n,
            "svc_bp_faced": [3] * n,   # 3 breaks = 7 holds
            "svc_bp_saved": [0] * n,
            "opp_svc_games_played": [10] * n,
            "opp_svc_bp_faced": [3] * n,
            "opp_svc_bp_saved": [0] * n,
        })
        m = compute_hold_diagnostics(out, df)
        assert m["hold_bias"] > 0.05

    def test_nan_svc_games_handled(self):
        n = 5
        out = _make_projection_output(n)
        df = pl.DataFrame({
            "svc_games_played": [None, 10, 10, None, 10],
            "svc_bp_faced": [None, 2, 2, None, 2],
            "svc_bp_saved": [None, 0, 0, None, 0],
            "opp_svc_games_played": [10, None, 10, 10, None],
            "opp_svc_bp_faced": [2, None, 2, 2, None],
            "opp_svc_bp_saved": [0, None, 0, 0, None],
        })
        m = compute_hold_diagnostics(out, df)
        assert "hold_bias" in m
        assert "hold_mae" in m


class TestSetScoreDiagnostics:
    """Tests for compute_set_score_diagnostics."""

    def test_returns_bias_keys(self):
        n = 30
        out = _make_projection_output(n)
        # All matches are 6-4, 6-4 (2 sets, player wins both).
        df = pl.DataFrame({
            "player_set1_games": [6] * n,
            "opp_set1_games": [4] * n,
            "player_set2_games": [6] * n,
            "opp_set2_games": [4] * n,
            "player_set3_games": [None] * n,
            "opp_set3_games": [None] * n,
            "player_set4_games": [None] * n,
            "opp_set4_games": [None] * n,
            "player_set5_games": [None] * n,
            "opp_set5_games": [None] * n,
        })
        m = compute_set_score_diagnostics(out, df)
        assert "set_score_bias_tight" in m
        assert "set_score_bias_blowout" in m

    def test_all_blowouts_negative_blowout_bias(self):
        # If every set is 6-0, actual blowout freq = 1.0. Predicted blowout
        # freq for equal-strength players (0.62) will be much less → negative bias.
        n = 20
        out = _make_projection_output(n)
        df = pl.DataFrame({
            "player_set1_games": [6] * n,
            "opp_set1_games": [0] * n,
            "player_set2_games": [6] * n,
            "opp_set2_games": [0] * n,
            "player_set3_games": [None] * n,
            "opp_set3_games": [None] * n,
            "player_set4_games": [None] * n,
            "opp_set4_games": [None] * n,
            "player_set5_games": [None] * n,
            "opp_set5_games": [None] * n,
        })
        m = compute_set_score_diagnostics(out, df)
        assert m["set_score_bias_blowout"] < 0

    def test_empty_when_no_valid_sets(self):
        n = 5
        out = _make_projection_output(n)
        df = pl.DataFrame({
            "player_set1_games": [None] * n,
            "opp_set1_games": [None] * n,
            "player_set2_games": [None] * n,
            "opp_set2_games": [None] * n,
            "player_set3_games": [None] * n,
            "opp_set3_games": [None] * n,
            "player_set4_games": [None] * n,
            "opp_set4_games": [None] * n,
            "player_set5_games": [None] * n,
            "opp_set5_games": [None] * n,
        })
        m = compute_set_score_diagnostics(out, df)
        assert m == {}


class TestTiebreakDiagnostics:
    """Tests for compute_tiebreak_diagnostics."""

    def test_no_tiebreaks(self):
        n = 20
        out = _make_projection_output(n)
        df = pl.DataFrame({
            "player_set1_games": [6] * n,
            "player_set2_games": [6] * n,
            "player_set3_games": [None] * n,
            "player_set4_games": [None] * n,
            "player_set5_games": [None] * n,
            "player_set1_tiebreak": [None] * n,
            "player_set2_tiebreak": [None] * n,
            "player_set3_tiebreak": [None] * n,
            "player_set4_tiebreak": [None] * n,
            "player_set5_tiebreak": [None] * n,
        })
        m = compute_tiebreak_diagnostics(out, df)
        assert m["tiebreak_rate_actual"] == 0.0
        assert m["tiebreak_rate_pred"] > 0  # model predicts some tiebreaks

    def test_all_tiebreaks(self):
        n = 20
        out = _make_projection_output(n)
        df = pl.DataFrame({
            "player_set1_games": [7] * n,
            "player_set2_games": [7] * n,
            "player_set3_games": [None] * n,
            "player_set4_games": [None] * n,
            "player_set5_games": [None] * n,
            "player_set1_tiebreak": [7] * n,
            "player_set2_tiebreak": [7] * n,
            "player_set3_tiebreak": [None] * n,
            "player_set4_tiebreak": [None] * n,
            "player_set5_tiebreak": [None] * n,
        })
        m = compute_tiebreak_diagnostics(out, df)
        assert m["tiebreak_rate_actual"] == 1.0
        assert m["tiebreak_rate_bias"] < 0  # pred < 1.0

    def test_empty_when_no_sets(self):
        n = 5
        out = _make_projection_output(n)
        df = pl.DataFrame({
            "player_set1_games": [None] * n,
            "player_set2_games": [None] * n,
            "player_set3_games": [None] * n,
            "player_set4_games": [None] * n,
            "player_set5_games": [None] * n,
            "player_set1_tiebreak": [None] * n,
            "player_set2_tiebreak": [None] * n,
            "player_set3_tiebreak": [None] * n,
            "player_set4_tiebreak": [None] * n,
            "player_set5_tiebreak": [None] * n,
        })
        m = compute_tiebreak_diagnostics(out, df)
        assert m == {}
