"""Tests for the shared tennis scoring math.

The point of consolidating the hold formula is that it is now the single thing
the projection chain and the model's features both depend on, so the tests have
to be worth that. Checking the closed form against itself proves nothing; the
correctness test here re-derives P(hold) by dynamic programming over the actual
point-by-point game tree and requires the algebra to match it.
"""

import numpy as np
import polars as pl
import pytest

from mvp.common.tennis_scoring import hold_probability

OPERATING_RANGE = (0.55, 0.74)


def _hold_by_dp(p: float, max_points: int = 120) -> float:
    """P(server wins the game), by DP over (server_points, returner_points).

    Deliberately knows nothing about the closed form — it walks the scoring
    rules directly (first to 4, win by 2, deuce/advantage as ordinary states
    that happen to recur). Truncation at `max_points` leaves residual mass on
    the order of (2pq)^((max_points-6)/2), which at p=0.5 is ~1e-17.
    """
    q = 1.0 - p
    memo: dict[tuple[int, int], float] = {}

    def dp(a: int, b: int) -> float:
        if a >= 4 and a - b >= 2:
            return 1.0
        if b >= 4 and b - a >= 2:
            return 0.0
        if a + b >= max_points:
            return 0.5
        if (a, b) in memo:
            return memo[(a, b)]
        r = p * dp(a + 1, b) + q * dp(a, b + 1)
        memo[(a, b)] = r
        return r

    return dp(0, 0)


class TestAgainstIndependentDP:
    @pytest.mark.parametrize(
        "p", [0.05, 0.2, 0.35, 0.5, 0.55, 0.62, 0.65, 0.74, 0.8, 0.95]
    )
    def test_closed_form_matches_point_by_point_dp(self, p):
        assert hold_probability(p) == pytest.approx(_hold_by_dp(p), abs=1e-12)

    def test_matches_dp_across_a_dense_sweep(self):
        grid = np.linspace(0.01, 0.99, 99)
        closed = hold_probability(grid)
        walked = np.array([_hold_by_dp(float(x)) for x in grid])
        np.testing.assert_allclose(closed, walked, atol=1e-12)


class TestBackendAgreement:
    """One implementation is only one implementation if every caller agrees."""

    def test_float_ndarray_series_and_expr_agree(self):
        vals = [0.35, 0.5, 0.58, 0.65, 0.74]

        from_float = [hold_probability(v) for v in vals]
        from_array = hold_probability(np.array(vals))
        from_series = hold_probability(pl.Series(vals))
        from_expr = (
            pl.DataFrame({"p": vals})
            .select(hold_probability(pl.col("p")).alias("h"))["h"]
        )

        np.testing.assert_allclose(from_array, from_float, atol=1e-15)
        np.testing.assert_allclose(from_series.to_numpy(), from_float, atol=1e-15)
        np.testing.assert_allclose(from_expr.to_numpy(), from_float, atol=1e-15)

    def test_array_in_array_out(self):
        out = hold_probability(np.array([0.6, 0.65]))
        assert isinstance(out, np.ndarray)
        assert out.shape == (2,)

    def test_expr_stays_lazy(self):
        assert isinstance(hold_probability(pl.col("p")), pl.Expr)


class TestBoundaryBehaviour:
    def test_endpoints_are_exact_not_approximate(self):
        # Exactly 0.0 and 1.0 — the reason the shared formula carries no
        # endpoint guard. If this ever becomes approximate, callers that clamp
        # are silently correcting the formula instead of bounding their input.
        assert hold_probability(0.0) == 0.0
        assert hold_probability(1.0) == 1.0

    def test_fair_point_gives_fair_game(self):
        assert hold_probability(0.5) == pytest.approx(0.5, abs=1e-15)

    def test_denominator_never_vanishes(self):
        grid = np.linspace(0.0, 1.0, 1001)
        assert np.all(grid**2 + (1 - grid) ** 2 >= 0.5 - 1e-12)
        assert np.all(np.isfinite(hold_probability(grid)))

    def test_strictly_increasing(self):
        grid = np.linspace(0.0, 1.0, 501)
        assert np.all(np.diff(hold_probability(grid)) > 0)

    def test_amplifies_edge_away_from_even(self):
        # A game magnifies a per-point edge: hold > p above 0.5, hold < p below.
        assert hold_probability(0.6) > 0.6
        assert hold_probability(0.4) < 0.4


class TestCurvatureOverOperatingRange:
    """The property that decides which way averaging over `p` moves the mean."""

    def test_inflects_at_one_half(self):
        grid = np.linspace(0.02, 0.98, 4801)
        second = np.gradient(np.gradient(hold_probability(grid), grid), grid)
        sign_changes = np.flatnonzero(np.diff(np.sign(second)) != 0)
        assert sign_changes.size == 1
        assert grid[sign_changes[0]] == pytest.approx(0.5, abs=2e-3)

    def test_concave_over_the_serve_range(self):
        lo, hi = OPERATING_RANGE
        grid = np.linspace(lo, hi, 401)
        second = np.gradient(np.gradient(hold_probability(grid), grid), grid)
        assert np.all(second < 0)

    def test_averaging_over_p_lowers_the_hold(self):
        # The consequence of concavity, stated as the thing a caller would get
        # wrong: E[hold(p)] < hold(E[p]) anywhere in the serve range.
        centre = 0.65
        spread = np.array([-0.04, -0.02, 0.0, 0.02, 0.04])
        assert hold_probability(centre + spread).mean() < hold_probability(centre)
