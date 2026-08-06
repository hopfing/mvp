"""Tests for TennisProjector — composes serve model with IID chain."""

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.chain import (
    MatchDistribution,
    match_distribution,
    p_service_game_win,
    p_tiebreak_game_win,
    set_score_distribution,
)
from mvp.projection.iid.projector import ProjectionOutput, TennisProjector
from mvp.projection.iid.serve_model import (
    IdentityServeModel,
    ServeWinProbEstimator,
)


class _FixedDrawServeModel(ServeWinProbEstimator):
    """Emits a prescribed set of posterior draws — the test double for a
    distributional estimator.

    `p_a_draws` / `p_b_draws` are per-draw scalars broadcast across matches, so
    a test can state the posterior directly instead of fitting one.
    """

    def __init__(self, p_a_draws: list[float], p_b_draws: list[float]) -> None:
        if len(p_a_draws) != len(p_b_draws):
            raise ValueError("draw counts must match")
        self.p_a_draws = list(p_a_draws)
        self.p_b_draws = list(p_b_draws)

    def fit(self, df: pl.DataFrame) -> None:
        return None

    @property
    def required_columns(self) -> list[str]:
        return []

    @property
    def n_draws(self) -> int:
        return len(self.p_a_draws)

    def predict(self, df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        return self.predict_draw(df, 0)

    def predict_draw(
        self, df: pl.DataFrame, draw: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        n = len(df)
        return (
            np.full(n, self.p_a_draws[draw], dtype=np.float64),
            np.full(n, self.p_b_draws[draw], dtype=np.float64),
        )


def _chain_at(p_a: float, p_b: float, n: int, best_of: int = 3):
    """The chain evaluated at a single point estimate — the pre-draw answer."""
    p_a_arr = np.full(n, p_a, dtype=np.float64)
    p_b_arr = np.full(n, p_b, dtype=np.float64)
    h_a = p_service_game_win(p_a_arr)
    h_b = p_service_game_win(p_b_arr)
    t_ab = p_tiebreak_game_win(p_a_arr, p_b_arr)
    bo = np.full(n, best_of, dtype=np.int64)
    return match_distribution(h_a, h_b, t_ab, bo)


def _pmf_variance(pmf_row: np.ndarray) -> float:
    idx = np.arange(len(pmf_row), dtype=np.float64)
    mean = float((pmf_row * idx).sum())
    return float((pmf_row * (idx - mean) ** 2).sum())


class TestTennisProjector:
    def _make_df(self, n=5, p_a=0.65, p_b=0.55, best_of=3):
        return pl.DataFrame(
            {
                "match_uid": [f"m{i}" for i in range(n)],
                "best_of": [best_of] * n,
                "player_pts_service_won_pct_90d": [p_a] * n,
                "opp_pts_service_won_pct_90d": [p_b] * n,
            }
        )

    def test_project_returns_correct_shape(self):
        df = self._make_df(n=10)
        projector = TennisProjector(IdentityServeModel(window=90))
        out = projector.project(df)
        assert isinstance(out, ProjectionOutput)
        assert isinstance(out.distribution, MatchDistribution)
        assert out.distribution.p_match_win_a.shape == (10,)
        assert len(out.match_uid) == 10
        assert out.best_of.shape == (10,)
        assert out.h_a.shape == (10,)
        assert out.h_b.shape == (10,)
        assert out.t_ab.shape == (10,)

    def test_project_favors_better_player(self):
        df = self._make_df(n=1, p_a=0.70, p_b=0.55)
        projector = TennisProjector(IdentityServeModel(window=90))
        out = projector.project(df)
        assert out.distribution.p_match_win_a[0] > 0.5

    def test_project_equal_players_half(self):
        df = self._make_df(n=3, p_a=0.62, p_b=0.62)
        projector = TennisProjector(IdentityServeModel(window=90))
        out = projector.project(df)
        np.testing.assert_allclose(out.distribution.p_match_win_a, 0.5, atol=1e-12)

    def test_project_mixed_bo3_bo5(self):
        df = pl.DataFrame(
            {
                "match_uid": ["m1", "m2", "m3", "m4"],
                "best_of": [3, 5, 3, 5],
                "player_pts_service_won_pct_90d": [0.65, 0.65, 0.65, 0.65],
                "opp_pts_service_won_pct_90d": [0.55, 0.55, 0.55, 0.55],
            }
        )
        projector = TennisProjector(IdentityServeModel(window=90))
        out = projector.project(df)
        # Bo5 amplifies favorite advantage
        assert out.distribution.p_match_win_a[1] > out.distribution.p_match_win_a[0]
        assert out.distribution.p_match_win_a[3] > out.distribution.p_match_win_a[2]

    def test_serve_outputs_attached(self):
        df = self._make_df(n=2, p_a=0.65, p_b=0.55)
        projector = TennisProjector(IdentityServeModel(window=90))
        out = projector.project(df)
        np.testing.assert_allclose(out.p_a_serve_win, 0.65)
        np.testing.assert_allclose(out.p_b_serve_win, 0.55)
        # Hold prob is monotone in serve prob, so h_a > h_b here
        assert (out.h_a > out.h_b).all()

    def test_missing_match_uid_raises(self):
        df = pl.DataFrame(
            {
                "best_of": [3],
                "player_pts_service_won_pct_90d": [0.6],
                "opp_pts_service_won_pct_90d": [0.5],
            }
        )
        projector = TennisProjector(IdentityServeModel(window=90))
        with pytest.raises(ValueError, match="match_uid"):
            projector.project(df)

    def test_missing_best_of_raises(self):
        df = pl.DataFrame(
            {
                "match_uid": ["m1"],
                "player_pts_service_won_pct_90d": [0.6],
                "opp_pts_service_won_pct_90d": [0.5],
            }
        )
        projector = TennisProjector(IdentityServeModel(window=90))
        with pytest.raises(ValueError, match="best_of"):
            projector.project(df)

    def test_missing_serve_columns_raises(self):
        df = pl.DataFrame(
            {
                "match_uid": ["m1"],
                "best_of": [3],
            }
        )
        projector = TennisProjector(IdentityServeModel(window=90))
        with pytest.raises(ValueError, match="serve-model column"):
            projector.project(df)

    def test_fit_passthrough(self):
        df = self._make_df(n=5)
        projector = TennisProjector(IdentityServeModel(window=90))
        projector.fit(df)  # Should not raise

    def test_match_uid_preserved(self):
        df = self._make_df(n=3)
        projector = TennisProjector(IdentityServeModel(window=90))
        out = projector.project(df)
        assert list(out.match_uid) == ["m0", "m1", "m2"]


class TestPosteriorDraws:
    """Propagating a distribution over `p` rather than a point estimate."""

    def _df(self, n=4, best_of=3):
        return pl.DataFrame(
            {
                "match_uid": [f"m{i}" for i in range(n)],
                "best_of": [best_of] * n,
            }
        )

    def test_single_draw_is_bit_identical_to_the_point_chain(self):
        """A point estimator must survive the draw machinery untouched.

        Not `allclose` — exact. Summing into zeros and dividing by 1.0 are
        both exact in IEEE754, so a one-draw run has to reproduce the direct
        chain call bit for bit. If this ever loosens to a tolerance, every
        historical projection has silently moved.
        """
        df = self._df(n=4)
        out = TennisProjector(_FixedDrawServeModel([0.65], [0.58])).project(df)
        expected = _chain_at(0.65, 0.58, n=4)

        assert out.distribution.p_match_win_a.tolist() == (
            expected.p_match_win_a.tolist()
        )
        assert out.distribution.total_games_pmf.tolist() == (
            expected.total_games_pmf.tolist()
        )
        assert out.distribution.spread_pmf.tolist() == expected.spread_pmf.tolist()
        assert out.distribution.expected_total_games.tolist() == (
            expected.expected_total_games.tolist()
        )

    def test_repeated_identical_draws_reproduce_the_point_answer(self):
        df = self._df(n=3)
        out = TennisProjector(
            _FixedDrawServeModel([0.66] * 5, [0.60] * 5)
        ).project(df)
        expected = _chain_at(0.66, 0.60, n=3)
        np.testing.assert_allclose(
            out.distribution.p_match_win_a, expected.p_match_win_a, rtol=1e-12,
        )

    def test_mixture_compresses_the_favorite_toward_even(self):
        """The Jensen shift: E_p[P(win|p)] != P(win|E[p]), and for a favorite
        the mixture is strictly lower.

        P(match win) saturates as the serve gap grows, so it is concave in the
        favorite's region and averaging over the posterior pulls the number
        down toward 0.5. This is not a second-order correction — the draws
        below move the favorite by >10 points. Any evaluation that assumes
        propagation only widens the distribution is reading the wrong number.
        """
        df = self._df(n=2)
        out = TennisProjector(
            _FixedDrawServeModel([0.58, 0.78], [0.62, 0.62])
        ).project(df)
        point = _chain_at(0.68, 0.62, n=2)

        mixture_win = float(out.distribution.p_match_win_a[0])
        point_win = float(point.p_match_win_a[0])

        assert mixture_win < point_win
        assert point_win - mixture_win > 0.10
        # Compression is toward even, not past it.
        assert mixture_win > 0.5

    def test_mixture_variance_obeys_law_of_total_variance(self):
        """Var(mixture) >= E[Var(total | p)] — the theorem, which always holds.

        Deliberately NOT asserting the mixture is wider than the CURRENT point
        projection Var(total | p_bar). Those are different comparisons and the
        second one is not signed: for closely-matched players Var(total | p) is
        concave in `p`, so spreading `p` lowers the average and the mixture
        comes out NARROWER than the point projection. Measured on the chain:
        widening at p_b in {0.50, 0.55}, narrowing by up to 3.5 games^2 at
        p_b = 0.62. An evaluation expecting totals to widen everywhere will
        misread the closely-matched slice as a bug.
        """
        df = self._df(n=1)
        p_a_draws, p_b = [0.58, 0.78], 0.62
        out = TennisProjector(
            _FixedDrawServeModel(p_a_draws, [p_b, p_b])
        ).project(df)

        mixture_var = _pmf_variance(out.distribution.total_games_pmf[0])
        per_draw_var = np.mean([
            _pmf_variance(_chain_at(p_a, p_b, n=1).total_games_pmf[0])
            for p_a in p_a_draws
        ])
        assert mixture_var >= per_draw_var - 1e-9

    def test_set_score_pmf_is_the_mixture_not_the_reduced_scalars(self):
        """The silent-wrong-number path is closed.

        `out.h_a` / `h_b` / `t_ab` are posterior MEANS under draws, so
        rebuilding the set-score pmf from them — which three consumers used to
        do — yields the pmf of a representative match rather than the mixture.
        Both are (N, 14) and neither raises, which is exactly why the
        projector now owns the computation.
        """
        df = self._df(n=2)
        out = TennisProjector(
            _FixedDrawServeModel([0.55, 0.80], [0.62, 0.62])
        ).project(df)

        rebuilt = set_score_distribution(out.h_a, out.h_b, out.t_ab)
        assert rebuilt.shape == out.set_score_pmf.shape
        assert not np.allclose(rebuilt, out.set_score_pmf, atol=1e-6)

        expected = np.mean(
            [
                set_score_distribution(
                    p_service_game_win(np.full(2, p_a)),
                    p_service_game_win(np.full(2, 0.62)),
                    p_tiebreak_game_win(np.full(2, p_a), np.full(2, 0.62)),
                )
                for p_a in (0.55, 0.80)
            ],
            axis=0,
        )
        np.testing.assert_allclose(out.set_score_pmf, expected, rtol=1e-12)

    def test_hold_prob_of_mean_p_is_not_the_mean_hold_prob(self):
        """The scalar fields are per-quantity posterior means, not mutually
        derivable — `h_a` is E[h(p)] while `p_a_serve_win` is E[p].

        `p_service_game_win` inflects at exactly p=0.5: convex below, concave
        above. Real serve probabilities live around [0.55, 0.74], entirely in
        the concave half, so E[h(p)] < h(E[p]) — the mixture pulls hold
        probability DOWN, by up to ~4 points at a +/-0.10 spread. Same
        direction as the favorite's win-prob compression, so the two stack.

        Consequence for evaluation: `hold_bias` (metrics.py, E[h] minus the
        realized hold rate) shifts negative under draws from the mixture
        alone, with no change in model quality. That will look like a
        regression and will not be one.
        """
        df = self._df(n=1)
        out = TennisProjector(
            _FixedDrawServeModel([0.55, 0.75], [0.62, 0.62])
        ).project(df)
        assert out.h_a[0] < p_service_game_win(out.p_a_serve_win[0])

    def test_hold_prob_jensen_flips_below_the_inflection(self):
        """Guards the claim above by pinning the inflection: with both draws
        under p=0.5 the curvature is convex and the inequality reverses."""
        df = self._df(n=1)
        out = TennisProjector(
            _FixedDrawServeModel([0.35, 0.45], [0.62, 0.62])
        ).project(df)
        assert out.h_a[0] > p_service_game_win(out.p_a_serve_win[0])

    def test_zero_draws_raises(self):
        df = self._df(n=1)
        with pytest.raises(ValueError, match="n_draws"):
            TennisProjector(_FixedDrawServeModel([], [])).project(df)
