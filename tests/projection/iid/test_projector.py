"""Tests for TennisProjector — composes serve model with IID chain."""

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.chain import MatchDistribution
from mvp.projection.iid.projector import ProjectionOutput, TennisProjector
from mvp.projection.iid.serve_model import IdentityServeModel


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
