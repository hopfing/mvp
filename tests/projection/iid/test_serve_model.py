"""Tests for IID serve win prob estimators."""

import math

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.serve_model import (
    LEAGUE_MEAN_SERVE_PROB,
    SERVE_PROB_MAX,
    SERVE_PROB_MIN,
    IdentityServeModel,
    MatchupServeModel,
)


class TestIdentityServeModel:
    def _make_df(self, p_a_vals, p_b_vals, window=90):
        suffix = f"_{window}d" if window is not None else ""
        return pl.DataFrame(
            {
                f"player_pts_service_won_pct{suffix}": p_a_vals,
                f"opp_pts_service_won_pct{suffix}": p_b_vals,
            }
        )

    def test_passthrough_inside_clip_range(self):
        df = self._make_df([0.6, 0.7, 0.55], [0.5, 0.65, 0.6])
        model = IdentityServeModel(window=90)
        p_a, p_b = model.predict(df)
        np.testing.assert_allclose(p_a, [0.6, 0.7, 0.55])
        np.testing.assert_allclose(p_b, [0.5, 0.65, 0.6])

    def test_clipping(self):
        df = self._make_df([0.10, 0.95, 0.50], [0.05, 0.99, 0.70])
        model = IdentityServeModel(window=90)
        p_a, p_b = model.predict(df)
        assert (p_a >= SERVE_PROB_MIN).all()
        assert (p_a <= SERVE_PROB_MAX).all()
        assert (p_b >= SERVE_PROB_MIN).all()
        assert (p_b <= SERVE_PROB_MAX).all()
        # The values that needed clipping land exactly on the bounds
        assert p_a[0] == SERVE_PROB_MIN
        assert p_a[1] == SERVE_PROB_MAX

    def test_nan_imputed_to_league_mean(self):
        df = self._make_df([0.6, math.nan, 0.5], [math.nan, 0.7, 0.65])
        model = IdentityServeModel(window=90)
        p_a, p_b = model.predict(df)
        assert np.isfinite(p_a).all()
        assert np.isfinite(p_b).all()
        assert p_a[1] == LEAGUE_MEAN_SERVE_PROB
        assert p_b[0] == LEAGUE_MEAN_SERVE_PROB

    def test_required_columns_with_window(self):
        model = IdentityServeModel(window=90)
        cols = model.required_columns
        assert cols == ["player_pts_service_won_pct_90d", "opp_pts_service_won_pct_90d"]

    def test_required_columns_no_window(self):
        model = IdentityServeModel(window=None)
        cols = model.required_columns
        assert cols == ["player_pts_service_won_pct", "opp_pts_service_won_pct"]

    def test_fit_is_no_op(self):
        df = self._make_df([0.6], [0.5])
        model = IdentityServeModel(window=90)
        model.fit(df)  # Should not raise

    def test_dtype_is_float64(self):
        df = self._make_df([0.6, 0.7], [0.5, 0.65])
        model = IdentityServeModel(window=90)
        p_a, p_b = model.predict(df)
        assert p_a.dtype == np.float64
        assert p_b.dtype == np.float64

    def test_custom_clip_bounds(self):
        df = self._make_df([0.40, 0.85], [0.40, 0.85])
        model = IdentityServeModel(window=90, clip_min=0.45, clip_max=0.80)
        p_a, p_b = model.predict(df)
        assert p_a[0] == 0.45
        assert p_a[1] == 0.80


class TestMatchupServeModel:
    """Tests for MatchupServeModel — predicts per-match serve rate from
    matchup features, training on both perspectives via player_↔opp_ swap."""

    def _make_df(self, n=200, rng_seed=0):
        """Synthetic matches where each player's actual serve rate is a known
        linear function of their own rolling rate plus the opponent's return
        strength signal."""
        rng = np.random.default_rng(rng_seed)
        # Each player has a rolling serve rate around 0.62.
        player_serve = np.clip(rng.normal(0.62, 0.04, n), 0.45, 0.80)
        opp_serve = np.clip(rng.normal(0.62, 0.04, n), 0.45, 0.80)
        # And a "return strength" signal. Higher opp return strength → A serves
        # worse this match.
        player_return = rng.normal(0.0, 1.0, n)
        opp_return = rng.normal(0.0, 1.0, n)

        # Ground truth: player A's actual serve rate is a function of their own
        # rolling rate adjusted DOWN by opponent return strength, and vice versa.
        actual_a = (
            0.7 * player_serve + 0.3 * 0.62 - 0.01 * opp_return
            + rng.normal(0.0, 0.005, n)
        )
        actual_b = (
            0.7 * opp_serve + 0.3 * 0.62 - 0.01 * player_return
            + rng.normal(0.0, 0.005, n)
        )
        served_a = 80
        served_b = 80
        won_a = (actual_a * served_a).round().astype(int)
        won_b = (actual_b * served_b).round().astype(int)

        return pl.DataFrame(
            {
                "match_uid": [f"m{i}" for i in range(n)],
                "best_of": [3] * n,
                "player_pts_service_won_pct_90d": player_serve.tolist(),
                "opp_pts_service_won_pct_90d": opp_serve.tolist(),
                "player_return_strength": player_return.tolist(),
                "opp_return_strength": opp_return.tolist(),
                # Player perspective is unprefixed in the parquet schema.
                "pts_service_pts_won": won_a.tolist(),
                "pts_service_pts_played": [served_a] * n,
                "opp_pts_service_pts_won": won_b.tolist(),
                "opp_pts_service_pts_played": [served_b] * n,
            }
        )

    def test_fit_then_predict_runs(self):
        df = self._make_df(n=200)
        model = MatchupServeModel(
            feature_columns=[
                "player_pts_service_won_pct_90d",
                "player_return_strength",
            ],
            regressor_type="ridge",
            regressor_params={"alpha": 1.0},
        )
        model.fit(df)
        p_a, p_b = model.predict(df)
        assert p_a.shape == (200,)
        assert p_b.shape == (200,)
        assert (p_a >= SERVE_PROB_MIN).all() and (p_a <= SERVE_PROB_MAX).all()
        assert (p_b >= SERVE_PROB_MIN).all() and (p_b <= SERVE_PROB_MAX).all()

    def test_predictions_track_inputs(self):
        """A player whose own rolling serve rate is higher should receive a
        higher predicted serve win pct than one whose rate is lower."""
        df = self._make_df(n=500, rng_seed=2)
        model = MatchupServeModel(
            feature_columns=[
                "player_pts_service_won_pct_90d",
                "player_return_strength",
            ],
            regressor_type="ridge",
            regressor_params={"alpha": 0.01},
        )
        model.fit(df)
        p_a, _ = model.predict(df)
        # Sort by player rolling rate; predicted should be monotonic in expectation.
        rolling = df["player_pts_service_won_pct_90d"].to_numpy()
        order = np.argsort(rolling)
        # Compare top 50 vs bottom 50
        top = p_a[order][-50:].mean()
        bot = p_a[order][:50].mean()
        assert top > bot

    def test_swap_predicts_other_perspective(self):
        """If we manually swap player_↔opp columns in the input and call
        predict, the resulting (p_a, p_b) should equal (p_b_orig, p_a_orig)."""
        df = self._make_df(n=100, rng_seed=3)
        model = MatchupServeModel(
            feature_columns=[
                "player_pts_service_won_pct_90d",
                "player_return_strength",
            ],
            regressor_type="ridge",
            regressor_params={"alpha": 0.5},
        )
        model.fit(df)
        p_a_orig, p_b_orig = model.predict(df)

        df_swapped = df.rename(
            {
                "player_pts_service_won_pct_90d": "_tmp_a",
                "opp_pts_service_won_pct_90d": "player_pts_service_won_pct_90d",
                "player_return_strength": "_tmp_b",
                "opp_return_strength": "player_return_strength",
            }
        ).rename(
            {
                "_tmp_a": "opp_pts_service_won_pct_90d",
                "_tmp_b": "opp_return_strength",
            }
        )
        p_a_swap, p_b_swap = model.predict(df_swapped)
        # Swapping inputs should swap outputs.
        np.testing.assert_allclose(p_a_swap, p_b_orig, atol=1e-10)
        np.testing.assert_allclose(p_b_swap, p_a_orig, atol=1e-10)

    def test_required_columns_includes_both_perspectives(self):
        model = MatchupServeModel(
            feature_columns=[
                "player_pts_service_won_pct_90d",
                "player_return_strength",
            ],
        )
        cols = set(model.required_columns)
        # Player versions
        assert "player_pts_service_won_pct_90d" in cols
        assert "player_return_strength" in cols
        # Opp versions (derived by prefix swap)
        assert "opp_pts_service_won_pct_90d" in cols
        assert "opp_return_strength" in cols
        # Raw service-stat target columns: player perspective is unprefixed,
        # opp perspective has the opp_ prefix.
        assert "pts_service_pts_won" in cols
        assert "pts_service_pts_played" in cols
        assert "opp_pts_service_pts_won" in cols
        assert "opp_pts_service_pts_played" in cols

    def test_match_level_columns_passed_through(self):
        """Match-level columns (no player_/opp_ prefix) are used as-is for
        both perspectives."""
        df = self._make_df(n=100, rng_seed=4).with_columns(
            pl.lit(1.0).alias("is_grand_slam"),
        )
        model = MatchupServeModel(
            feature_columns=["player_pts_service_won_pct_90d"],
            match_level_columns=["is_grand_slam"],
        )
        model.fit(df)
        p_a, p_b = model.predict(df)
        assert p_a.shape == (100,)
        assert "is_grand_slam" in model.required_columns

    def test_predict_before_fit_raises(self):
        df = self._make_df(n=10)
        model = MatchupServeModel(
            feature_columns=["player_pts_service_won_pct_90d"],
        )
        with pytest.raises(RuntimeError, match="before fit"):
            model.predict(df)

    def test_empty_feature_columns_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            MatchupServeModel(feature_columns=[])

    def test_swap_perspective_helper(self):
        assert MatchupServeModel._swap_perspective("player_foo") == "opp_foo"
        assert MatchupServeModel._swap_perspective("opp_bar") == "player_bar"
        # No prefix → unchanged (treated as match-level)
        assert MatchupServeModel._swap_perspective("best_of") == "best_of"
