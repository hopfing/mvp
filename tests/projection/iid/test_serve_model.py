"""Tests for IID serve win prob estimators."""

import math

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.score_state import ScoreState
from mvp.projection.iid.serve_model import (
    LEAGUE_MEAN_SERVE_PROB,
    SERVE_PROB_MAX,
    SERVE_PROB_MIN,
    IdentityServeModel,
    MatchupServeModel,
    ScoreStateChainServeModel,
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


class TestServeWinProbEstimatorDefaultStateFn:
    """The base class default `predict_state_fn` freezes scalar `predict()`
    output into constant, state-independent callables. This invariant is what
    lets scalar serve models flow unchanged through the stateful chain path."""

    def test_identity_default_state_fn_is_constant(self):
        df = pl.DataFrame(
            {
                "player_pts_service_won_pct_90d": [0.65, 0.60],
                "opp_pts_service_won_pct_90d": [0.58, 0.62],
            }
        )
        model = IdentityServeModel(window=90)
        p_a_fn, p_b_fn = model.predict_state_fn(df)
        p_a_expected, p_b_expected = model.predict(df)

        s1 = ScoreState(
            serve_num=1, game_score_server="0", game_score_returner="0",
            is_tiebreak=False,
            set_score_server_games=0, set_score_returner_games=0,
            sets_won_server=0, sets_won_returner=0, best_of=3,
        )
        s2 = ScoreState(
            serve_num=2, game_score_server="40", game_score_returner="AD",
            is_tiebreak=True,
            set_score_server_games=6, set_score_returner_games=6,
            sets_won_server=1, sets_won_returner=1, best_of=3,
        )
        np.testing.assert_array_equal(p_a_fn(s1), p_a_fn(s2))
        np.testing.assert_array_equal(p_b_fn(s1), p_b_fn(s2))
        np.testing.assert_array_equal(p_a_fn(s1), p_a_expected)
        np.testing.assert_array_equal(p_b_fn(s1), p_b_expected)


class _ConstStubScoreStateModel:
    """Minimal ScoreStateServeModel stub that returns a constant.

    Used to exercise the ChainServeModel routing/swap logic without the noise
    of a fitted classifier. Satisfies the duck-typed contract the chain wrapper
    needs: `match_feature_names`, `point_feature_names`, `predict_proba`.
    """

    def __init__(
        self,
        match_feature_names: list[str],
        point_feature_names: list[str],
        const: float = 0.6,
    ) -> None:
        self.match_feature_names = list(match_feature_names)
        self.point_feature_names = list(point_feature_names)
        self.feature_names = list(match_feature_names) + list(point_feature_names)
        self._const = const

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        return np.full(X.shape[0], self._const, dtype=np.float64)


class _XDependentStubScoreStateModel:
    """Stub whose output is a deterministic function of the feature matrix.

    Returns `sigmoid(X @ weights)`. Lets tests assert that state-varying
    features produce varying output.
    """

    def __init__(
        self,
        match_feature_names: list[str],
        point_feature_names: list[str],
        weights: np.ndarray,
    ) -> None:
        self.match_feature_names = list(match_feature_names)
        self.point_feature_names = list(point_feature_names)
        self.feature_names = list(match_feature_names) + list(point_feature_names)
        self._weights = np.asarray(weights, dtype=np.float64)
        if self._weights.shape != (len(self.feature_names),):
            raise ValueError(
                f"weights {self._weights.shape} must match feature count "
                f"{len(self.feature_names)}"
            )

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        z = X.astype(np.float64) @ self._weights
        return 1.0 / (1.0 + np.exp(-z))


def _make_chain_with_stub(
    stub_model,
    *,
    match_level_features: list[str] | None = None,
    point_level_features: list[str] | None = None,
    match_feature_cols: list[str] | None = None,
) -> "ScoreStateChainServeModel":
    """Build a ScoreStateChainServeModel and inject a post-fit state.

    fit() has a full data-pipeline (parquet load + FeatureEngine + join). Unit
    tests for routing / state logic don't need any of that — they need a model
    whose `_model` attribute is a stub and whose `_match_feature_cols` is set.
    This helper bypasses fit() for those tests.

    If `match_feature_cols` is given without `match_level_features`, the init
    validation still requires one of the feature lists to be non-empty — we
    synthesize placeholder specs that are overridden below.
    """
    match_level = list(match_level_features or [])
    point_level = list(point_level_features or [])
    if match_feature_cols and not match_level:
        match_level = [f"__stub__{i}" for i in range(len(match_feature_cols))]
    chain_model = ScoreStateChainServeModel(
        model_type="logistic",
        match_level_features=match_level,
        point_level_features=point_level,
    )
    chain_model._model = stub_model  # type: ignore[assignment]
    if match_feature_cols is not None:
        chain_model._match_feature_cols = list(match_feature_cols)
    else:
        chain_model._match_feature_cols = chain_model._resolve_match_feature_cols()
    return chain_model


class TestMatchFeatureIsDiff:
    """_resolve_match_feature_cols must flag anti-symmetric (player-only) diff
    features so the perspective swap negates rather than reading a nonexistent
    opp_ column.

    Regression: a single transform (e.g. style_matchup, mirror=False) emits BOTH
    a mirror pair (player_/opp_vs_opp_style_resid) AND a player-only diff
    (player_vs_opp_style_resid_diff). The transform's mirror flag alone can't
    tell them apart; the naive registry.get(base_name) lookup KeyError'd on both
    and defaulted is_diff=False, so the swap read a nonexistent
    opp_vs_opp_style_resid_diff column and crashed mid-FS.
    """

    @pytest.mark.parametrize(
        "spec, expected_is_diff",
        [
            # transform diff, player-only → the exact column that crashed FS
            ("player_vs_opp_style_resid_diff", True),
            # transform mirror pair (opp_ counterpart exists) → NOT a diff
            ("player_vs_opp_style_resid", False),
            # directly-registered mirror=False diffs
            ("player_elo_surface_diff", True),
            ("player_glicko_rd_diff", True),
            # directly-registered mirror=True feature
            ("player_win_pct(days=30)", False),
        ],
    )
    def test_is_diff_classification(self, spec, expected_is_diff):
        model = ScoreStateChainServeModel(
            model_type="logistic",
            match_level_features=[spec],
            point_level_features=[],
        )
        model._resolve_match_feature_cols()  # populates _match_feature_is_diff
        assert model._match_feature_is_diff == [expected_is_diff]


class TestScoreStateChainServeModel:
    """Tests for the bridge between trained point-grain score-state classifiers
    and the IID chain's serve-estimator interface.

    Unit tests here bypass fit() via `_make_chain_with_stub`. The full
    fit-and-predict round-trip is covered by the E2E integration test."""

    def _neutral_state(self, best_of: int = 3) -> ScoreState:
        return ScoreState(
            serve_num=1, game_score_server="0", game_score_returner="0",
            is_tiebreak=False,
            set_score_server_games=0, set_score_returner_games=0,
            sets_won_server=0, sets_won_returner=0, best_of=best_of,
        )

    def test_is_state_aware(self):
        chain_model = ScoreStateChainServeModel(
            model_type="logistic",
            match_level_features=[],
            point_level_features=["is_break_point"],
        )
        assert chain_model.is_state_aware is True

    def test_required_columns_translates_server_returner_prefix(self):
        stub = _ConstStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d", "returner_return_strength"],
            point_feature_names=[],
        )
        chain_model = _make_chain_with_stub(
            stub,
            match_feature_cols=["server_serve_pct_90d", "returner_return_strength"],
        )
        cols = set(chain_model.required_columns)
        assert "player_serve_pct_90d" in cols
        assert "opp_serve_pct_90d" in cols
        assert "player_return_strength" in cols
        assert "opp_return_strength" in cols
        assert "server_serve_pct_90d" not in cols
        assert "returner_return_strength" not in cols

    def test_required_columns_excludes_state_derivable_point_features(self):
        stub = _ConstStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d"],
            point_feature_names=["is_break_point", "is_surface_hard"],
        )
        chain_model = _make_chain_with_stub(
            stub,
            point_level_features=["is_break_point", "is_surface_hard"],
            match_feature_cols=["server_serve_pct_90d"],
        )
        cols = set(chain_model.required_columns)
        assert "is_break_point" not in cols
        # is_surface_hard is a derivation — the model requires its source
        # column (`surface`) and materializes the flag internally.
        assert "is_surface_hard" not in cols
        assert "surface" in cols

    def test_required_columns_includes_best_of(self):
        stub = _ConstStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d"],
            point_feature_names=[],
        )
        chain_model = _make_chain_with_stub(
            stub, match_feature_cols=["server_serve_pct_90d"],
        )
        assert "best_of" in chain_model.required_columns

    def test_empty_feature_lists_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            ScoreStateChainServeModel(
                model_type="logistic",
                match_level_features=[],
                point_level_features=[],
            )

    def test_predict_state_fn_before_fit_raises(self):
        chain_model = ScoreStateChainServeModel(
            model_type="logistic",
            match_level_features=[],
            point_level_features=["is_break_point"],
        )
        df = pl.DataFrame({"best_of": [3]})
        with pytest.raises(RuntimeError, match="before fit"):
            chain_model.predict_state_fn(df)

    def test_predict_returns_two_arrays_at_neutral_state(self):
        stub = _ConstStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d"],
            point_feature_names=[],
            const=0.63,
        )
        chain_model = _make_chain_with_stub(
            stub, match_feature_cols=["server_serve_pct_90d"],
        )
        df = pl.DataFrame(
            {
                "player_serve_pct_90d": [0.65, 0.60, 0.70],
                "opp_serve_pct_90d": [0.58, 0.62, 0.55],
                "best_of": [3, 3, 5],
            }
        )
        p_a, p_b = chain_model.predict(df)
        assert p_a.shape == (3,)
        assert p_b.shape == (3,)
        np.testing.assert_allclose(p_a, 0.63)
        np.testing.assert_allclose(p_b, 0.63)

    def test_perspective_swap_with_asymmetric_features(self):
        """A model whose output depends on the feature values should produce
        different p_a and p_b when player_ and opp_ columns are asymmetric."""
        stub = _XDependentStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d", "returner_serve_pct_90d"],
            point_feature_names=[],
            weights=np.array([5.0, -5.0]),
        )
        chain_model = _make_chain_with_stub(
            stub,
            match_feature_cols=["server_serve_pct_90d", "returner_serve_pct_90d"],
        )
        df = pl.DataFrame(
            {
                "player_serve_pct_90d": [0.80],
                "opp_serve_pct_90d": [0.40],
                "best_of": [3],
            }
        )
        p_a, p_b = chain_model.predict(df)
        assert p_a[0] > p_b[0]
        np.testing.assert_allclose(p_a[0], 1.0 / (1.0 + np.exp(-2.0)), rtol=1e-10)
        np.testing.assert_allclose(p_b[0], 1.0 / (1.0 + np.exp(2.0)), rtol=1e-10)

    def test_state_independent_when_no_state_derivable_features(self):
        """Match-level-only model: p_fn should be identical across ScoreStates."""
        stub = _XDependentStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d"],
            point_feature_names=[],
            weights=np.array([2.0]),
        )
        chain_model = _make_chain_with_stub(
            stub, match_feature_cols=["server_serve_pct_90d"],
        )
        df = pl.DataFrame(
            {
                "player_serve_pct_90d": [0.65],
                "opp_serve_pct_90d": [0.60],
                "best_of": [3],
            }
        )
        p_a_fn, _ = chain_model.predict_state_fn(df)
        s1 = self._neutral_state()
        s2 = ScoreState(
            serve_num=2, game_score_server="40", game_score_returner="30",
            is_tiebreak=False,
            set_score_server_games=5, set_score_returner_games=4,
            sets_won_server=1, sets_won_returner=0, best_of=3,
        )
        np.testing.assert_allclose(p_a_fn(s1), p_a_fn(s2))

    def test_state_aware_output_varies_with_break_point(self):
        """Adding is_break_point as a point-level feature must produce different
        output at break-point vs non-break-point states."""
        stub = _XDependentStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d"],
            point_feature_names=["is_break_point"],
            weights=np.array([1.0, -3.0]),
        )
        chain_model = _make_chain_with_stub(
            stub,
            point_level_features=["is_break_point"],
            match_feature_cols=["server_serve_pct_90d"],
        )
        df = pl.DataFrame(
            {
                "player_serve_pct_90d": [0.65],
                "opp_serve_pct_90d": [0.60],
                "best_of": [3],
            }
        )
        p_a_fn, _ = chain_model.predict_state_fn(df)

        non_bp = self._neutral_state()
        bp = ScoreState(
            serve_num=1, game_score_server="0", game_score_returner="40",
            is_tiebreak=False,
            set_score_server_games=0, set_score_returner_games=0,
            sets_won_server=0, sets_won_returner=0, best_of=3,
        )
        p_non_bp = p_a_fn(non_bp)[0]
        p_bp = p_a_fn(bp)[0]
        assert p_bp < p_non_bp
        np.testing.assert_allclose(p_non_bp, 1.0 / (1.0 + np.exp(-0.65)), rtol=1e-10)
        np.testing.assert_allclose(p_bp, 1.0 / (1.0 + np.exp(2.35)), rtol=1e-10)

    def test_match_constant_point_feature_broadcast_from_df(self):
        """A non-state-derivable point feature (e.g. `is_surface_hard`) is
        broadcast from the df, not from ScoreState."""
        stub = _XDependentStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d"],
            point_feature_names=["is_surface_hard"],
            weights=np.array([1.0, 2.0]),
        )
        chain_model = _make_chain_with_stub(
            stub,
            point_level_features=["is_surface_hard"],
            match_feature_cols=["server_serve_pct_90d"],
        )
        df = pl.DataFrame(
            {
                "player_serve_pct_90d": [0.65, 0.65],
                "opp_serve_pct_90d": [0.60, 0.60],
                "is_surface_hard": [1.0, 0.0],
                "best_of": [3, 3],
            }
        )
        p_a_fn, _ = chain_model.predict_state_fn(df)
        p = p_a_fn(self._neutral_state())
        assert p[0] > p[1]
        np.testing.assert_allclose(p[0], 1.0 / (1.0 + np.exp(-(0.65 + 2.0))), rtol=1e-10)
        np.testing.assert_allclose(p[1], 1.0 / (1.0 + np.exp(-(0.65))), rtol=1e-10)

    def test_match_constant_point_feature_missing_raises(self):
        stub = _ConstStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d"],
            point_feature_names=["is_surface_hard"],
        )
        chain_model = _make_chain_with_stub(
            stub,
            point_level_features=["is_surface_hard"],
            match_feature_cols=["server_serve_pct_90d"],
        )
        df = pl.DataFrame(
            {
                "player_serve_pct_90d": [0.65],
                "opp_serve_pct_90d": [0.60],
                "best_of": [3],
            }
        )
        with pytest.raises(KeyError, match="is_surface_hard"):
            chain_model.predict_state_fn(df)

    def test_scalar_equivalence_through_stateful_chain(self):
        """Match-level-only wrapped model through `match_distribution_from_state_fn`
        produces output identical to the scalar `match_distribution` path. This
        guarantees existing scalar behavior is preserved when the chain is
        upgraded to state-aware mode."""
        from mvp.projection.iid.chain import (
            match_distribution, p_service_game_win, p_tiebreak_game_win,
        )
        from mvp.projection.iid.stateful_chain import match_distribution_from_state_fn

        stub = _XDependentStubScoreStateModel(
            match_feature_names=["server_serve_pct_90d", "returner_serve_pct_90d"],
            point_feature_names=[],
            weights=np.array([2.0, -2.0]),
        )
        chain_model = _make_chain_with_stub(
            stub,
            match_feature_cols=["server_serve_pct_90d", "returner_serve_pct_90d"],
        )
        df = pl.DataFrame(
            {
                "player_serve_pct_90d": [0.65, 0.58, 0.70],
                "opp_serve_pct_90d": [0.60, 0.62, 0.55],
                "best_of": [3, 3, 5],
            }
        )
        p_a, p_b = chain_model.predict(df)
        p_a_fn, p_b_fn = chain_model.predict_state_fn(df)

        best_of = np.array([3, 3, 5], dtype=np.int64)
        stateful = match_distribution_from_state_fn(p_a_fn, p_b_fn, p_a, p_b, best_of)

        h_a = p_service_game_win(p_a)
        h_b = p_service_game_win(p_b)
        t_ab = p_tiebreak_game_win(p_a, p_b)
        scalar = match_distribution(h_a, h_b, t_ab, best_of)

        np.testing.assert_allclose(
            stateful.p_match_win_a, scalar.p_match_win_a, rtol=1e-9,
        )
        np.testing.assert_allclose(
            stateful.total_games_pmf, scalar.total_games_pmf, rtol=1e-9, atol=1e-12,
        )
        np.testing.assert_allclose(
            stateful.spread_pmf, scalar.spread_pmf, rtol=1e-9, atol=1e-12,
        )


class TestScoreStateChainServeModelE2E:
    """End-to-end integration: fit on synthetic points, project through
    TennisProjector, verify distributional output.

    Covers the full wiring: parquet load → inline fit → stateful chain path.
    Uses `match_level_features=[]` to avoid FeatureEngine / matches.parquet;
    only the point-level training path is exercised here.
    """

    def test_fit_predict_project_roundtrip(self, tmp_path):
        from mvp.projection.iid.projector import TennisProjector

        rng = np.random.default_rng(42)
        n_matches = 30
        match_uids = [f"m{i:03d}" for i in range(n_matches)]
        points_rows: list[dict] = []
        for i, uid in enumerate(match_uids):
            server_id = f"p{i * 2:04d}"
            returner_id = f"p{i * 2 + 1:04d}"
            for _ in range(80):
                is_bp = bool(rng.random() < 0.2)
                # Server wins ~70% normally but ~35% at break points.
                p_true = 0.35 if is_bp else 0.70
                y = int(rng.random() < p_true)
                points_rows.append(
                    {
                        "match_uid": uid,
                        "server_id": server_id,
                        "returner_id": returner_id,
                        "point_won_by_server": y,
                        "is_break_point": is_bp,
                    }
                )
        points_df = pl.DataFrame(points_rows)
        points_path = tmp_path / "points.parquet"
        points_df.write_parquet(points_path)

        # Match-grain df: one row per match.
        match_df = pl.DataFrame(
            {
                "match_uid": match_uids,
                "best_of": [3] * n_matches,
            }
        )

        chain_model = ScoreStateChainServeModel(
            model_type="logistic",
            match_level_features=[],
            point_level_features=["is_break_point"],
            points_path=points_path,
        )
        chain_model.fit(match_df)

        # State-aware assertion: server's probability should be lower at BP.
        p_a_fn, _ = chain_model.predict_state_fn(match_df)
        neutral = ScoreState(
            serve_num=1, game_score_server="0", game_score_returner="0",
            is_tiebreak=False,
            set_score_server_games=0, set_score_returner_games=0,
            sets_won_server=0, sets_won_returner=0, best_of=3,
        )
        bp = ScoreState(
            serve_num=1, game_score_server="0", game_score_returner="40",
            is_tiebreak=False,
            set_score_server_games=0, set_score_returner_games=0,
            sets_won_server=0, sets_won_returner=0, best_of=3,
        )
        p_neutral = p_a_fn(neutral)
        p_bp = p_a_fn(bp)
        # Synthetic signal: BP should pull server probability down.
        assert p_bp[0] < p_neutral[0]

        # Project via TennisProjector — exercises the stateful dispatch path.
        projector = TennisProjector(chain_model)
        projector.fit(match_df)  # ScoreStateChainServeModel.fit is called again; idempotent
        out = projector.project(match_df)
        dist = out.distribution

        # Shape + normalization checks.
        assert dist.p_match_win_a.shape == (n_matches,)
        assert np.all((dist.p_match_win_a >= 0) & (dist.p_match_win_a <= 1))
        np.testing.assert_allclose(
            dist.total_games_pmf.sum(axis=1), 1.0, atol=1e-8,
        )
        np.testing.assert_allclose(
            dist.spread_pmf.sum(axis=1), 1.0, atol=1e-8,
        )
        # Set-outcome marginals should sum to 1 per match too.
        total_set_prob = np.zeros(n_matches, dtype=np.float64)
        for vec in dist.set_outcome_probs.values():
            total_set_prob += vec
        np.testing.assert_allclose(total_set_prob, 1.0, atol=1e-8)

    def test_score_test_points_returns_point_metrics(self, tmp_path):
        rng = np.random.default_rng(7)
        n_matches = 40
        match_uids = [f"m{i:03d}" for i in range(n_matches)]
        points_rows: list[dict] = []
        for i, uid in enumerate(match_uids):
            for _ in range(60):
                is_bp = bool(rng.random() < 0.2)
                p_true = 0.35 if is_bp else 0.70
                y = int(rng.random() < p_true)
                points_rows.append(
                    {
                        "match_uid": uid,
                        "server_id": f"p{i * 2:04d}",
                        "returner_id": f"p{i * 2 + 1:04d}",
                        "point_won_by_server": y,
                        "is_break_point": is_bp,
                    }
                )
        points_df = pl.DataFrame(points_rows)
        points_path = tmp_path / "points.parquet"
        points_df.write_parquet(points_path)

        train_uids = match_uids[:30]
        test_uids = match_uids[30:]
        train_df = pl.DataFrame(
            {"match_uid": train_uids, "best_of": [3] * len(train_uids)}
        )
        test_df = pl.DataFrame(
            {"match_uid": test_uids, "best_of": [3] * len(test_uids)}
        )

        chain_model = ScoreStateChainServeModel(
            model_type="logistic",
            match_level_features=[],
            point_level_features=["is_break_point"],
            points_path=points_path,
        )
        chain_model.fit(train_df)

        metrics = chain_model.score_test_points(test_df)

        # Standard compute_metrics keys, all prefixed.
        for k in (
            "point_log_loss",
            "point_brier_score",
            "point_roc_auc",
            "point_accuracy",
            "point_calibration_error",
        ):
            assert k in metrics, f"missing {k}"
        assert 0.0 < metrics["point_log_loss"] < 2.0
        assert 0.0 <= metrics["point_brier_score"] <= 1.0
        assert 0.0 <= metrics["point_accuracy"] <= 1.0

    def test_score_test_points_before_fit_raises(self, tmp_path):
        chain_model = ScoreStateChainServeModel(
            model_type="logistic",
            match_level_features=[],
            point_level_features=["is_break_point"],
            points_path=tmp_path / "points.parquet",
        )
        with pytest.raises(RuntimeError, match="before fit"):
            chain_model.score_test_points(
                pl.DataFrame({"match_uid": ["m000"], "best_of": [3]}),
            )
