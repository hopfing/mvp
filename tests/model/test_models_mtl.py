"""Tests for XGBoostMTLModel, the heterogeneous MTL objective, and the
custom multi-output eval metric in models.py."""

from __future__ import annotations

import pickle

import numpy as np
import pytest

from mvp.model.models import (
    XGBoostMTLModel,
    _mtl_heterogeneous_objective,
    _mtl_heterogeneous_objective_factory,
    _mtl_primary_logloss_eval,
    _sigmoid,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_synthetic(n_rows: int = 200, n_features: int = 5, seed: int = 7) -> tuple[np.ndarray, np.ndarray]:
    """Synthetic dataset with 4 targets:
    - col 0: binary `won` correlated with first feature
    - col 1: game_margin — continuous, correlated with first feature
    - col 2: set_margin — small-integer, correlated with first feature
    - col 3: set_count — 2 or 3, weakly correlated

    Returns (X, y) where y is shape [n_rows, 4].
    """
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n_rows, n_features))
    score = X[:, 0] + 0.3 * rng.normal(size=n_rows)
    won = (score > 0.0).astype(np.int64)
    game_margin = (8.0 * score + rng.normal(size=n_rows)).astype(np.float64)
    set_margin = np.clip(np.round(score), -3, 3).astype(np.float64)
    set_count = np.where(np.abs(score) > 0.5, 2, 3).astype(np.float64)
    y = np.stack([won.astype(np.float64), game_margin, set_margin, set_count], axis=1)
    return X.astype(np.float64), y


def _make_dmatrix(X: np.ndarray, y: np.ndarray):
    import xgboost as xgb
    return xgb.DMatrix(X, label=y)


# ---------------------------------------------------------------------------
# Objective function: shapes, math, diagonal Hessian, weight scaling
# ---------------------------------------------------------------------------


class TestMTLObjective:
    """Direct unit tests on _mtl_heterogeneous_objective."""

    def test_gradient_and_hessian_shapes_match_predt(self):
        """grad and hess have the same shape as predt: [n_rows, num_target]."""
        n_rows, num_target = 10, 4
        X = np.zeros((n_rows, 2))
        y = np.random.default_rng(0).normal(size=(n_rows, num_target))
        y[:, 0] = (y[:, 0] > 0).astype(np.float64)  # primary is 0/1
        dtrain = _make_dmatrix(X, y)
        predt = np.random.default_rng(1).normal(size=(n_rows, num_target))
        weights = np.array([1.0, 0.1, 0.1, 0.1])

        grad, hess = _mtl_heterogeneous_objective(predt, dtrain, weights)

        assert grad.shape == predt.shape
        assert hess.shape == predt.shape

    def test_primary_column_uses_logistic_formula(self):
        """Column 0 grad = (sigmoid(predt) - y) * w0; hess = sigmoid*(1-sigmoid) * w0."""
        n_rows, num_target = 5, 3
        X = np.zeros((n_rows, 1))
        y = np.array([[1, 0, 0], [0, 0, 0], [1, 0, 0], [1, 0, 0], [0, 0, 0]], dtype=np.float64)
        dtrain = _make_dmatrix(X, y)
        predt = np.array(
            [[0.5, 0, 0], [-0.5, 0, 0], [1.0, 0, 0], [-1.0, 0, 0], [0.0, 0, 0]],
            dtype=np.float64,
        )
        weights = np.array([2.0, 0.0, 0.0])  # only primary weight non-zero

        grad, hess = _mtl_heterogeneous_objective(predt, dtrain, weights)

        p = _sigmoid(predt[:, 0])
        expected_grad = (p - y[:, 0]) * 2.0
        expected_hess = p * (1.0 - p) * 2.0
        np.testing.assert_allclose(grad[:, 0], expected_grad, rtol=1e-12)
        np.testing.assert_allclose(hess[:, 0], expected_hess, rtol=1e-12)

    def test_aux_columns_use_squared_error_formula(self):
        """Cols 1+ grad = (predt - y) * w; hess = w (constant)."""
        n_rows, num_target = 4, 3
        X = np.zeros((n_rows, 1))
        y = np.array([[0, 1.5, -0.5], [1, 2.0, 0.3], [0, -1.0, 1.0], [1, 0.0, 0.0]], dtype=np.float64)
        dtrain = _make_dmatrix(X, y)
        predt = np.array([[0, 1.0, 0.0], [0, 2.5, 0.0], [0, -0.5, 1.5], [0, 0.5, -0.5]], dtype=np.float64)
        weights = np.array([0.0, 1.5, 0.5])

        grad, hess = _mtl_heterogeneous_objective(predt, dtrain, weights)

        # col 1 — float32-precision tolerance because DMatrix stores labels
        # as float32 internally; reshape produces ~6e-9 round-trip error
        # vs the float64 reference computation in the test scope.
        np.testing.assert_allclose(
            grad[:, 1], (predt[:, 1] - y[:, 1]) * 1.5, rtol=1e-6,
        )
        np.testing.assert_allclose(hess[:, 1], np.full(n_rows, 1.5), rtol=1e-6)
        # col 2
        np.testing.assert_allclose(
            grad[:, 2], (predt[:, 2] - y[:, 2]) * 0.5, rtol=1e-6,
        )
        np.testing.assert_allclose(hess[:, 2], np.full(n_rows, 0.5), rtol=1e-6)

    def test_hessian_diagonal_across_targets(self):
        """Per-row hess[i, j] only depends on predt[i, j] (and weights[j]).

        Verified by perturbing predt at one (row, col) and confirming hess at
        all OTHER (row, col) entries are unchanged. This is the XGBoost
        diagonal-Hessian constraint.
        """
        n_rows, num_target = 6, 4
        X = np.zeros((n_rows, 1))
        y = np.random.default_rng(2).normal(size=(n_rows, num_target))
        y[:, 0] = (y[:, 0] > 0).astype(np.float64)
        dtrain = _make_dmatrix(X, y)
        predt = np.random.default_rng(3).normal(size=(n_rows, num_target))
        weights = np.array([1.0, 0.3, 0.5, 0.7])

        _, hess_base = _mtl_heterogeneous_objective(predt, dtrain, weights)

        # Perturb a single entry (row=2, col=1) and recompute.
        predt_perturbed = predt.copy()
        predt_perturbed[2, 1] += 0.5
        _, hess_perturbed = _mtl_heterogeneous_objective(predt_perturbed, dtrain, weights)

        # The only entry that may differ is (2, 1) itself — and even that
        # shouldn't differ for cols 1+ because their hess is constant per
        # weight (squared-error). For col 0, predt[2, 0] was unchanged, so
        # hess[2, 0] must be unchanged. All others must be equal.
        diff_mask = ~np.isclose(hess_base, hess_perturbed)
        # No off-diagonal cross-target leakage means diff is zero everywhere.
        assert not diff_mask.any(), (
            f"Diagonal-Hessian violated at entries: {np.argwhere(diff_mask).tolist()}"
        )

    def test_zero_weight_zeros_out_gradient_and_hessian(self):
        """weights[i] = 0 → grad[:, i] = 0 and hess[:, i] = 0 for that column."""
        n_rows, num_target = 5, 3
        X = np.zeros((n_rows, 1))
        y = np.zeros((n_rows, num_target))
        y[:, 0] = np.array([1, 0, 1, 0, 1])
        dtrain = _make_dmatrix(X, y)
        predt = np.random.default_rng(4).normal(size=(n_rows, num_target))
        weights = np.array([1.0, 0.0, 0.0])

        grad, hess = _mtl_heterogeneous_objective(predt, dtrain, weights)

        np.testing.assert_allclose(grad[:, 1], 0.0)
        np.testing.assert_allclose(grad[:, 2], 0.0)
        np.testing.assert_allclose(hess[:, 1], 0.0)
        np.testing.assert_allclose(hess[:, 2], 0.0)

    def test_factory_returns_picklable_callable(self):
        """_mtl_heterogeneous_objective_factory yields a picklable partial
        — required so the trained booster can be pickled with the objective.
        """
        weights = np.array([1.0, 0.5, 0.5])
        fn = _mtl_heterogeneous_objective_factory(weights)
        restored = pickle.loads(pickle.dumps(fn))
        # Call both with identical inputs and verify outputs match
        n_rows, num_target = 4, 3
        X = np.zeros((n_rows, 1))
        y = np.zeros((n_rows, num_target))
        y[:, 0] = np.array([1, 0, 1, 0])
        dtrain = _make_dmatrix(X, y)
        predt = np.random.default_rng(5).normal(size=(n_rows, num_target))

        g0, h0 = fn(predt, dtrain)
        g1, h1 = restored(predt, dtrain)
        np.testing.assert_allclose(g0, g1)
        np.testing.assert_allclose(h0, h1)


# ---------------------------------------------------------------------------
# Custom eval metric: matches sklearn log_loss on primary head
# ---------------------------------------------------------------------------


class TestMTLEvalMetric:
    def test_matches_sklearn_logloss_on_primary_head(self):
        """`_mtl_primary_logloss_eval` value matches sklearn.log_loss computed
        on column 0 only — the multi-output analog of binary:logistic."""
        from sklearn.metrics import log_loss

        rng = np.random.default_rng(6)
        n_rows, num_target = 200, 4
        X = np.zeros((n_rows, 1))
        y = rng.normal(size=(n_rows, num_target))
        y[:, 0] = (y[:, 0] > 0).astype(np.float64)
        dtrain = _make_dmatrix(X, y)
        predt = rng.normal(size=(n_rows, num_target))

        name, value = _mtl_primary_logloss_eval(predt, dtrain)
        assert name == "primary_logloss"

        # Reference: sigmoid the primary column, run sklearn log_loss
        p = _sigmoid(predt[:, 0])
        ref = log_loss(y[:, 0], p, labels=[0, 1])
        assert abs(value - ref) < 1e-10


# ---------------------------------------------------------------------------
# XGBoostMTLModel: fit, predict, standardization, pickle, sample weights
# ---------------------------------------------------------------------------


class TestXGBoostMTLModel:
    @pytest.fixture
    def synthetic(self):
        X, y = _make_synthetic(n_rows=400)
        return X, y

    @pytest.fixture
    def model_params(self):
        return {
            "n_estimators": 30,
            "max_depth": 3,
            "learning_rate": 0.1,
            "weight_won": 1.0,
            "weight_game_margin": 0.1,
            "weight_set_margin": 0.1,
            "weight_set_count": 0.1,
        }

    def test_fit_predict_proba_shape_and_range(self, synthetic, model_params):
        X, y = synthetic
        model = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin", "set_margin", "set_count"],
        )
        model.fit(X[:300], y[:300])
        proba = model.predict_proba(X[300:])
        assert proba.shape == (100,)
        assert ((proba >= 0.0) & (proba <= 1.0)).all()

    def test_fit_predict_aux_shape_and_inverse_transform(self, synthetic, model_params):
        """predict_aux returns aux head predictions on ORIGINAL scale.

        Range check: game_margin predictions should land in roughly the same
        scale as the training targets (~10s of games), not 0-1 standardized.
        """
        X, y = synthetic
        model = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin", "set_margin", "set_count"],
        )
        model.fit(X[:300], y[:300])
        aux = model.predict_aux(X[300:])
        assert aux.shape == (100, 3)
        # game_margin training range was roughly ±20; predictions should be
        # in a comparable scale (not narrowly clustered near zero like a
        # standardized signal would be).
        assert aux[:, 0].std() > 1.0, (
            "game_margin predictions appear to be on standardized scale "
            "rather than inverse-transformed back to original"
        )

    def test_fit_rejects_1d_y(self, synthetic, model_params):
        X, y = synthetic
        model = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin", "set_margin", "set_count"],
        )
        with pytest.raises(ValueError, match="2D y"):
            model.fit(X[:100], y[:100, 0])  # 1D primary only

    def test_fit_rejects_wrong_num_target(self, synthetic, model_params):
        X, y = synthetic
        model = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin"],  # 2 targets
        )
        with pytest.raises(ValueError, match="target columns"):
            model.fit(X[:100], y[:100])  # y has 4 columns

    def test_target_names_required(self, model_params):
        with pytest.raises(ValueError, match="primary target"):
            XGBoostMTLModel(params=model_params, target_names=[])

    def test_loss_weights_extracted_from_params(self, model_params):
        params = dict(model_params)
        params["weight_won"] = 3.0
        params["weight_game_margin"] = 0.5
        model = XGBoostMTLModel(
            params=params,
            target_names=["won", "game_margin", "set_margin", "set_count"],
        )
        # Defaults for the unset aux weights
        assert model.loss_weights[0] == 3.0  # won
        assert model.loss_weights[1] == 0.5  # game_margin
        # Other weights take defaults from model_params (0.1)
        assert model.loss_weights[2] == 0.1
        assert model.loss_weights[3] == 0.1
        # Verify weight_* keys are NOT leaked into XGB params
        assert "weight_won" not in model.params
        assert "weight_game_margin" not in model.params

    def test_stale_weight_keys_dropped_defensively(self):
        """weight_* keys for targets NOT in target_names get silently dropped
        so they don't leak to XGBoost as unknown parameters (which would
        produce a UserWarning). This covers the case of stale config from a
        previous MTLConfig.auxiliary_targets value."""
        params = {
            "n_estimators": 10,
            "max_depth": 3,
            "weight_won": 1.0,
            "weight_game_margin": 0.1,
            "weight_set_count": 0.1,  # set_count NOT in target_names below
            "weight_some_unused_target": 0.5,  # also stale
        }
        model = XGBoostMTLModel(
            params=params,
            target_names=["won", "game_margin"],
        )
        # Configured weights extracted
        assert model.loss_weights[0] == 1.0
        assert model.loss_weights[1] == 0.1
        # Stale weight_* keys dropped — none reach XGB params
        for key in model.params:
            assert not key.startswith("weight_"), (
                f"Stale weight key leaked into XGB params: {key}"
            )

    def test_pickle_round_trip_preserves_predictions(self, synthetic, model_params):
        """After pickle round-trip, predict_proba and predict_aux return the
        same values — both standardization parameters and the booster survive.
        """
        X, y = synthetic
        model = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin", "set_margin", "set_count"],
        )
        model.fit(X[:300], y[:300])

        proba_before = model.predict_proba(X[300:])
        aux_before = model.predict_aux(X[300:])

        restored = pickle.loads(pickle.dumps(model))
        proba_after = restored.predict_proba(X[300:])
        aux_after = restored.predict_aux(X[300:])

        np.testing.assert_allclose(proba_before, proba_after, rtol=1e-10)
        np.testing.assert_allclose(aux_before, aux_after, rtol=1e-10)
        # Verify standardization parameters survived
        np.testing.assert_allclose(model._aux_mean, restored._aux_mean)
        np.testing.assert_allclose(model._aux_std, restored._aux_std)

    def test_sample_weight_propagates_through_dmatrix(self, synthetic, model_params):
        """Silent correctness gate: non-uniform sample_weight must produce a
        different booster than uniform weight. If `dtrain.set_weight()` is not
        called under multi-output, training silently ignores weights and
        produces correct-looking but unweighted output.
        """
        X, y = synthetic
        rng = np.random.default_rng(99)
        # Highly skewed weights to maximize the chance of a detectable diff
        skewed = rng.uniform(0.1, 10.0, size=300)

        m_unweighted = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin", "set_margin", "set_count"],
        )
        m_unweighted.fit(X[:300], y[:300])

        m_weighted = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin", "set_margin", "set_count"],
        )
        m_weighted.fit(X[:300], y[:300], sample_weight=skewed)

        proba_u = m_unweighted.predict_proba(X[300:])
        proba_w = m_weighted.predict_proba(X[300:])

        # Predictions must materially differ — if they're nearly identical,
        # sample_weight was silently dropped during DMatrix construction.
        max_abs_diff = np.abs(proba_u - proba_w).max()
        assert max_abs_diff > 1e-4, (
            f"Weighted vs unweighted predictions barely differ "
            f"(max abs diff = {max_abs_diff:.2e}); sample_weight likely not propagated"
        )

    def test_zero_std_aux_column_does_not_explode(self, model_params):
        """A degenerate aux column with all-equal values should be handled
        gracefully (std=0 guard substitutes 1.0)."""
        rng = np.random.default_rng(11)
        X = rng.normal(size=(100, 3))
        y = np.zeros((100, 3))
        y[:, 0] = (rng.normal(size=100) > 0).astype(np.float64)
        y[:, 1] = rng.normal(size=100)
        y[:, 2] = 7.0  # constant — std=0

        model = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin", "set_margin"],
        )
        model.fit(X, y)
        # Std for the constant column should have been clamped to 1.0
        assert model._aux_std[1] == 1.0

    def test_predict_before_fit_raises(self, model_params):
        model = XGBoostMTLModel(
            params=model_params,
            target_names=["won", "game_margin"],
        )
        with pytest.raises(RuntimeError, match="not fitted"):
            model.predict_proba(np.zeros((5, 3)))
        with pytest.raises(RuntimeError, match="not fitted"):
            model.predict_aux(np.zeros((5, 3)))
