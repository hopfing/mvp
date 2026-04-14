"""Tests for score-state serve model."""

import numpy as np
import pytest

from mvp.projection.iid.score_state_model import LogisticScoreStateModel, build_score_state_model


class TestLogisticScoreStateModel:
    def test_fit_predict_shape(self):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(1000, 3))
        # Target: slightly favor higher X[:, 0]
        logits = 0.5 * X[:, 0] + 0.2 * X[:, 1]
        p = 1 / (1 + np.exp(-logits))
        y = (rng.uniform(size=1000) < p).astype(int)

        model = LogisticScoreStateModel(feature_names=["f0", "f1", "f2"])
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (1000,)
        assert np.all((probs >= 0) & (probs <= 1))

    def test_coef_summary_reflects_signal(self):
        rng = np.random.default_rng(1)
        X = rng.normal(size=(5000, 3))
        # Strong positive signal on f0
        logits = 2.0 * X[:, 0]
        p = 1 / (1 + np.exp(-logits))
        y = (rng.uniform(size=5000) < p).astype(int)

        model = LogisticScoreStateModel(feature_names=["f0", "f1", "f2"])
        model.fit(X, y)
        summary = model.coef_summary()
        assert summary is not None
        # f0 coefficient should be strongly positive
        assert summary["coefs"]["f0"] > 0.5
        # f1, f2 should be near zero (no signal)
        assert abs(summary["coefs"]["f1"]) < 0.5
        assert abs(summary["coefs"]["f2"]) < 0.5

    def test_nan_handled_at_predict(self):
        X_train = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0, 8.0]])
        y_train = np.array([0, 1, 0, 1])
        model = LogisticScoreStateModel(feature_names=["a", "b"])
        model.fit(X_train, y_train)

        # Predict with a NaN — should be imputed to column mean, not crash.
        X_test = np.array([[np.nan, 5.0]])
        probs = model.predict_proba(X_test)
        assert probs.shape == (1,)
        assert 0.0 <= probs[0] <= 1.0

    def test_predict_before_fit_raises(self):
        model = LogisticScoreStateModel(feature_names=["a", "b"])
        with pytest.raises(RuntimeError):
            model.predict_proba(np.zeros((1, 2)))

    def test_builder_dispatch(self):
        m = build_score_state_model(type_="logistic", feature_names=["a", "b"])
        assert isinstance(m, LogisticScoreStateModel)

    def test_builder_xgboost(self):
        from mvp.projection.iid.score_state_model import XGBoostScoreStateModel
        m = build_score_state_model(type_="xgboost", feature_names=["a", "b"])
        assert isinstance(m, XGBoostScoreStateModel)

    def test_xgboost_fit_predict(self):
        rng = np.random.default_rng(2)
        X = rng.normal(size=(500, 3))
        logits = 0.8 * X[:, 0] - 0.4 * X[:, 2]
        p = 1 / (1 + np.exp(-logits))
        y = (rng.uniform(size=500) < p).astype(int)

        m = build_score_state_model(
            type_="xgboost",
            feature_names=["f0", "f1", "f2"],
            params={"n_estimators": 20, "max_depth": 3},
        )
        m.fit(X, y)
        probs = m.predict_proba(X)
        assert probs.shape == (500,)
        assert np.all((probs >= 0) & (probs <= 1))
        summary = m.coef_summary()
        assert summary is not None
        # f0 importance should be highest among the three
        imps = summary["feature_importances"]
        assert imps["f0"] >= max(imps["f1"], imps["f2"])

    def test_builder_unknown_type(self):
        with pytest.raises(ValueError, match="unknown"):
            build_score_state_model(type_="not-a-type", feature_names=["a"])
