"""Tests for projection regression model wrappers."""

import numpy as np
import pytest

from mvp.projection.models import get_regression_model


class TestModelFactory:
    """Tests for regression model factory."""

    def test_get_xgb_regressor(self):
        model = get_regression_model("xgb_regressor", {"max_depth": 3})
        assert model is not None
        assert hasattr(model, "fit")
        assert hasattr(model, "predict")

    def test_get_linear(self):
        model = get_regression_model("linear", {})
        assert model is not None

    def test_get_ridge(self):
        model = get_regression_model("ridge", {"alpha": 1.0})
        assert model is not None

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown regression model type"):
            get_regression_model("logistic", {})


class TestModelTraining:
    """Tests for regression model fit/predict."""

    @pytest.fixture
    def sample_data(self) -> tuple[np.ndarray, np.ndarray]:
        np.random.seed(42)
        X = np.random.randn(100, 5)
        y = 10 + 2 * X[:, 0] - X[:, 1] + np.random.randn(100) * 0.5
        return X, y

    def test_xgb_regressor_fit_predict(self, sample_data):
        X, y = sample_data
        model = get_regression_model("xgb_regressor", {"max_depth": 3, "n_estimators": 10})
        model.fit(X, y)
        preds = model.predict(X)
        assert preds.shape == (100,)
        assert np.isfinite(preds).all()

    def test_linear_fit_predict(self, sample_data):
        X, y = sample_data
        model = get_regression_model("linear", {})
        model.fit(X, y)
        preds = model.predict(X)
        assert preds.shape == (100,)
        assert np.isfinite(preds).all()

    def test_ridge_fit_predict(self, sample_data):
        X, y = sample_data
        model = get_regression_model("ridge", {"alpha": 1.0})
        model.fit(X, y)
        preds = model.predict(X)
        assert preds.shape == (100,)
        assert np.isfinite(preds).all()

    def test_xgb_predict_before_fit_raises(self):
        model = get_regression_model("xgb_regressor", {})
        with pytest.raises(RuntimeError, match="Model not fitted"):
            model.predict(np.random.randn(5, 3))

    def test_linear_predict_before_fit_raises(self):
        model = get_regression_model("linear", {})
        with pytest.raises(RuntimeError, match="Model not fitted"):
            model.predict(np.random.randn(5, 3))

    def test_ridge_predict_before_fit_raises(self):
        model = get_regression_model("ridge", {})
        with pytest.raises(RuntimeError, match="Model not fitted"):
            model.predict(np.random.randn(5, 3))

    def test_predictions_reasonable(self, sample_data):
        """Predictions should be in a reasonable range for the target."""
        X, y = sample_data
        model = get_regression_model("xgb_regressor", {"max_depth": 3, "n_estimators": 50})
        model.fit(X, y)
        preds = model.predict(X)
        # R² > 0 means better than predicting the mean
        ss_res = np.sum((y - preds) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1.0 - ss_res / ss_tot
        assert r2 > 0.5
