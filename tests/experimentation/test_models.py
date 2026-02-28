"""Tests for model wrappers."""

import numpy as np
import pytest

from mvp.experimentation.models import get_model


class TestModelFactory:
    """Tests for model factory."""

    def test_get_xgboost_model(self):
        """Get XGBoost model wrapper."""
        model = get_model("xgboost", {"max_depth": 3})
        assert model is not None
        assert hasattr(model, "fit")
        assert hasattr(model, "predict_proba")

    def test_get_logistic_model(self):
        """Get logistic regression model wrapper."""
        model = get_model("logistic", {"C": 1.0})
        assert model is not None
        assert hasattr(model, "fit")
        assert hasattr(model, "predict_proba")

    def test_unknown_model_raises(self):
        """Unknown model type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown model type"):
            get_model("unknown_model", {})


class TestModelTraining:
    """Tests for model training."""

    @pytest.fixture
    def sample_data(self) -> tuple[np.ndarray, np.ndarray]:
        """Create sample training data."""
        np.random.seed(42)
        X = np.random.randn(100, 5)
        y = (X[:, 0] > 0).astype(int)
        return X, y

    def test_xgboost_fit_predict(self, sample_data):
        """XGBoost model can fit and predict."""
        X, y = sample_data
        model = get_model("xgboost", {"max_depth": 3, "n_estimators": 10})
        model.fit(X, y)
        probs = model.predict_proba(X)

        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_logistic_fit_predict(self, sample_data):
        """Logistic model can fit and predict."""
        X, y = sample_data
        model = get_model("logistic", {"C": 1.0})
        model.fit(X, y)
        probs = model.predict_proba(X)

        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_xgboost_predict_before_fit_raises(self):
        """Calling predict_proba before fit raises RuntimeError."""
        model = get_model("xgboost", {})
        with pytest.raises(RuntimeError, match="Model not fitted"):
            model.predict_proba(np.random.randn(5, 3))

    def test_logistic_predict_before_fit_raises(self):
        """Calling predict_proba before fit raises RuntimeError."""
        model = get_model("logistic", {})
        with pytest.raises(RuntimeError, match="Model not fitted"):
            model.predict_proba(np.random.randn(5, 3))
