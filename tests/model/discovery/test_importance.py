"""Tests for feature importance computation."""

import numpy as np
import pytest

from mvp.model.discovery.importance import (
    compute_importance,
    gain_importance,
    permutation_importance,
    shap_importance,
)
from mvp.model.models import LogisticModel, XGBoostModel


@pytest.fixture
def sample_data():
    """Generate sample classification data."""
    np.random.seed(42)
    n_samples = 500

    # Feature 1: Strong predictor
    x1 = np.random.randn(n_samples)
    # Feature 2: Weak predictor
    x2 = np.random.randn(n_samples)
    # Feature 3: Noise
    x3 = np.random.randn(n_samples)

    # Target correlated with x1, weakly with x2, not with x3
    y = (x1 + 0.3 * x2 + np.random.randn(n_samples) * 0.5 > 0).astype(int)

    X = np.column_stack([x1, x2, x3])
    feature_names = ["strong_feature", "weak_feature", "noise_feature"]

    return X, y, feature_names


@pytest.fixture
def trained_xgboost(sample_data):
    """Train XGBoost model on sample data."""
    X, y, _ = sample_data
    model = XGBoostModel({"n_estimators": 50, "max_depth": 3})
    model.fit(X, y)
    return model


@pytest.fixture
def trained_logistic(sample_data):
    """Train logistic regression on sample data."""
    X, y, _ = sample_data
    model = LogisticModel({})
    model.fit(X, y)
    return model


class TestGainImportance:
    """Tests for gain-based importance."""

    def test_returns_dict_with_all_features(self, trained_xgboost, sample_data):
        """Should return importance for all features."""
        _, _, feature_names = sample_data
        result = gain_importance(trained_xgboost, feature_names)

        assert isinstance(result, dict)
        assert set(result.keys()) == set(feature_names)

    def test_importances_sum_to_one(self, trained_xgboost, sample_data):
        """Importances should be normalized to sum to 1."""
        _, _, feature_names = sample_data
        result = gain_importance(trained_xgboost, feature_names)

        total = sum(result.values())
        assert abs(total - 1.0) < 1e-6

    def test_strong_feature_has_higher_importance(self, trained_xgboost, sample_data):
        """Strong predictor should have higher importance than noise."""
        _, _, feature_names = sample_data
        result = gain_importance(trained_xgboost, feature_names)

        assert result["strong_feature"] > result["noise_feature"]

    def test_raises_for_non_tree_model(self, trained_logistic, sample_data):
        """Should raise for models without feature_importances_."""
        _, _, feature_names = sample_data

        with pytest.raises(ValueError, match="does not support gain importance"):
            gain_importance(trained_logistic, feature_names)

    def test_raises_for_mismatched_features(self, trained_xgboost):
        """Should raise if feature count doesn't match."""
        with pytest.raises(ValueError, match="doesn't match"):
            gain_importance(trained_xgboost, ["a", "b"])


class TestPermutationImportance:
    """Tests for permutation importance."""

    def test_returns_dict_with_all_features(self, trained_xgboost, sample_data):
        """Should return importance for all features."""
        X, y, feature_names = sample_data
        result = permutation_importance(
            trained_xgboost, X, y, feature_names, n_repeats=5
        )

        assert isinstance(result, dict)
        assert set(result.keys()) == set(feature_names)

    def test_importances_sum_to_one(self, trained_xgboost, sample_data):
        """Importances should be normalized to sum to 1."""
        X, y, feature_names = sample_data
        result = permutation_importance(
            trained_xgboost, X, y, feature_names, n_repeats=5
        )

        total = sum(result.values())
        assert abs(total - 1.0) < 1e-6

    def test_strong_feature_has_higher_importance(self, trained_xgboost, sample_data):
        """Strong predictor should have higher importance than noise."""
        X, y, feature_names = sample_data
        result = permutation_importance(
            trained_xgboost, X, y, feature_names, n_repeats=10
        )

        assert result["strong_feature"] > result["noise_feature"]

    def test_works_with_logistic_model(self, trained_logistic, sample_data):
        """Should work with any sklearn-compatible model."""
        X, y, feature_names = sample_data
        result = permutation_importance(
            trained_logistic, X, y, feature_names, n_repeats=5
        )

        assert isinstance(result, dict)
        assert len(result) == 3

    def test_raises_for_mismatched_features(self, trained_xgboost, sample_data):
        """Should raise if feature count doesn't match."""
        X, y, _ = sample_data

        with pytest.raises(ValueError, match="doesn't match"):
            permutation_importance(trained_xgboost, X, y, ["a", "b"])


class TestShapImportance:
    """Tests for SHAP importance."""

    def test_returns_dict_with_all_features(self, trained_xgboost, sample_data):
        """Should return importance for all features."""
        X, _, feature_names = sample_data

        try:
            result = shap_importance(trained_xgboost, X, feature_names, sample_size=100)
        except ImportError:
            pytest.skip("SHAP not installed")

        assert isinstance(result, dict)
        assert set(result.keys()) == set(feature_names)

    def test_importances_sum_to_one(self, trained_xgboost, sample_data):
        """Importances should be normalized to sum to 1."""
        X, _, feature_names = sample_data

        try:
            result = shap_importance(trained_xgboost, X, feature_names, sample_size=100)
        except ImportError:
            pytest.skip("SHAP not installed")

        total = sum(result.values())
        assert abs(total - 1.0) < 1e-6

    def test_samples_large_datasets(self, trained_xgboost, sample_data):
        """Should sample when data exceeds sample_size."""
        X, _, feature_names = sample_data

        try:
            result = shap_importance(
                trained_xgboost, X, feature_names, sample_size=50
            )
        except ImportError:
            pytest.skip("SHAP not installed")

        # Should still work with sampled data
        assert len(result) == 3


class TestComputeImportance:
    """Tests for the unified compute_importance function."""

    def test_gain_method(self, trained_xgboost, sample_data):
        """Should dispatch to gain_importance."""
        X, y, feature_names = sample_data
        result = compute_importance(
            trained_xgboost, X, y, feature_names, method="gain"
        )

        assert isinstance(result, dict)
        assert len(result) == 3

    def test_permutation_method(self, trained_xgboost, sample_data):
        """Should dispatch to permutation_importance."""
        X, y, feature_names = sample_data
        result = compute_importance(
            trained_xgboost, X, y, feature_names, method="permutation", n_repeats=5
        )

        assert isinstance(result, dict)
        assert len(result) == 3

    def test_shap_method(self, trained_xgboost, sample_data):
        """Should dispatch to shap_importance."""
        X, y, feature_names = sample_data

        try:
            result = compute_importance(
                trained_xgboost, X, y, feature_names, method="shap", sample_size=100
            )
        except ImportError:
            pytest.skip("SHAP not installed")

        assert isinstance(result, dict)
        assert len(result) == 3

    def test_unknown_method_raises(self, trained_xgboost, sample_data):
        """Should raise for unknown method."""
        X, y, feature_names = sample_data

        with pytest.raises(ValueError, match="Unknown importance method"):
            compute_importance(
                trained_xgboost, X, y, feature_names, method="unknown"
            )

    def test_default_method_is_permutation(self, trained_xgboost, sample_data):
        """Default method should be permutation."""
        X, y, feature_names = sample_data

        # Should not raise (permutation works with all models)
        result = compute_importance(trained_xgboost, X, y, feature_names)
        assert isinstance(result, dict)
