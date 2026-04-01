"""Tests for model wrappers."""

import numpy as np
import pytest

from mvp.model.models import get_model


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

    def test_get_random_forest_model(self):
        """Get random forest model wrapper."""
        model = get_model("random_forest", {"n_estimators": 10})
        assert model is not None
        assert hasattr(model, "fit")
        assert hasattr(model, "predict_proba")

    def test_get_neural_net_model(self):
        """Get neural net model wrapper."""
        model = get_model("neural_net", {"hidden_layers": [32, 16]})
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

    def test_random_forest_fit_predict(self, sample_data):
        """Random forest model can fit and predict."""
        X, y = sample_data
        model = get_model("random_forest", {"n_estimators": 10})
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

    def test_random_forest_predict_before_fit_raises(self):
        """Calling predict_proba before fit raises RuntimeError."""
        model = get_model("random_forest", {})
        with pytest.raises(RuntimeError, match="Model not fitted"):
            model.predict_proba(np.random.randn(5, 3))

    def test_neural_net_fit_predict(self, sample_data):
        """Neural net model can fit and predict."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [32, 16],
            "epochs": 20,
            "patience": 5,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)

        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_predict_before_fit_raises(self):
        """Calling predict_proba before fit raises RuntimeError."""
        model = get_model("neural_net", {})
        with pytest.raises(RuntimeError, match="Model not fitted"):
            model.predict_proba(np.random.randn(5, 3))

    def test_neural_net_with_sample_weight(self, sample_data):
        """Neural net model accepts sample weights."""
        X, y = sample_data
        weights = np.ones(len(y))
        weights[:50] = 2.0
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
        })
        model.fit(X, y, sample_weight=weights)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_serialization(self, sample_data, tmp_path):
        """Neural net model survives joblib round-trip."""
        import joblib

        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
        })
        model.fit(X, y)
        probs_before = model.predict_proba(X)

        path = tmp_path / "model.pkl"
        joblib.dump(model, path)
        loaded = joblib.load(path)
        probs_after = loaded.predict_proba(X)

        np.testing.assert_array_almost_equal(probs_before, probs_after)

    def test_neural_net_with_embeddings(self, sample_data):
        """Neural net with player embeddings can fit and predict."""
        X, y = sample_data  # X is (100, 5)
        player_ids = np.random.randint(0, 20, size=(100, 1))
        X_with_ids = np.hstack([X, player_ids])

        model = get_model("neural_net", {
            "hidden_layers": [32, 16],
            "epochs": 20,
            "patience": 5,
            "embedding_dim": 8,
            "embedding_col_idx": 5,
            "n_players": 20,
        })
        model.fit(X_with_ids, y)
        probs = model.predict_proba(X_with_ids)

        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)
        # Verify embedding layer exists in the module
        assert model.embedding_dim == 8
        assert hasattr(model._module, "embedding")

    def test_neural_net_embedding_serialization(self, sample_data, tmp_path):
        """Neural net with embeddings survives joblib round-trip."""
        import joblib

        X, y = sample_data
        player_ids = np.random.randint(0, 20, size=(100, 1))
        X_with_ids = np.hstack([X, player_ids])

        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "embedding_dim": 8,
            "embedding_col_idx": 5,
            "n_players": 20,
        })
        model.fit(X_with_ids, y)
        probs_before = model.predict_proba(X_with_ids)

        path = tmp_path / "emb_model.pkl"
        joblib.dump(model, path)
        loaded = joblib.load(path)
        probs_after = loaded.predict_proba(X_with_ids)

        np.testing.assert_array_almost_equal(probs_before, probs_after)

    def test_neural_net_no_embedding_unchanged(self, sample_data):
        """Neural net without embedding params still works as plain MLP."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)

    # --- Training enhancement tests ---

    def test_neural_net_label_smoothing(self, sample_data):
        """Neural net with label smoothing can fit and predict."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "label_smoothing": 0.05,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_weight_decay(self, sample_data):
        """Neural net with weight decay uses AdamW."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "weight_decay": 0.001,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_lr_scheduler(self, sample_data):
        """Neural net with ReduceLROnPlateau can fit and predict."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "lr_scheduler": "plateau",
            "lr_scheduler_factor": 0.5,
            "lr_scheduler_patience": 3,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_grad_clipping(self, sample_data):
        """Neural net with gradient clipping can fit and predict."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "grad_clip_norm": 1.0,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_layer_norm(self, sample_data):
        """Neural net with layer normalization can fit and predict."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "layer_norm": True,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_batch_and_layer_norm_raises(self):
        """Setting both batch_norm and layer_norm raises ValueError."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            get_model("neural_net", {
                "batch_norm": True,
                "layer_norm": True,
            })

    def test_neural_net_all_enhancements_combined(self, sample_data):
        """Neural net with all enhancements enabled can fit and predict."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [32, 16],
            "epochs": 15,
            "patience": 5,
            "label_smoothing": 0.05,
            "weight_decay": 0.001,
            "lr_scheduler": "plateau",
            "lr_scheduler_patience": 3,
            "grad_clip_norm": 1.0,
            "layer_norm": True,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_enhanced_serialization(self, sample_data, tmp_path):
        """Neural net with new params survives joblib round-trip."""
        import joblib

        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "label_smoothing": 0.05,
            "weight_decay": 0.001,
            "layer_norm": True,
            "grad_clip_norm": 1.0,
        })
        model.fit(X, y)
        probs_before = model.predict_proba(X)

        path = tmp_path / "enhanced_model.pkl"
        joblib.dump(model, path)
        loaded = joblib.load(path)
        probs_after = loaded.predict_proba(X)

        np.testing.assert_array_almost_equal(probs_before, probs_after)

    def test_neural_net_finetune_with_enhancements(self, sample_data):
        """Fine-tuning phase also uses enhancements."""
        X, y = sample_data
        model = get_model("neural_net", {
            "hidden_layers": [16],
            "epochs": 10,
            "patience": 5,
            "finetune_frac": 0.3,
            "finetune_epochs": 5,
            "finetune_patience": 3,
            "label_smoothing": 0.05,
            "weight_decay": 0.001,
            "lr_scheduler": "plateau",
            "grad_clip_norm": 1.0,
        })
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)

    def test_neural_net_defaults_backward_compatible(self):
        """New params all default to no-op values."""
        model = get_model("neural_net", {})
        assert model.label_smoothing == 0.0
        assert model.lr_scheduler is None
        assert model.lr_scheduler_factor == 0.5
        assert model.lr_scheduler_patience == 5
        assert model.weight_decay == 0.0
        assert model.grad_clip_norm is None
        assert model.layer_norm is False

    def test_neural_net_dual_embedding_fit_predict(self, sample_data):
        """Neural net with dual player+opponent embeddings can fit and predict."""
        X, y = sample_data  # X is (100, 5)
        player_ids = np.random.randint(0, 20, size=(100, 1))
        opp_ids = np.random.randint(0, 20, size=(100, 1))
        X_with_ids = np.hstack([X, player_ids, opp_ids])

        model = get_model("neural_net", {
            "hidden_layers": [32, 16],
            "epochs": 20,
            "patience": 5,
            "embedding_dim": 8,
            "embedding_col_idx": 5,
            "opp_embedding_col_idx": 6,
            "n_players": 20,
        })
        model.fit(X_with_ids, y)
        probs = model.predict_proba(X_with_ids)

        assert probs.shape == (100,)
        assert all(0 <= p <= 1 for p in probs)
        assert hasattr(model._module, "embedding")

    def test_neural_net_opp_embedding_without_player_raises(self):
        """Setting opp_embedding_col_idx without player embedding raises ValueError."""
        with pytest.raises(ValueError, match="opp_embedding_col_idx requires"):
            get_model("neural_net", {
                "opp_embedding_col_idx": 6,
            })
