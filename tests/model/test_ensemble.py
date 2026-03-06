"""Tests for ensemble model support."""

import json

import numpy as np
import pytest
import yaml

from mvp.model.config import (
    EnsembleBaseModelRef,
    EnsembleParams,
    ExperimentConfig,
)
from mvp.model.diagnostics import DiagnosticResults, EnsembleDiagnostics
from mvp.model.models import EnsembleModel, get_model


def _spec(model_type, indices, weight=1.0, params=None):
    """Shorthand for base model spec dicts in tests."""
    return {
        "type": model_type,
        "params": params or {},
        "feature_indices": indices,
        "weight": weight,
    }


@pytest.fixture
def sample_data() -> tuple[np.ndarray, np.ndarray]:
    """Create sample training data with 5 features."""
    np.random.seed(42)
    X = np.random.randn(200, 5)
    y = (X[:, 0] + X[:, 1] > 0).astype(int)
    return X, y


@pytest.fixture
def fitted_ensemble(sample_data):
    """Fitted EnsembleModel with two sub-models."""
    X, y = sample_data
    model = EnsembleModel({"strategy": "weighted_average"})
    model.configure([
        _spec("logistic", [0, 1, 2], weight=0.6),
        _spec("logistic", [0, 3, 4], weight=0.4),
    ])
    model.fit(X, y)
    return model


class TestEnsembleConfig:
    def test_ensemble_config_valid(self):
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
model:
  type: ensemble
  params:
    strategy: weighted_average
    base_models:
      - config: models/logistic_elo.yaml
        weight: 0.6
      - config: models/rf_full.yaml
        weight: 0.4
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.model.type == "ensemble"
        assert config.features is None

    def test_ensemble_params_validation(self):
        params = EnsembleParams(
            strategy="weighted_average",
            base_models=[
                EnsembleBaseModelRef(config="a.yaml", weight=0.6),
                EnsembleBaseModelRef(config="b.yaml", weight=0.4),
            ],
        )
        assert params.strategy == "weighted_average"
        assert len(params.base_models) == 2

    def test_ensemble_config_no_features_ok(self):
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
model:
  type: ensemble
  params:
    strategy: average
    base_models:
      - config: models/a.yaml
"""
        config = ExperimentConfig.from_yaml(yaml_str)
        assert config.features is None

    def test_non_ensemble_config_requires_features(self):
        yaml_str = """
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
model:
  type: logistic
"""
        with pytest.raises(
            ValueError, match="features is required"
        ):
            ExperimentConfig.from_yaml(yaml_str)


class TestEnsembleModel:
    def test_get_ensemble_model(self):
        model = get_model("ensemble", {"strategy": "average"})
        assert isinstance(model, EnsembleModel)

    def test_ensemble_average(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "average"})
        model.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        model.fit(X, y)
        probs = model.predict_proba(X)
        assert probs.shape == (200,)
        assert all(0 <= p <= 1 for p in probs)

        per_model = model.predict_proba_per_model(X)
        expected = np.mean(
            [per_model[0], per_model[1]], axis=0
        )
        np.testing.assert_allclose(probs, expected)

    def test_ensemble_weighted_average(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "weighted_average"})
        model.configure([
            _spec("logistic", [0, 1, 2], weight=3.0),
            _spec("logistic", [0, 3, 4], weight=1.0),
        ])
        model.fit(X, y)
        probs = model.predict_proba(X)

        per_model = model.predict_proba_per_model(X)
        expected = np.average(
            [per_model[0], per_model[1]],
            axis=0,
            weights=[0.75, 0.25],
        )
        np.testing.assert_allclose(probs, expected)

    def test_ensemble_predict_before_fit_raises(self):
        model = EnsembleModel({"strategy": "average"})
        model.configure([_spec("logistic", [0])])
        with pytest.raises(RuntimeError, match="not fitted"):
            model.predict_proba(np.random.randn(5, 3))

    def test_ensemble_configure_before_fit_required(self):
        model = EnsembleModel({"strategy": "average"})
        with pytest.raises(RuntimeError, match="not configured"):
            model.fit(
                np.random.randn(5, 3),
                np.array([0, 1, 0, 1, 0]),
            )

    def test_ensemble_different_feature_subsets(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "average"})
        model.configure([
            _spec("logistic", [0, 1]),
            _spec("logistic", [2, 3, 4]),
        ])
        model.fit(X, y)

        per_model = model.predict_proba_per_model(X)
        assert len(per_model) == 2
        assert per_model[0].shape == (200,)
        assert per_model[1].shape == (200,)

    def test_ensemble_weights_normalize(self, sample_data):
        X, y = sample_data

        model_a = EnsembleModel({"strategy": "weighted_average"})
        model_a.configure([
            _spec("logistic", [0, 1], weight=3.0),
            _spec("logistic", [2, 3], weight=1.0),
        ])
        model_a.fit(X, y)

        model_b = EnsembleModel({"strategy": "weighted_average"})
        model_b.configure([
            _spec("logistic", [0, 1], weight=0.75),
            _spec("logistic", [2, 3], weight=0.25),
        ])
        model_b.fit(X, y)

        np.testing.assert_allclose(
            model_a.predict_proba(X),
            model_b.predict_proba(X),
        )

    def test_ensemble_single_model_equals_base(self, sample_data):
        X, y = sample_data

        solo = get_model("logistic", {})
        solo.fit(X[:, :3], y)
        solo_probs = solo.predict_proba(X[:, :3])

        ensemble = EnsembleModel({"strategy": "average"})
        ensemble.configure([_spec("logistic", [0, 1, 2])])
        ensemble.fit(X, y)
        ensemble_probs = ensemble.predict_proba(X)

        np.testing.assert_allclose(
            solo_probs, ensemble_probs, atol=1e-10
        )

    def test_ensemble_sklearn_wrapper(
        self, fitted_ensemble, sample_data
    ):
        X, _ = sample_data
        wrapper = fitted_ensemble._model
        result = wrapper.predict_proba(X)
        assert result.shape == (200, 2)
        np.testing.assert_allclose(
            result[:, 0] + result[:, 1], 1.0
        )


class TestEnsembleDiagnostics:
    @pytest.fixture
    def ensemble_data(self):
        np.random.seed(42)
        n = 100
        y_true = np.random.randint(0, 2, n)
        model_a = np.clip(
            y_true + np.random.randn(n) * 0.2, 0.05, 0.95
        )
        model_b = np.clip(
            y_true + np.random.randn(n) * 0.3, 0.05, 0.95
        )
        ensemble_prob = (model_a + model_b) / 2
        return y_true, ensemble_prob, [model_a, model_b]

    def test_per_model_metrics(self, ensemble_data):
        y_true, ensemble_prob, per_model = ensemble_data
        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
        )
        metrics = result["per_model_metrics"]
        assert "model_a" in metrics
        assert "model_b" in metrics
        assert "ensemble" in metrics
        assert "accuracy" in metrics["model_a"]

    def test_correlation(self, ensemble_data):
        y_true, ensemble_prob, per_model = ensemble_data
        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
        )
        corr = result["correlation"]
        assert corr["matrix"][0][0] == 1.0
        assert corr["matrix"][1][1] == 1.0
        assert 0 < corr["matrix"][0][1] < 1.0

    def test_agreement_categories(self, ensemble_data):
        y_true, ensemble_prob, per_model = ensemble_data
        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
        )
        agreement = result["agreement"]
        total = (
            agreement["all_agree_correct"]
            + agreement["all_agree_wrong"]
            + agreement["disagree_ensemble_correct"]
            + agreement["disagree_ensemble_wrong"]
        )
        assert total == agreement["total"]

    def test_contribution(self, ensemble_data):
        y_true, ensemble_prob, per_model = ensemble_data
        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
        )
        contrib = result["contribution"]
        assert "model_a" in contrib
        assert "log_loss_delta" in contrib["model_a"]
        assert "calibration_delta" in contrib["model_a"]

    def test_non_ensemble_has_no_ensemble_diagnostics(self):
        results = DiagnosticResults(
            segments={},
            calibration={
                "calibration_error": 0.0,
                "calibration_max_error": 0.0,
            },
            errors={"error_rate_80plus": 0.0},
            temporal={"temporal_drift": 0.0},
        )
        assert results.ensemble is None
        metrics = results.metrics
        assert not any(
            k.startswith("ensemble_") for k in metrics
        )

    def test_ensemble_diagnostics_in_json(self, ensemble_data):
        y_true, ensemble_prob, per_model = ensemble_data
        diag = EnsembleDiagnostics()
        ediag_result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
        )
        results = DiagnosticResults(
            segments={},
            calibration={
                "calibration_error": 0.0,
                "calibration_max_error": 0.0,
            },
            errors={"error_rate_80plus": 0.0},
            temporal={"temporal_drift": 0.0},
            ensemble=ediag_result,
        )
        parsed = json.loads(results.to_json())
        assert "ensemble" in parsed
        assert "per_model_metrics" in parsed["ensemble"]


class TestEnsembleImportance:
    def test_gain_importance_ensemble_fails_gracefully(
        self, fitted_ensemble
    ):
        from mvp.model.discovery.importance import (
            gain_importance,
        )

        with pytest.raises(ValueError, match="ensemble"):
            gain_importance(
                fitted_ensemble,
                ["a", "b", "c", "d", "e"],
            )

    def test_shap_importance_ensemble_fails_gracefully(
        self, fitted_ensemble, sample_data
    ):
        from mvp.model.discovery.importance import (
            shap_importance,
        )

        X, _ = sample_data
        with pytest.raises(ValueError, match="ensemble"):
            shap_importance(
                fitted_ensemble,
                X,
                ["a", "b", "c", "d", "e"],
            )

    def test_permutation_importance_ensemble(
        self, fitted_ensemble, sample_data
    ):
        from mvp.model.discovery.importance import (
            permutation_importance,
        )

        X, y = sample_data
        result = permutation_importance(
            fitted_ensemble,
            X,
            y,
            ["f0", "f1", "f2", "f3", "f4"],
            n_repeats=3,
        )
        assert len(result) == 5
        assert all(v >= 0 for v in result.values())


class TestEnsembleRunnerIntegration:
    def test_resolve_ensemble(self, tmp_path):
        """_resolve_ensemble resolves base model configs."""
        base1 = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
            "features": {"include": ["elo"]},
            "model": {"type": "logistic"},
        }
        base2 = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
            "features": {"include": ["elo", "serve"]},
            "model": {
                "type": "random_forest",
                "params": {"n_estimators": 10},
            },
        }

        base1_path = tmp_path / "base1.yaml"
        base2_path = tmp_path / "base2.yaml"
        with open(base1_path, "w") as f:
            yaml.dump(base1, f)
        with open(base2_path, "w") as f:
            yaml.dump(base2, f)

        ensemble = {
            "data": {
                "date_range": {
                    "start": "2020-01-01",
                    "end": "2025-12-31",
                }
            },
            "model": {
                "type": "ensemble",
                "params": {
                    "strategy": "weighted_average",
                    "base_models": [
                        {
                            "config": str(base1_path),
                            "weight": 0.6,
                        },
                        {
                            "config": str(base2_path),
                            "weight": 0.4,
                        },
                    ],
                },
            },
        }

        ensemble_path = tmp_path / "ensemble.yaml"
        with open(ensemble_path, "w") as f:
            yaml.dump(ensemble, f)

        from mvp.model.runner import ExperimentRunner

        runner = ExperimentRunner.__new__(ExperimentRunner)
        runner.config_path = ensemble_path
        runner.config = ExperimentConfig.from_file(
            str(ensemble_path)
        )

        specs, base_specs = runner._resolve_ensemble()

        assert specs == ["elo", "serve"]
        assert len(base_specs) == 2
        assert base_specs[0]["type"] == "logistic"
        assert base_specs[1]["type"] == "random_forest"
        assert "feature_indices" in base_specs[0]
        assert "feature_indices" in base_specs[1]
