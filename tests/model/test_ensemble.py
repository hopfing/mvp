"""Tests for ensemble model support."""

import json

import numpy as np
import pytest
import yaml

import polars as pl

from mvp.model.config import (
    EnsembleBaseModelRef,
    EnsembleParams,
    ExperimentConfig,
)
from mvp.model.diagnostics import (
    FIXED_CONDITIONS,
    MAGNITUDE_FEATURES,
    SIGNED_BUCKET_FEATURES,
    DiagnosticResults,
    Diagnostics,
    EnsembleDiagnostics,
    _build_conditions,
    _build_correction_breakdowns,
    _build_cross_conditions,
    _resolve_column,
)
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

    def test_consensus_buckets(self, ensemble_data):
        y_true, ensemble_prob, per_model = ensemble_data
        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
        )
        consensus = result["consensus"]
        buckets = consensus["buckets"]
        assert len(buckets) > 0
        # All buckets have required fields
        for b in buckets:
            assert "label" in b
            assert "count" in b
            assert "accuracy" in b
            assert 0 <= b["accuracy"] <= 1
        # Counts sum to total matches
        total = sum(b["count"] for b in buckets)
        assert total == len(y_true)

    def test_consensus_with_three_models(self):
        np.random.seed(42)
        n = 200
        y_true = np.random.randint(0, 2, n)
        # Three models with varying noise
        model_a = np.clip(y_true + np.random.randn(n) * 0.2, 0.05, 0.95)
        model_b = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)
        model_c = np.clip(y_true + np.random.randn(n) * 0.5, 0.05, 0.95)
        per_model = [model_a, model_b, model_c]
        ensemble_prob = np.mean(per_model, axis=0)

        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([1 / 3, 1 / 3, 1 / 3]),
            ["a", "b", "c"],
        )
        buckets = result["consensus"]["buckets"]
        labels = [b["label"] for b in buckets]
        # With 3 models, possible labels are 3-0, 2-1, 1-2
        assert all(l in ["3-0", "2-1", "1-2"] for l in labels)
        # Higher consensus should generally have higher accuracy
        acc_by_label = {b["label"]: b["accuracy"] for b in buckets}
        if "3-0" in acc_by_label and "2-1" in acc_by_label:
            assert acc_by_label["3-0"] >= acc_by_label["2-1"]

    def test_dissenter_with_three_models(self):
        np.random.seed(42)
        n = 500
        y_true = np.random.randint(0, 2, n)
        # model_c is much noisier — will be lone dissenter more often
        model_a = np.clip(y_true + np.random.randn(n) * 0.15, 0.05, 0.95)
        model_b = np.clip(y_true + np.random.randn(n) * 0.15, 0.05, 0.95)
        model_c = np.clip(y_true + np.random.randn(n) * 0.6, 0.05, 0.95)
        per_model = [model_a, model_b, model_c]
        ensemble_prob = np.mean(per_model, axis=0)

        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([1 / 3, 1 / 3, 1 / 3]),
            ["a", "b", "c"],
        )
        dissenter = result["dissenter"]
        assert "a" in dissenter
        assert "b" in dissenter
        assert "c" in dissenter
        # model_c should be lone dissenter more often
        assert dissenter["c"]["count"] > dissenter["a"]["count"]
        # Each entry has required fields
        for d in dissenter.values():
            assert "count" in d
            assert "dissenter_correct" in d
            assert "majority_correct" in d

    def test_dissenter_skipped_with_two_models(self, ensemble_data):
        y_true, ensemble_prob, per_model = ensemble_data
        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            per_model,
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
        )
        # Dissenter analysis requires 3+ models
        assert result["dissenter"] == {}

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

        specs, base_specs, date_ranges = runner._resolve_ensemble()

        assert specs == ["elo", "serve"]
        assert len(base_specs) == 2
        assert base_specs[0]["type"] == "logistic"
        assert base_specs[1]["type"] == "random_forest"
        assert "feature_indices" in base_specs[0]
        assert "feature_indices" in base_specs[1]

    def test_resolve_ensemble_extracts_date_ranges(self, tmp_path):
        """Date ranges from base configs are returned."""
        base1 = {
            "data": {"date_range": {"start": "2010-01-01", "end": "2025-12-31"}},
            "features": {"include": ["elo"]},
            "model": {"type": "logistic"},
        }
        base2 = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "features": {"include": ["elo"]},
            "model": {"type": "logistic"},
        }
        base1_path = tmp_path / "base1.yaml"
        base2_path = tmp_path / "base2.yaml"
        with open(base1_path, "w") as f:
            yaml.dump(base1, f)
        with open(base2_path, "w") as f:
            yaml.dump(base2, f)

        ensemble = {
            "data": {"date_range": {"start": "2020-01-01", "end": "2025-12-31"}},
            "model": {
                "type": "ensemble",
                "params": {
                    "strategy": "average",
                    "base_models": [
                        {"config": str(base1_path)},
                        {"config": str(base2_path)},
                    ],
                },
            },
        }
        ensemble_path = tmp_path / "ensemble.yaml"
        with open(ensemble_path, "w") as f:
            yaml.dump(ensemble, f)

        from datetime import date

        from mvp.model.runner import ExperimentRunner

        runner = ExperimentRunner.__new__(ExperimentRunner)
        runner.config_path = ensemble_path
        runner.config = ExperimentConfig.from_file(str(ensemble_path))

        _, _, date_ranges = runner._resolve_ensemble()

        assert len(date_ranges) == 2
        assert date_ranges[0].start == date(2010, 1, 1)
        assert date_ranges[1].start == date(2020, 1, 1)

    def test_resolve_ensemble_warns_on_filter_mismatch(self, tmp_path):
        """Warning emitted when base config filters differ from ensemble."""
        base = {
            "data": {
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
                "filters": {"draw_type": "doubles"},
            },
            "features": {"include": ["elo"]},
            "model": {"type": "logistic"},
        }
        base_path = tmp_path / "base.yaml"
        with open(base_path, "w") as f:
            yaml.dump(base, f)

        ensemble = {
            "data": {
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
                "filters": {"draw_type": "singles"},
            },
            "model": {
                "type": "ensemble",
                "params": {
                    "strategy": "average",
                    "base_models": [{"config": str(base_path)}],
                },
            },
        }
        ensemble_path = tmp_path / "ensemble.yaml"
        with open(ensemble_path, "w") as f:
            yaml.dump(ensemble, f)

        import warnings

        from mvp.model.runner import ExperimentRunner

        runner = ExperimentRunner.__new__(ExperimentRunner)
        runner.config_path = ensemble_path
        runner.config = ExperimentConfig.from_file(str(ensemble_path))

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner._resolve_ensemble()
            assert len(w) == 1
            assert "filters" in str(w[0].message)

    def test_resolve_ensemble_no_warning_when_filters_match(self, tmp_path):
        """No warning when base and ensemble filters are the same."""
        base = {
            "data": {
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
                "filters": {"draw_type": "singles"},
            },
            "features": {"include": ["elo"]},
            "model": {"type": "logistic"},
        }
        base_path = tmp_path / "base.yaml"
        with open(base_path, "w") as f:
            yaml.dump(base, f)

        ensemble = {
            "data": {
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
                "filters": {"draw_type": "singles"},
            },
            "model": {
                "type": "ensemble",
                "params": {
                    "strategy": "average",
                    "base_models": [{"config": str(base_path)}],
                },
            },
        }
        ensemble_path = tmp_path / "ensemble.yaml"
        with open(ensemble_path, "w") as f:
            yaml.dump(ensemble, f)

        import warnings

        from mvp.model.runner import ExperimentRunner

        runner = ExperimentRunner.__new__(ExperimentRunner)
        runner.config_path = ensemble_path
        runner.config = ExperimentConfig.from_file(str(ensemble_path))

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner._resolve_ensemble()
            filter_warnings = [x for x in w if "filters" in str(x.message)]
            assert len(filter_warnings) == 0


class TestEnsemblePerModelData:
    """Tests for per-model date ranges in ensemble training."""

    def test_per_model_data_used(self, sample_data):
        """Sub-models train on custom data when per_model_data provided."""
        X, y = sample_data
        model = EnsembleModel({"strategy": "average"})
        model.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])

        # Train with default data
        model.fit(X, y)
        probs_default = model.predict_proba(X).copy()

        # Train with per-model data (smaller dataset for model 0)
        model2 = EnsembleModel({"strategy": "average"})
        model2.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        X_small = X[:50]
        y_small = y[:50]
        model2.fit(X, y, per_model_data=[(X_small, y_small), None])
        probs_custom = model2.predict_proba(X)

        # Predictions should differ because model 0 was trained on different data
        assert not np.allclose(probs_default, probs_custom)

    def test_per_model_data_none_unchanged(self, sample_data):
        """per_model_data=None preserves existing behavior."""
        X, y = sample_data

        model1 = EnsembleModel({"strategy": "average"})
        model1.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        model1.fit(X, y)

        model2 = EnsembleModel({"strategy": "average"})
        model2.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        model2.fit(X, y, per_model_data=None)

        np.testing.assert_allclose(
            model1.predict_proba(X),
            model2.predict_proba(X),
        )

    def test_per_model_data_mixed(self, sample_data):
        """Mixed per_model_data: None entries use default X/y."""
        X, y = sample_data

        # All default
        model_all_default = EnsembleModel({"strategy": "average"})
        model_all_default.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        model_all_default.fit(X, y)

        # Only model 1 gets custom data
        model_mixed = EnsembleModel({"strategy": "average"})
        model_mixed.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        X_alt = X[:100]
        y_alt = y[:100]
        model_mixed.fit(X, y, per_model_data=[None, (X_alt, y_alt)])

        probs_default = model_all_default.predict_proba(X)
        probs_mixed = model_mixed.predict_proba(X)

        # Should differ (model 1 trained on different data)
        assert not np.allclose(probs_default, probs_mixed)


class TestStacking:
    def test_stacking_config_rejects_weights(self):
        with pytest.raises(ValueError, match="weight is not allowed"):
            EnsembleParams(
                strategy="stacking",
                base_models=[
                    EnsembleBaseModelRef(config="a.yaml", weight=2.0),
                    EnsembleBaseModelRef(config="b.yaml"),
                ],
            )

    def test_stacking_config_accepts_default_weights(self):
        params = EnsembleParams(
            strategy="stacking",
            base_models=[
                EnsembleBaseModelRef(config="a.yaml"),
                EnsembleBaseModelRef(config="b.yaml"),
            ],
        )
        assert params.strategy == "stacking"

    def test_stacking_fit_meta(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "stacking"})
        model.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        model.fit(X, y)

        per_model = model.predict_proba_per_model(X)
        X_meta = np.column_stack(per_model)
        model.set_meta_feature_names(["model_a", "model_b"])
        model.fit_meta(X_meta, y)
        assert model._meta_model is not None

    def test_stacking_predict_proba_without_fit_meta_raises(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "stacking"})
        model.configure([_spec("logistic", [0, 1, 2])])
        model.fit(X, y)
        with pytest.raises(RuntimeError, match="Meta-model not fitted"):
            model.predict_proba(X)

    def test_stacking_predict_proba_returns_valid(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "stacking"})
        model.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        model.fit(X, y)

        per_model = model.predict_proba_per_model(X)
        X_meta = np.column_stack(per_model)
        model.set_meta_feature_names(["model_a", "model_b"])
        model.fit_meta(X_meta, y)

        probs = model.predict_proba(X)
        assert probs.shape == (200,)
        assert np.all((probs >= 0) & (probs <= 1))

    def test_stacking_get_meta_coefficients(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "stacking"})
        model.configure([
            _spec("logistic", [0, 1, 2]),
            _spec("logistic", [0, 3, 4]),
        ])
        model.fit(X, y)

        per_model = model.predict_proba_per_model(X)
        X_meta = np.column_stack(per_model)
        model.set_meta_feature_names(["model_a", "model_b"])
        model.fit_meta(X_meta, y)

        intercept, coefs = model.get_meta_coefficients()
        assert isinstance(intercept, float)
        assert set(coefs.keys()) == {"model_a", "model_b"}
        assert all(isinstance(v, float) for v in coefs.values())

    def test_stacking_get_meta_coefficients_before_fit_raises(self):
        model = EnsembleModel({"strategy": "stacking"})
        with pytest.raises(RuntimeError, match="Meta-model not fitted"):
            model.get_meta_coefficients()

    def test_stacking_fit_meta_too_few_samples(self, sample_data):
        X, y = sample_data
        model = EnsembleModel({"strategy": "stacking"})
        model.configure([_spec("logistic", [0, 1, 2])])
        model.fit(X, y)
        with pytest.raises(ValueError, match="at least 2"):
            model.fit_meta(np.array([[0.5]]), np.array([1]))

    def test_stacking_diagnostics_include_meta_coefficients(self):
        np.random.seed(42)
        n = 100
        y_true = np.random.randint(0, 2, n)
        model_a = np.clip(y_true + np.random.randn(n) * 0.2, 0.05, 0.95)
        model_b = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)
        ensemble_prob = (model_a + model_b) / 2

        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            [model_a, model_b],
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
            strategy="stacking",
            meta_intercept=-0.5,
            meta_coefficients={"model_a": 1.2, "model_b": 0.8},
        )
        assert result["meta_intercept"] == -0.5
        assert result["meta_coefficients"] == {"model_a": 1.2, "model_b": 0.8}
        assert result["meta_feature_names"] == ["model_a", "model_b"]

    def test_stacking_contribution_refits_meta(self):
        np.random.seed(42)
        n = 200
        y_true = np.random.randint(0, 2, n)
        model_a = np.clip(y_true + np.random.randn(n) * 0.2, 0.05, 0.95)
        model_b = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)
        model_c = np.clip(y_true + np.random.randn(n) * 0.4, 0.05, 0.95)
        ensemble_prob = np.mean([model_a, model_b, model_c], axis=0)

        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            [model_a, model_b, model_c],
            np.array([1 / 3, 1 / 3, 1 / 3]),
            ["a", "b", "c"],
            strategy="stacking",
        )
        contrib = result["contribution"]
        assert "a" in contrib
        assert "b" in contrib
        assert "c" in contrib
        assert "log_loss_delta" in contrib["a"]


class TestErrorConditions:
    def test_error_conditions_with_fixed_and_magnitude(self):
        np.random.seed(42)
        n = 200
        y_true = np.random.randint(0, 2, n)
        y_prob = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)

        df = pl.DataFrame({
            "player_elo_surface_diff": np.random.randn(n) * 100,
            "player_age_diff": np.random.randn(n) * 4,
            "svc_pts_won_pct_matchup_365d": np.random.randn(n) * 0.05,
        })

        diag = Diagnostics()
        result = diag._error_conditions(df, y_true, y_prob)

        assert "conditions" in result
        assert "total_errors" in result
        assert result["total_errors"] >= 0

        labels = [c["label"] for c in result["conditions"]]
        # Fixed conditions
        assert "Large Elo gap (>150)" in labels
        assert "Small Elo gap (<75)" in labels
        # Magnitude conditions (resolved via prefix)
        assert "Svc pts matchup ≥p90" in labels
        assert "Svc pts matchup ≥p95" in labels

        for c in result["conditions"]:
            assert 0.0 <= c["accuracy"] <= 1.0
            assert c["n_matches"] > 0
            assert c["n_errors"] >= 0
            assert 0.0 <= c["error_share"] <= 1.0

    def test_error_conditions_signed_buckets(self):
        np.random.seed(42)
        n = 200
        y_true = np.random.randint(0, 2, n)
        y_prob = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)

        df = pl.DataFrame({
            "player_svc_elo_matchup": np.random.randn(n) * 80,
        })

        diag = Diagnostics()
        result = diag._error_conditions(df, y_true, y_prob)

        labels = [c["label"] for c in result["conditions"]]
        assert "Svc Elo matchup ≤p10" in labels
        assert "Svc Elo matchup p25-p75" in labels
        assert "Svc Elo matchup ≥p90" in labels

    def test_error_conditions_skips_missing_columns(self):
        np.random.seed(42)
        n = 50
        y_true = np.random.randint(0, 2, n)
        y_prob = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)

        df = pl.DataFrame({"some_other_col": np.random.randn(n)})

        diag = Diagnostics()
        result = diag._error_conditions(df, y_true, y_prob)

        assert result["conditions"] == []
        assert result["total_errors"] >= 0

    def test_correction_analysis_breakdowns(self):
        np.random.seed(42)
        n = 500
        y_true = np.random.randint(0, 2, n)
        primary_preds = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)
        ensemble_preds = np.clip(y_true + np.random.randn(n) * 0.2, 0.05, 0.95)

        df = pl.DataFrame({
            "player_elo_surface_diff": np.random.randn(n) * 100,
            "player_age_diff": np.random.randn(n) * 5,
            "is_hard": np.random.choice([0.0, 1.0], n),
            "is_clay": np.random.choice([0.0, 1.0], n),
            "player_svc_elo_matchup": np.random.randn(n) * 50,
            "player_ret_elo_matchup": np.random.randn(n) * 50,
        })

        ediag = EnsembleDiagnostics()
        result = ediag._correction_analysis(y_true, primary_preds, ensemble_preds, df)

        assert "sections" in result
        section_names = [s["section"] for s in result["sections"]]
        assert "By Surface" in section_names
        assert "By Elo Gap" in section_names
        assert "By Age Gap" in section_names
        assert "By Svc Elo Matchup" in section_names
        assert "By Ret Elo Matchup" in section_names

        for section in result["sections"]:
            for r in section["rows"]:
                assert "label" in r
                assert "n_matches" in r
                assert "primary_accuracy" in r
                assert "ensemble_accuracy" in r
                assert abs(r["improvement"] - (r["ensemble_accuracy"] - r["primary_accuracy"])) < 1e-10

    def test_correction_analysis_via_compute(self):
        np.random.seed(42)
        n = 100
        y_true = np.random.randint(0, 2, n)
        model_a = np.clip(y_true + np.random.randn(n) * 0.3, 0.05, 0.95)
        model_b = np.clip(y_true + np.random.randn(n) * 0.2, 0.05, 0.95)
        ensemble_prob = (model_a + model_b) / 2

        df = pl.DataFrame({
            "player_elo_surface_diff": np.random.randn(n) * 100,
            "player_age_diff": np.random.randn(n) * 5,
        })

        diag = EnsembleDiagnostics()
        result = diag.compute(
            y_true,
            ensemble_prob,
            [model_a, model_b],
            np.array([0.5, 0.5]),
            ["model_a", "model_b"],
            combined_df=df,
        )
        assert "correction_analysis" in result
        assert "sections" in result["correction_analysis"]

    def test_resolve_column_exact_match(self):
        df = pl.DataFrame({"player_elo_surface_diff": [1.0]})
        assert _resolve_column(df, "player_elo_surface_diff") == "player_elo_surface_diff"

    def test_resolve_column_parameterized(self):
        df = pl.DataFrame({
            "svc_pts_won_pct_matchup_180d": [1.0],
            "svc_pts_won_pct_matchup_365d": [2.0],
        })
        # Should pick longest horizon
        assert _resolve_column(df, "svc_pts_won_pct_matchup") == "svc_pts_won_pct_matchup_365d"

    def test_resolve_column_missing(self):
        df = pl.DataFrame({"unrelated_col": [1.0]})
        assert _resolve_column(df, "player_elo_surface_diff") is None

    def test_build_conditions_empty_df(self):
        df = pl.DataFrame({"unrelated": [1.0, 2.0]})
        conditions = _build_conditions(df)
        assert conditions == []

    def test_build_cross_conditions_needs_elo(self):
        df = pl.DataFrame({"svc_pts_won_pct_matchup_365d": [1.0, 2.0]})
        # No elo column → no cross conditions
        conditions = _build_cross_conditions(df)
        assert conditions == []

    def test_build_correction_breakdowns(self):
        np.random.seed(42)
        n = 200
        df = pl.DataFrame({
            "is_hard": np.random.choice([0.0, 1.0], n),
            "is_clay": np.random.choice([0.0, 1.0], n),
            "is_grass": np.random.choice([0.0, 1.0], n),
            "player_elo_surface_diff": np.random.randn(n) * 100,
            "player_age_diff": np.random.randn(n) * 5,
            "player_svc_elo_matchup": np.random.randn(n) * 50,
            "player_ret_elo_matchup": np.random.randn(n) * 50,
        })
        sections = _build_correction_breakdowns(df)
        section_names = [s[0] for s in sections]
        assert "By Surface" in section_names
        assert "By Elo Gap" in section_names
        assert "By Age Gap" in section_names
        assert "By Svc Elo Matchup" in section_names
        assert "By Ret Elo Matchup" in section_names
        # Tertile sections should have 3 buckets each
        for name, buckets in sections:
            if name.startswith("By Svc") or name.startswith("By Ret"):
                assert len(buckets) == 3

    def test_build_correction_breakdowns_missing_columns(self):
        df = pl.DataFrame({"unrelated": [1.0, 2.0]})
        sections = _build_correction_breakdowns(df)
        assert sections == []

    def test_fixed_conditions_extensible(self):
        assert isinstance(FIXED_CONDITIONS, list)
        original_len = len(FIXED_CONDITIONS)
        FIXED_CONDITIONS.append(("Test", "test_col", lambda a: a > 0))
        assert len(FIXED_CONDITIONS) == original_len + 1
        FIXED_CONDITIONS.pop()
        assert len(FIXED_CONDITIONS) == original_len
