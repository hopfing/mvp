"""Tests for Optuna-based hyperparameter tuning."""

from unittest.mock import MagicMock

import pytest

from mvp.model.tuning import DEFAULT_SEARCH_SPACES, HIDDEN_LAYERS_MAP, _decode_params, suggest_params


class TestSuggestParams:
    """Tests for the suggest_params helper."""

    def test_suggest_int(self):
        """suggest_params calls trial.suggest_int for int-typed params."""
        trial = MagicMock()
        trial.suggest_int.return_value = 5
        space = {"max_depth": {"type": "int", "low": 3, "high": 8}}

        result = suggest_params(trial, space)

        trial.suggest_int.assert_called_once_with("max_depth", 3, 8)
        assert result == {"max_depth": 5}

    def test_suggest_int_with_step(self):
        """suggest_params passes step to suggest_int when present."""
        trial = MagicMock()
        trial.suggest_int.return_value = 200
        space = {"n_estimators": {"type": "int", "low": 100, "high": 800, "step": 50}}

        result = suggest_params(trial, space)

        trial.suggest_int.assert_called_once_with("n_estimators", 100, 800, step=50)
        assert result == {"n_estimators": 200}

    def test_suggest_float(self):
        """suggest_params calls trial.suggest_float for float-typed params."""
        trial = MagicMock()
        trial.suggest_float.return_value = 0.05
        space = {"learning_rate": {"type": "float", "low": 0.01, "high": 0.3, "log": True}}

        result = suggest_params(trial, space)

        trial.suggest_float.assert_called_once_with("learning_rate", 0.01, 0.3, log=True)
        assert result == {"learning_rate": 0.05}

    def test_suggest_float_no_log(self):
        """suggest_params defaults log=False when not specified."""
        trial = MagicMock()
        trial.suggest_float.return_value = 0.8
        space = {"subsample": {"type": "float", "low": 0.5, "high": 1.0}}

        result = suggest_params(trial, space)

        trial.suggest_float.assert_called_once_with("subsample", 0.5, 1.0, log=False)
        assert result == {"subsample": 0.8}

    def test_suggest_categorical(self):
        """suggest_params calls trial.suggest_categorical for categorical params."""
        trial = MagicMock()
        trial.suggest_categorical.return_value = "gini"
        space = {"criterion": {"type": "categorical", "choices": ["gini", "log_loss"]}}

        result = suggest_params(trial, space)

        trial.suggest_categorical.assert_called_once_with("criterion", ["gini", "log_loss"])
        assert result == {"criterion": "gini"}

    def test_suggest_multiple_params(self):
        """suggest_params handles a mix of param types."""
        trial = MagicMock()
        trial.suggest_int.return_value = 5
        trial.suggest_float.return_value = 0.1
        trial.suggest_categorical.return_value = True
        space = {
            "max_depth": {"type": "int", "low": 3, "high": 8},
            "learning_rate": {"type": "float", "low": 0.01, "high": 0.3, "log": True},
            "bootstrap": {"type": "categorical", "choices": [True, False]},
        }

        result = suggest_params(trial, space)

        assert "max_depth" in result
        assert "learning_rate" in result
        assert "bootstrap" in result

    def test_unknown_type_raises(self):
        """suggest_params raises ValueError for unknown param types."""
        trial = MagicMock()
        space = {"bad_param": {"type": "unknown", "low": 0, "high": 1}}

        with pytest.raises(ValueError, match="Unknown param type"):
            suggest_params(trial, space)


class TestDecodeParams:
    """Tests for _decode_params helper."""

    def test_decodes_hidden_layers_string(self):
        """String hidden_layers gets decoded to list."""
        params = {"hidden_layers": "64-32", "dropout": 0.3}
        result = _decode_params(params)
        assert result["hidden_layers"] == [64, 32]
        assert result["dropout"] == 0.3

    def test_passthrough_when_no_hidden_layers(self):
        """Params without hidden_layers pass through unchanged."""
        params = {"max_depth": 5, "learning_rate": 0.1}
        result = _decode_params(params)
        assert result == params

    def test_hidden_layers_map_covers_all_choices(self):
        """HIDDEN_LAYERS_MAP has an entry for every neural_net choice."""
        nn_choices = DEFAULT_SEARCH_SPACES["neural_net"]["hidden_layers"]["choices"]
        for choice in nn_choices:
            assert choice in HIDDEN_LAYERS_MAP, f"Missing map entry for '{choice}'"


class TestDefaultSearchSpaces:
    """Tests for DEFAULT_SEARCH_SPACES structure."""

    @pytest.mark.parametrize("model_type", ["xgboost", "logistic", "random_forest", "neural_net"])
    def test_all_model_types_present(self, model_type):
        """All expected model types have search spaces defined."""
        assert model_type in DEFAULT_SEARCH_SPACES

    @pytest.mark.parametrize("model_type", list(DEFAULT_SEARCH_SPACES.keys()))
    def test_all_params_have_valid_type(self, model_type):
        """Every param in every search space has a valid type field."""
        for param_name, spec in DEFAULT_SEARCH_SPACES[model_type].items():
            assert "type" in spec, f"{model_type}.{param_name} missing 'type'"
            assert spec["type"] in ("int", "float", "categorical"), (
                f"{model_type}.{param_name} has invalid type: {spec['type']}"
            )

    @pytest.mark.parametrize("model_type", list(DEFAULT_SEARCH_SPACES.keys()))
    def test_int_float_have_bounds(self, model_type):
        """Int and float params have low and high bounds."""
        for param_name, spec in DEFAULT_SEARCH_SPACES[model_type].items():
            if spec["type"] in ("int", "float"):
                assert "low" in spec, f"{model_type}.{param_name} missing 'low'"
                assert "high" in spec, f"{model_type}.{param_name} missing 'high'"

    @pytest.mark.parametrize("model_type", list(DEFAULT_SEARCH_SPACES.keys()))
    def test_categorical_has_choices(self, model_type):
        """Categorical params have a choices list."""
        for param_name, spec in DEFAULT_SEARCH_SPACES[model_type].items():
            if spec["type"] == "categorical":
                assert "choices" in spec, f"{model_type}.{param_name} missing 'choices'"
                assert len(spec["choices"]) >= 2, (
                    f"{model_type}.{param_name} needs at least 2 choices"
                )
