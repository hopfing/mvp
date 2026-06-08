"""Tests for Optuna-based hyperparameter tuning."""

import importlib
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from mvp.model.tuning import DEFAULT_SEARCH_SPACES, HIDDEN_LAYERS_MAP, HyperparamTuner, _decode_params, suggest_params


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

    def _conditional_space(self):
        return {
            "tree_method": {
                "type": "categorical", "choices": ["hist", "approx", "exact"],
            },
            "max_bin": {
                "type": "categorical", "choices": [128, 256, 512],
                "condition": {"param": "tree_method", "in": ["hist", "approx"]},
            },
        }

    def test_condition_skips_dependent_when_controller_excludes(self):
        """A conditional param is not suggested when its controller excludes it."""
        trial = MagicMock()
        trial.suggest_categorical.return_value = "exact"

        result = suggest_params(trial, self._conditional_space())

        assert result == {"tree_method": "exact"}  # max_bin skipped
        trial.suggest_categorical.assert_called_once_with(
            "tree_method", ["hist", "approx", "exact"]
        )

    def test_condition_includes_dependent_when_controller_matches(self):
        """A conditional param IS suggested when its controller matches."""
        trial = MagicMock()
        trial.suggest_categorical.return_value = "hist"

        result = suggest_params(trial, self._conditional_space())

        assert "max_bin" in result
        assert trial.suggest_categorical.call_count == 2

    def test_condition_reads_controller_from_fixed(self):
        """A pinned controller (not in the space) still gates the dependent."""
        trial = MagicMock()
        trial.suggest_categorical.return_value = 256
        space = {"max_bin": self._conditional_space()["max_bin"]}

        # Pinned tree_method=hist -> max_bin suggested.
        result = suggest_params(trial, space, fixed={"tree_method": "hist"})
        assert "max_bin" in result

        # Pinned tree_method=exact -> max_bin skipped.
        result2 = suggest_params(MagicMock(), space, fixed={"tree_method": "exact"})
        assert result2 == {}

    def test_default_space_conditions_reference_earlier_controllers(self):
        """Every condition points at a known param that precedes the dependent."""
        for model_type, space in DEFAULT_SEARCH_SPACES.items():
            names = list(space.keys())
            for idx, (name, spec) in enumerate(space.items()):
                cond = spec.get("condition")
                if cond is None:
                    continue
                ctrl = cond["param"]
                assert "in" in cond, f"{model_type}.{name} condition missing 'in'"
                assert ctrl in names, (
                    f"{model_type}.{name} condition references unknown '{ctrl}'"
                )
                assert names.index(ctrl) < idx, (
                    f"{model_type}.{name} controller '{ctrl}' must precede it"
                )


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

    @pytest.mark.parametrize(
        "norm,batch,layer",
        [("none", False, False), ("batch", True, False), ("layer", False, True)],
    )
    def test_normalization_expands_to_booleans(self, norm, batch, layer):
        """normalization choice expands to the two mutually-exclusive booleans."""
        result = _decode_params({"normalization": norm})
        assert result["batch_norm"] is batch
        assert result["layer_norm"] is layer
        assert "normalization" not in result

    def test_normalization_choices_never_enable_both(self):
        """No normalization choice can produce batch_norm and layer_norm both True."""
        for choice in DEFAULT_SEARCH_SPACES["neural_net"]["normalization"]["choices"]:
            result = _decode_params({"normalization": choice})
            assert not (result["batch_norm"] and result["layer_norm"])


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


class TestHyperparamTuner:
    """Tests for the Optuna-based HyperparamTuner."""

    @pytest.fixture(autouse=True)
    def ensure_features_registered(self, isolated_registry):
        """Ensure features are registered before each test."""
        import mvp.model.features.h2h
        import mvp.model.features.ranking
        import mvp.model.features.serve
        import mvp.model.features.win_rate

        importlib.reload(mvp.model.features.h2h)
        importlib.reload(mvp.model.features.ranking)
        importlib.reload(mvp.model.features.serve)
        importlib.reload(mvp.model.features.win_rate)

    @pytest.fixture
    def sample_matches(self, tmp_path: Path) -> Path:
        """Create sample matches parquet file."""
        df = pl.DataFrame(
            {
                "match_uid": [f"M{i}" for i in range(200)],
                "player_id": [f"P{i % 10}" for i in range(200)],
                "opp_id": [f"P{(i + 5) % 10}" for i in range(200)],
                "effective_match_date": [
                    f"2024-01-{(i % 28) + 1:02d}" for i in range(200)
                ],
                "won": [i % 2 == 0 for i in range(200)],
                "player_rankings_points": [1000 - i for i in range(200)],
                "opp_rankings_points": [500 + i for i in range(200)],
                "circuit": ["tour" for _ in range(200)],
            }
        ).with_columns(pl.col("effective_match_date").str.to_datetime())
        path = tmp_path / "matches.parquet"
        df.write_parquet(path)
        return path

    @pytest.fixture
    def sample_config(self, tmp_path: Path) -> Path:
        """Create sample experiment config."""
        config_str = """
name: test_tune
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: logistic
  params:
    C: 1.0
    # Full logistic search space (C + l1_ratio) so the baseline is complete and
    # is enqueued as trial 0. A partial config intentionally skips the baseline.
    l1_ratio: 0.0
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 50
  test_size: 25
"""
        path = tmp_path / "test_tune.yaml"
        path.write_text(config_str)
        return path

    def test_init_creates_study(self, sample_config, sample_matches, tmp_path):
        """Tuner creates an Optuna study on init."""
        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
        )
        assert tuner.study is not None
        assert tuner.study.study_name == "test_tune"

    def test_init_uses_default_search_space(self, sample_config, sample_matches, tmp_path):
        """Tuner uses DEFAULT_SEARCH_SPACES for known model types."""
        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
        )
        assert "C" in tuner.search_space

    def test_init_custom_search_space(self, sample_config, sample_matches, tmp_path):
        """Tuner accepts a custom search space."""
        custom = {"C": {"type": "float", "low": 0.1, "high": 5.0}}
        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            search_space=custom,
        )
        assert tuner.search_space == custom

    def test_param_overrides_pin_values(self, sample_config, sample_matches, tmp_path):
        """--param overrides remove params from search space."""
        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            param_overrides={"C": 1.0},
        )
        # C should be removed from search space since it's pinned
        assert "C" not in tuner.search_space

    def test_run_single_objective(self, sample_config, sample_matches, tmp_path):
        """Tuner runs trials and stores results."""
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())

        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            metrics=["log_loss"],
        )
        tuner.run(n_trials=2)

        assert len(tuner.study.trials) == 2
        # All metrics stored as user attrs
        for trial in tuner.study.trials:
            assert "log_loss" in trial.user_attrs
            assert "calibration_error" in trial.user_attrs

    def test_run_enqueues_baseline(self, sample_config, sample_matches, tmp_path):
        """First trial uses the baseline params from config."""
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())

        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            metrics=["log_loss"],
        )
        tuner.run(n_trials=1)

        first_trial = tuner.study.trials[0]
        assert first_trial.params.get("C") == 1.0

    def test_run_resumes_from_existing_study(self, sample_config, sample_matches, tmp_path):
        """Running twice accumulates trials in the same study."""
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())

        kwargs = dict(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            metrics=["log_loss"],
        )
        tuner1 = HyperparamTuner(**kwargs)
        tuner1.run(n_trials=2)

        tuner2 = HyperparamTuner(**kwargs)
        tuner2.run(n_trials=2)

        assert len(tuner2.study.trials) == 4

    def test_multi_objective(self, sample_config, sample_matches, tmp_path):
        """Tuner supports multi-objective optimization."""
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())

        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            metrics=["log_loss", "calibration_error"],
        )
        tuner.run(n_trials=2)

        assert len(tuner.study.trials) == 2
        # Multi-objective trials have multiple values
        for trial in tuner.study.trials:
            assert len(trial.values) == 2
