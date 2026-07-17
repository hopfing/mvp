"""Tests for Optuna-based hyperparameter tuning."""

import importlib
import logging
from pathlib import Path
from unittest.mock import MagicMock

import optuna
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
metrics:
  objective:
    - log_loss
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

        # outer_folds=1: the synthetic fixture has only 2 folds — this exercises
        # the run wiring, not the methodology fold floor (which gates real runs).
        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            outer_folds=1,
        )
        tuner.run(n_trials=2)

        assert len(tuner.study.trials) == 2
        # All metrics stored as user attrs
        for trial in tuner.study.trials:
            assert "log_loss" in trial.user_attrs
            assert "calibration_error" in trial.user_attrs
            # Raw and deployment-frame (global-Platt) outer-block metrics both land.
            assert "holdout_log_loss" in trial.user_attrs
            assert "holdout_cal_log_loss" in trial.user_attrs

    def test_calibrated_frame_search_end_to_end(self, sample_matches, tmp_path):
        """A prob-scale objective with >=2 tuning folds computes the calibrated
        objective end-to-end: trials are flagged calibrated-frame and carry the
        calibrated in-fold metric (cal_*)."""
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())
        cfg = tmp_path / "cal_e2e.yaml"
        cfg.write_text(
            """
name: cal_e2e
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
    l1_ratio: 0.0
metrics:
  objective:
    - log_loss
validation:
  type: walk_forward
  n_splits: 3
  min_train_size: 50
  test_size: 25
"""
        )
        # 3 folds - 1 holdout = 2 tuning folds → the nested calibrated objective
        # is computable (needs >=2 folds).
        tuner = HyperparamTuner(
            config_path=cfg,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            outer_folds=1,
        )
        assert tuner.search_calibrated is True
        tuner.run(n_trials=1)
        trial = tuner.study.trials[0]
        assert trial.user_attrs.get("_tuning_mode") == "calibrated"
        assert "cal_log_loss" in trial.user_attrs

    def test_calibrated_fallback_warns_once(
        self, sample_config, sample_matches, tmp_path, caplog
    ):
        """A calibrated-frame study that can't compute the calibrated objective
        (here: only 1 tuning fold) warns once and falls back to the raw objective
        (no cal_* attrs), rather than silently mislabeling itself calibrated."""
        import logging
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())
        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            outer_folds=1,  # 2 folds - 1 holdout = 1 tuning fold → can't nest
        )
        assert tuner.search_calibrated is True
        with caplog.at_level(logging.WARNING):
            tuner.run(n_trials=2)
        warned = [r for r in caplog.records if "effectively raw" in r.getMessage()]
        assert len(warned) == 1  # once, not per-trial
        assert "cal_log_loss" not in tuner.study.trials[0].user_attrs

    def test_run_parallel_trials(self, sample_config, sample_matches, tmp_path):
        """parallel_trials=2 runs all trials (1 serial warm-up + fan-out), sets a
        positive per-trial thread split, and does NOT leak the injected n_jobs
        into the recorded search params (so a promoted config keeps its own)."""
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())

        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            outer_folds=1,
        )
        tuner.run(n_trials=3, parallel_trials=2)

        # 1 synchronous warm-up trial + 2 fanned-out = 3 total.
        assert len(tuner.study.trials) == 3
        assert tuner._per_trial_n_jobs is not None and tuner._per_trial_n_jobs >= 1
        # n_jobs is injected only into the transient fit config, never the
        # recorded search params (it isn't a tuned dimension).
        for trial in tuner.study.trials:
            assert "n_jobs" not in trial.params

    def test_run_enqueues_baseline(self, sample_config, sample_matches, tmp_path):
        """First trial uses the baseline params from config."""
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())

        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            outer_folds=1,
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
            outer_folds=1,
        )
        tuner1 = HyperparamTuner(**kwargs)
        tuner1.run(n_trials=2)

        tuner2 = HyperparamTuner(**kwargs)
        tuner2.run(n_trials=2)

        assert len(tuner2.study.trials) == 4

    def test_multi_objective(self, sample_config, sample_matches, tmp_path):
        """Tuner supports multi-objective optimization."""
        import mlflow
        import yaml
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())

        # Multi-objective = a 2-metric objective in the config (ES off). The
        # classification tuner reads metrics.objective, not a `metrics=` arg.
        cfg = yaml.safe_load(sample_config.read_text())
        cfg["metrics"]["objective"] = ["log_loss", "calibration_error"]
        sample_config.write_text(yaml.safe_dump(cfg))

        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            outer_folds=1,
        )
        tuner.run(n_trials=2)

        assert len(tuner.study.trials) == 2
        # Multi-objective trials have multiple values
        for trial in tuner.study.trials:
            assert len(trial.values) == 2

    # --- Forward-aligned objective (v2) ---------------------------------------

    def test_outer_folds_validation(self, sample_config, tmp_path):
        """outer_folds < 1 is rejected at construction."""
        with pytest.raises(ValueError, match="outer_folds must be >= 1"):
            HyperparamTuner(
                config_path=sample_config,
                state_dir=tmp_path / "tuning",
                outer_folds=0,
            )

    def test_outer_folds_and_seed_stored(self, sample_config, tmp_path):
        """New knobs are stored and a fresh study is stamped with the frame."""
        tuner = HyperparamTuner(
            config_path=sample_config,
            state_dir=tmp_path / "tuning",
            outer_folds=2,
            seed=123,
        )
        assert tuner.outer_folds == 2
        assert tuner.seed == 123
        assert tuner.study.user_attrs.get("objective_frame") == "forward_cal_v1"
        assert tuner.study.user_attrs.get("outer_folds") == 2

    def test_run_one_uses_forward_objective(
        self, sample_config, sample_matches, tmp_path, monkeypatch
    ):
        """_run_one builds the classification runner with the forward objective:
        holdout_folds=outer_folds, inner_cv_folds=0 (no within-window inner CV),
        calibrate=False."""
        tuner = HyperparamTuner(
            config_path=sample_config,
            matches_path=sample_matches,
            cache_dir=tmp_path / "cache",
            state_dir=tmp_path / "tuning",
            outer_folds=3,
        )
        captured: dict = {}

        class _FakeRunner:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def run(self, trial=None):
                return {
                    "metrics": {"log_loss": 0.6, "calibration_error": 0.02},
                    "holdout_metrics": {"log_loss": 0.61, "roc_auc": 0.74},
                    "holdout_metrics_calibrated": {"log_loss": 0.60, "roc_auc": 0.74},
                }

        monkeypatch.setattr("mvp.model.runner.ExperimentRunner", _FakeRunner)
        result = tuner._run_one({"C": 1.0})

        assert captured["holdout_folds"] == 3
        assert captured["inner_cv_folds"] == 0
        assert captured["calibrate"] is False
        assert captured["report_calibrated_holdout"] is True
        # log_loss objective is probability-scale → calibrated-frame search.
        assert captured["report_calibrated_objective"] is True
        assert result["metrics"]["log_loss"] == 0.6
        # Deployment-frame outer-block metrics propagate through _run_one.
        assert result["holdout_metrics_calibrated"]["log_loss"] == 0.60

    def test_prob_scale_objective_is_calibrated_frame(self, sample_config, tmp_path):
        """A probability-scale objective (log_loss) searches the calibrated frame."""
        tuner = HyperparamTuner(
            config_path=sample_config, state_dir=tmp_path / "tuning", outer_folds=1
        )
        assert tuner.search_calibrated is True
        assert tuner.study.user_attrs.get("objective_frame") == "forward_cal_v1"

    def test_ranking_objective_stays_raw_frame(self, tmp_path):
        """A pure-ranking objective (roc_auc) is Platt-invariant, so the study
        searches the raw frame and is stamped forward_v2."""
        cfg = tmp_path / "rank.yaml"
        cfg.write_text(
            """
name: rank
data:
  date_range:
    start: "2020-01-01"
    end: "2025-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: logistic
  params:
    C: 1.0
    l1_ratio: 0.0
metrics:
  objective:
    - roc_auc
validation:
  type: date_sliding
  train_months: 12
  test_months: 3
"""
        )
        tuner = HyperparamTuner(
            config_path=cfg, state_dir=tmp_path / "tuning", outer_folds=4
        )
        assert tuner.search_calibrated is False
        assert tuner.study.user_attrs.get("objective_frame") == "forward_v2"

    def test_objective_metric_value_routes_by_frame(self, sample_config, tmp_path):
        """In a calibrated-frame study, prob-scale metrics read metrics_calibrated,
        ranking metrics read raw, and a missing calibrated dict falls back to raw."""
        tuner = HyperparamTuner(
            config_path=sample_config, state_dir=tmp_path / "tuning", outer_folds=1
        )
        assert tuner.search_calibrated is True
        result = {
            "metrics": {"log_loss": 0.62, "roc_auc": 0.74},
            "metrics_calibrated": {"log_loss": 0.60, "roc_auc": 0.99},
        }
        assert tuner._objective_metric_value(result, "log_loss") == 0.60  # calibrated
        assert tuner._objective_metric_value(result, "roc_auc") == 0.74   # raw (invariant)
        result["metrics_calibrated"] = None
        assert tuner._objective_metric_value(result, "log_loss") == 0.62  # raw fallback

    def test_frame_guard_rejects_legacy_study(self, sample_config, tmp_path):
        """A study with prior trials lacking the forward_v2 frame is refused —
        the legacy within-window objective is incomparable to the forward one."""
        state_dir = tmp_path / "tuning"
        state_dir.mkdir(parents=True, exist_ok=True)
        storage = f"sqlite:///{state_dir / 'test_tune.db'}?timeout=30"
        legacy = optuna.create_study(
            study_name="test_tune", storage=storage, directions=["minimize"],
        )
        legacy.add_trial(
            optuna.trial.create_trial(
                params={},
                distributions={},
                value=0.6,
                state=optuna.trial.TrialState.COMPLETE,
            )
        )
        with pytest.raises(ValueError, match="fresh study"):
            HyperparamTuner(
                config_path=sample_config,
                state_dir=state_dir,
                outer_folds=1,
            )

    def _date_sliding_config(self, tmp_path, start, end, name, test_months=3):
        cfg = f"""
name: {name}
data:
  date_range:
    start: "{start}"
    end: "{end}"
features:
  include:
    - player_ranking_points_diff
model:
  type: logistic
  params:
    C: 1.0
    l1_ratio: 0.0
metrics:
  objective:
    - log_loss
validation:
  type: date_sliding
  train_months: 12
  test_months: {test_months}
"""
        path = tmp_path / f"{name}.yaml"
        path.write_text(cfg)
        return path

    def test_preflight_rejects_short_span(self, tmp_path):
        """A date_sliding span too short to leave >=5 inner folds fails fast."""
        cfg = self._date_sliding_config(
            tmp_path, "2024-01-01", "2025-06-30", "short_span"
        )
        with pytest.raises(ValueError, match="forward-aligned tune"):
            HyperparamTuner(
                config_path=cfg,
                state_dir=tmp_path / "tuning",
                outer_folds=4,
            )

    def test_preflight_allows_long_span(self, tmp_path):
        """A long date_sliding span passes preflight and stamps the frame."""
        cfg = self._date_sliding_config(
            tmp_path, "2020-01-01", "2025-12-31", "long_span"
        )
        tuner = HyperparamTuner(
            config_path=cfg,
            state_dir=tmp_path / "tuning",
            outer_folds=4,
        )
        assert tuner.outer_folds == 4
        assert tuner.study.user_attrs.get("objective_frame") == "forward_cal_v1"

    def test_preflight_covers_date_expanding(self, tmp_path):
        """date_expanding configs (the real de_ family) are preflighted too."""
        cfg = tmp_path / "de_short.yaml"
        cfg.write_text(
            """
name: de_short
data:
  date_range:
    start: "2024-01-01"
    end: "2025-06-30"
features:
  include:
    - player_ranking_points_diff
model:
  type: logistic
  params:
    C: 1.0
    l1_ratio: 0.0
metrics:
  objective:
    - log_loss
validation:
  type: date_expanding
  initial_train_months: 12
  test_months: 3
"""
        )
        with pytest.raises(ValueError, match="forward-aligned tune"):
            HyperparamTuner(
                config_path=cfg, state_dir=tmp_path / "tuning", outer_folds=4
            )

    def test_preflight_boundary_passes_with_plus_one(self, tmp_path):
        """A span the splitter yields as exactly outer_folds + MIN inner folds must
        pass. 2021-01..2024-03 = 9 folds; without the +1 fold-count correction the
        estimate is 8 → 4 inner → spurious rejection. Guards the off-by-one."""
        cfg = self._date_sliding_config(
            tmp_path, "2021-01-01", "2024-03-31", "boundary"
        )
        tuner = HyperparamTuner(
            config_path=cfg, state_dir=tmp_path / "tuning", outer_folds=4
        )
        assert tuner.study.user_attrs.get("objective_frame") == "forward_cal_v1"

    def test_preflight_failure_leaves_no_study(self, tmp_path):
        """A construction rejected by preflight must not create/stamp a study, so a
        corrected retry (even with a different outer_folds) isn't falsely rejected
        by the frame guard."""
        short = self._date_sliding_config(
            tmp_path, "2024-01-01", "2025-06-30", "retry"
        )
        with pytest.raises(ValueError, match="forward-aligned tune"):
            HyperparamTuner(
                config_path=short, state_dir=tmp_path / "tuning", outer_folds=4
            )
        # Same config stem (same study db); corrected span + different outer_folds.
        long = self._date_sliding_config(
            tmp_path, "2020-01-01", "2025-12-31", "retry"
        )
        tuner = HyperparamTuner(
            config_path=long, state_dir=tmp_path / "tuning", outer_folds=6
        )
        assert tuner.outer_folds == 6
        assert tuner.study.user_attrs.get("outer_folds") == 6

    def test_preflight_warns_but_allows_thin_span(self, tmp_path, caplog):
        """A 12-month stride leaving 2-4 inner folds warns but proceeds — the
        operator chose the outer_folds split, and a thin forward tune is valid.
        2021-01..2026-11 at test_months=12 = 4 folds; outer_folds=1 → 3 inner."""
        cfg = self._date_sliding_config(
            tmp_path, "2021-01-01", "2026-11-30", "thin_span", test_months=12
        )
        with caplog.at_level(logging.WARNING):
            tuner = HyperparamTuner(
                config_path=cfg, state_dir=tmp_path / "tuning", outer_folds=1
            )
        # Proceeds: study is created and stamped, not rejected.
        assert tuner.study.user_attrs.get("objective_frame") == "forward_cal_v1"
        assert any(
            "trustworthy forward-aligned tune" in r.message and r.levelno == logging.WARNING
            for r in caplog.records
        )

    def test_preflight_blocks_below_two_inner(self, tmp_path):
        """The hard floor is 2 inner folds, not 5: a span leaving exactly 1 inner
        fold still fails fast (the calibrated objective can't be computed below 2).
        2021-01..2024-02 at test_months=12 = 2 folds; outer_folds=1 → 1 inner."""
        cfg = self._date_sliding_config(
            tmp_path, "2021-01-01", "2024-02-28", "one_inner", test_months=12
        )
        with pytest.raises(ValueError, match="at least 2 search folds"):
            HyperparamTuner(
                config_path=cfg, state_dir=tmp_path / "tuning", outer_folds=1
            )
