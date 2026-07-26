"""Tuner behavior for IID/projection configs (`serve_model:` shape).

These paths were reachable but half-wired: the objective could not be set from
the CLI, `--outer-folds` was accepted and silently ignored, and the baseline
trial was skipped for any config that didn't set every search-space key.
"""

from pathlib import Path
from textwrap import dedent

import pytest

from mvp.model.tuning import HyperparamTuner, _is_iid_config

IID_YAML = dedent("""
    data:
      date_range:
        start: 2023-01-01
        end: 2026-01-01
    features:
      include:
        - player_glicko_diff
    serve_model:
      type: score_state
      model_type: xgboost
      match_level_features:
        - player_glicko_diff
      point_level_features:
        - sets_won_asymmetry
      params:
        n_estimators: 100
        max_depth: 3
        learning_rate: 0.05
    metrics:
      objective: iid_crps_total_games
    validation:
      type: date_expanding
      initial_train_months: 12
      test_months: 6
""")

NO_OBJECTIVE_YAML = IID_YAML.replace(
    "metrics:\n  objective: iid_crps_total_games\n", "",
)


@pytest.fixture
def iid_config(tmp_path) -> Path:
    p = tmp_path / "iid_tune.yaml"
    p.write_text(IID_YAML, encoding="utf-8")
    return p


def _tuner(config_path, tmp_path, **kw):
    return HyperparamTuner(
        config_path=config_path,
        cache_dir=tmp_path / "cache",
        state_dir=tmp_path / "tuning",
        **kw,
    )


class TestIIDDetection:
    def test_serve_model_config_is_iid(self):
        assert _is_iid_config({"serve_model": {"type": "score_state"}})

    def test_classification_config_is_not(self):
        assert not _is_iid_config({"model": {"type": "xgboost"}})


class TestObjective:
    def test_read_from_the_config(self, iid_config, tmp_path):
        assert _tuner(iid_config, tmp_path).metrics == ["iid_crps_total_games"]

    def test_missing_objective_is_a_hard_error(self, tmp_path):
        """No silent `mae` fallback: that optimized a point-estimate metric on
        configs whose features were selected against a distributional one."""
        p = tmp_path / "no_obj.yaml"
        p.write_text(NO_OBJECTIVE_YAML, encoding="utf-8")
        with pytest.raises(ValueError, match="requires metrics.objective"):
            _tuner(p, tmp_path)

    def test_error_names_a_valid_iid_metric(self, tmp_path):
        p = tmp_path / "no_obj.yaml"
        p.write_text(NO_OBJECTIVE_YAML, encoding="utf-8")
        with pytest.raises(ValueError, match="iid_crps_total_games"):
            _tuner(p, tmp_path)

    def test_crps_study_minimizes(self, iid_config, tmp_path):
        tuner = _tuner(iid_config, tmp_path)
        assert [d.name for d in tuner.study.directions] == ["MINIMIZE"]

    def test_multi_objective_builds_a_pareto_study(self, tmp_path):
        p = tmp_path / "multi.yaml"
        p.write_text(
            IID_YAML.replace(
                "objective: iid_crps_total_games",
                "objective:\n    - iid_crps_total_games\n    - iid_total_cal",
            ),
            encoding="utf-8",
        )
        tuner = _tuner(p, tmp_path)
        assert tuner.metrics == ["iid_crps_total_games", "iid_total_cal"]
        assert len(tuner.study.directions) == 2

    def test_search_space_comes_from_serve_model_model_type(self, iid_config, tmp_path):
        tuner = _tuner(iid_config, tmp_path)
        assert tuner.model_type == "xgboost"
        assert "max_depth" in tuner.search_space

    def test_iid_never_searches_the_calibrated_frame(self, iid_config, tmp_path):
        """There is no Platt path for IID; a calibrated-frame study would be a lie."""
        assert _tuner(iid_config, tmp_path).search_calibrated is False


class TestOuterFolds:
    def test_explicit_outer_folds_is_rejected(self, iid_config, tmp_path):
        """It was accepted and then never passed to the runner — no outer block
        exists for IID, so the flag implied a holdout that isn't there."""
        with pytest.raises(ValueError, match="not supported for IID"):
            _tuner(iid_config, tmp_path, outer_folds=4)

    def test_omitting_it_is_fine(self, iid_config, tmp_path):
        assert _tuner(iid_config, tmp_path).outer_folds >= 1


class TestBaselineEnqueue:
    def test_partial_baseline_is_still_enqueued(self, iid_config, tmp_path):
        """The config sets 3 of ~17 search-space keys. Requiring all of them meant
        trial 0 was a random draw instead of the config's own hyperparameters."""
        tuner = _tuner(iid_config, tmp_path)
        base = tuner._get_base_params()
        assert set(base) < set(tuner.search_space)  # strict subset

        tuner._enqueue_baseline()
        queued = [
            t for t in tuner.study.get_trials(deepcopy=False)
            if t.state.name == "WAITING"
        ]
        assert len(queued) == 1
        fixed = queued[0].system_attrs.get("fixed_params", {})
        assert fixed["max_depth"] == 3
        assert fixed["n_estimators"] == 100
        assert "gamma" not in fixed  # unset keys are left to the sampler

    def test_base_params_read_from_serve_model(self, iid_config, tmp_path):
        params = _tuner(iid_config, tmp_path)._get_base_params()
        assert params["learning_rate"] == 0.05
