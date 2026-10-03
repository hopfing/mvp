"""`tuning.search_space`: per-config adjustments merged over the default
search space (narrow, fix or drop a parameter without editing tuning.py)."""

import pytest

from mvp.model.config import ExperimentConfig
from mvp.model.tuning import DEFAULT_SEARCH_SPACES, apply_search_space_overrides


class TestMerge:
    def test_partial_spec_keeps_type_and_narrows(self):
        space = DEFAULT_SEARCH_SPACES["xgboost"]
        out = apply_search_space_overrides(
            space, {"max_depth": {"low": 2, "high": 4}}
        )
        assert out["max_depth"] == {"type": "int", "low": 2, "high": 4}
        assert out["learning_rate"] == space["learning_rate"]  # untouched
        assert space["max_depth"]["high"] == 11  # defaults not mutated

    def test_categorical_fix_and_drop(self):
        # The IID space keeps the conditional entries this merge must preserve.
        space = DEFAULT_SEARCH_SPACES["xgboost_iid"]
        out = apply_search_space_overrides(
            space, {"grow_policy": {"choices": ["depthwise"]}, "max_leaves": None}
        )
        assert out["grow_policy"]["choices"] == ["depthwise"]
        assert out["grow_policy"]["condition"] == space["grow_policy"]["condition"]
        assert "max_leaves" not in out

    def test_unknown_param_needs_full_spec(self):
        space = DEFAULT_SEARCH_SPACES["xgboost"]
        with pytest.raises(ValueError, match="full spec"):
            apply_search_space_overrides(space, {"booster": {"choices": ["dart"]}})
        out = apply_search_space_overrides(
            space, {"booster": {"type": "categorical", "choices": ["gbtree", "dart"]}}
        )
        assert out["booster"]["type"] == "categorical"

    def test_bad_bounds_fail_early(self):
        space = DEFAULT_SEARCH_SPACES["xgboost"]
        with pytest.raises(ValueError, match="low <= high"):
            apply_search_space_overrides(space, {"max_depth": {"low": 6, "high": 2}})
        with pytest.raises(ValueError, match="needs choices"):
            apply_search_space_overrides(space, {"tree_method": {"choices": []}})

    def test_no_overrides_is_identity(self):
        space = DEFAULT_SEARCH_SPACES["xgboost"]
        assert apply_search_space_overrides(space, None) == space
        assert apply_search_space_overrides(space, {}) == space


class TestDefaultSpaces:
    def test_xgboost_iid_space_is_frozen(self):
        """The IID serve-point models keep the pre-Grinsztajn space."""
        space = DEFAULT_SEARCH_SPACES["xgboost_iid"]
        assert space["max_depth"]["high"] == 8
        assert space["scale_pos_weight"]["low"] < space["scale_pos_weight"]["high"]
        assert space["max_delta_step"]["low"] < space["max_delta_step"]["high"]

    def test_fixed_entries_reach_trial_params(self):
        """Single-value entries are suggested like any other, so the value is
        recorded in the trial's params."""
        import optuna

        from mvp.model.tuning import suggest_params

        study = optuna.create_study()
        trial = study.ask()
        suggest_params(trial, DEFAULT_SEARCH_SPACES["xgboost"])
        assert trial.params["scale_pos_weight"] == 1.0
        assert trial.params["max_delta_step"] == 0
        assert trial.params["max_leaves"] == 0
        assert trial.params["grow_policy"] == "depthwise"


class TestConfig:
    def test_block_parses_and_is_optional(self):
        base = {
            "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
            "features": {"include": ["player_elo_surface_diff"]},
            "model": {"type": "xgboost", "params": {"n_estimators": 5}},
            "target": "won",
        }
        assert ExperimentConfig.model_validate(base).tuning is None
        cfg = ExperimentConfig.model_validate({
            **base,
            "tuning": {"search_space": {"max_depth": {"low": 2, "high": 4}, "max_leaves": None}},
        })
        assert cfg.tuning.search_space["max_depth"] == {"low": 2, "high": 4}
        assert cfg.tuning.search_space["max_leaves"] is None


class TestBaselineDrop:
    def test_config_values_outside_a_narrowed_space_are_dropped(self):
        from mvp.model.tuning import drop_out_of_space

        space = apply_search_space_overrides(
            DEFAULT_SEARCH_SPACES["xgboost"],
            {"max_depth": {"low": 2, "high": 4}, "grow_policy": {"choices": ["depthwise"]}},
        )
        baseline = {"max_depth": 8, "grow_policy": "lossguide", "learning_rate": 0.0144, "tree_method": "hist"}
        out, dropped = drop_out_of_space(baseline, space)
        assert out == {"learning_rate": 0.0144, "tree_method": "hist"}
        assert sorted(dropped) == ["grow_policy", "max_depth"]
