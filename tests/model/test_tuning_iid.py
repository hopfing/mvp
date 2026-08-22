"""Tuner behavior for IID/projection configs (`serve_model:` shape).

These paths were reachable but half-wired: the objective could not be set from
the CLI, `--outer-folds` was accepted and silently ignored, and the baseline
trial was skipped for any config that didn't set every search-space key.
"""

from pathlib import Path
from textwrap import dedent

import optuna
import pytest
import yaml

from mvp.model.tuning import (
    NAMESPACE_FLAT,
    NAMESPACE_JOINT,
    HyperparamTuner,
    _is_iid_config,
    is_joint_two_level,
    split_joint_params,
    study_param_namespace,
)

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


TWO_LEVEL_YAML = dedent("""
    data:
      date_range:
        start: 2023-01-01
        end: 2026-01-01
    features:
      include:
        - player_glicko_diff
    serve_model:
      type: two_level
      model_type: xgboost
      first_in_match_features:
        - player_glicko_diff
      first_in_point_features: []
      first_in_params: {}
      win_first_match_features:
        - player_glicko_diff
      win_first_point_features: []
      win_second_match_features:
        - player_glicko_diff
      win_second_point_features: []
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


@pytest.fixture
def two_level_config(tmp_path) -> Path:
    p = tmp_path / "tl_tune.yaml"
    p.write_text(TWO_LEVEL_YAML, encoding="utf-8")
    return p


@pytest.fixture
def flat_two_level_config(tmp_path) -> Path:
    """Two-level with an INTERCEPT-ONLY first_in arm — `two_level_flat`'s shape.

    Searched flat (one block, bare param names) because that arm never fits a
    model, so `first_in_params` is dead config.
    """
    p = tmp_path / "tl_flat_tune.yaml"
    p.write_text(
        TWO_LEVEL_YAML.replace(
            "  first_in_match_features:\n    - player_glicko_diff",
            "  first_in_match_features: []",
        ),
        encoding="utf-8",
    )
    return p


class TestServeBlockSelection:
    """A two-level model is three fits sharing one `params` block.

    They are not comparable fits — the win branches train on millions of POINT
    rows, `first_in` on tens of thousands of MATCH-grain rows against a
    different target — so one shared setting cannot suit both, and a tune that
    can only write `params` tunes `first_in` by proxy. `ServeModelConfig`
    already splits here; the tuner did not.
    """

    def test_default_writes_params(self, two_level_config, tmp_path):
        """No --serve-block on a searchable two-level config is JOINT, so both
        blocks get written and `serve_block` stays at its single-block default.

        The params must be prefixed here: this config's first_in arm has
        features, so the study that would produce them is joint. Handing a bare
        `max_depth` to a joint tuner is the namespace error `split_joint_params`
        now raises on rather than silently discarding.
        """
        import yaml

        t = _tuner(two_level_config, tmp_path)
        path = t._build_trial_config({"win_max_depth": 5, "fi_max_depth": 4})
        sm = yaml.safe_load(Path(path).read_text(encoding="utf-8"))["serve_model"]
        assert sm["params"]["max_depth"] == 5
        assert sm["first_in_params"]["max_depth"] == 4
        assert t.serve_block == "params"

    def test_first_in_block_writes_its_own_key(self, two_level_config, tmp_path):
        import yaml

        t = _tuner(two_level_config, tmp_path, serve_block="first_in_params")
        path = t._build_trial_config({"max_depth": 5, "n_estimators": 250})
        written = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        sm = written["serve_model"]
        assert sm["first_in_params"]["max_depth"] == 5
        assert sm["first_in_params"]["n_estimators"] == 250
        # The win branches must be left exactly as the config had them.
        assert sm["params"]["max_depth"] == 3
        assert sm["params"]["n_estimators"] == 100

    def test_params_block_leaves_first_in_alone(self, two_level_config, tmp_path):
        import yaml

        t = _tuner(two_level_config, tmp_path, serve_block="params")
        path = t._build_trial_config({"max_depth": 5})
        sm = yaml.safe_load(Path(path).read_text(encoding="utf-8"))["serve_model"]
        assert sm["params"]["max_depth"] == 5
        assert sm["first_in_params"] == {}

    def test_empty_first_in_baseline_inherits_params(self, two_level_config, tmp_path):
        """`first_in_params: {}` means the arm RUNS on `params`
        (serve_model.py:140), so that is its incumbent. Enqueueing an empty
        baseline would make trial 0 a random draw and discard it."""
        t = _tuner(two_level_config, tmp_path, serve_block="first_in_params")
        base = t._get_base_params()
        assert base["learning_rate"] == 0.05
        assert base["max_depth"] == 3

    def test_the_two_blocks_get_separate_studies(self, two_level_config, tmp_path):
        """Both blocks suggest the same param NAMES. Sharing a study under
        `load_if_exists=True` would resume the win-branch trials as first_in
        ones and silently reinterpret every value."""
        a = _tuner(two_level_config, tmp_path, serve_block="params")
        b = _tuner(two_level_config, tmp_path, serve_block="first_in_params")
        assert a.study_key != b.study_key
        assert a.study.study_name != b.study.study_name

    def test_the_joint_study_keeps_the_bare_stem(self, two_level_config, tmp_path):
        t = _tuner(two_level_config, tmp_path)
        assert t.joint_two_level
        assert t.study_key == two_level_config.stem

    def test_every_override_is_suffixed_away_from_the_joint_study(
        self, two_level_config, tmp_path,
    ):
        """Including `params`. The joint study suggests PREFIXED names, so a
        bare-name single-block study sharing the stem would resume one as the
        other and reinterpret every value."""
        joint = _tuner(two_level_config, tmp_path)
        for block in ("params", "first_in_params"):
            t = _tuner(two_level_config, tmp_path, serve_block=block)
            assert t.study_key != joint.study_key, block

    def test_single_level_config_rejects_the_first_in_block(
        self, iid_config, tmp_path,
    ):
        """One fit, one params block. Silently accepting would write a key
        `build_serve_model` never reads for `type: score_state`."""
        with pytest.raises(ValueError, match="requires serve_model.type=two_level"):
            _tuner(iid_config, tmp_path, serve_block="first_in_params")

    def test_unknown_block_is_refused(self, two_level_config, tmp_path):
        with pytest.raises(ValueError, match="serve_block must be one of"):
            _tuner(two_level_config, tmp_path, serve_block="win_first_params")


class TestJointTwoLevelSearch:
    """A two-level config is searched JOINTLY by default.

    The objective exists only on the composed `p`, so a `params` trial is only
    scorable against some `first_in_params` and vice versa — the fits are
    independent, the optima are not. One study samples both.
    """

    def test_two_level_defaults_to_joint(self, two_level_config, tmp_path):
        assert _tuner(two_level_config, tmp_path).joint_two_level

    def test_single_level_does_not(self, iid_config, tmp_path):
        t = _tuner(iid_config, tmp_path)
        assert not t.joint_two_level
        assert not t.is_two_level

    def test_search_space_is_prefixed_for_both_blocks(
        self, two_level_config, tmp_path,
    ):
        space = _tuner(two_level_config, tmp_path).search_space
        assert "win_max_depth" in space
        assert "fi_max_depth" in space
        # No bare names left to collide with a single-block study.
        assert "max_depth" not in space

    def test_conditions_are_prefixed_with_their_controller(
        self, two_level_config, tmp_path,
    ):
        """`condition.param` names a sibling key and is resolved by exact name.
        Prefixing keys without prefixing the reference leaves the conditional
        reading a name that no longer exists — it skips silently, it does not
        raise."""
        space = _tuner(two_level_config, tmp_path).search_space
        for name, spec in space.items():
            cond = spec.get("condition")
            if cond is None:
                continue
            prefix = name.split("_")[0] + "_"
            assert cond["param"].startswith(prefix), (name, cond["param"])
            assert cond["param"] in space, cond["param"]

    def test_first_in_space_drops_the_knobs_it_cannot_use(
        self, two_level_config, tmp_path,
    ):
        """`first_in` is an XGBRegressor on ~6 features. Classifier knobs are
        inert and stacked feature samplers cannot differentiate."""
        space = _tuner(two_level_config, tmp_path).search_space
        for dropped in ("scale_pos_weight", "max_delta_step",
                        "colsample_bylevel", "colsample_bynode"):
            assert f"fi_{dropped}" not in space, dropped
            assert f"win_{dropped}" in space, dropped
        assert space["fi_max_depth"]["high"] == 6
        assert space["win_max_depth"]["high"] == 8
        # The knob the split exists for keeps its full range.
        assert space["fi_min_child_weight"] == space["win_min_child_weight"]

    def test_a_trial_writes_both_blocks(self, two_level_config, tmp_path):
        import yaml

        t = _tuner(two_level_config, tmp_path)
        path = t._build_trial_config({"win_max_depth": 7, "fi_max_depth": 4})
        sm = yaml.safe_load(Path(path).read_text(encoding="utf-8"))["serve_model"]
        assert sm["params"]["max_depth"] == 7
        assert sm["first_in_params"]["max_depth"] == 4
        # Unsampled keys carry over from the config rather than vanishing.
        assert sm["params"]["n_estimators"] == 100
        assert sm["first_in_params"]["n_estimators"] == 100
        # No prefixed key leaks into the written config.
        assert not [k for k in sm["params"] if k.startswith(("win_", "fi_"))]

    def test_baseline_is_prefixed_and_first_in_inherits(
        self, two_level_config, tmp_path,
    ):
        base = _tuner(two_level_config, tmp_path)._get_base_params()
        assert base["win_max_depth"] == 3
        # `first_in_params: {}` RUNS on `params`, so that is its incumbent.
        assert base["fi_max_depth"] == 3
        assert base["fi_n_estimators"] == 100

    def test_classification_is_untouched(self, tmp_path):
        """The joint path must not reach a config with no serve_model."""
        from textwrap import dedent as _d

        p = tmp_path / "clf.yaml"
        p.write_text(_d("""
            data:
              date_range:
                start: 2023-01-01
                end: 2026-01-01
            features:
              include:
                - player_glicko_diff
            model:
              type: logistic
              params:
                C: 1.0
            metrics:
              objective: log_loss
            validation:
              type: date_expanding
              initial_train_months: 12
              test_months: 6
        """), encoding="utf-8")
        t = _tuner(p, tmp_path, outer_folds=1)
        assert not t.is_two_level and not t.joint_two_level
        assert t.study_key == "clf"
        assert "C" in t.search_space


class TestInterceptOnlyFirstIn:
    """A first_in arm with no features never reads `first_in_params`.

    `FirstServeInModel` sets `_model = None` and returns its training base rate
    (two_level_serve_model.py:193-199). Searching that block would spend a third
    of the study's dimensions on values that cannot move the score — against an
    objective that already separates poorly. `projections/two_level_flat.yaml`
    is exactly this config, and it is one of the arms we intend to tune.
    """

    @pytest.fixture
    def flat_config(self, tmp_path) -> Path:
        p = tmp_path / "tl_flat.yaml"
        p.write_text(
            TWO_LEVEL_YAML.replace(
                "  first_in_match_features:\n    - player_glicko_diff",
                "  first_in_match_features: []",
            ),
            encoding="utf-8",
        )
        return p

    def test_not_joint_when_the_arm_is_a_constant(self, flat_config, tmp_path):
        t = _tuner(flat_config, tmp_path)
        assert t.is_two_level
        assert not t.first_in_is_fitted
        assert not t.joint_two_level

    def test_falls_back_to_a_single_bare_block(self, flat_config, tmp_path):
        space = _tuner(flat_config, tmp_path).search_space
        assert "max_depth" in space
        assert not [k for k in space if k.startswith(("win_", "fi_"))]

    def test_a_point_feature_alone_is_enough_to_make_it_joint(
        self, flat_config, tmp_path,
    ):
        """first_in takes match-constant point features (the surface one-hots),
        so an arm with only those is still a fitted model."""
        p = tmp_path / "tl_pt.yaml"
        p.write_text(
            flat_config.read_text(encoding="utf-8").replace(
                "first_in_point_features: []",
                "first_in_point_features:\n    - is_surface_hard",
            ),
            encoding="utf-8",
        )
        t = _tuner(p, tmp_path)
        assert t.first_in_is_fitted
        assert t.joint_two_level

    def test_the_override_refuses_a_block_nothing_reads(
        self, flat_config, tmp_path,
    ):
        with pytest.raises(ValueError, match="intercept-only"):
            _tuner(flat_config, tmp_path, serve_block="first_in_params")

    def test_the_params_override_is_still_allowed(self, flat_config, tmp_path):
        t = _tuner(flat_config, tmp_path, serve_block="params")
        assert t.serve_block == "params"


class TestSplitJointParamsIsStrict:
    """An unprefixed key is proof the caller has the namespace wrong.

    Dropping it was the `iid-sweep` bug: a flat study's bare `max_depth` matched
    neither prefix, both blocks came back empty, and the merge onto the base
    config was a no-op that produced N identical "trial" configs.
    """

    def test_prefixed_keys_split(self):
        win, fi = split_joint_params(
            {"win_max_depth": 7, "fi_max_depth": 4, "win_subsample": 0.8},
        )
        assert win == {"max_depth": 7, "subsample": 0.8}
        assert fi == {"max_depth": 4}

    def test_a_bare_key_raises(self):
        with pytest.raises(ValueError, match="max_depth"):
            split_joint_params({"win_max_depth": 7, "max_depth": 3})

    def test_an_all_bare_dict_raises_rather_than_returning_empties(self):
        with pytest.raises(ValueError, match="neither the 'win_' nor the 'fi_'"):
            split_joint_params({"max_depth": 7, "n_estimators": 150})


class TestJointPredicateIsShared:
    """ONE definition. `iid-sweep` kept a second copy and lost a condition."""

    def test_all_three_conditions(self):
        fitted = {"type": "two_level", "first_in_match_features": ["x"]}
        assert is_joint_two_level(fitted, None)
        # ...and each condition on its own is enough to make it flat.
        assert not is_joint_two_level(fitted, "params")
        assert not is_joint_two_level({"type": "score_state"}, None)
        assert not is_joint_two_level(
            {"type": "two_level", "first_in_match_features": []}, None,
        )

    def test_a_point_feature_alone_makes_it_joint(self):
        assert is_joint_two_level(
            {
                "type": "two_level",
                "first_in_match_features": [],
                "first_in_point_features": ["is_surface_hard"],
            },
            None,
        )

    def test_the_tuner_uses_it(self, two_level_config, flat_two_level_config, tmp_path):
        for path in (two_level_config, flat_two_level_config):
            sm = yaml.safe_load(path.read_text(encoding="utf-8"))["serve_model"]
            assert _tuner(path, tmp_path).joint_two_level == is_joint_two_level(
                sm, None,
            ), path.name


class TestStudyParamNamespace:
    def test_the_stamp_wins(self, two_level_config, tmp_path):
        t = _tuner(two_level_config, tmp_path)
        assert t.study.user_attrs["param_namespace"] == NAMESPACE_JOINT
        assert study_param_namespace(t.study) == NAMESPACE_JOINT

    def test_a_flat_config_stamps_flat(self, flat_two_level_config, tmp_path):
        t = _tuner(flat_two_level_config, tmp_path)
        assert study_param_namespace(t.study) == NAMESPACE_FLAT

    def test_an_unstamped_study_is_read_from_its_trials(self, tmp_path):
        """Studies written before the stamp existed — `two_level_flat.db` is one
        — stay readable without a re-tune."""
        def _study(name, params):
            s = optuna.create_study(
                study_name=name, storage=f"sqlite:///{tmp_path / 'u.db'}",
                direction="minimize",
            )
            s.add_trial(optuna.trial.create_trial(
                params=params,
                distributions={
                    k: optuna.distributions.IntDistribution(3, 8) for k in params
                },
                value=1.0,
            ))
            return s

        assert study_param_namespace(_study("bare", {"max_depth": 5})) == (
            NAMESPACE_FLAT
        )
        assert study_param_namespace(_study("pre", {"win_max_depth": 5})) == (
            NAMESPACE_JOINT
        )


class TestNamespaceFlipIsRefused:
    """Both callers read jointness off the study, but the WRITER derives it from
    the config — so editing the first_in feature lists mid-study would start
    writing a second encoding into one study. Same guard shape as
    `objective_frame`."""

    def test_adding_first_in_features_under_a_flat_study_raises(self, tmp_path):
        cfg = tmp_path / "flip.yaml"
        cfg.write_text(
            TWO_LEVEL_YAML.replace(
                "  first_in_match_features:\n    - player_glicko_diff",
                "  first_in_match_features: []",
            ),
            encoding="utf-8",
        )
        _tuner(cfg, tmp_path)  # stamps 'flat', enqueues the baseline
        cfg.write_text(TWO_LEVEL_YAML, encoding="utf-8")
        with pytest.raises(ValueError, match="param namespace"):
            _tuner(cfg, tmp_path)

    def test_resuming_the_same_shape_is_fine(self, flat_two_level_config, tmp_path):
        _tuner(flat_two_level_config, tmp_path)
        _tuner(flat_two_level_config, tmp_path)
