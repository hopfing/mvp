"""Trial selection + config materialization for iid-sweep."""

from __future__ import annotations

from textwrap import dedent

import optuna
import pytest
import yaml

from mvp.model.sweep_select import (
    missing_metric_trials,
    norm_value,
    select_diverse,
    select_top,
)
from mvp.projection.iid import sweep

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
        n_jobs: 4
    validation:
      type: date_expanding
      initial_train_months: 12
      test_months: 6
""")


def _trial(depth: int, lr: float, crps: float):
    return optuna.trial.create_trial(
        params={"max_depth": depth, "learning_rate": lr},
        distributions={
            "max_depth": optuna.distributions.IntDistribution(3, 8),
            "learning_rate": optuna.distributions.FloatDistribution(
                0.01, 0.15, log=True,
            ),
        },
        values=[crps],
        user_attrs={"iid_crps_total_games": crps, "_tuning_mode": "raw"},
    )


@pytest.fixture
def trials():
    return [
        _trial(3, 0.02, 2.91),
        _trial(4, 0.03, 2.90),
        _trial(5, 0.05, 2.88),
        _trial(6, 0.09, 2.93),
        _trial(8, 0.14, 2.95),
    ]


class TestNormValue:
    def test_linear_bounds(self):
        d = optuna.distributions.FloatDistribution(0.0, 10.0)
        assert norm_value(d, 0.0) == 0.0
        assert norm_value(d, 10.0) == 1.0

    def test_log_scale_is_log_aware(self):
        d = optuna.distributions.FloatDistribution(0.01, 1.0, log=True)
        assert norm_value(d, 0.1) == pytest.approx(0.5, abs=1e-9)

    def test_categorical_single_choice_is_zero(self):
        d = optuna.distributions.CategoricalDistribution(["only"])
        assert norm_value(d, "only") == 0.0


class TestSelectDiverse:
    def test_returns_requested_count(self, trials):
        assert len(select_diverse(trials, 3)) == 3

    def test_returns_distinct_trials(self, trials):
        # create_trial() leaves number=-1 until a study assigns one, so identify
        # by params rather than by trial number.
        picked = select_diverse(trials, 3)
        assert len({t.params["max_depth"] for t in picked}) == 3

    def test_deterministic(self, trials):
        a = [t.params for t in select_diverse(trials, 3)]
        b = [t.params for t in select_diverse(trials, 3)]
        assert a == b

    def test_spreads_to_the_extremes(self, trials):
        """Maximin should reach the corners, not clump near the metric's best."""
        depths = {t.params["max_depth"] for t in select_diverse(trials, 3)}
        assert 3 in depths and 8 in depths

    def test_asking_for_more_than_available_returns_all(self, trials):
        assert len(select_diverse(trials, 99)) == len(trials)


class TestSelectTop:
    def test_orders_by_minimize_metric(self, trials):
        picked = select_top(trials, "iid_crps_total_games", 2)
        assert [t.user_attrs["iid_crps_total_games"] for t in picked] == [2.88, 2.90]

    def test_maximize_metric_flips_order(self, trials):
        for t in trials:
            t.user_attrs["roc_auc"] = 1.0 - t.user_attrs["iid_crps_total_games"] / 10
        picked = select_top(trials, "roc_auc", 1)
        assert picked[0].user_attrs["iid_crps_total_games"] == 2.88

    def test_bare_metric_is_used_verbatim(self, trials):
        """IID studies have no holdout block; the key must not be prefixed."""
        assert len(select_top(trials, "iid_crps_total_games", 2)) == 2

    def test_missing_metric_is_counted(self, trials):
        assert missing_metric_trials(trials, "nope") == len(trials)
        assert missing_metric_trials(trials, "iid_crps_total_games") == 0


class TestBuildTrialConfig:
    def test_merges_into_serve_model_params(self, tmp_path, trials):
        base = tmp_path / "cfg.yaml"
        base.write_text(IID_YAML, encoding="utf-8")
        out = sweep.build_trial_config(base, {}, trials[2], joint=False)
        assert out["serve_model"]["params"]["max_depth"] == 5
        assert out["serve_model"]["params"]["learning_rate"] == 0.05

    def test_preserves_params_the_trial_does_not_set(self, tmp_path, trials):
        base = tmp_path / "cfg.yaml"
        base.write_text(IID_YAML, encoding="utf-8")
        out = sweep.build_trial_config(base, {}, trials[2], joint=False)
        assert out["serve_model"]["params"]["n_jobs"] == 4

    def test_pinned_params_win(self, tmp_path, trials):
        base = tmp_path / "cfg.yaml"
        base.write_text(IID_YAML, encoding="utf-8")
        out = sweep.build_trial_config(base, {"max_depth": 7}, trials[2], joint=False)
        assert out["serve_model"]["params"]["max_depth"] == 7

    def test_classification_config_is_rejected(self, tmp_path, trials):
        base = tmp_path / "clf.yaml"
        base.write_text(
            yaml.safe_dump({"model": {"type": "xgboost", "params": {}}}),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="no `serve_model:` block"):
            sweep.build_trial_config(base, {}, trials[0], joint=False)

    def test_result_is_a_loadable_config(self, tmp_path, trials):
        from mvp.projection.iid.config import IIDProjectionConfig

        base = tmp_path / "cfg.yaml"
        base.write_text(IID_YAML, encoding="utf-8")
        out_path = tmp_path / "materialized.yaml"
        out_path.write_text(
            yaml.safe_dump(sweep.build_trial_config(base, {}, trials[2], joint=False)),
            encoding="utf-8",
        )
        cfg = IIDProjectionConfig.from_file(str(out_path))
        assert cfg.serve_model.params["max_depth"] == 5


class TestSelectTrials:
    def test_topn_requires_sort(self, trials):
        with pytest.raises(ValueError, match="requires a sort metric"):
            sweep.select_trials(trials, 2, select="topn")

    def test_diverse_is_the_default(self, trials):
        picked = sweep.select_trials(trials, 2)
        assert len(picked) == 2


class TestMaterialize:
    """End-to-end selection: study on disk -> runnable configs with distinct fps."""

    @pytest.fixture
    def env(self, tmp_path, monkeypatch, trials):
        root = tmp_path / "dataroot"
        (root / "tuning").mkdir(parents=True)
        monkeypatch.setenv("MVP_DATA_ROOT", str(root))

        base = tmp_path / "totals.yaml"
        base.write_text(IID_YAML, encoding="utf-8")

        storage = f"sqlite:///{root / 'tuning' / 'totals.db'}"
        study = optuna.create_study(
            study_name="totals", storage=storage, direction="minimize",
        )
        for t in trials:
            study.add_trial(t)
        return base, root

    def test_writes_one_config_per_trial(self, env, tmp_path):
        base, _root = env
        entries = sweep.materialize(
            str(base), 3, out_dir=tmp_path / "out",
        )
        assert len(entries) == 3
        for e in entries:
            assert e.config_path.exists()

    def test_fingerprints_are_distinct(self, env, tmp_path):
        """The whole point of the sweep: N variants that don't overwrite each other."""
        base, _root = env
        entries = sweep.materialize(str(base), 3, out_dir=tmp_path / "out")
        assert len({e.fp for e in entries}) == 3

    def test_unique_stems_encode_trial_number(self, env, tmp_path):
        base, _root = env
        entries = sweep.materialize(str(base), 2, out_dir=tmp_path / "out")
        assert all(e.unique_stem.startswith("totals__d") for e in entries)
        assert all(f"_t{e.trial_number}" in e.unique_stem for e in entries)

    def test_parent_stem_is_carried_for_grouping(self, env, tmp_path):
        base, _root = env
        entries = sweep.materialize(str(base), 2, out_dir=tmp_path / "out")
        assert {e.parent_stem for e in entries} == {"totals"}

    def test_materialized_configs_differ_in_hyperparameters(self, env, tmp_path):
        base, _root = env
        entries = sweep.materialize(str(base), 3, out_dir=tmp_path / "out")
        depths = {
            yaml.safe_load(e.config_path.read_text())["serve_model"]["params"]["max_depth"]
            for e in entries
        }
        assert len(depths) == 3

    def test_topn_records_the_sort_value(self, env, tmp_path):
        base, _root = env
        entries = sweep.materialize(
            str(base), 2, select="topn", sort="iid_crps_total_games",
            out_dir=tmp_path / "out",
        )
        assert entries[0].sort_value == 2.88
        assert all(e.unique_stem.startswith("totals__h") for e in entries)

    def test_config_without_a_study_is_evaluated_as_is(self, env, tmp_path):
        base, _root = env
        untuned = tmp_path / "untuned.yaml"
        untuned.write_text(IID_YAML, encoding="utf-8")
        entries = sweep.materialize(str(untuned), 5, out_dir=tmp_path / "out")
        assert len(entries) == 1
        assert entries[0].trial_number == -1


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
        n_jobs: 4
    validation:
      type: date_expanding
      initial_train_months: 12
      test_months: 6
""")


class TestSweepServeBlock:
    """A sweep must materialize the block the trials were SEARCHING.

    `mvp tune` writes one study per (config, block) because both blocks suggest
    identical param names. A sweep that hardcodes the config stem opens the
    win-branch study for a config whose `first_in` block was tuned, and then
    writes those params into `params` — reproducing neither trial.
    """

    @pytest.fixture
    def two_level(self, tmp_path):
        p = tmp_path / "tl.yaml"
        p.write_text(TWO_LEVEL_YAML, encoding="utf-8")
        return p

    def test_first_in_trials_land_in_first_in_params(self, two_level, trials):
        out = sweep.build_trial_config(
            two_level, {}, trials[2], "first_in_params", joint=False)
        assert out["serve_model"]["first_in_params"]["max_depth"] == 5
        # The win branches keep what the config had.
        assert out["serve_model"]["params"]["max_depth"] == 3

    def test_params_trials_leave_first_in_alone(self, two_level, trials):
        out = sweep.build_trial_config(two_level, {}, trials[2], "params", joint=False)
        assert out["serve_model"]["params"]["max_depth"] == 5
        assert out["serve_model"]["first_in_params"] == {}

    def test_empty_first_in_block_starts_from_params(self, two_level, trials):
        """`first_in_params: {}` runs on `params` (serve_model.py:140), so the
        unsampled keys must carry over rather than vanish."""
        out = sweep.build_trial_config(
            two_level, {}, trials[2], "first_in_params", joint=False)
        fi = out["serve_model"]["first_in_params"]
        assert fi["n_estimators"] == 100      # inherited, not sampled
        assert fi["max_depth"] == 5           # sampled, overrides the inherited 3

    def test_unknown_block_is_refused(self, two_level, trials):
        with pytest.raises(ValueError, match="serve_block must be None or one of"):
            sweep.build_trial_config(
                two_level, {}, trials[2], "win_first_params", joint=False)

    def test_load_study_uses_the_same_key_the_tuner_wrote(self, tmp_path):
        """One definition of the key, or the sweep opens a study that isn't
        there — or worse, the wrong block's."""
        from mvp.model.tuning import tuning_study_key

        db = tmp_path / "tl.db"
        storage = f"sqlite:///{db}"
        for block in ("params", "first_in_params"):
            s = optuna.create_study(
                study_name=tuning_study_key("tl", block),
                storage=storage, direction="minimize",
            )
            s.add_trial(_trial(3 if block == "params" else 8, 0.05, 1.0))

        _st, _p, params_trials = sweep.load_study("tl", tmp_path, "params")
        _st, _p, fi_trials = sweep.load_study("tl", tmp_path, "first_in_params")
        assert params_trials[0].params["max_depth"] == 3
        assert fi_trials[0].params["max_depth"] == 8

    def test_a_missing_block_names_what_is_present(self, tmp_path):
        db = tmp_path / "tl.db"
        s = optuna.create_study(
            study_name="tl", storage=f"sqlite:///{db}", direction="minimize",
        )
        s.add_trial(_trial(3, 0.05, 1.0))
        with pytest.raises(RuntimeError, match="present: tl"):
            sweep.load_study("tl", tmp_path, "first_in_params")


# A two-level config whose first_in arm has NO features. That arm is
# intercept-only, so `mvp tune` searches it FLAT — one block, bare param names —
# and the study's trials look exactly like a single-level config's.
# `projections/two_level_flat.yaml` is this shape.
TWO_LEVEL_FLAT_YAML = TWO_LEVEL_YAML.replace(
    "      first_in_match_features:\n        - player_glicko_diff\n",
    "      first_in_match_features: []\n",
)


class TestFlatNamespaceTwoLevel:
    """The bug: a flat study read as joint materializes the base config, N times.

    `build_trial_config` re-derived jointness as "two_level and no --serve-block",
    dropping the tuner's third condition (`first_in_is_fitted`). For a config of
    this shape the tuner wrote bare param names and the sweep split them under
    the joint convention, where they matched neither prefix and were discarded.
    Every materialized config came out identical to the base, all hashed to one
    fingerprint, and each run overwrote the last — `iid-rank` showed one row for
    five trials, with no error and nothing in the sweep output to say so.
    """

    @pytest.fixture
    def flat_two_level(self, tmp_path):
        p = tmp_path / "tlf.yaml"
        p.write_text(TWO_LEVEL_FLAT_YAML, encoding="utf-8")
        return p

    def test_bare_trials_reach_params(self, flat_two_level, trials):
        out = sweep.build_trial_config(flat_two_level, {}, trials[2], joint=False)
        assert out["serve_model"]["params"]["max_depth"] == 5
        assert out["serve_model"]["params"]["learning_rate"] == 0.05

    def test_dead_first_in_block_is_left_alone(self, flat_two_level, trials):
        """The joint branch expanded `first_in_params: {}` into a copy of
        `params`. That arm never fits a model, so the block is inert — but it is
        in the fingerprint's optional keys, so writing it split the hash off the
        base config's for no reason."""
        out = sweep.build_trial_config(flat_two_level, {}, trials[2], joint=False)
        assert out["serve_model"]["first_in_params"] == {}

    def test_distinct_trials_produce_distinct_configs(self, flat_two_level, trials):
        """The whole point of a sweep. Under the bug these were byte-identical."""
        rendered = {
            yaml.safe_dump(
                sweep.build_trial_config(flat_two_level, {}, t, joint=False),
                default_flow_style=False,
            )
            for t in trials[:3]
        }
        assert len(rendered) == 3

    def test_reading_a_flat_study_as_joint_is_refused(self, flat_two_level, trials):
        """Belt and braces: if the namespace is ever wrong again, it raises here
        instead of quietly writing the base config."""
        with pytest.raises(ValueError, match="neither the 'win_' nor the 'fi_'"):
            sweep.build_trial_config(flat_two_level, {}, trials[2], joint=True)

    def test_naming_a_block_on_a_joint_study_is_refused(self, flat_two_level, trials):
        with pytest.raises(ValueError, match="contradiction"):
            sweep.build_trial_config(
                flat_two_level, {}, trials[2], "params", joint=True,
            )


class TestFingerprintCollisionWarning:
    def test_colliding_entries_are_named(self, caplog):
        entries = [
            sweep.SweepEntry(
                unique_stem=f"cfg__h0{i}_t{i}", parent_stem="cfg",
                trial_number=i, rank=i, config_path=None, fp=fp,
            )
            for i, fp in enumerate(("aaa", "aaa", "bbb"), 1)
        ]
        with caplog.at_level("WARNING"):
            sweep._warn_fingerprint_collisions(entries)
        assert "2 entries hash to one fingerprint" in caplog.text
        assert "cfg__h01_t1" in caplog.text and "cfg__h02_t2" in caplog.text
        assert "bbb" not in caplog.text

    def test_distinct_fingerprints_are_quiet(self, caplog):
        entries = [
            sweep.SweepEntry(
                unique_stem=f"cfg__h0{i}_t{i}", parent_stem="cfg",
                trial_number=i, rank=i, config_path=None, fp=f"fp{i}",
            )
            for i in (1, 2)
        ]
        with caplog.at_level("WARNING"):
            sweep._warn_fingerprint_collisions(entries)
        assert caplog.text == ""
