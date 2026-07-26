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
        out = sweep.build_trial_config(base, {}, trials[2])
        assert out["serve_model"]["params"]["max_depth"] == 5
        assert out["serve_model"]["params"]["learning_rate"] == 0.05

    def test_preserves_params_the_trial_does_not_set(self, tmp_path, trials):
        base = tmp_path / "cfg.yaml"
        base.write_text(IID_YAML, encoding="utf-8")
        out = sweep.build_trial_config(base, {}, trials[2])
        assert out["serve_model"]["params"]["n_jobs"] == 4

    def test_pinned_params_win(self, tmp_path, trials):
        base = tmp_path / "cfg.yaml"
        base.write_text(IID_YAML, encoding="utf-8")
        out = sweep.build_trial_config(base, {"max_depth": 7}, trials[2])
        assert out["serve_model"]["params"]["max_depth"] == 7

    def test_classification_config_is_rejected(self, tmp_path, trials):
        base = tmp_path / "clf.yaml"
        base.write_text(
            yaml.safe_dump({"model": {"type": "xgboost", "params": {}}}),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="no `serve_model:` block"):
            sweep.build_trial_config(base, {}, trials[0])

    def test_result_is_a_loadable_config(self, tmp_path, trials):
        from mvp.projection.iid.config import IIDProjectionConfig

        base = tmp_path / "cfg.yaml"
        base.write_text(IID_YAML, encoding="utf-8")
        out_path = tmp_path / "materialized.yaml"
        out_path.write_text(
            yaml.safe_dump(sweep.build_trial_config(base, {}, trials[2])),
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
