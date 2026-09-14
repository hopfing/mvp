"""Offset-free restricted_logloss tuning scores every trial on ONE fixed
population: the baseline model's confident out-of-fold rows, built once per
study, persisted beside the study DB keyed by (match_uid, player_id), and
re-aligned by key on resume."""

import importlib
import random
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from mvp.model.tuning import HyperparamTuner


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking

    importlib.reload(mvp.model.features.ranking)


@pytest.fixture
def matches(tmp_path: Path) -> Path:
    random.seed(11)
    rows = []
    base = date(2024, 1, 1)
    for i in range(400):
        d = base + timedelta(days=i // 4)
        pr, orank = random.randint(1, 200), random.randint(1, 200)
        won = random.random() < (0.7 if pr < orank else 0.3)
        me, other = f"P{i % 20:02d}", f"P{(i + 10) % 20:02d}"
        sides = ((me, other, pr, orank, won), (other, me, orank, pr, not won))
        for pid, oid, a, b, w in sides:
            rows.append({
                "match_uid": f"M{i:04d}",
                "player_id": pid, "opp_id": oid,
                "effective_match_date": d, "won": w,
                "player_rankings_points": 1000 - a * 4,
                "opp_rankings_points": 1000 - b * 4,
                "circuit": "tour",
            })
    path = tmp_path / "matches.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return path


_CONFIG = """
name: rll_mask_tune
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
features:
  include:
    - player_ranking_points_diff
model:
  type: xgboost
  params:
    n_estimators: 10
    max_depth: 3
metrics:
  objective:
    - restricted_logloss
validation:
  type: walk_forward
  n_splits: 3
  min_train_size: 100
  test_size: 50
"""


@pytest.fixture
def config(tmp_path: Path) -> Path:
    path = tmp_path / "rll_mask_tune.yaml"
    path.write_text(_CONFIG)
    return path


def _tuner(config: Path, matches: Path, tmp_path: Path) -> HyperparamTuner:
    import mlflow

    mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())
    return HyperparamTuner(
        config_path=config,
        matches_path=matches,
        cache_dir=tmp_path / "cache",
        state_dir=tmp_path / "tuning",
        outer_folds=1,
        search_space={"max_depth": {"type": "int", "low": 2, "high": 4}},
    )


class TestOffsetFreeFixedPopulation:
    def test_mask_is_built_once_persisted_and_applied(self, config, matches, tmp_path):
        tuner = _tuner(config, matches, tmp_path)
        assert tuner._rll_mask is not None
        assert tuner.rll_mask_path.exists()
        mask = pl.read_parquet(tuner.rll_mask_path)
        assert set(mask.columns) == {"match_uid", "player_id", "fold_idx", "confident"}
        assert 0 < mask["confident"].mean() < 1
        attrs = tuner.study.user_attrs
        assert attrs["rll_mask_rows"] == mask.height
        assert attrs["rll_mask_folds"] == 3
        assert len(attrs["rll_mask_coverage"]) == 3

        tuner.run(n_trials=2)
        for t in tuner.study.trials:
            # Every trial carries the override marker and its own coverage,
            # in both the raw and the calibrated-frame attrs.
            assert "restricted_logloss_own_mask" in t.user_attrs
            assert 0 < t.user_attrs["restricted_logloss_own_coverage"] < 1
            assert "holdout_cal_restricted_logloss_own_mask" in t.user_attrs

    def test_resume_reuses_the_persisted_mask(self, config, matches, tmp_path):
        t1 = _tuner(config, matches, tmp_path)
        t1.run(n_trials=1)
        stamp = t1.rll_mask_path.stat().st_mtime_ns
        t2 = _tuner(config, matches, tmp_path)
        assert t2.rll_mask_path.stat().st_mtime_ns == stamp
        assert t2._rll_mask.equals(t1._rll_mask)
        t2.run(n_trials=1)
        assert len(t2.study.trials) == 2

    def test_resume_without_mask_file_refuses(self, config, matches, tmp_path):
        t1 = _tuner(config, matches, tmp_path)
        t1.run(n_trials=1)
        t1.rll_mask_path.unlink()
        with pytest.raises(ValueError, match="cannot be recovered"):
            _tuner(config, matches, tmp_path)

    def test_fresh_study_rebuilds_over_a_leftover_file(self, config, matches, tmp_path):
        """Deleting the study DB is the documented fresh start; the new study
        must build and stamp its own population, not adopt the old file."""
        import shutil

        t1 = _tuner(config, matches, tmp_path)
        t1.run(n_trials=1)
        # A state dir holding the leftover parquet and no study DB is what
        # "delete the DB" leaves behind (the open SQLite handle keeps the
        # file locked on Windows, so the leftover is staged in a fresh dir).
        fresh = tmp_path / "fresh"
        (fresh / "tuning").mkdir(parents=True)
        leftover_path = fresh / "tuning" / t1.rll_mask_path.name
        shutil.copy(t1.rll_mask_path, leftover_path)
        leftover = leftover_path.stat().st_mtime_ns
        t2 = _tuner(config, matches, fresh)
        assert t2.rll_mask_path == leftover_path
        assert leftover_path.stat().st_mtime_ns != leftover
        assert t2.study.user_attrs["rll_mask_rows"] == t2._rll_mask.height
        assert len(t2.study.trials) == 1  # only the enqueued baseline

    def test_pinned_params_enter_the_reference_model(
        self, config, matches, tmp_path, monkeypatch
    ):
        seen: dict = {}
        real = HyperparamTuner._build_trial_config

        def spy(self, params):
            seen.setdefault("first", dict(params))
            return real(self, params)

        monkeypatch.setattr(HyperparamTuner, "_build_trial_config", spy)
        import mlflow
        mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())
        HyperparamTuner(
            config_path=config, matches_path=matches,
            cache_dir=tmp_path / "cache", state_dir=tmp_path / "tuning",
            outer_folds=1,
            search_space={"max_depth": {"type": "int", "low": 2, "high": 4}},
            param_overrides={"max_depth": 4},
        )
        assert seen["first"]["max_depth"] == 4

    def test_log_loss_objective_builds_no_mask(self, config, matches, tmp_path):
        config.write_text(_CONFIG.replace("- restricted_logloss", "- log_loss"))
        tuner = _tuner(config, matches, tmp_path)
        assert tuner._rll_mask is None
        assert not tuner.rll_mask_path.exists()

    def test_offset_config_builds_no_mask(self, config, matches, tmp_path):
        config.write_text(
            _CONFIG + "offset:\n  feature: player_ranking_points_diff\n"
        )
        tuner = _tuner(config, matches, tmp_path)
        assert tuner._rll_mask is None
        assert not tuner.rll_mask_path.exists()
