"""restricted_logloss as a tune objective is scored on a FIXED population — the
offset's confident rows — instead of each trial's own cut (findings
2026-08-26 §7d). Helpers in runner.py plus one end-to-end run."""

import importlib
import random
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from mvp.model.metrics import RESTRICTED_LOGLOSS_TAU
from mvp.model.runner import (
    ExperimentRunner,
    _fixed_score_mask,
    _masked_log_loss,
    _pooled_score_mask,
    _with_fixed_rll,
)


class TestHelpers:
    def test_mask_is_the_offsets_confident_rows(self):
        margin = np.array([0.0, 0.5, -0.5, 2.0, -2.0])
        p = 1 / (1 + np.exp(-margin))
        expected = np.abs(p - 0.5) > RESTRICTED_LOGLOSS_TAU
        np.testing.assert_array_equal(_fixed_score_mask(margin), expected)
        assert list(_fixed_score_mask(margin)) == [False, False, False, True, True]

    def test_override_replaces_metric_and_keeps_own_cut(self):
        y = np.array([1, 0, 1, 0, 1, 1])
        p = np.array([0.9, 0.2, 0.55, 0.45, 0.8, 0.3])
        mask = np.array([True, True, False, False, True, True])
        metrics = {"log_loss": 0.5, "restricted_logloss": 0.42}
        out = _with_fixed_rll(metrics, y, p, mask)
        assert out["restricted_logloss"] == pytest.approx(
            _masked_log_loss(y[mask], p[mask])
        )
        assert out["restricted_logloss_own_mask"] == 0.42
        assert out["log_loss"] == 0.5
        assert metrics["restricted_logloss"] == 0.42  # input untouched

    def test_noop_without_mask_or_metric(self):
        y = np.array([1, 0])
        p = np.array([0.7, 0.3])
        m = {"restricted_logloss": 0.4}
        assert _with_fixed_rll(m, y, p, None) is m
        assert _with_fixed_rll({"log_loss": 0.3}, y, p, np.array([True, False])) == {
            "log_loss": 0.3
        }
        assert _with_fixed_rll(None, y, p, np.array([True, False])) is None

    def test_misaligned_mask_raises(self):
        with pytest.raises(ValueError, match="misaligned"):
            _with_fixed_rll(
                {"restricted_logloss": 0.4}, np.array([1, 0, 1]), np.array([0.5] * 3),
                np.array([True, False]),
            )

    def test_empty_population_raises_instead_of_nan(self):
        with pytest.raises(ValueError, match="population is empty"):
            _with_fixed_rll(
                {"restricted_logloss": 0.4}, np.array([1, 0]), np.array([0.5, 0.5]),
                np.array([False, False]),
            )

    def test_pooled_mask_requires_every_fold(self):
        a = {"score_mask": np.array([True, False])}
        b = {"score_mask": np.array([False, True, True])}
        np.testing.assert_array_equal(
            _pooled_score_mask([a, b]), [True, False, False, True, True]
        )
        assert _pooled_score_mask([a, {"score_mask": None}]) is None
        assert _pooled_score_mask([]) is None


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking

    importlib.reload(mvp.model.features.ranking)


@pytest.fixture
def matches(tmp_path: Path) -> Path:
    random.seed(7)
    rows = []
    base = date(2024, 1, 1)
    for i in range(500):
        d = base + timedelta(days=i // 5)
        pr, orank = random.randint(1, 200), random.randint(1, 200)
        won = random.random() < (0.65 if pr < orank else 0.35)
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
name: fixed_rll_test
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
    n_estimators: 20
    max_depth: 3
offset:
  feature: player_ranking_points_diff
metrics:
  objective:
    - restricted_logloss
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
"""


class TestRunnerEndToEnd:
    def _run(self, tmp_path: Path, matches: Path, config: str, **kw) -> dict:
        import mlflow

        cfg = tmp_path / "config.yaml"
        cfg.write_text(config)
        mlflow_dir = tmp_path / "mlruns"
        mlflow.set_tracking_uri(f"file://{mlflow_dir}")
        return ExperimentRunner(
            config_path=cfg, matches_path=matches,
            cache_dir=tmp_path / "cache", mlflow_dir=mlflow_dir, **kw,
        ).run()

    def test_tune_path_frames_and_holdout_use_the_mask(self, matches, tmp_path):
        """The way `mvp tune` runs the runner: calibrate=False, holdout folds,
        calibrated-frame objective and calibrated holdout reporting."""
        r = self._run(
            tmp_path, matches, _CONFIG.replace("n_splits: 2", "n_splits: 3"),
            calibrate=False, holdout_folds=1,
            report_calibrated_objective=True, report_calibrated_holdout=True,
        )
        preds = r["all_predictions"]
        tuning, holdout = preds[:-1], preds[-1:]
        for block, src in (
            (r["metrics"], tuning), (r["holdout_metrics"], holdout),
        ):
            y = np.concatenate([p["y_true"] for p in src])
            prob = np.concatenate([p["y_prob"] for p in src])
            mask = np.concatenate([p["score_mask"] for p in src])
            assert block["restricted_logloss"] == pytest.approx(
                _masked_log_loss(y[mask], prob[mask]), rel=1e-9
            )
            own = block["restricted_logloss_own_mask"]
            assert own != block["restricted_logloss"]
        # Calibrated frames: the value is on Platt-transformed probabilities
        # the test can't cheaply reproduce, but the override must have run.
        for block in (r["metrics_calibrated"], r["holdout_metrics_calibrated"]):
            assert block is not None
            assert "restricted_logloss_own_mask" in block
            assert np.isfinite(block["restricted_logloss"])

    def test_inner_cv_is_refused_with_rll_objective(self, matches, tmp_path):
        with pytest.raises(ValueError, match="inner_cv_folds"):
            self._run(
                tmp_path, matches, _CONFIG.replace("n_splits: 2", "n_splits: 3"),
                calibrate=False, holdout_folds=1, inner_cv_folds=2,
            )

    def test_objective_is_log_loss_on_the_offsets_rows(self, matches, tmp_path):
        r = self._run(tmp_path, matches, _CONFIG)
        preds = r["all_predictions"]
        assert all(p["score_mask"] is not None for p in preds)
        y = np.concatenate([p["y_true"] for p in preds])
        prob = np.concatenate([p["y_prob"] for p in preds])
        mask = np.concatenate([p["score_mask"] for p in preds])
        assert 0 < mask.mean() < 1
        assert r["metrics"]["restricted_logloss"] == pytest.approx(
            _masked_log_loss(y[mask], prob[mask]), rel=1e-9
        )
        assert "restricted_logloss_own_mask" in r["metrics"]
        for p, fm in zip(preds, r["fold_metrics"]):
            m = p["score_mask"]
            assert fm["restricted_logloss"] == pytest.approx(
                _masked_log_loss(p["y_true"][m], p["y_prob"][m]), rel=1e-9
            )

    def test_other_objectives_untouched(self, matches, tmp_path):
        cfg = _CONFIG.replace("- restricted_logloss", "- log_loss")
        r = self._run(tmp_path, matches, cfg)
        assert all(p["score_mask"] is None for p in r["all_predictions"])
        assert "restricted_logloss_own_mask" not in r["metrics"]

    def test_rll_objective_without_offset_is_refused(self, matches, tmp_path):
        offset_block = "offset:\n  feature: player_ranking_points_diff\n"
        no_offset = _CONFIG.replace(offset_block, "")
        with pytest.raises(ValueError, match="fixed scoring population"):
            self._run(tmp_path, matches, no_offset)
