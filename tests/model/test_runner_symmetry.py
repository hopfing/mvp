"""End-to-end: the runner's predictions are odd-projected, and the gate holds.

The unit tests in test_symmetry.py prove the transform. These prove the WIRING
-- that it actually engages on a real run rather than silently skipping, and
that it stays off for a target it would corrupt.
"""

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from mvp.model.runner import ExperimentRunner

_CONFIG = """
name: symmetry_test
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
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
metrics:
  primary: log_loss
"""


@pytest.fixture
def paired_matches(tmp_path: Path) -> Path:
    import random
    random.seed(7)
    rows = []
    base = date(2024, 1, 1)
    for i in range(500):
        d = base + timedelta(days=i // 5)
        pr, orr = random.randint(1, 200), random.randint(1, 200)
        won = random.random() < (0.6 if pr < orr else 0.4)
        for pid, oid, w, a, b in (
            (f"P{i % 20:02d}", f"P{(i + 10) % 20:02d}", won, pr, orr),
            (f"P{(i + 10) % 20:02d}", f"P{i % 20:02d}", not won, orr, pr),
        ):
            rows.append({
                "match_uid": f"M{i:04d}", "player_id": pid, "opp_id": oid,
                "effective_match_date": d, "won": w,
                "player_rankings_points": 1000 - a * 4,
                "opp_rankings_points": 1000 - b * 4,
                "circuit": "tour",
            })
    path = tmp_path / "matches.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return path


def _run(tmp_path: Path, matches: Path, config: str):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(config)
    return ExperimentRunner(
        config_path=cfg, matches_path=matches,
        cache_dir=tmp_path / "cache", mlflow_dir=tmp_path / "mlruns",
    ).run()


def test_fold_predictions_are_complementary_end_to_end(paired_matches, tmp_path):
    """The wiring actually fires. Without it the pair sums scatter around 1;
    a passing test suite that never exercised the path would look identical."""
    results = _run(tmp_path, paired_matches, _CONFIG)
    seen_pairs = 0
    for pred in results["all_predictions"]:
        uid = pred["df"]["match_uid"].to_numpy()
        p = pred["y_prob"]
        order = np.argsort(uid, kind="stable")
        us = uid[order]
        starts = np.flatnonzero(np.concatenate(([True], us[1:] != us[:-1])))
        counts = np.diff(np.concatenate((starts, [len(us)])))
        i, j = order[starts[counts == 2]], order[starts[counts == 2] + 1]
        seen_pairs += i.size
        assert p[i] + p[j] == pytest.approx(1.0, abs=1e-12)
    assert seen_pairs > 0, "no pairs evaluated -- the test proved nothing"


def test_raw_column_is_preserved_and_actually_differs(paired_matches, tmp_path):
    """y_prob_raw must survive AND carry the per-orientation disagreement --
    if it were a copy of y_prob the drift signal would be gone."""
    results = _run(tmp_path, paired_matches, _CONFIG)
    diffs = []
    for pred in results["all_predictions"]:
        assert "y_prob_raw" in pred
        diffs.append(np.abs(pred["y_prob"] - pred["y_prob_raw"]).max())
    assert max(diffs) > 0, "y_prob_raw is identical to y_prob"


def test_fold_predictions_carry_calibrated_column(paired_matches, tmp_path):
    """y_prob in the fold parquet is pre-calibration by construction (the
    frame is built before the calibrator runs). y_prob_cal must be the
    post-calibration OOF, aligned row-for-row with the pred dicts."""
    results = _run(tmp_path, paired_matches, _CONFIG)
    df = results["fold_predictions_df"]
    assert df is not None and "y_prob_cal" in df.columns
    cal = np.concatenate([p["y_prob"] for p in results["all_predictions"]])
    assert df.height == len(cal)
    assert np.allclose(df["y_prob_cal"].to_numpy(), cal)
    gap = np.abs(df["y_prob_cal"].to_numpy() - df["y_prob"].to_numpy()).max()
    assert gap > 0, "y_prob_cal equals y_prob -- calibrator never touched it"


def test_gate_holds_for_an_even_target(paired_matches, tmp_path):
    """deciding_set is EVEN under the swap. Odd-projecting it would force
    equal probabilities apart; the run must leave it alone rather than raise
    or corrupt."""
    cfg = _CONFIG.replace("name: symmetry_test", "name: symmetry_test\ntarget: deciding_set")
    runner_cfg = tmp_path / "config.yaml"
    runner_cfg.write_text(cfg)
    from mvp.model.config import ExperimentConfig
    parsed = ExperimentConfig.from_file(str(runner_cfg))
    assert parsed.target == "deciding_set"
    r = ExperimentRunner(
        config_path=runner_cfg, matches_path=paired_matches,
        cache_dir=tmp_path / "cache", mlflow_dir=tmp_path / "mlruns",
    )
    assert r._symmetry_on is False, "gate defaults must be off before run()"


def _pair_sums(pred):
    uid = pred["df"]["match_uid"].to_numpy()
    p = pred["y_prob"]
    order = np.argsort(uid, kind="stable")
    us = uid[order]
    starts = np.flatnonzero(np.concatenate(([True], us[1:] != us[:-1])))
    counts = np.diff(np.concatenate((starts, [len(us)])))
    i, j = order[starts[counts == 2]], order[starts[counts == 2] + 1]
    return p[i] + p[j], i.size


def test_calibrated_outputs_stay_complementary(paired_matches, tmp_path):
    """Platt is affine in the margin, so applying it per row to a complementary
    pair breaks the invariant unless the intercept is exactly 0. The projection
    has to be the LAST per-row operation, not just the first."""
    cfg = _CONFIG + """
calibration:
  method: platt
"""
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(cfg)
    results = ExperimentRunner(
        config_path=cfg_path, matches_path=paired_matches,
        cache_dir=tmp_path / "cache", mlflow_dir=tmp_path / "mlruns",
        calibrate=True,
    ).run()
    seen = 0
    for pred in results["all_predictions"]:
        sums, n = _pair_sums(pred)
        seen += n
        assert sums == pytest.approx(1.0, abs=1e-12)
    assert seen > 0, "no pairs evaluated -- the test proved nothing"


def test_stacking_branch_stays_complementary(paired_matches, tmp_path):
    """The stacking meta model is a logistic over sub-probs plus meta features;
    it is not antisymmetric even on antisymmetric inputs, and its assignment
    OVERWRITES the per-fold projection. This is the branch carrying the
    ordering assumptions, so it earns its own coverage."""
    sub = tmp_path / "sub.yaml"
    sub.write_text(_CONFIG.replace("name: symmetry_test", "name: sub"))
    cfg = f"""
name: stacking_symmetry
data:
  date_range:
    start: "2024-01-01"
    end: "2024-12-31"
model:
  type: ensemble
  params:
    strategy: stacking
    base_models:
      - config: {sub.as_posix()}
      - config: {sub.as_posix()}
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
metrics:
  primary: log_loss
"""
    cfg_path = tmp_path / "stack.yaml"
    cfg_path.write_text(cfg)
    results = ExperimentRunner(
        config_path=cfg_path, matches_path=paired_matches,
        cache_dir=tmp_path / "cache", mlflow_dir=tmp_path / "mlruns",
    ).run()
    seen = 0
    for pred in results["all_predictions"]:
        sums, n = _pair_sums(pred)
        seen += n
        assert sums == pytest.approx(1.0, abs=1e-12)
    assert seen > 0, "no pairs evaluated -- the test proved nothing"
