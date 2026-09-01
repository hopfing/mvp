"""The runner's `source=` override: evaluations run from temp configs get
tagged with their FAMILY identity in source.txt, not the temp file's random
stem — the gap that made sweep/discovery trials unfindable by tag
(model-rank, `mvp compare`)."""

import importlib
import random
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from mvp.model.runner import ExperimentRunner


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking

    importlib.reload(mvp.model.features.ranking)


@pytest.fixture
def matches(tmp_path: Path) -> Path:
    random.seed(7)
    rows = []
    base = date(2024, 1, 1)
    for i in range(400):
        d = base + timedelta(days=i // 4)
        pr, orank = random.randint(1, 200), random.randint(1, 200)
        won = random.random() < (0.65 if pr < orank else 0.35)
        me, other = f"P{i % 20:02d}", f"P{(i + 10) % 20:02d}"
        for pid, oid, a, b, w in (
            (me, other, pr, orank, won), (other, me, orank, pr, not won),
        ):
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
    n_estimators: 5
    max_depth: 2
validation:
  type: walk_forward
  n_splits: 2
  min_train_size: 100
  test_size: 50
"""


def _run(tmp_path, matches, monkeypatch, source):
    import mlflow

    import mvp.common.config_hash as config_hash

    # config_hash binds get_data_root at import; patch ITS reference so the
    # fingerprint artifacts land in tmp, never the real evaluations root.
    data_root = tmp_path / "data"
    monkeypatch.setattr(config_hash, "get_data_root", lambda: data_root)

    cfg = tmp_path / "tmp_trial_config.yaml"
    cfg.write_text(_CONFIG)
    mlflow_dir = tmp_path / "mlruns"
    mlflow.set_tracking_uri(f"file://{mlflow_dir}")
    ExperimentRunner(
        config_path=cfg, matches_path=matches,
        cache_dir=tmp_path / "cache", mlflow_dir=mlflow_dir, source=source,
    ).run()
    dirs = list((data_root / "model_evaluations").iterdir())
    assert len(dirs) == 1
    line = (dirs[0] / "source.txt").read_text(encoding="utf-8").splitlines()[0]
    return line.split("\t")[0]


def test_source_override_tags_the_family(tmp_path, matches, monkeypatch):
    assert _run(tmp_path, matches, monkeypatch, "my_family") == "my_family"


def test_default_stays_the_config_stem(tmp_path, matches, monkeypatch):
    assert _run(tmp_path, matches, monkeypatch, None) == "tmp_trial_config"
