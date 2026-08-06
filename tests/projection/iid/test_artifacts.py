"""Fingerprint-keyed evaluation artifacts + the shared serve-model builder.

Two regressions are guarded here:
  * runner and backtest each had their own `_build_serve_model` and they drifted —
    only the backtest passed `gap_shrink`, so one config was scored by two
    different models depending on the entrypoint.
  * backtest artifacts were keyed by config filename stem, so every hyperparameter
    variant of a config overwrote the previous variant's outputs.
"""

from __future__ import annotations

import json
from textwrap import dedent

import polars as pl
import pytest

from mvp.projection.iid import (
    artifacts,
    evaluation,
    projection_run,
    runner,
    serve_model,
)
from mvp.projection.iid.config import IIDProjectionConfig

BASE_YAML = dedent("""
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
    validation:
      type: date_expanding
      initial_train_months: 12
      test_months: 6
""")


def _write_config(tmp_path, yaml_text=BASE_YAML, name="cfg.yaml"):
    p = tmp_path / name
    p.write_text(yaml_text, encoding="utf-8")
    return p


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "dataroot"
    root.mkdir()
    monkeypatch.setenv("MVP_DATA_ROOT", str(root))
    return root


class TestSharedServeModelBuilder:
    def test_runner_and_projection_run_share_one_builder(self):
        """The divergence was two copies of this function. One object, one behavior."""
        assert runner.build_serve_model is serve_model.build_serve_model
        assert projection_run.build_serve_model is serve_model.build_serve_model

    def test_gap_shrink_reaches_the_model(self, tmp_path):
        cfg_path = _write_config(
            tmp_path,
            BASE_YAML.replace(
                "model_type: xgboost", "model_type: xgboost\n  gap_shrink: 0.7",
            ),
        )
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        assert cfg.serve_model.gap_shrink == 0.7
        model = serve_model.build_serve_model(cfg.serve_model)
        assert model.gap_shrink == 0.7

    def test_clip_bounds_reach_the_model(self, tmp_path):
        cfg = IIDProjectionConfig.from_file(str(_write_config(tmp_path)))
        model = serve_model.build_serve_model(cfg.serve_model)
        assert model.clip_min == cfg.serve_model.clip_min
        assert model.clip_max == cfg.serve_model.clip_max

    def test_unknown_type_raises(self, tmp_path):
        cfg = IIDProjectionConfig.from_file(str(_write_config(tmp_path)))
        cfg.serve_model.type = "nope"
        with pytest.raises(ValueError, match="Unknown serve model type"):
            serve_model.build_serve_model(cfg.serve_model)


class TestArtifactPaths:
    def test_hp_variants_do_not_collide(self, tmp_path, data_root):
        """The stem-collision regression: same filename stem, different HPs."""
        variant_dir = tmp_path / "variant"
        variant_dir.mkdir()
        a_path = _write_config(tmp_path, BASE_YAML, "same_stem.yaml")
        b_path = _write_config(
            variant_dir, BASE_YAML.replace("max_depth: 3", "max_depth: 7"),
            "same_stem.yaml",
        )
        a = IIDProjectionConfig.from_file(str(a_path))
        b = IIDProjectionConfig.from_file(str(b_path))

        assert a_path.stem == b_path.stem  # the old key would have collided
        assert projection_run.artifact_path(a, a_path) != projection_run.artifact_path(
            b, b_path
        )
        assert evaluation.ledger_path(a, a_path) != evaluation.ledger_path(b, b_path)

    def test_artifacts_live_outside_model_evaluations(self, tmp_path, data_root):
        """model_evaluations/ is wiped weekly by the classification pipeline."""
        cfg_path = _write_config(tmp_path)
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        parts = evaluation.ledger_path(cfg, cfg_path).parts
        assert "projection_evaluations" in parts
        assert "model_evaluations" not in parts


class TestRecordRun:
    def test_writes_snapshot_and_source(self, tmp_path, data_root):
        cfg_path = _write_config(tmp_path)
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        fp_dir = artifacts.record_run(cfg, cfg_path)
        assert (fp_dir / "config.yaml").exists()
        sources = artifacts.read_sources(fp_dir)
        assert sources and sources[0][0] == "cfg"

    def test_source_groups_trials_under_a_parent(self, tmp_path, data_root):
        """A sweep passes the parent stem so iid-rank can group the variants."""
        cfg_path = _write_config(tmp_path)
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        fp_dir = artifacts.record_run(
            cfg, cfg_path, source="parent_config", run_id="parent_config__d01_t7",
        )
        names = {s[0] for s in artifacts.read_sources(fp_dir)}
        run_ids = {s[1] for s in artifacts.read_sources(fp_dir)}
        assert names == {"parent_config"}
        assert run_ids == {"parent_config__d01_t7"}

    def test_repeated_record_is_idempotent(self, tmp_path, data_root):
        cfg_path = _write_config(tmp_path)
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        artifacts.record_run(cfg, cfg_path, source="p", run_id="r")
        fp_dir = artifacts.record_run(cfg, cfg_path, source="p", run_id="r")
        assert len(artifacts.read_sources(fp_dir)) == 1


class TestProjectionJson:
    def test_round_trip_drops_unserializable_members(self, tmp_path, data_root):
        cfg_path = _write_config(tmp_path)
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        fp_dir = artifacts.record_run(cfg, cfg_path)
        result = {
            "metrics": {"iid_crps_total_games": 1.23},
            "fold_metrics": [{"iid_crps_total_games": 1.2}, {"iid_crps_total_games": 1.26}],
            "n_folds": 2,
            "n_matches": 100,
            "run_id": None,
            "diagnostics": object(),   # not serializable
            "_config": cfg,            # bulky, snapshotted separately
        }
        artifacts.write_projection_json(fp_dir, result)
        loaded = artifacts.read_projection_json(fp_dir)
        assert loaded["metrics"]["iid_crps_total_games"] == 1.23
        assert len(loaded["fold_metrics"]) == 2
        assert "diagnostics" not in loaded
        assert "_config" not in loaded

    def test_missing_returns_none(self, tmp_path):
        assert artifacts.read_projection_json(tmp_path) is None


class TestPmfParquet:
    def test_round_trip(self, tmp_path, data_root):
        cfg_path = _write_config(tmp_path)
        cfg = IIDProjectionConfig.from_file(str(cfg_path))
        fp_dir = artifacts.record_run(cfg, cfg_path)
        pmf = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "actual_total": [21.0, 23.0],
            "total_games_pmf": [[0.1, 0.9], [0.4, 0.6]],
        })
        path = artifacts.write_pmf_parquet(fp_dir, pmf)
        back = pl.read_parquet(path)
        assert back["match_uid"].to_list() == ["m1", "m2"]
        assert back["total_games_pmf"][0].to_list() == [0.1, 0.9]


class TestClvJson:
    def test_missing_returns_none(self, tmp_path):
        assert artifacts.read_clv_json(tmp_path) is None

    def test_read(self, tmp_path):
        (tmp_path / "clv.json").write_text(json.dumps({"avg_clvpin": 0.004}))
        assert artifacts.read_clv_json(tmp_path)["avg_clvpin"] == 0.004


class TestCutoverPurge:
    """The fingerprint dir is odds-source-blind, so a cutover leaves two
    contracts at one path unless the old one is removed.

    `_canonicalize_iid_config` hashes data / features / metrics / serve_model /
    validation and knows nothing about which odds source produced the ledger, so
    a post-cutover run lands in the same `<fp>/` as the run it supersedes.
    """

    def _fp(self, data_root, name="aaa111", files=()):
        d = data_root / "projection_evaluations" / name
        d.mkdir(parents=True, exist_ok=True)
        (d / "projection.json").write_text("{}", encoding="utf-8")
        for f in files:
            (d / f).write_text("x", encoding="utf-8")
        return d

    def test_dry_run_lists_without_deleting(self, data_root):
        d = self._fp(data_root, files=("backtest.csv", "clv.json"))
        found = artifacts.purge_stale_artifacts(dry_run=True)
        assert {p.name for p in found} == {"backtest.csv", "clv.json"}
        assert (d / "backtest.csv").exists()

    def test_it_removes_only_the_pre_cutover_artifacts(self, data_root):
        """Everything else in a fingerprint dir is odds-independent and must
        survive — deleting the pmf or the trained model would force a retrain."""
        d = self._fp(
            data_root,
            files=("backtest.csv", "clv.json", "total_games_pmf.parquet",
                   "serve_model.joblib", "config.yaml"),
        )
        artifacts.purge_stale_artifacts(dry_run=False)
        assert not (d / "backtest.csv").exists()
        assert not (d / "clv.json").exists()
        for keep in ("total_games_pmf.parquet", "serve_model.joblib",
                     "config.yaml", "projection.json"):
            assert (d / keep).exists()

    def test_a_clean_dir_yields_nothing(self, data_root):
        self._fp(data_root, files=("total_games_pmf.parquet",))
        assert artifacts.purge_stale_artifacts(dry_run=True) == []

    def test_the_new_ledger_is_never_purged(self, data_root):
        d = self._fp(data_root, files=("backtest.parquet", "backtest.csv"))
        artifacts.purge_stale_artifacts(dry_run=False)
        assert (d / "backtest.parquet").exists()
        assert not (d / "backtest.csv").exists()
