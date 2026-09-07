"""`iid-pin`: a projection evaluation becomes referenceable by stem under
projections/, and the pinned stem resolves to the evaluation it came from."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from mvp.common.config_hash import compute_iid_fingerprint
from mvp.model.features import prior
from mvp.projection.iid import pin as pin_mod
from mvp.projection.iid.config import IIDProjectionConfig
from mvp.projection.iid.pin import default_stem, locate, pin

_PROJ_YAML = textwrap.dedent(
    """
    data:
      date_range:
        start: "2024-01-01"
        end: "2025-12-31"
      filters:
        draw_type: singles
    features:
      include:
        - pts_service_won_pct(days=90)
    serve_model:
      type: identity
      window: 90
    """
)


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "dataroot"
    (root / "projection_evaluations").mkdir(parents=True)
    monkeypatch.setenv("MVP_DATA_ROOT", str(root))
    monkeypatch.setenv("MVP_ARTIFACT_ROOT", str(root))
    monkeypatch.setattr(
        prior, "PROJECTION_EVALUATIONS_ROOT", root / "projection_evaluations",
    )
    return {
        "eval_root": root / "projection_evaluations",
        "projections": tmp_path / "projections",
        "models": tmp_path / "models",
    }


def _make_eval(
    eval_root: Path, *, text: str = _PROJ_YAML, parent: str = "parent",
    run_tag: str = "parent__d01_t12", fp: str | None = None, artifacts=(),
) -> Path:
    """An evaluation dir holding a snapshot and a source line. `fp` defaults
    to the snapshot's true fingerprint; pass another to fake a stale one."""
    scratch = eval_root / "_snapshot.yaml"
    scratch.write_text(text, encoding="utf-8")
    true_fp = compute_iid_fingerprint(
        IIDProjectionConfig.from_file(scratch), config_path=scratch,
    )
    scratch.unlink()
    d = eval_root / (fp or true_fp)
    d.mkdir()
    (d / "config.yaml").write_text(text, encoding="utf-8")
    (d / "source.txt").write_text(
        f"{parent}\t{run_tag}\t2026-09-05T15:42:23\n", encoding="utf-8",
    )
    for name in artifacts:
        (d / name).write_bytes(b"")
    return d


def _pin(env, ref, **kw):
    return pin(
        ref, projection_dir=env["projections"], model_dirs=(env["models"],), **kw,
    )


class TestLocate:
    def test_by_fingerprint(self, env):
        d = _make_eval(env["eval_root"])
        assert locate(d.name) == d

    def test_by_run_tag(self, env):
        d = _make_eval(env["eval_root"], run_tag="parent__d02_t7")
        assert locate("parent__d02_t7") == d

    def test_parent_stem_is_not_a_run_tag(self, env):
        """Field 1 groups a sweep's trials under the parent; matching on it
        would hand the parent stem an arbitrary trial."""
        _make_eval(env["eval_root"], parent="parent", run_tag="parent__d01_t12")
        with pytest.raises(FileNotFoundError, match="neither"):
            locate("parent")

    def test_default_stem_is_the_run_tag(self, env):
        d = _make_eval(env["eval_root"], run_tag="parent__d01_t12")
        assert default_stem(d) == "parent__d01_t12"


class TestPin:
    def test_writes_snapshot_under_run_tag(self, env):
        d = _make_eval(env["eval_root"], run_tag="parent__d01_t12")
        res = _pin(env, d.name)
        assert res.config_path == env["projections"] / "parent__d01_t12.yaml"
        assert res.config_path.read_text(encoding="utf-8") == _PROJ_YAML
        assert res.fp == d.name
        assert res.spec == "player_prior_logit(model=parent__d01_t12)"

    def test_pinned_stem_resolves_to_the_evaluation(self, env):
        """The point of the exercise: the resolver lands on the dir whose
        artifacts already exist, not on an empty fingerprint dir."""
        d = _make_eval(env["eval_root"])
        res = _pin(env, "parent__d01_t12")
        src = prior.resolve_prior(
            res.stem,
            config_dirs=(env["models"],),
            projection_config_dirs=(env["projections"],),
        )
        assert src.kind == "projection"
        assert src.eval_dir == d

    def test_as_overrides_the_stem(self, env):
        d = _make_eval(env["eval_root"])
        res = _pin(env, d.name, stem="srv_bri_pinned")
        assert res.config_path.name == "srv_bri_pinned.yaml"

    def test_production_writes_to_the_versioned_dir(self, env):
        d = _make_eval(env["eval_root"])
        res = _pin(env, d.name, production=True)
        assert res.config_path == (
            env["projections"] / "production" / "parent__d01_t12.yaml"
        )

    def test_model_namespace_clash_is_refused(self, env):
        d = _make_eval(env["eval_root"])
        env["models"].mkdir()
        (env["models"] / "parent__d01_t12.yaml").write_text("x", encoding="utf-8")
        with pytest.raises(ValueError, match="both namespaces"):
            _pin(env, d.name)
        assert not (env["projections"] / "parent__d01_t12.yaml").exists()

    def test_identical_existing_copy_is_fine(self, env):
        d = _make_eval(env["eval_root"])
        _pin(env, d.name)
        _pin(env, d.name)  # idempotent

    def test_different_existing_copy_needs_force(self, env):
        d = _make_eval(env["eval_root"])
        env["projections"].mkdir()
        target = env["projections"] / "parent__d01_t12.yaml"
        target.write_text(_PROJ_YAML.replace("90", "30"), encoding="utf-8")
        with pytest.raises(FileExistsError, match="--force"):
            _pin(env, d.name)
        assert "30" in target.read_text(encoding="utf-8")
        _pin(env, d.name, force=True)
        assert target.read_text(encoding="utf-8") == _PROJ_YAML

    def test_production_pin_shadowed_by_scratch_is_refused(self, env):
        d = _make_eval(env["eval_root"])
        env["projections"].mkdir()
        (env["projections"] / "parent__d01_t12.yaml").write_text(
            _PROJ_YAML.replace("90", "30"), encoding="utf-8",
        )
        with pytest.raises(FileExistsError, match="shadowed"):
            _pin(env, d.name, production=True)

    def test_stale_snapshot_is_removed_again(self, env):
        """A snapshot that no longer fingerprints to its dir would pin a stem
        that resolves to an empty evaluation."""
        d = _make_eval(env["eval_root"], fp="deadbeefdead")
        with pytest.raises(RuntimeError, match="removed"):
            _pin(env, d.name)
        assert not (env["projections"] / "parent__d01_t12.yaml").exists()

    def test_missing_artifacts_are_named_with_their_producer(self, env):
        d = _make_eval(env["eval_root"], artifacts=("fold_match_win.parquet",))
        res = _pin(env, d.name)
        assert res.missing == ("total_games_pmf.parquet", "serve_model.joblib")
        assert res.missing_commands == [
            "poetry run py -m mvp iid-backtest parent__d01_t12",
        ]

    def test_complete_evaluation_has_nothing_missing(self, env):
        d = _make_eval(env["eval_root"], artifacts=(
            "fold_match_win.parquet", "total_games_pmf.parquet", "serve_model.joblib",
        ))
        assert _pin(env, d.name).missing == ()

    def test_no_snapshot_is_refused(self, env):
        d = _make_eval(env["eval_root"])
        (d / "config.yaml").unlink()
        with pytest.raises(FileNotFoundError, match="config.yaml"):
            _pin(env, d.name)

    def test_defaults_point_at_the_repo_dirs(self):
        assert pin_mod.PROJECTION_DIR == Path("projections")
        assert pin_mod.MODEL_DIRS == prior.CONFIG_DIRS
