"""The projection prior source: a stem under projections/ resolves to an IID
projection evaluation, and the prior column is the chain's OOF match-win
probability — nested-Platt-calibrated like every other prior — in both
orientations."""

import textwrap
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from mvp.model.features import prior

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


def _write_proj_cfg(d: Path, stem: str) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{stem}.yaml"
    p.write_text(_PROJ_YAML, encoding="utf-8")
    return p


def _fold_match_win(
    days: list[date], folds: list[int], wins: list[int] | None = None,
    probs: list[float] | None = None,
) -> pl.DataFrame:
    n = len(days)
    if wins is None:
        wins = [i % 2 for i in range(n)]
    if probs is None:
        probs = list(np.linspace(0.55, 0.75, n))
    return pl.DataFrame({
        "match_uid": [f"M{i}" for i in range(n)],
        "player_id": [f"A{i}" for i in range(n)],
        "opp_id": [f"B{i}" for i in range(n)],
        "effective_match_date": days,
        "fold_idx": pl.Series(folds, dtype=pl.Int32),
        "p_match_win_a": probs,
        "won_a": pl.Series(wins, dtype=pl.Int8),
    })


# Two folds, two rows each, both classes in each fold — the smallest artifact
# the nested calibration can run on.
_DAYS_2X2 = [date(2025, 1, 5), date(2025, 1, 9), date(2025, 7, 2), date(2025, 7, 8)]
_FOLDS_2X2 = [1, 1, 2, 2]
_WINS_2X2 = [1, 0, 1, 0]


class TestResolve:
    def test_projection_stem_resolves_via_iid_fingerprint(self, tmp_path, monkeypatch):
        from mvp.common.config_hash import compute_iid_fingerprint
        from mvp.projection.iid.config import IIDProjectionConfig

        proj_dir = tmp_path / "projections"
        cfg_path = _write_proj_cfg(proj_dir, "proj_base")
        monkeypatch.setattr(prior, "PROJECTION_EVALUATIONS_ROOT", tmp_path / "pe")

        src = prior.resolve_prior(
            "proj_base",
            config_dirs=(tmp_path / "models",),
            projection_config_dirs=(proj_dir,),
        )
        fp = compute_iid_fingerprint(
            IIDProjectionConfig.from_file(cfg_path), config_path=cfg_path
        )
        assert src.kind == "projection"
        assert src.fp == fp
        assert src.eval_dir == tmp_path / "pe" / fp
        assert src.forward_train_end == date(2025, 12, 31)
        assert "iid-project" in src.regenerate_command

    def test_stem_in_both_namespaces_is_refused(self, tmp_path, monkeypatch):
        import yaml

        models = tmp_path / "models"
        models.mkdir()
        (models / "dupe.yaml").write_text(yaml.dump({
            "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
            "features": {"include": ["player_elo_surface_diff"]},
            "model": {"type": "xgboost", "params": {"n_estimators": 5}},
            "target": "won",
        }))
        proj_dir = tmp_path / "projections"
        _write_proj_cfg(proj_dir, "dupe")
        with pytest.raises(ValueError, match="both the model namespace"):
            prior.resolve_prior(
                "dupe", config_dirs=(models,), projection_config_dirs=(proj_dir,),
            )

    def test_missing_artifacts_refuse_without_regenerate(self, tmp_path, monkeypatch):
        """Train/serve (regenerate=False) refuse with the iid-project
        command, same as the model-prior convention."""
        proj_dir = tmp_path / "projections"
        _write_proj_cfg(proj_dir, "proj_base")
        monkeypatch.setattr(prior, "PROJECTION_EVALUATIONS_ROOT", tmp_path / "pe")
        src = prior.resolve_prior(
            "proj_base",
            config_dirs=(tmp_path / "models",),
            projection_config_dirs=(proj_dir,),
        )
        assert not prior.prior_artifacts_ready(src)
        with pytest.raises(FileNotFoundError, match="iid-project"):
            prior.ensure_prior_artifacts(src, regenerate=False)

    def test_regenerate_runs_the_projection_runner(self, tmp_path, monkeypatch):
        """The discovery driver's regenerate=True auto-runs the PROJECTION
        runner (never ExperimentRunner), which writes the artifact."""
        proj_dir = tmp_path / "projections"
        _write_proj_cfg(proj_dir, "proj_base")
        monkeypatch.setattr(prior, "PROJECTION_EVALUATIONS_ROOT", tmp_path / "pe")
        src = prior.resolve_prior(
            "proj_base",
            config_dirs=(tmp_path / "models",),
            projection_config_dirs=(proj_dir,),
        )

        calls: list[str] = []

        class _FakeRunner:
            def __init__(self, config_path):
                calls.append(str(config_path))

            def run(self):
                src.eval_dir.mkdir(parents=True, exist_ok=True)
                _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
                    src.fold_match_win
                )
                return {}

        import mvp.projection.iid.runner as proj_runner

        monkeypatch.setattr(proj_runner, "IIDProjectionRunner", _FakeRunner)
        prior.ensure_prior_artifacts(src, regenerate=True)
        assert calls == [str(src.config_path)]
        assert prior.prior_artifacts_ready(src)


def _source(tmp_path: Path, end: date = date(2025, 12, 31)) -> prior.PriorSource:
    return prior.PriorSource(
        model="proj_base", config_path=tmp_path / "proj_base.yaml",
        fp="abc123abc123", eval_dir=tmp_path / "pe" / "abc123abc123",
        kind="projection", forward_train_end=end,
    )


class TestFrame:
    def test_both_orientations_and_fold_train_ends(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
            src.fold_match_win
        )

        frame = prior.build_prior_frame(src)
        assert frame.height == 8  # two orientations per match
        by_match = frame.group_by("match_uid").agg(
            pl.col("prior_prob").sum().alias("s"),
            pl.col("prior_logit").sum().alias("sl"),
        )
        assert np.allclose(by_match["s"].to_numpy(), 1.0)
        assert np.allclose(by_match["sl"].to_numpy(), 0.0, atol=1e-9)
        ends = dict(zip(frame["match_uid"].to_list(), frame["prior_train_end"].to_list()))
        assert ends["M0"] == date(2025, 1, 4)
        assert ends["M2"] == date(2025, 7, 1)
        assert set(frame["prior_kind"].to_list()) == {"proj_fold_oof_nested_cal"}
        m0 = frame.filter(pl.col("match_uid") == "M0")
        assert set(m0["player_id"].to_list()) == {"A0", "B0"}

    def test_nested_calibration_is_fit_out_of_fold(self, tmp_path):
        """Raw 0.60 everywhere, outcomes ~90% wins: the calibrated prior must
        move above the raw value, and each fold's transform comes from the
        other fold's rows."""
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        n = 40
        days = [date(2025, 1 + 6 * (i >= n // 2), 5 + i % 20) for i in range(n)]
        folds = [1] * (n // 2) + [2] * (n // 2)
        wins = [1 if i % 10 else 0 for i in range(n)]  # 90% wins
        _fold_match_win(days, folds, wins, probs=[0.60] * n).write_parquet(
            src.fold_match_win
        )
        frame = prior.build_prior_frame(src)
        a_rows = frame.filter(pl.col("player_id").str.starts_with("A"))
        assert float(a_rows["prior_prob"].mean()) > 0.65

    def test_single_fold_refused(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_match_win(
            [date(2025, 1, 5), date(2025, 1, 6)], [1, 1], [1, 0]
        ).write_parquet(src.fold_match_win)
        with pytest.raises(ValueError, match="nested calibration needs"):
            prior.build_prior_frame(src)

    def test_forward_rows_globally_calibrated_with_config_end(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
            src.fold_match_win
        )
        pl.DataFrame({
            "match_uid": ["F1"], "player_id": ["A9"], "opp_id": ["B9"],
            "effective_match_date": [date(2026, 2, 1)],
            "p_match_win_a": [0.6],
        }).write_parquet(src.pmf_parquet)

        frame = prior.build_prior_frame(src)
        f1 = frame.filter(pl.col("match_uid") == "F1")
        assert set(f1["prior_kind"].to_list()) == {"proj_forward_cal"}
        assert set(f1["prior_train_end"].to_list()) == {date(2025, 12, 31)}
        assert f1.filter(pl.col("player_id") == "A9")["prior_prob"].sum() + \
            f1.filter(pl.col("player_id") == "B9")["prior_prob"].sum() == pytest.approx(1.0)

    def test_old_pmf_without_ids_is_skipped(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
            src.fold_match_win
        )
        pl.DataFrame({
            "match_uid": ["F1"], "p_match_win_a": [0.6],
        }).write_parquet(src.pmf_parquet)

        frame = prior.build_prior_frame(src)
        assert set(frame["prior_kind"].to_list()) == {"proj_fold_oof_nested_cal"}

    def test_forward_row_on_or_before_train_end_is_refused(self, tmp_path):
        src = _source(tmp_path, end=date(2026, 3, 1))
        src.eval_dir.mkdir(parents=True)
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
            src.fold_match_win
        )
        pl.DataFrame({
            "match_uid": ["F1"], "player_id": ["A9"], "opp_id": ["B9"],
            "effective_match_date": [date(2026, 2, 1)],  # <= train end
            "p_match_win_a": [0.6],
        }).write_parquet(src.pmf_parquet)
        with pytest.raises(ValueError, match="on/before their train end"):
            prior.build_prior_frame(src)

    def test_overlap_between_sources_is_refused(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
            src.fold_match_win
        )
        pl.DataFrame({
            "match_uid": ["M0"], "player_id": ["A0"], "opp_id": ["B0"],
            "effective_match_date": [date(2026, 2, 1)],
            "p_match_win_a": [0.6],
        }).write_parquet(src.pmf_parquet)
        with pytest.raises(ValueError, match="refusing to splice"):
            prior.build_prior_frame(src)

    def test_missing_columns_in_fold_artifact_raise(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        for col in ("opp_id", "won_a"):
            _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).drop(col).write_parquet(
                src.fold_match_win
            )
            with pytest.raises(ValueError, match="missing columns"):
                prior.build_prior_frame(src)

    def test_salt_tracks_the_projection_artifacts(self, tmp_path):
        import os

        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        assert src.salt() == "abc123abc123:-:-"
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
            src.fold_match_win
        )
        s1 = src.salt()
        os.utime(src.fold_match_win, (1_700_000_000, 1_700_000_000))
        assert src.salt() != s1

    def test_single_class_fold_complement_refused_with_domain_message(self, tmp_path):
        """A raw fallback would splice calibration states; a degenerate
        complement gets a named refusal, not an sklearn traceback."""
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, wins=[1, 1, 1, 0]).write_parquet(
            src.fold_match_win
        )  # fold 1 is all wins -> fold 2's calibrator has a single-class fit
        with pytest.raises(ValueError, match="single-class"):
            prior.build_prior_frame(src)


class TestSweepTagFallback:
    def test_sweep_trials_are_not_matched_by_the_parent_stem(self, tmp_path, monkeypatch):
        """Projection source.txt field 1 is the GROUPING source — the parent
        stem for every sweep trial. The fallback must match the RUN tag
        (field 2), or resolving a parent config grabs an arbitrary trial."""
        proj_dir = tmp_path / "projections"
        _write_proj_cfg(proj_dir, "proj_base")
        root = tmp_path / "pe"
        monkeypatch.setattr(prior, "PROJECTION_EVALUATIONS_ROOT", root)

        trial = root / "aaaaaaaaaaaa"
        trial.mkdir(parents=True)
        (trial / "source.txt").write_text("proj_base\tproj_base__h03_t17\t2026-01-01\n")
        (trial / "config.yaml").write_text(_PROJ_YAML)
        plain = root / "bbbbbbbbbbbb"
        plain.mkdir()
        (plain / "source.txt").write_text("proj_base\tproj_base\t2026-01-02\n")
        (plain / "config.yaml").write_text(_PROJ_YAML)

        src = prior.resolve_prior(
            "proj_base",
            config_dirs=(tmp_path / "models",),
            projection_config_dirs=(proj_dir,),
        )
        # the computed fingerprint has no dir, so the fallback fires — and it
        # must pick the plain run, never the sweep trial (whose snapshot here
        # is even equivalent: its RUN tag is what disqualifies it)
        assert src.eval_dir == plain

    def test_unreadable_snapshot_is_skipped(self, tmp_path, monkeypatch):
        proj_dir = tmp_path / "projections"
        _write_proj_cfg(proj_dir, "proj_base")
        root = tmp_path / "pe"
        monkeypatch.setattr(prior, "PROJECTION_EVALUATIONS_ROOT", root)
        bad = root / "aaaaaaaaaaaa"
        bad.mkdir(parents=True)
        (bad / "source.txt").write_text("proj_base\tproj_base\t2026-01-01\n")
        (bad / "config.yaml").write_text("serve_model: 5\n")
        src = prior.resolve_prior(
            "proj_base",
            config_dirs=(tmp_path / "models",),
            projection_config_dirs=(proj_dir,),
        )
        assert src.eval_dir == root / src.fp
        assert src.fp != "aaaaaaaaaaaa"

    def test_config_edit_never_resolves_to_the_stale_run(self, tmp_path, monkeypatch):
        """The 2026-08-31 regression: editing the config produced a new
        fingerprint and the fallback silently served the pre-edit run by
        tag. Now the stale dir's snapshot fingerprints differently and is
        ignored: resolution lands on the (empty) true-fingerprint dir and
        the regenerate path engages."""
        proj_dir = tmp_path / "projections"
        cfg_path = _write_proj_cfg(proj_dir, "proj_base")
        root = tmp_path / "pe"
        monkeypatch.setattr(prior, "PROJECTION_EVALUATIONS_ROOT", root)

        stale = root / "cccccccccccc"
        stale.mkdir(parents=True)
        (stale / "source.txt").write_text("proj_base\tproj_base\t2026-01-01\n")
        (stale / "config.yaml").write_text(_PROJ_YAML)
        _fold_match_win(_DAYS_2X2, _FOLDS_2X2, _WINS_2X2).write_parquet(
            stale / "fold_match_win.parquet"
        )

        cfg_path.write_text(
            _PROJ_YAML.replace('start: "2024-01-01"', 'start: "2022-01-01"'),
            encoding="utf-8",
        )
        src = prior.resolve_prior(
            "proj_base",
            config_dirs=(tmp_path / "models",),
            projection_config_dirs=(proj_dir,),
        )
        assert src.eval_dir != stale
        assert not prior.prior_artifacts_ready(src)
