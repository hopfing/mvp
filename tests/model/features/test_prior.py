"""The prior transform's resolution and splice (features/prior.py): a stage
names its base model by config stem; the fingerprint is computed from that
config; the OOF frame is spliced from the evaluation artifacts."""

from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model.features import prior
from mvp.model.prior_naming import prior_column, prior_model_of, prior_spec

_CFG = {
    "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
    "features": {"include": ["player_elo_surface_diff"]},
    "model": {"type": "xgboost", "params": {"n_estimators": 5, "n_jobs": 1}},
    "target": "won",
}


def _write_cfg(d: Path, stem: str) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{stem}.yaml"
    p.write_text(yaml.dump(_CFG))
    return p


class TestNaming:
    def test_spec_column_model_round_trip(self):
        s = prior_spec("stage1_lead_residual__h19_t218")
        assert s == "player_prior_logit(model=stage1_lead_residual__h19_t218)"
        assert prior_column("x") == "player_prior_logit_x"
        assert prior_model_of(s) == "stage1_lead_residual__h19_t218"
        assert prior_model_of("player_prior_logit(model='q')") == "q"
        assert prior_model_of(prior_column("abc")) == "abc"
        assert prior_model_of("player_lead_logit") is None
        assert prior_model_of("player_elo_diff") is None


class TestResolve:
    def test_config_found_in_search_order(self, tmp_path):
        a, b = tmp_path / "models", tmp_path / "models" / "production"
        _write_cfg(b, "base")
        assert prior.find_prior_config("base", (a, b)) == b / "base.yaml"
        _write_cfg(a, "base")
        assert prior.find_prior_config("base", (a, b)) == a / "base.yaml"
        with pytest.raises(FileNotFoundError, match="no missing.yaml"):
            prior.find_prior_config("missing", (a, b))

    def test_fingerprint_is_a_function_of_the_config(self, tmp_path, monkeypatch):
        from mvp.common.config_hash import compute_fingerprint
        from mvp.model.config import ExperimentConfig

        cfg = _write_cfg(tmp_path / "models", "base")
        monkeypatch.setattr(prior, "EVALUATIONS_ROOT", tmp_path / "evals")
        src = prior.resolve_prior("base", (tmp_path / "models",))
        fp = compute_fingerprint(ExperimentConfig.from_file(str(cfg)), config_path=cfg)
        assert src.fp == fp
        assert src.eval_dir == tmp_path / "evals" / fp
        assert src.stem == "base"
        assert "base.yaml" in src.regenerate_command

    def test_snapshot_form_and_source_tag_fallback(self, tmp_path, monkeypatch):
        """A config copied from an evaluation snapshot carries a top-level
        `metrics_objective`; it loads, and fingerprints like the original
        (metrics.objective). A production copy that dropped it fingerprints
        differently, so the evaluation tagged with the stem is used."""
        import yaml

        from mvp.common.config_hash import compute_fingerprint
        from mvp.model.config import ExperimentConfig

        models = tmp_path / "models"
        models.mkdir()
        original = {**_CFG, "metrics": {"objective": ["log_loss"]}}
        (models / "orig.yaml").write_text(yaml.dump(original))
        fp_orig = compute_fingerprint(
            ExperimentConfig.model_validate(original), config_path=models / "orig.yaml"
        )
        snapshot = {**_CFG, "metrics_objective": ["log_loss"]}
        (models / "snap.yaml").write_text(yaml.dump(snapshot))
        monkeypatch.setattr(prior, "EVALUATIONS_ROOT", tmp_path / "evals")
        assert prior.resolve_prior("snap", (models,)).fp == fp_orig

        # stripped copy: different fingerprint, no dir there -> source tag
        (models / "prod").mkdir()
        (models / "prod" / "stripped.yaml").write_text(yaml.dump(_CFG))
        evals = tmp_path / "evals"
        (evals / "aaaaaaaaaaaa").mkdir(parents=True)
        (evals / "aaaaaaaaaaaa" / "source.txt").write_text("stripped\tdeadbeef\t2026-01-01\n")
        (evals / "bbbbbbbbbbbb").mkdir()
        (evals / "bbbbbbbbbbbb" / "source.txt").write_text("other\tdeadbeef\t2026-01-01\n")
        src = prior.resolve_prior("stripped", (models / "prod",))
        assert src.fp == "aaaaaaaaaaaa"
        assert src.eval_dir == evals / "aaaaaaaaaaaa"

    def test_missing_artifacts_refuse_unless_regenerating(self, tmp_path, monkeypatch):
        _write_cfg(tmp_path / "models", "base")
        monkeypatch.setattr(prior, "EVALUATIONS_ROOT", tmp_path / "evals")
        src = prior.resolve_prior("base", (tmp_path / "models",))
        assert not prior.prior_artifacts_ready(src)
        with pytest.raises(FileNotFoundError, match="Evaluate the base model first"):
            prior.ensure_prior_artifacts(src, regenerate=False)


def _fold_predictions(days: list[date], fold_idx: list[int]) -> pl.DataFrame:
    n = len(days)
    return pl.DataFrame({
        "match_uid": [f"M{i}" for i in range(n)],
        "player_id": ["A"] * n,
        "effective_match_date": days,
        "fold_idx": fold_idx,
        "y_prob_cal": np.linspace(0.3, 0.7, n),
        "y_prob": np.linspace(0.2, 0.8, n),
    })


def _source(tmp_path: Path) -> prior.PriorSource:
    return prior.PriorSource(
        model="base", config_path=tmp_path / "base.yaml", fp="abcdef012345",
        eval_dir=tmp_path / "evals" / "abcdef012345",
    )


class TestSplice:
    def test_fold_oof_train_end_is_the_day_before_the_fold(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_predictions(
            [date(2024, 1, 5), date(2024, 1, 9), date(2024, 7, 2), date(2024, 7, 30)],
            [0, 0, 1, 1],
        ).write_parquet(src.fold_predictions)
        assert prior.prior_artifacts_ready(src)

        frame = prior.build_prior_frame(src)
        assert frame.height == 4
        assert set(frame["prior_kind"].to_list()) == {"fold_oof_nested_cal"}
        ends = dict(zip(frame["match_uid"].to_list(), frame["prior_train_end"].to_list()))
        assert ends["M0"] == ends["M1"] == date(2024, 1, 4)
        assert ends["M2"] == ends["M3"] == date(2024, 7, 1)
        np.testing.assert_allclose(
            frame["prior_logit"].to_numpy(),
            np.log(frame["prior_prob"].to_numpy() / (1 - frame["prior_prob"].to_numpy())),
        )

    def test_backtest_rows_spliced_with_fold_cutoffs(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_predictions([date(2024, 3, 1), date(2024, 3, 2)], [0, 0]).write_parquet(
            src.fold_predictions
        )
        pl.DataFrame({
            "match_uid": ["B1", "B2", "B0"],
            "player_id": ["A", "A", "A"],
            "effective_match_date": ["2025-01-10 00:00", "2025-02-20 00:00", "2024-12-01 00:00"],
            "model_prob": [0.6, 0.4, 0.5],
        }).write_csv(src.backtest_csv)
        bt = tmp_path / "backtests" / "lead" / "base"
        bt.mkdir(parents=True)
        for tag in ("2025-01-01", "2025-02-01"):
            (bt / f"lead_{tag}.joblib").write_bytes(b"")
        (bt / "lead_2025-01-01_cal_tiers.joblib").write_bytes(b"")

        frame = prior.build_prior_frame(src, backtests_root=tmp_path / "backtests")
        kinds = dict(zip(frame["match_uid"].to_list(), frame["prior_kind"].to_list()))
        assert kinds == {
            "M0": "fold_oof_nested_cal", "M1": "fold_oof_nested_cal",
            "B1": "backtest_fold_cal", "B2": "backtest_fold_cal",
        }  # B0 predates the first cutoff and is dropped
        ends = dict(zip(frame["match_uid"].to_list(), frame["prior_train_end"].to_list()))
        assert ends["B1"] == date(2024, 12, 31)
        assert ends["B2"] == date(2025, 1, 31)

    def test_refuses_uncalibrated_and_leaky_sources(self, tmp_path):
        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        _fold_predictions([date(2024, 1, 5)], [0]).drop("y_prob_cal").write_parquet(
            src.fold_predictions
        )
        assert not prior.prior_artifacts_ready(src)
        with pytest.raises(ValueError, match="y_prob_cal"):
            prior.build_prior_frame(src)
        # overlap between the two sources is refused, never silently merged
        _fold_predictions([date(2024, 3, 1)], [0]).write_parquet(src.fold_predictions)
        pl.DataFrame({
            "match_uid": ["M0"], "player_id": ["A"],
            "effective_match_date": ["2025-01-10 00:00"], "model_prob": [0.6],
        }).write_csv(src.backtest_csv)
        bt = tmp_path / "backtests" / "lead" / "base"
        bt.mkdir(parents=True)
        (bt / "lead_2025-01-01.joblib").write_bytes(b"")
        with pytest.raises(ValueError, match="both the fold OOF and the backtest"):
            prior.build_prior_frame(src, backtests_root=tmp_path / "backtests")

    def test_salt_tracks_the_artifacts(self, tmp_path):
        import os

        src = _source(tmp_path)
        src.eval_dir.mkdir(parents=True)
        assert src.salt() == "abcdef012345:-:-"
        _fold_predictions([date(2024, 1, 5)], [0]).write_parquet(src.fold_predictions)
        s1 = src.salt()
        os.utime(src.fold_predictions, (1_700_000_000, 1_700_000_000))
        assert src.salt() != s1
