"""`data.date_range.holdout_end` in the runner: `through_holdout=True` extends
the rows to holdout_end and holds out every fold that starts after `end`, so
the selection folds are scored exactly as without the flag and the held-out
year reaches only the `holdout_*` outputs."""

import importlib
import json
import random
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from mvp.model.runner import ExperimentRunner


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking

    importlib.reload(mvp.model.features.ranking)


def _write_matches(path: Path, first: date, last: date, seed: int = 11) -> Path:
    random.seed(seed)
    rows = []
    n_days = (last - first).days + 1
    for i in range(n_days * 2):
        d = first + timedelta(days=i // 2)
        pr, orank = random.randint(1, 200), random.randint(1, 200)
        won = random.random() < (0.65 if pr < orank else 0.35)
        me, other = f"P{i % 20:02d}", f"P{(i + 10) % 20:02d}"
        for pid, oid, a, b, w in (
            (me, other, pr, orank, won), (other, me, orank, pr, not won),
        ):
            rows.append({
                "match_uid": f"M{i:05d}",
                "player_id": pid, "opp_id": oid,
                "effective_match_date": d, "won": w,
                "player_rankings_points": 1000 - a * 4,
                "opp_rankings_points": 1000 - b * 4,
                "circuit": "tour",
            })
    pl.DataFrame(rows).write_parquet(path)
    return path


@pytest.fixture
def matches(tmp_path: Path) -> Path:
    return _write_matches(
        tmp_path / "matches.parquet", date(2023, 1, 1), date(2025, 6, 30)
    )


_MODELS = {
    "xgboost": "  type: xgboost\n  params:\n    n_estimators: 10\n    max_depth: 2\n",
    "logistic": "  type: logistic\n",
}


def _config(
    model: str = "xgboost",
    start: str = "2023-01-01",
    end: str = "2024-12-31",
    holdout_end: str | None = "2025-06-30",
    test_months: int = 6,
) -> str:
    holdout = f'    holdout_end: "{holdout_end}"\n' if holdout_end else ""
    return (
        "data:\n  date_range:\n"
        f'    start: "{start}"\n    end: "{end}"\n{holdout}'
        "features:\n  include:\n    - player_ranking_points_diff\n"
        f"model:\n{_MODELS[model]}"
        "validation:\n  type: date_expanding\n  initial_train_months: 12\n"
        f"  test_months: {test_months}\n"
    )


def _runner(
    tmp_path: Path, matches: Path, config: str, name: str = "cfg", **kw
) -> ExperimentRunner:
    cfg = tmp_path / f"{name}.yaml"
    cfg.write_text(config)
    kw.setdefault("log_to_mlflow", False)
    return ExperimentRunner(
        config_path=cfg, matches_path=matches, cache_dir=tmp_path / "cache", **kw,
    )


@pytest.fixture
def artifact_root(tmp_path, monkeypatch) -> Path:
    import mlflow

    import mvp.common.config_hash as config_hash

    # config_hash binds get_artifact_root at import; patch ITS reference so the
    # fingerprint artifacts land in tmp, never the real evaluations root.
    root = tmp_path / "data"
    monkeypatch.setattr(config_hash, "get_artifact_root", lambda: root)
    mlflow.set_tracking_uri(f"file://{tmp_path / 'mlruns'}")
    return root


class TestSelectionFoldsUnchanged:
    @pytest.mark.parametrize("model", ["xgboost", "logistic"])
    def test_rows_up_to_end_identical_with_flag_on_and_off(
        self, tmp_path, matches, model
    ):
        off = _runner(tmp_path, matches, _config(model)).run()
        on = _runner(tmp_path, matches, _config(model), through_holdout=True).run()

        assert off["n_folds"] == 2
        assert on["n_folds"] == 3
        assert on["holdout_fold_indices"] == [2]
        for a, b in zip(off["all_predictions"], on["all_predictions"][:2]):
            np.testing.assert_array_equal(a["y_prob"], b["y_prob"])
            np.testing.assert_array_equal(a["y_true"], b["y_true"])
        assert on["metrics"] == off["metrics"]
        assert on["fold_metrics"] == off["fold_metrics"]
        assert on["train_metrics"] == off["train_metrics"]
        assert on["fold_meta"] == off["fold_meta"]
        assert off["holdout_metrics"] is None

    def test_held_out_fold_only_in_holdout_outputs(self, tmp_path, matches):
        on = _runner(tmp_path, matches, _config(), through_holdout=True).run()
        assert [m["test_start"] for m in on["fold_meta"]] == [
            date(2024, 1, 1), date(2024, 7, 1),
        ]
        assert len(on["holdout_fold_meta"]) == 1
        assert on["holdout_fold_meta"][0]["test_start"] == date(2025, 1, 1)
        assert on["holdout_fold_meta"][0]["test_end"] == date(2025, 6, 30)
        assert on["holdout_metrics"]["log_loss"] > 0
        assert len(on["holdout_fold_metrics"]) == 1

    def test_flag_without_holdout_end_does_nothing(self, tmp_path, matches):
        cfg = _config(end="2025-06-30", holdout_end=None)
        on = _runner(tmp_path, matches, cfg, through_holdout=True).run()
        assert on["n_folds"] == 3
        assert on["holdout_metrics"] is None

    def test_flag_with_holdout_folds_raises(self, tmp_path, matches):
        with pytest.raises(ValueError, match="through_holdout sets holdout_folds itself"):
            _runner(
                tmp_path, matches, _config(), through_holdout=True, holdout_folds=1,
            )


class TestFoldPlacement:
    def test_straddling_fold_raises(self, tmp_path, matches):
        runner = _runner(
            tmp_path, matches, _config(end="2024-09-30"), through_holdout=True,
        )
        with pytest.raises(
            ValueError,
            match=r"fold 2 \(2024-07-01\.\.2025-01-01\) straddles end 2024-09-30; "
            "align end with the fold boundaries",
        ):
            runner.run()

    def test_no_fold_after_end_raises(self, tmp_path):
        short = _write_matches(
            tmp_path / "short.parquet", date(2023, 1, 1), date(2024, 12, 31)
        )
        runner = _runner(tmp_path, short, _config(), through_holdout=True)
        with pytest.raises(
            ValueError, match="no fold starts after end; holdout_end adds no held-out fold",
        ):
            runner.run()

    def test_no_fold_before_end_raises(self, tmp_path, matches):
        runner = _runner(
            tmp_path, matches, _config(end="2023-12-31"), through_holdout=True,
        )
        with pytest.raises(
            ValueError, match="no fold ends by end; nothing is left to select on",
        ):
            runner.run()


class TestArtifacts:
    def _fp_dirs(self, root: Path) -> list[Path]:
        evals = root / "model_evaluations"
        return list(evals.iterdir()) if evals.exists() else []

    def test_held_out_read_writes_is_holdout_and_the_holdout_block(
        self, tmp_path, matches, artifact_root
    ):
        r = _runner(
            tmp_path, matches, _config(), log_to_mlflow=True,
            mlflow_dir=tmp_path / "mlruns", through_holdout=True,
        ).run()
        (fp_dir,) = self._fp_dirs(artifact_root)

        fold = pl.read_parquet(fp_dir / "fold_predictions.parquet")
        assert fold["is_holdout"].dtype == pl.Boolean
        held = fold.filter(pl.col("is_holdout"))
        assert held["fold_idx"].unique().to_list() == [3]
        assert held["effective_match_date"].cast(pl.Date).min() == date(2025, 1, 1)
        kept = fold.filter(~pl.col("is_holdout"))
        assert kept["effective_match_date"].cast(pl.Date).max() <= date(2024, 12, 31)

        diag = json.loads((fp_dir / "diagnostics.json").read_text())
        block = diag["holdout"]
        assert block["end"] == "2024-12-31"
        assert block["holdout_end"] == "2025-06-30"
        assert block["n_folds"] == 1
        assert block["fold_meta"][0]["test_start"] == "2025-01-01"
        assert block["fold_meta"][0]["test_end"] == "2025-06-30"
        assert block["fold_meta"][0]["n_rows"] == held.height
        assert block["fold_meta"][0]["log_loss"] == pytest.approx(
            r["holdout_fold_metrics"][0]["log_loss"]
        )
        assert block["metrics"]["log_loss"] == pytest.approx(
            r["holdout_metrics"]["log_loss"]
        )

        # Feature importance averages the selection folds' models only.
        assert len(r["fold_feature_importances"]) == 3
        (feat,) = r["feature_columns"]
        selection = [fi.get(feat, 0.0) for fi in r["fold_feature_importances"][:2]]
        (row,) = diag["feature_importance"]
        assert row["mean_gain"] == pytest.approx(sum(selection) / 2)

    def test_no_holdout_block_without_held_out_folds(
        self, tmp_path, matches, artifact_root
    ):
        _runner(
            tmp_path, matches, _config(end="2025-06-30", holdout_end=None),
            log_to_mlflow=True, mlflow_dir=tmp_path / "mlruns",
        ).run()
        (fp_dir,) = self._fp_dirs(artifact_root)
        assert "holdout" not in json.loads((fp_dir / "diagnostics.json").read_text())
        fold = pl.read_parquet(fp_dir / "fold_predictions.parquet")
        assert not fold["is_holdout"].any()

    def test_flag_off_run_on_a_holdout_config_writes_nothing(
        self, tmp_path, matches, artifact_root, caplog
    ):
        with caplog.at_level("INFO", logger="mvp.model.runner"):
            _runner(
                tmp_path, matches, _config(), log_to_mlflow=True,
                mlflow_dir=tmp_path / "mlruns",
            ).run()
        assert self._fp_dirs(artifact_root) == []
        assert (
            "not writing artifacts: this config has holdout_end; only "
            "held-out-read runs write its evaluation"
        ) in caplog.text


class TestEnsembleWideFrame:
    def test_df_wide_extends_to_holdout_end(self, tmp_path):
        """A base model with an earlier start trains each fold from the wide
        frame. The second held-out fold trains on rows after `end`, so its raw
        predictions match the run whose `end` IS holdout_end only when the wide
        frame reaches holdout_end too."""
        matches = _write_matches(
            tmp_path / "wide.parquet", date(2022, 7, 1), date(2025, 6, 30)
        )
        base = tmp_path / "base_a.yaml"
        base.write_text(_config(start="2022-07-01", holdout_end=None))

        def ensemble(end: str, holdout_end: str | None) -> str:
            holdout = f'    holdout_end: "{holdout_end}"\n' if holdout_end else ""
            return (
                "data:\n  date_range:\n"
                f'    start: "2023-01-01"\n    end: "{end}"\n{holdout}'
                "model:\n  type: ensemble\n  params:\n    strategy: average\n"
                f"    base_models:\n      - config: {base.as_posix()}\n"
                "validation:\n  type: date_expanding\n  initial_train_months: 12\n"
                "  test_months: 3\n"
            )

        on = _runner(
            tmp_path, matches, ensemble("2024-12-31", "2025-06-30"), name="ens_on",
            through_holdout=True,
        ).run()
        full = _runner(
            tmp_path, matches, ensemble("2025-06-30", None), name="ens_full",
        ).run()
        assert on["holdout_fold_indices"] == [4, 5]
        assert full["n_folds"] == 6
        np.testing.assert_allclose(
            on["all_predictions"][5]["y_prob_raw"],
            full["all_predictions"][5]["y_prob_raw"],
        )


class TestHoldoutPriorCheck:
    _STEM = "base_lr"

    def _stage_config(self) -> str:
        return (
            "data:\n  date_range:\n"
            '    start: "2023-01-01"\n    end: "2024-12-31"\n'
            '    holdout_end: "2025-06-30"\n'
            "features:\n  include:\n    - player_ranking_points_diff\n"
            "model:\n  type: xgboost\n  params:\n    n_estimators: 5\n"
            f"offset:\n  prior: {self._STEM}\n"
            "validation:\n  type: date_expanding\n  initial_train_months: 12\n"
            "  test_months: 6\n"
        )

    def _frame(self, after_end: list[float | None]) -> pl.DataFrame:
        days = [date(2024, 12, 30), date(2024, 12, 31)] + [
            date(2025, 1, 1) + timedelta(days=i) for i in range(len(after_end))
        ] + [date(2026, 1, 5)]
        prior = [0.1, -0.2, *after_end, 0.3]
        return pl.DataFrame({
            "effective_match_date": days,
            f"player_prior_logit_{self._STEM}": pl.Series(prior, dtype=pl.Float64),
        })

    def test_all_null_after_end_raises(self, tmp_path, matches):
        runner = _runner(
            tmp_path, matches, self._stage_config(), through_holdout=True,
        )
        with pytest.raises(
            ValueError,
            match=f"prior {self._STEM} has no predictions after 2024-12-31; "
            "evaluate the base model through its holdout first",
        ):
            runner._check_holdout_prior(self._frame([None, None, None]))

    def test_partly_null_after_end_passes(self, tmp_path, matches):
        runner = _runner(
            tmp_path, matches, self._stage_config(), through_holdout=True,
        )
        runner._check_holdout_prior(self._frame([None, 0.4, None]))

    def test_flag_off_skips_the_check(self, tmp_path, matches):
        runner = _runner(tmp_path, matches, self._stage_config())
        runner._check_holdout_prior(self._frame([None, None, None]))

    def test_run_checks_before_filtering(self, tmp_path, matches, monkeypatch):
        """The not_null filter would silently drop the whole held-out year;
        run() refuses first."""
        import mvp.model.runner as runner_mod

        runner = _runner(
            tmp_path, matches, self._stage_config(), through_holdout=True,
        )
        # The prior column's not_null filter key would otherwise be looked up
        # in the real matches.parquet schema.
        monkeypatch.setattr(runner_mod, "get_filter_feature_specs", lambda f: [])
        monkeypatch.setattr(runner, "_resolve_prior_sources", lambda specs: None)
        monkeypatch.setattr(
            runner.engine, "compute",
            lambda specs, extra_columns=None: self._frame([None, None]),
        )
        with pytest.raises(ValueError, match="has no predictions after 2024-12-31"):
            runner.run()
