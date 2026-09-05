"""Production owns its prior artifacts (mvp.model.prior_promotion).

The failures being pinned: production read its prior inputs from
fingerprint-keyed scratch that the weekly wipe deletes from, ad-hoc runs
overwrite, config edits relocate, and code changes silently stale. `mvp train`
now regenerates every dependency base-first and copies it to a promoted store
that `resolve_prior` prefers; the tick preflights that store before scoring.
"""

import json
import textwrap
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import joblib
import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model import prior_promotion as pp
from mvp.model.features import prior

# --- fixtures ---------------------------------------------------------------

_LEAD_CFG = {
    "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
    "features": {"include": ["player_elo_surface_diff"]},
    "model": {"type": "xgboost", "params": {"n_estimators": 5, "n_jobs": 1}},
    "target": "won",
}


def _stage_cfg(base: str) -> dict:
    return {
        "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "features": {"include": ["player_elo_surface_diff"]},
        "model": {"type": "xgboost", "params": {"n_estimators": 5, "n_jobs": 1}},
        "offset": {"prior": base},
        "target": "won",
    }


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


def _write(d: Path, stem: str, cfg: dict | str) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{stem}.yaml"
    p.write_text(cfg if isinstance(cfg, str) else yaml.dump(cfg), encoding="utf-8")
    return p


def _fold_predictions() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "match_uid": ["M0", "M1"],
            "player_id": ["A", "A"],
            "effective_match_date": [date(2024, 3, 1), date(2024, 3, 2)],
            "fold_idx": [0, 0],
            "y_prob_cal": [0.6, 0.4],
        }
    )


def _backtest_csv() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "match_uid": ["B1", "B2"],
            "player_id": ["A", "A"],
            "effective_match_date": ["2025-01-10 00:00", "2025-02-20 00:00"],
            "model_prob": [0.6, 0.4],
        }
    )


def _fold_match_win() -> pl.DataFrame:
    from mvp.common.chain_shape import SHAPE_COLUMNS

    days = [date(2025, 1, 5), date(2025, 1, 9), date(2025, 7, 2), date(2025, 7, 8)]
    n = len(days)
    return pl.DataFrame(
        {
            "match_uid": [f"M{i}" for i in range(n)],
            "player_id": [f"A{i}" for i in range(n)],
            "opp_id": [f"B{i}" for i in range(n)],
            "effective_match_date": days,
            "fold_idx": pl.Series([1, 1, 2, 2], dtype=pl.Int32),
            "p_match_win_a": list(np.linspace(0.55, 0.75, n)),
            "won_a": pl.Series([1, 0, 1, 0], dtype=pl.Int8),
            **{c: [0.5] * n for c in SHAPE_COLUMNS},
        }
    )


def _pmf() -> pl.DataFrame:
    from mvp.common.chain_shape import SHAPE_COLUMNS

    return pl.DataFrame(
        {
            "match_uid": ["F0"],
            "player_id": ["A0"],
            "opp_id": ["B0"],
            "effective_match_date": [date(2026, 2, 1)],
            "p_match_win_a": [0.6],
            **{c: [0.5] for c in SHAPE_COLUMNS},
        }
    )


@pytest.fixture
def roots(tmp_path, monkeypatch):
    """Every resolution seam pointed at tmp: configs, evaluations, backtests,
    projections, and the promoted store."""
    monkeypatch.setattr(prior, "CONFIG_DIRS", (tmp_path / "models",))
    monkeypatch.setattr(prior, "PROJECTION_CONFIG_DIRS", (tmp_path / "projections",))
    monkeypatch.setattr(prior, "EVALUATIONS_ROOT", tmp_path / "evals")
    monkeypatch.setattr(prior, "PROJECTION_EVALUATIONS_ROOT", tmp_path / "pe")
    monkeypatch.setattr(prior, "BACKTESTS_ROOT", tmp_path / "backtests")
    monkeypatch.setattr(prior, "PROMOTED_PRIORS_ROOT", tmp_path / "promoted")
    prior._cached_frame.cache_clear()
    return tmp_path


def _model_evaluation(roots: Path, stem: str, *, with_ledger=True, with_joblibs=True):
    """A complete model-kind evaluation for `stem` where the producers write it."""
    _write(roots / "models", stem, _LEAD_CFG)
    src = prior.resolve_prior(stem, promoted=False)
    src.eval_dir.mkdir(parents=True, exist_ok=True)
    _fold_predictions().write_parquet(src.fold_predictions)
    (src.eval_dir / "source.txt").write_text(f"{stem}\tdeadbeef\t2026-01-01\n")
    if with_ledger:
        _backtest_csv().write_csv(src.backtest_csv)
    if with_joblibs:
        bt = roots / "backtests" / "lead" / stem
        bt.mkdir(parents=True, exist_ok=True)
        for tag in ("2025-01-01", "2025-02-01"):
            (bt / f"lead_{tag}.joblib").write_bytes(b"")
    return src


def _projection_evaluation(
    roots: Path, stem: str, *, projector_text: str | None = None
):
    """A complete projection-kind evaluation for `stem`. `projector_text` is the
    config text stored in the projector; defaults to the file's own."""
    cfg_path = _write(roots / "projections", stem, _PROJ_YAML)
    src = prior.resolve_prior(stem, promoted=False)
    src.eval_dir.mkdir(parents=True, exist_ok=True)
    _fold_match_win().write_parquet(src.fold_match_win)
    _pmf().write_parquet(src.pmf_parquet)
    joblib.dump(
        {
            "serve_model": object(),
            "config_path": str(cfg_path),
            "config_yaml": projector_text if projector_text is not None else _PROJ_YAML,
            "n_train": 10,
        },
        src.eval_dir / "serve_model.joblib",
    )
    return src


# --- what a config depends on ------------------------------------------------


class TestDeclaredPriorStems:
    def test_every_declaration_site_is_read(self):
        cfg = SimpleNamespace(
            features=SimpleNamespace(
                include=["player_elo_diff", "player_prior_logit(model=a)"],
                compute_only=["chain_shape(model=b)"],
            ),
            data=SimpleNamespace(
                filters={"player_prior_logit_c": "not_null", "circuit": ["tour"]}
            ),
            offset=SimpleNamespace(feature="player_prior_logit(model=d)"),
        )
        stems = pp.declared_prior_stems(
            cfg,
            resolved_specs=["player_prior_logit(model=e)", "player_age_diff"],
            entry_filters={"player_prior_logit_f": "not_null", "draw_type": "singles"},
        )
        # declaration order: include, compute_only, config filters, entry
        # filters, offset, ensemble union
        assert stems == ["a", "b", "c", "f", "d", "e"]

    def test_deduplicates_and_ignores_non_priors(self):
        cfg = SimpleNamespace(
            features=SimpleNamespace(
                include=["player_prior_logit(model=a)", "player_prior_logit(model=a)"],
                compute_only=None,
            ),
            data=SimpleNamespace(filters=None),
            offset=None,
        )
        assert pp.declared_prior_stems(cfg) == ["a"]
        assert pp.prior_stem_of("player_melo_diff") is None
        assert pp.prior_stem_of("chain_shape(model='x')") == "x"

    def test_ensemble_without_features_block(self):
        cfg = SimpleNamespace(
            features=None, data=SimpleNamespace(filters=None), offset=None
        )
        assert pp.declared_prior_stems(cfg, ["player_prior_logit(model=z)"]) == ["z"]


class TestDependencyOrder:
    def test_base_comes_before_the_stage_that_offsets_on_it(self, roots):
        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        order = [s.model for s in pp.dependency_order(["stage1"])]
        assert order == ["lead", "stage1"]

    def test_each_stem_once_even_when_reached_twice(self, roots):
        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        order = [s.model for s in pp.dependency_order(["lead", "stage1", "lead"])]
        assert order == ["lead", "stage1"]

    def test_recurses_through_an_ensemble_priors_bases(self, roots):
        """An ensemble's design matrix is its bases' `features.include`; a prior
        a base declares is a dependency of anything offsetting on the ensemble."""
        _write(roots / "models", "lead", _LEAD_CFG)
        base = dict(_LEAD_CFG)
        base["features"] = {"include": ["player_prior_logit(model=lead)"]}
        base_path = _write(roots / "models", "base_a", base)
        _write(
            roots / "models",
            "ens",
            {
                "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
                "model": {
                    "type": "ensemble",
                    "params": {"base_models": [{"config": str(base_path)}]},
                },
                "target": "won",
            },
        )
        _write(roots / "models", "stage1", _stage_cfg("ens"))
        order = [s.model for s in pp.dependency_order(["stage1"])]
        assert order == ["lead", "ens", "stage1"]

    def test_cycle_is_refused(self, roots):
        _write(roots / "models", "a", _stage_cfg("b"))
        _write(roots / "models", "b", _stage_cfg("a"))
        with pytest.raises(ValueError, match="cycle"):
            pp.dependency_order(["a"])

    def test_resolves_the_evaluation_side_not_the_promoted_copy(self, roots):
        """Ordering must work for a stem that is not promoted yet, and must
        not be redirected by a promoted copy that already exists."""
        src = _model_evaluation(roots, "lead")
        pp.promote_prior("lead")
        (order,) = pp.dependency_order(["lead"])
        assert order.eval_dir == src.eval_dir
        assert prior.resolve_prior("lead").eval_dir == prior.promoted_dir("lead")


# --- regenerate ---------------------------------------------------------------


class TestRegeneratePrior:
    def test_model_kind_runs_evaluation_then_backtest_with_retrain(
        self, roots, monkeypatch
    ):
        import mvp.model.backtest as bt
        import mvp.model.runner as runner

        _write(roots / "models", "lead", _LEAD_CFG)
        src = prior.resolve_prior("lead", promoted=False)
        calls: list[tuple] = []

        class _Runner:
            def __init__(self, config_path):
                calls.append(("evaluate", Path(config_path)))

            def run(self):
                return {}

        def _backtest(config_path, **kw):
            calls.append(("backtest", Path(config_path), kw.get("retrain")))

        monkeypatch.setattr(runner, "ExperimentRunner", _Runner)
        monkeypatch.setattr(bt, "run_backtest", _backtest)
        # Fold artifacts from an earlier validation schedule: the backtest
        # never clears them, and promotion would freeze the stale union of
        # boundaries into cutoffs.json.
        monkeypatch.setattr(bt, "ARTIFACT_ROOT", roots / "backtests" / "lead")
        stale = roots / "backtests" / "lead" / "lead"
        stale.mkdir(parents=True)
        (stale / "lead_2019-01-01.joblib").write_bytes(b"")
        pp.regenerate_prior(src)
        assert calls == [
            ("evaluate", src.config_path),
            ("backtest", src.config_path, True),
        ]
        assert not stale.exists()

    def test_projection_kind_runs_walk_forward_then_refits_projector(
        self, roots, monkeypatch
    ):
        import mvp.projection.iid.projection_run as proj_run
        import mvp.projection.iid.runner as proj_runner

        _write(roots / "projections", "proj", _PROJ_YAML)
        src = prior.resolve_prior("proj", promoted=False)
        calls: list[tuple] = []

        class _Runner:
            def __init__(self, config_path):
                calls.append(("walk_forward", Path(config_path)))

            def run(self):
                return {}

        def _projection(config_path, **kw):
            calls.append(("forward", Path(config_path), kw.get("retrain")))

        monkeypatch.setattr(proj_runner, "IIDProjectionRunner", _Runner)
        monkeypatch.setattr(proj_run, "run_projection", _projection)
        pp.regenerate_prior(src)
        # retrain=True is the point: train_or_load would keep a projector whose
        # config text still matches, which is how a pre-fix fit survives.
        assert calls == [
            ("walk_forward", src.config_path),
            ("forward", src.config_path, True),
        ]


# --- promote -------------------------------------------------------------------


class TestPromoteModelKind:
    def test_copies_artifacts_writes_cutoffs_and_redirects_resolution(self, roots):
        src = _model_evaluation(roots, "lead")
        dst = pp.promote_prior("lead")

        assert dst == prior.promoted_dir("lead")
        for name in (
            "fold_predictions.parquet",
            "backtest.csv",
            "source.txt",
            "cutoffs.json",
            "promoted.json",
        ):
            assert (dst / name).exists(), name
        assert json.loads((dst / "cutoffs.json").read_text()) == [
            "2025-01-01",
            "2025-02-01",
        ]
        manifest = json.loads((dst / "promoted.json").read_text())
        assert manifest["stem"] == "lead" and manifest["kind"] == "model"
        assert manifest["fingerprint"] == src.fp
        assert not list(dst.glob("*.tmp"))

        # Everything downstream now resolves to the promoted copy; promotion
        # itself still reaches the evaluation dir.
        assert prior.resolve_prior("lead").eval_dir == dst
        assert prior.resolve_prior("lead", promoted=False).eval_dir == src.eval_dir

    def test_promoted_forward_rows_do_not_need_the_lead_joblibs(self, roots):
        """The wipe deletes backtests/lead too. cutoffs.json carries the fold
        dates, so the forward splice survives that."""
        import shutil

        _model_evaluation(roots, "lead")
        pp.promote_prior("lead")
        shutil.rmtree(roots / "backtests")

        frame = prior.build_prior_frame(prior.resolve_prior("lead"))
        kinds = dict(zip(frame["match_uid"].to_list(), frame["prior_kind"].to_list()))
        assert kinds["B1"] == "backtest_fold_cal"
        assert kinds["B2"] == "backtest_fold_cal"
        ends = dict(
            zip(frame["match_uid"].to_list(), frame["prior_train_end"].to_list())
        )
        assert ends["B1"] == date(2024, 12, 31)
        assert ends["B2"] == date(2025, 1, 31)

    def test_repromotion_replaces_in_place(self, roots):
        src = _model_evaluation(roots, "lead")
        pp.promote_prior("lead")
        pl.DataFrame(
            {
                "match_uid": ["B9"],
                "player_id": ["A"],
                "effective_match_date": ["2025-02-25 00:00"],
                "model_prob": [0.7],
            }
        ).write_csv(src.backtest_csv)
        dst = pp.promote_prior("lead")
        assert pl.read_csv(dst / "backtest.csv")["match_uid"].to_list() == ["B9"]
        assert dst.is_dir()

    def test_refuses_an_incomplete_evaluation_and_copies_nothing(self, roots):
        _model_evaluation(roots, "lead", with_ledger=False)
        with pytest.raises(pp.PreflightError, match="forward artifact"):
            pp.promote_prior("lead")
        assert not prior.promoted_dir("lead").exists()

    def test_refuses_a_stem_whose_config_copies_differ(self, roots, monkeypatch):
        """Scratch is searched before the versioned dir. A serving box has only
        the versioned copy, so a promotion from a differing scratch copy would
        be a promotion of a config that box never resolves."""
        scratch, versioned = roots / "models", roots / "models" / "production"
        monkeypatch.setattr(prior, "CONFIG_DIRS", (scratch, versioned))
        _model_evaluation(roots, "lead")  # writes the scratch copy
        _write(versioned, "lead", _LEAD_CFG)  # identical: fine
        pp.promote_prior("lead")

        edited = dict(_LEAD_CFG)
        edited["model"] = {
            "type": "xgboost", "params": {"n_estimators": 9, "n_jobs": 1},
        }
        _write(versioned, "lead", edited)
        with pytest.raises(pp.PreflightError, match="fingerprint differently"):
            pp.promote_prior("lead")
        with pytest.raises(pp.PreflightError, match="fingerprint differently"):
            pp.verify_promoted("lead")

    def test_refuses_when_the_ledger_cannot_be_dated(self, roots):
        _model_evaluation(roots, "lead", with_joblibs=False)
        with pytest.raises(pp.PreflightError, match="cannot be dated"):
            pp.promote_prior("lead")


class TestPromoteProjectionKind:
    def test_copies_the_three_artifacts_and_the_projector_resolves_there(self, roots):
        from mvp.model.projection_serving import check_served_projector

        _projection_evaluation(roots, "proj")
        dst = pp.promote_prior("proj")
        for name in (
            "fold_match_win.parquet",
            "total_games_pmf.parquet",
            "serve_model.joblib",
            "promoted.json",
        ):
            assert (dst / name).exists(), name
        assert not (dst / "cutoffs.json").exists()
        assert check_served_projector("proj") == dst / "serve_model.joblib"
        assert pp.verify_promoted("proj").eval_dir == dst

    def test_refuses_a_projector_from_different_config_text(self, roots):
        """The projector and the pmf must come from the same evaluation. A
        projector whose stored config text differs is not that."""
        _projection_evaluation(
            roots, "proj", projector_text=_PROJ_YAML + "\n# edited\n"
        )
        with pytest.raises(pp.PreflightError, match="different config text"):
            pp.promote_prior("proj")
        assert not prior.promoted_dir("proj").exists()


# --- preflight -----------------------------------------------------------------


class TestVerifyPromoted:
    def test_unpromoted_stem_names_the_command(self, roots):
        _model_evaluation(roots, "lead")
        with pytest.raises(pp.PreflightError, match="not promoted") as exc:
            pp.verify_promoted("lead")
        assert "mvp train" in str(exc.value)

    def test_missing_manifest_means_promotion_did_not_complete(self, roots):
        """The manifest is written last. Without it the copy is not trusted:
        resolution does not redirect there and preflight says so."""
        src = _model_evaluation(roots, "lead")
        dst = pp.promote_prior("lead")
        (dst / "promoted.json").unlink()
        assert prior.resolve_prior("lead").eval_dir == src.eval_dir
        with pytest.raises(pp.PreflightError, match="not promoted"):
            pp.verify_promoted("lead")

    def test_promoted_copy_of_an_edited_config_is_not_used(self, roots):
        """Editing the config moves its fingerprint. The promotion of the OLD
        text is not an evaluation of the new one, so resolution falls through
        to the (empty) fingerprint dir and preflight names the mismatch."""
        _model_evaluation(roots, "lead")
        dst = pp.promote_prior("lead")
        edited = dict(_LEAD_CFG)
        edited["model"] = {
            "type": "xgboost",
            "params": {"n_estimators": 7, "n_jobs": 1},
        }
        _write(roots / "models", "lead", edited)

        src = prior.resolve_prior("lead")
        assert src.eval_dir != dst
        assert src.eval_dir == prior.resolve_prior("lead", promoted=False).eval_dir
        with pytest.raises(pp.PreflightError, match="config changed since promotion"):
            pp.verify_promoted("lead")

    def test_missing_cutoffs_is_refused(self, roots):
        _model_evaluation(roots, "lead")
        dst = pp.promote_prior("lead")
        (dst / "cutoffs.json").unlink()
        with pytest.raises(pp.PreflightError, match="cutoffs.json"):
            pp.verify_promoted("lead")

    def test_deleted_or_schema_stale_copy_is_not_used(self, roots):
        """A promoted copy that fails the readers' readiness checks is not
        redirected to -- otherwise a reader-schema change would pin every
        reader on a copy only `mvp train` can replace -- and preflight says
        why it fell through."""
        src = _model_evaluation(roots, "lead")
        dst = pp.promote_prior("lead")
        (dst / "fold_predictions.parquet").unlink()
        assert prior.resolve_prior("lead").eval_dir == src.eval_dir
        with pytest.raises(pp.PreflightError, match="incomplete or predates"):
            pp.verify_promoted("lead")

    def test_crash_mid_repromotion_leaves_the_store_unpromoted(
        self, roots, monkeypatch
    ):
        """The previous manifest is removed before the first copy. A crash in
        the copy window must not leave a mixed set that resolves as promoted."""
        src = _model_evaluation(roots, "lead")
        pp.promote_prior("lead")
        assert prior.resolve_prior("lead").eval_dir == prior.promoted_dir("lead")

        calls = {"n": 0}
        real = pp._replace_copy

        def _dies_on_second(a, b):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("box lost power")
            real(a, b)

        monkeypatch.setattr(pp, "_replace_copy", _dies_on_second)
        with pytest.raises(RuntimeError, match="lost power"):
            pp.promote_prior("lead")
        assert not (prior.promoted_dir("lead") / "promoted.json").exists()
        assert prior.resolve_prior("lead").eval_dir == src.eval_dir
        with pytest.raises(pp.PreflightError, match="not promoted"):
            pp.verify_promoted("lead")


# --- the predictor's hooks ---------------------------------------------------------


def _production(
    roots: Path, *, active: str, stages=(), voters=(), active_filters=None
) -> Path:
    prod = {
        "active": {
            "config": str(roots / "models" / f"{active}.yaml"),
            "artifact": str(roots / f"{active}.joblib"),
            "train_date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "filters": active_filters or {},
        },
        "stages": [
            {
                "config": str(roots / "models" / f"{s}.yaml"),
                "artifact": str(roots / f"{s}.joblib"),
                "train_date_range": {"start": "2024-01-01", "end": "2024-12-31"},
                "filters": {f"player_prior_logit_{active}": "not_null"},
            }
            for s in stages
        ],
        "voters": [
            {
                "name": v,
                "config": str(roots / "models" / f"{v}.yaml"),
                "artifact": str(roots / f"{v}.joblib"),
                "scoped": False,
            }
            for v in voters
        ],
        "history": [],
    }
    p = roots / "production.yaml"
    p.write_text(yaml.dump(prod))
    return p


def _predictor(prod_path: Path, roots: Path):
    from mvp.model.predictor import ProductionPredictor

    return ProductionPredictor(
        production_config_path=prod_path,
        matches_path=roots / "matches.parquet",
        cache_dir=roots / "cache",
        predictions_path=roots / "predictions.parquet",
    )


class TestPredictorHooks:
    def test_prior_dependencies_span_every_entry(self, roots):
        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        _write(roots / "models", "voter1", _stage_cfg("lead"))
        prod = _production(roots, active="lead", stages=["stage1"], voters=["voter1"])
        assert _predictor(prod, roots).prior_dependencies() == ["lead"]

    def test_promote_priors_regenerates_then_promotes_base_first(
        self, roots, monkeypatch
    ):
        import mvp.model.predictor as pred

        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        _write(roots / "models", "stage2", _stage_cfg("stage1"))
        prod = _production(roots, active="lead", stages=["stage1", "stage2"])
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            pred,
            "regenerate_prior",
            lambda s: calls.append(("regenerate", s.model)),
        )
        monkeypatch.setattr(
            pred,
            "promote_prior",
            lambda stem: calls.append(("promote", stem)),
        )
        promoted = _predictor(prod, roots).promote_priors()
        assert promoted == ["lead", "stage1"]
        assert calls == [
            ("regenerate", "lead"),
            ("promote", "lead"),
            ("regenerate", "stage1"),
            ("promote", "stage1"),
        ]

    def test_preflight_verifies_every_dependency(self, roots, monkeypatch):
        import mvp.model.predictor as pred

        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        prod = _production(roots, active="lead", stages=["stage1"])
        checked: list[str] = []
        monkeypatch.setattr(pred, "verify_promoted", lambda stem: checked.append(stem))
        assert _predictor(prod, roots).preflight() == ["lead"]
        assert checked == ["lead"]

    def test_preflight_refuses_without_promotion(self, roots):
        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        prod = _production(roots, active="lead", stages=["stage1"])
        with pytest.raises(pp.PreflightError, match="not promoted"):
            _predictor(prod, roots).preflight()

    def test_active_offsetting_on_a_prior_is_refused(self, roots):
        """Nothing fills an offset prior for the lead: `active` has no
        upstream. Both promotion and preflight refuse the shape."""
        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        prod = _production(roots, active="stage1")
        with pytest.raises(pp.PreflightError, match="offsets on prior"):
            _predictor(prod, roots).preflight()
        with pytest.raises(pp.PreflightError, match="offsets on prior"):
            _predictor(prod, roots).promote_priors()

    def test_lead_without_priors_has_nothing_to_promote(self, roots, monkeypatch):
        import mvp.model.predictor as pred

        _write(roots / "models", "lead", _LEAD_CFG)
        prod = _production(roots, active="lead")
        monkeypatch.setattr(
            pred,
            "regenerate_prior",
            lambda s: (_ for _ in ()).throw(AssertionError("regenerated")),
        )
        p = _predictor(prod, roots)
        assert p.promote_priors() == []
        assert p.preflight() == []


class TestSharesLeadDomain:
    def test_prior_clause_and_list_order_are_ignored(self):
        from mvp.model.predictor import _shares_lead_domain

        stage = {
            "circuit": ["chal", "tour"],
            "draw_type": "singles",
            "player_prior_logit_lead": "not_null",
        }
        lead = {"circuit": ["tour", "chal"], "draw_type": "singles"}
        assert _shares_lead_domain(stage, lead)
        assert _shares_lead_domain(None, {})
        assert _shares_lead_domain({"player_prior_logit_x": "not_null"}, None)

    def test_narrower_stage_does_not_share(self):
        from mvp.model.predictor import _shares_lead_domain

        lead = {"circuit": ["tour", "chal"], "draw_type": "singles"}
        assert not _shares_lead_domain({**lead, "surface": "Clay"}, lead)
        assert not _shares_lead_domain(
            {"circuit": ["tour"], "draw_type": "singles"}, lead,
        )


class TestStageScoringNothing:
    def test_zero_scored_is_a_stage_error(self, roots, monkeypatch):
        """A stage that raises degrades loudly; one that scores 0/N used to
        degrade silently. Same outcome on the sheet, so same report."""
        _write(roots / "models", "lead", _LEAD_CFG)
        _write(roots / "models", "stage1", _stage_cfg("lead"))
        prod = _production(roots, active="lead", stages=["stage1"])
        p = _predictor(prod, roots)
        monkeypatch.setattr(p, "_predict_raw", lambda *a, **k: {})
        predictions = pl.DataFrame(
            {
                "match_uid": ["M1", "M2"],
                "p1_id": ["A", "C"],
                "p2_id": ["B", "D"],
                "p1_win_prob": [0.6, 0.4],
            }
        )
        out = p._apply_stages(predictions, p.config["stages"], None)
        assert p._stage_errors == ["stage stage1: scored 0/2 matches"]
        assert out["model_version"].to_list() == ["lead", "lead"]

    def _predictions(self):
        return pl.DataFrame({
            "match_uid": ["M1", "M2"], "p1_id": ["A", "C"], "p2_id": ["B", "D"],
            "p1_win_prob": [0.6, 0.4],
        })

    def test_narrower_scoped_stage_scoring_nothing_is_its_normal_day(
        self, roots, monkeypatch
    ):
        """A stage scoped to clay on a hard-court day scores 0/N by design;
        the degrade to the lead is the intended behaviour, not an alert."""
        _write(roots / "models", "lead", _LEAD_CFG)
        clay = _stage_cfg("lead")
        clay["data"] = {**clay["data"], "filters": {"surface": "Clay"}}
        _write(roots / "models", "stage1", clay)
        prod = _production(roots, active="lead", stages=["stage1"])
        p = _predictor(prod, roots)
        monkeypatch.setattr(p, "_predict_raw", lambda *a, **k: {})
        p._apply_stages(self._predictions(), p.config["stages"], None)
        assert p._stage_errors == []

    def test_unscoped_stage_scoring_nothing_is_an_error_regardless(
        self, roots, monkeypatch
    ):
        """`scoped: false` means the stage must score everything the lead did,
        whatever its own config filters say."""
        _write(roots / "models", "lead", _LEAD_CFG)
        clay = _stage_cfg("lead")
        clay["data"] = {**clay["data"], "filters": {"surface": "Clay"}}
        _write(roots / "models", "stage1", clay)
        prod = _production(roots, active="lead", stages=["stage1"])
        raw = yaml.safe_load(prod.read_text())
        raw["stages"][0]["scoped"] = False
        prod.write_text(yaml.dump(raw))
        p = _predictor(prod, roots)
        monkeypatch.setattr(p, "_predict_raw", lambda *a, **k: {})
        p._apply_stages(self._predictions(), p.config["stages"], None)
        assert p._stage_errors == ["stage stage1: scored 0/2 matches"]
