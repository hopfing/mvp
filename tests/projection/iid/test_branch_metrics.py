"""Per-branch selection metrics (plan: 2026-09-03-per-branch-selection-metrics).

A `serve_component` FS run can be scored on the branch's own target instead of
the composed chain: win_first on `point_won_by_server` over `serve == 1` rows,
win_second over `serve == 2`, first_in on the (match, server) first-serve rate.
The chain metrics are untouched; `metric:` picks.

Also covers the two pre-existing defects the work had to fix first — D1
(first_in point-level candidates were inert) and D2 (promotion wrote an
objective the tune cannot use).
"""

from datetime import date, timedelta
from textwrap import dedent

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid import serve_discovery as sd
from mvp.projection.iid.config import (
    IIDMetricsConfig,
    IIDProjectionConfig,
    ServeDiscoveryConfig,
)
from mvp.projection.iid.metric_registry import (
    METRICS,
    base_metric_of,
    is_branch_metric,
    is_chain_metric,
    needs_match_grain_prep,
)
from mvp.projection.iid.serve_discovery import ServeDiscoverySelector
from mvp.projection.iid.serve_model import apply_serve_branch
from mvp.projection.iid.two_level_serve_model import (
    FIRST_IN,
    WIN_FIRST,
    WIN_SECOND,
    FirstServeInModel,
    TwoLevelServeModel,
    _ConstantBranch,
)

from tests.projection.iid.test_serve_discovery_swap_side import (
    MIRROR_SPEC,
    MirroringFakeEngine,
)

_SCORER = dedent("""
    scoring_model:
      type: xgboost
      params:
        n_estimators: 8
        max_depth: 2
        learning_rate: 0.1
        tree_method: hist
        random_state: 42
        n_jobs: 1
""")


def _matches(n: int) -> pl.DataFrame:
    rows = []
    for i in range(n):
        a, b = 1000 + i, 2000 + i
        for player_id, opp_id in ((a, b), (b, a)):
            rows.append({
                "match_uid": f"m{i:03d}",
                "player_id": player_id,
                "opp_id": opp_id,
                "best_of": 3,
                "won": player_id == a,
                "effective_match_date": date(2024, 1, 1) + timedelta(days=i),
                "reason": None,
                "surface": "hard" if i % 2 else "clay",
                "circuit": "tour",
                "draw_type": "singles",
                "player_set1_games": 6, "player_set2_games": 4, "player_set3_games": 6,
                "player_set4_games": None, "player_set5_games": None,
                "opp_set1_games": 4, "opp_set2_games": 6, "opp_set3_games": 3,
                "opp_set4_games": None, "opp_set5_games": None,
            })
    return pl.DataFrame(rows)


def _points(n_matches: int, per_match: int = 60) -> pl.DataFrame:
    """Both players serve. First-serve-in rate and point outcome both depend
    on the server, so a fitted branch can beat a constant on either target."""
    rng = np.random.default_rng(5)
    rows = []
    for i in range(n_matches):
        a, b = 1000 + i, 2000 + i
        surface = "hard" if i % 2 else "clay"
        for k in range(per_match):
            server, returner = (a, b) if (k // 5) % 2 == 0 else (b, a)
            fi_p = 0.70 if server == a else 0.54
            first_in = rng.random() < fi_p
            win_p = (0.72 if first_in else 0.50) + (0.04 if server == a else 0.0)
            rows.append({
                "match_uid": f"m{i:03d}",
                "server_id": server,
                "returner_id": returner,
                "serve": 1 if first_in else 2,
                "point_won_by_server": bool(rng.random() < win_p),
                "is_break_point": bool(k % 9 == 4),
                "is_set_point": False,
                "is_match_point": False,
                "is_tiebreak": False,
                "set_score_server_games": 2,
                "set_score_returner_games": 3,
                "sets_won_server": 0,
                "sets_won_returner": 1,
                "game_score_server": "30",
                "game_score_returner": "15",
                "set_num": 1,
                "game_num": 6,
                "point_num": k + 1,
                "surface": surface,
                "best_of": 3,
            })
    return pl.DataFrame(rows)


def _config(tmp_path, *, metric: str, component: str | None,
            fixed: dict[str, list[str]] | None = None,
            point_pool: list[str] | None = None,
            name: str | None = None) -> str:
    """Built as a dict and dumped, not string-templated — an f-string
    substitution inside a `dedent` block destroys its common prefix."""
    import yaml

    fixed = fixed or {}
    cfg: dict = {
        "data": {
            "date_range": {"start": "2024-01-01", "end": "2024-12-31"},
            "filters": {"circuit": ["tour"], "draw_type": "singles"},
        },
        "validation": {
            "type": "walk_forward", "n_splits": 2,
            "min_train_size": 10, "test_size": 6,
        },
        # Point-grain splits, used by `run()`'s base-matrix build. The defaults
        # want 60k point rows; the fixture has ~1.8k.
        "point_validation": {
            "type": "walk_forward", "n_splits": 2,
            "min_train_size": 200, "test_size": 100,
        },
        "metric": metric,
        "features": {
            "base_match_level_features": [],
            "candidate_match_level_features": [MIRROR_SPEC],
            "candidate_point_level_features": (
                ["is_break_point"] if point_pool is None else list(point_pool)
            ),
        },
        "scoring_model": {
            "type": "xgboost",
            "params": {
                "n_estimators": 8, "max_depth": 2, "learning_rate": 0.1,
                "tree_method": "hist", "random_state": 42, "n_jobs": 1,
            },
        },
    }
    if component:
        cfg["serve_component"] = component
        cfg["serve_model"] = {
            "type": "two_level",
            "model_type": "xgboost",
            "first_in_match_features": list(fixed.get(FIRST_IN, [])),
            "first_in_point_features": [],
            "win_first_match_features": list(fixed.get(WIN_FIRST, [])),
            "win_first_point_features": [],
            "win_second_match_features": list(fixed.get(WIN_SECOND, [])),
            "win_second_point_features": [],
        }
    path = tmp_path / f"fs_{name or component or 'single'}.yaml"
    path.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return str(path)


def _selector(tmp_path, config_path, *, prepare=True,
              checkpoint: bool = False) -> ServeDiscoverySelector:
    n = 30
    matches_path = tmp_path / "matches.parquet"
    _matches(n).write_parquet(matches_path)
    points_path = tmp_path / "points.parquet"
    _points(n).write_parquet(points_path)
    sel = ServeDiscoverySelector(
        config_path=config_path, matches_path=matches_path,
        points_path=points_path, cache_dir=tmp_path / "cache",
        # The per-round history is written beside the checkpoint, so a test
        # that reads the round-1 ranking has to ask for one.
        checkpoint_path=(tmp_path / "cp.json") if checkpoint else None,
    )
    if not prepare:
        return sel
    sel._prepare_match_data(
        match_pool=[MIRROR_SPEC], engine=MirroringFakeEngine(), cache_key="k",
    )
    return sel


class TestRegistry:
    def test_branch_specs(self):
        names = {n for n, s in METRICS.items() if s.grain == "branch"}
        assert names == {
            "branch_log_loss", "branch_brier", "branch_roc_auc",
            "branch_calibration_error", "branch_rate_wmse",
        }
        assert base_metric_of("branch_log_loss") == "log_loss"
        assert base_metric_of("branch_brier") == "brier_score"
        assert base_metric_of("branch_roc_auc") == "roc_auc"
        assert base_metric_of("branch_calibration_error") == "calibration_error"
        assert base_metric_of("branch_rate_wmse") is None

    def test_directions_match_the_classification_counterparts(self):
        from mvp.model.metrics import MAXIMIZE_METRICS  # noqa: F401  (existence)
        from mvp.projection.iid.metric_registry import direction_of

        assert direction_of("branch_log_loss") == "minimize"
        assert direction_of("branch_brier") == "minimize"
        assert direction_of("branch_calibration_error") == "minimize"
        assert direction_of("branch_roc_auc") == "maximize"
        assert direction_of("branch_rate_wmse") == "minimize"

    def test_grain_predicates_are_distinct(self):
        assert is_branch_metric("branch_log_loss")
        assert not is_chain_metric("branch_log_loss")
        # branch needs the match-grain prep but does NOT score through the chain
        assert needs_match_grain_prep("branch_log_loss")
        assert needs_match_grain_prep("iid_match_win_log_loss")
        assert not needs_match_grain_prep("log_loss")


class TestConfigGuardrails:
    def test_branch_metric_requires_serve_component(self, tmp_path):
        with pytest.raises(ValueError, match="requires serve_component"):
            ServeDiscoveryConfig.from_file(
                _config(tmp_path, metric="branch_log_loss", component=None)
            )

    def test_branch_metric_with_component_loads(self, tmp_path):
        cfg = ServeDiscoveryConfig.from_file(
            _config(tmp_path, metric="branch_log_loss", component=WIN_FIRST)
        )
        assert cfg.metric == "branch_log_loss"

    @pytest.mark.parametrize(
        "objective", ["branch_log_loss", "log_loss", "brier_score", "roc_auc"]
    )
    def test_non_chain_objective_is_refused(self, objective):
        with pytest.raises(ValueError, match="chain-grain"):
            IIDMetricsConfig(objective=[objective])

    def test_chain_objective_still_accepted(self):
        assert IIDMetricsConfig(objective=["iid_crps_spread"]).objective == [
            "iid_crps_spread"
        ]


class TestPromotion:
    def _cfg(self, tmp_path, metric, component=WIN_FIRST):
        return ServeDiscoveryConfig.from_file(
            _config(tmp_path, metric=metric, component=component)
        )

    def test_branch_run_omits_the_objective(self, tmp_path):
        emitted = self._cfg(tmp_path, "branch_log_loss").to_iid_projection_config_dict(
            [MIRROR_SPEC], [], model_type="xgboost", model_params={},
        )
        assert "objective" not in emitted["metrics"]
        # and the result is loadable, so the failure surfaces at tune time
        IIDProjectionConfig.model_validate(emitted)

    def test_chain_run_carries_its_metric(self, tmp_path):
        emitted = self._cfg(tmp_path, "iid_crps_spread").to_iid_projection_config_dict(
            [MIRROR_SPEC], [], model_type="xgboost", model_params={},
        )
        assert emitted["metrics"]["objective"] == ["iid_crps_spread"]
        IIDProjectionConfig.model_validate(emitted)

    def test_lines_survive_promotion(self, tmp_path):
        cfg = self._cfg(tmp_path, "iid_crps_spread")
        cfg.metrics.total_lines = [20.5, 21.5]
        cfg.metrics.spread_lines = [-2.5, 2.5]
        emitted = cfg.to_iid_projection_config_dict(
            [MIRROR_SPEC], [], model_type="xgboost", model_params={},
        )
        assert emitted["metrics"]["total_lines"] == [20.5, 21.5]
        assert emitted["metrics"]["spread_lines"] == [-2.5, 2.5]

    def test_description_records_the_selecting_metric(self, tmp_path):
        emitted = self._cfg(tmp_path, "branch_brier").to_iid_projection_config_dict(
            [MIRROR_SPEC], [], model_type="xgboost", model_params={},
        )
        assert "win_first selected on branch_brier" in emitted["description"]


class TestBranchScoring:
    @pytest.mark.parametrize("component,branch", [(WIN_FIRST, 1), (WIN_SECOND, 2)])
    def test_target_is_the_branch_own_rows(self, tmp_path, component, branch,
                                           monkeypatch):
        """Not merely 'two components score differently' — capture the actual
        y_true handed to compute_metrics and pin it to apply_serve_branch."""
        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=component,
            fixed={WIN_FIRST: [], WIN_SECOND: []},
        ))
        seen: list[np.ndarray] = []
        import mvp.model.metrics as mm
        orig = mm.compute_metrics

        def spy(y_true, y_prob, **kw):
            seen.append(np.asarray(y_true))
            return orig(y_true, y_prob, **kw)

        monkeypatch.setattr(mm, "compute_metrics", spy)
        score = sel._score_cv_branch([MIRROR_SPEC], [])
        assert np.isfinite(score)
        assert seen
        fold = sel._chain_folds[0]
        expected = apply_serve_branch(fold.test_points, branch)
        expected = expected.filter(pl.col("point_won_by_server").is_not_null())
        assert len(seen[0]) == len(expected)
        assert seen[0].tolist() == expected["point_won_by_server"].cast(
            pl.Int64
        ).to_list()

    def test_raw_point_candidate_scores(self, tmp_path):
        """The held-out frame is narrowed to the union over the point POOL, so
        a raw point candidate must be present in it."""
        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
            point_pool=["is_break_point", "sets_won_server"],
        ))
        assert np.isfinite(sel._score_cv_branch([MIRROR_SPEC], ["is_break_point"]))
        assert np.isfinite(sel._score_cv_branch([MIRROR_SPEC], ["sets_won_server"]))

    def test_constant_branch_raises_explicitly(self, tmp_path):
        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
        ))
        with pytest.raises(TypeError, match="_ConstantBranch"):
            sel._score_win_branch(_ConstantBranch(serve_branch=1),
                                  sel._chain_folds[0], "log_loss")

    def test_held_out_rows_are_disjoint_from_train(self, tmp_path):
        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
        ))
        for fold in sel._chain_folds:
            test_uids = set(fold.test_points["match_uid"].to_list())
            train_uids = set(fold.points["match_uid"].to_list())
            assert test_uids == set(fold.test_df["match_uid"].to_list())
            assert test_uids.isdisjoint(train_uids)

    def test_preloaded_points_still_released(self, tmp_path):
        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
        ))
        assert sel._preloaded_points is None


class TestFirstInBranchScoring:
    def _sel(self, tmp_path):
        return _selector(tmp_path, _config(
            tmp_path, metric="branch_rate_wmse", component=FIRST_IN,
            fixed={WIN_FIRST: [MIRROR_SPEC]}, point_pool=[],
        ))

    def test_aggregate_matches_the_training_aggregation(self, tmp_path):
        sel = self._sel(tmp_path)
        fold = sel._chain_folds[0]
        raw = pl.read_parquet(sel.points_path).filter(
            pl.col("match_uid").is_in(fold.test_df["match_uid"].to_list())
        )
        model = FirstServeInModel(model_type="xgboost", match_level_features=[])
        assert fold.test_first_in.sort(["match_uid", "server_id"]).equals(
            model._aggregate(raw).select(
                "match_uid", "server_id", "_n_serve_pts", "_n_first_in"
            ).sort(["match_uid", "server_id"])
        )

    def test_perfect_prediction_scores_zero_and_the_swap_does_not(self, tmp_path):
        """A and B have deliberately different realised rates, so a model that
        predicts each exactly scores 0 under the correct join and > 0 under a
        swapped one."""
        sel = self._sel(tmp_path)
        fold = sel._chain_folds[0]
        rate = (fold.test_first_in["_n_first_in"]
                / fold.test_first_in["_n_serve_pts"])
        truth = dict(zip(
            zip(fold.test_first_in["match_uid"].to_list(),
                fold.test_first_in["server_id"].to_list()),
            rate.to_list(), strict=True,
        ))
        a = np.array([truth[(u, p)] for u, p in zip(
            fold.test_df["match_uid"].to_list(),
            fold.test_df["player_id"].to_list(), strict=True)])
        b = np.array([truth[(u, o)] for u, o in zip(
            fold.test_df["match_uid"].to_list(),
            fold.test_df["opp_id"].to_list(), strict=True)])
        assert not np.allclose(a, b)  # the sides really do differ

        class _Stub:
            def __init__(self, fa, fb):
                self._fa, self._fb = fa, fb

            def _first_in_for(self, df):
                return self._fa, self._fb

        assert sel._score_first_in(_Stub(a, b), fold) == pytest.approx(0.0, abs=1e-12)
        assert sel._score_first_in(_Stub(b, a), fold) > 0.0

    def test_mis_keyed_join_raises_rather_than_scoring(self, tmp_path):
        sel = self._sel(tmp_path)
        fold = sel._chain_folds[0]
        dupe = pl.concat([fold.test_first_in, fold.test_first_in])
        bad = _replace_fold(fold, test_first_in=dupe)

        class _Stub:
            def _first_in_for(self, df):
                z = np.full(len(df), 0.6)
                return z, z

        # polars raises on the 1:1 validation itself; naming it keeps the test
        # from passing on some unrelated error.
        with pytest.raises(pl.exceptions.ComputeError, match="1:1"):
            sel._score_first_in(_Stub(), bad)


def _replace_fold(fold, **kw):
    from dataclasses import replace

    return replace(fold, **kw)


def _round1_ranking(sel) -> list[str]:
    """Feature names scored in round 1, from the run's history file."""
    import json

    from mvp.model.discovery.selection import _fs_history_path

    path = _fs_history_path(sel.checkpoint_path) if sel.checkpoint_path else None
    if path is None or not path.exists():
        return []
    first = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    return [f for f, _ in first["ranking"]]


class TestDispatch:
    def _spy(self, monkeypatch):
        calls = {"chain": 0, "branch": 0}
        orig_chain = ServeDiscoverySelector._score_cv_chain
        orig_branch = ServeDiscoverySelector._score_cv_branch

        def chain(self, *a, **k):
            calls["chain"] += 1
            return orig_chain(self, *a, **k)

        def branch(self, *a, **k):
            calls["branch"] += 1
            return orig_branch(self, *a, **k)

        monkeypatch.setattr(ServeDiscoverySelector, "_score_cv_chain", chain)
        monkeypatch.setattr(ServeDiscoverySelector, "_score_cv_branch", branch)
        dp = {"n": 0}
        real_dp = sd.match_distribution_from_state_fn

        def dp_spy(*a, **k):
            dp["n"] += 1
            return real_dp(*a, **k)

        monkeypatch.setattr(sd, "match_distribution_from_state_fn", dp_spy)
        return calls, dp

    def test_serial_path_routes_to_branch(self, tmp_path, monkeypatch):
        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
        ))
        calls, dp = self._spy(monkeypatch)
        sel._score_cv(None, None, [MIRROR_SPEC], [])
        assert calls["branch"] == 1 and calls["chain"] == 0
        assert dp["n"] == 0  # never through the chain

    def test_parallel_loop_routes_to_branch(self, tmp_path, monkeypatch):
        """Through `run()` with n_parallel_candidates > 1, so the assertion is
        on the closure the parallel loop actually calls. Calling
        `_score_cv_match_grain` directly would pass even if that closure still
        called `_score_cv_chain` — which is the one site that would silently
        score a branch run through the chain in every real run."""
        path = _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
            point_pool=["is_break_point"], name="par",
        )
        import yaml

        cfg = yaml.safe_load(open(path))
        cfg["n_parallel_candidates"] = 2
        cfg["features"]["max_features"] = 1  # one round is enough
        open(path, "w").write(yaml.safe_dump(cfg, sort_keys=False))

        sel = _selector(tmp_path, path, prepare=False)
        calls, dp = self._spy(monkeypatch)
        monkeypatch.setattr(
            ServeDiscoverySelector, "_pre_cache_all",
            lambda self, **kw: (MirroringFakeEngine(), "k"),
        )
        sel.run()
        assert calls["branch"] > 0, "parallel loop never reached the branch scorer"
        assert calls["chain"] == 0
        assert dp["n"] == 0

    def test_chain_metric_still_routes_to_chain(self, tmp_path, monkeypatch):
        sel = _selector(tmp_path, _config(
            tmp_path, metric="iid_match_win_log_loss", component=WIN_FIRST,
        ))
        calls, dp = self._spy(monkeypatch)
        sel._score_cv_match_grain([MIRROR_SPEC], [])
        assert calls["chain"] == 1 and calls["branch"] == 0
        assert dp["n"] > 0

    def test_concurrent_matches_sequential(self, tmp_path):
        from concurrent.futures import ThreadPoolExecutor

        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
            point_pool=["is_break_point", "sets_won_server"],
        ))
        cands = [([MIRROR_SPEC], []), ([], ["is_break_point"]),
                 ([], ["sets_won_server"])]
        seq = [sel._score_cv_match_grain(m, p) for m, p in cands]
        with ThreadPoolExecutor(max_workers=3) as ex:
            par = list(ex.map(lambda c: sel._score_cv_match_grain(*c), cands))
        assert par == seq

    def test_chain_incompatible_point_features_excluded_for_branch(
        self, tmp_path, monkeypatch,
    ):
        """`point_num` invalidates the deuce closed form and the endpoint is
        still a chain config, so a branch run must not select it either.
        Asserted on the observable — the round-1 ranking — not on the registry
        constant, which would pass with the gate reverted."""
        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_log_loss", component=WIN_FIRST,
            point_pool=["is_break_point", "point_num"], name="incompat",
        ), prepare=False, checkpoint=True)
        monkeypatch.setattr(
            ServeDiscoverySelector, "_pre_cache_all",
            lambda self, **kw: (MirroringFakeEngine(), "k"),
        )
        result = sel.run()
        assert "point_num" not in result.selected_point_level
        # and it was never even scored
        ranked = _round1_ranking(sel)
        assert ranked, "no round-1 ranking recorded"
        assert "point_num" not in ranked
        assert "is_break_point" in ranked


class TestFirstInPointFeaturesReachTheModel:
    """D1: they used to be carried into the aggregate and never read."""

    def _fit(self, tmp_path, *, match_feats, point_feats):
        pts = _points(20)
        df = _matches(20).unique(subset=["match_uid"], keep="first")
        m = FirstServeInModel(
            model_type="xgboost", match_level_features=match_feats,
            point_level_features=point_feats,
            params={"n_estimators": 6, "max_depth": 2, "random_state": 0},
        )
        m.fit(df, preloaded_points=pts, preloaded_match_features=None)
        return m

    def test_point_only_arm_fits_a_model(self, tmp_path):
        """With `first_in_match_features == []` — the round-1 case in the
        base_fi run — a point candidate must now produce a real fit."""
        m = self._fit(tmp_path, match_feats=[], point_feats=["is_surface_hard"])
        assert m._model is not None
        assert m._design_cols() == ["is_surface_hard"]

    def test_no_features_at_all_is_still_intercept_only(self, tmp_path):
        m = self._fit(tmp_path, match_feats=[], point_feats=[])
        assert m._model is None

    def test_point_only_arm_predicts_something_other_than_the_base_rate(self):
        """The observed defect (fs_runs/20260821_base_fi): all three surface
        one-hots scored byte-identically because all three predicted the
        constant base rate. A point-only arm must now VARY across matches and
        differ from the base rate — the fixture's surfaces have different
        first-serve populations, and the whole point of the arm is to see it.
        """
        pts = _points(20)
        df = _matches(20).unique(subset=["match_uid"], keep="first")
        m = FirstServeInModel(
            model_type="xgboost", match_level_features=[],
            point_level_features=["is_surface_hard"],
            params={"n_estimators": 6, "max_depth": 2, "random_state": 0},
        )
        m.fit(df, preloaded_points=pts)
        two = TwoLevelServeModel(
            model_type="xgboost",
            first_in_match_features=[], first_in_point_features=["is_surface_hard"],
            win_first_match_features=[], win_first_point_features=[],
            win_second_match_features=[], win_second_point_features=[],
        )
        two._first_in = m
        a, b = two._first_in_for(df)
        assert len(np.unique(np.round(a, 9))) > 1, "arm still predicts a constant"
        assert not np.allclose(a, m._base_rate)
        # first-serve-in does not depend on which side is called A
        assert np.allclose(a, b)

    def test_missing_point_column_raises(self, tmp_path):
        m = FirstServeInModel(
            model_type="xgboost", match_level_features=[],
            point_level_features=["is_break_point"],
        )
        pts = _points(5).drop("is_break_point")
        with pytest.raises(KeyError, match="point features absent"):
            m.fit(_matches(5).unique(subset=["match_uid"]), preloaded_points=pts)


class TestMetricComponentPairing:
    """A branch metric and the component must agree about the target.

    The scorer dispatches on the COMPONENT, so a mismatched pair would run to
    completion and label every artifact with a metric it did not compute.
    """

    def test_point_metric_with_first_in_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="serve_component=first_in must use"):
            ServeDiscoveryConfig.from_file(_config(
                tmp_path, metric="branch_log_loss", component=FIRST_IN,
                name="mismatch_a",
            ))

    def test_rate_metric_with_a_win_branch_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="requires serve_component=first_in"):
            ServeDiscoveryConfig.from_file(_config(
                tmp_path, metric="branch_rate_wmse", component=WIN_FIRST,
                name="mismatch_b",
            ))

    @pytest.mark.parametrize("metric,component", [
        ("branch_log_loss", WIN_FIRST),
        ("branch_brier", WIN_SECOND),
        ("branch_rate_wmse", FIRST_IN),
        ("iid_crps_spread", FIRST_IN),   # chain metrics pair with anything
    ])
    def test_valid_pairings_load(self, tmp_path, metric, component):
        cfg = ServeDiscoveryConfig.from_file(_config(
            tmp_path, metric=metric, component=component,
            name=f"ok_{metric}_{component}",
        ))
        assert cfg.metric == metric


class TestFirstInParquetPath:
    """A promoted first_in config with point features must be trainable.

    `projection_run` trains through `projector.fit(df)` with no preloaded
    points, so `FirstServeInModel.fit` reads the parquet itself — and its
    derived point features need their source columns in that read.
    """

    def test_point_only_arm_fits_from_the_parquet(self, tmp_path):
        points_path = tmp_path / "pts.parquet"
        _points(12).write_parquet(points_path)
        df = _matches(12).unique(subset=["match_uid"], keep="first")
        m = FirstServeInModel(
            model_type="xgboost", match_level_features=[],
            point_level_features=["is_surface_hard"],
            params={"n_estimators": 6, "max_depth": 2, "random_state": 0},
            points_path=points_path,
        )
        m.fit(df)  # no preloaded_points -- the artifact-building path
        assert m._model is not None

    def test_read_cols_cover_derived_sources(self):
        m = FirstServeInModel(
            model_type="xgboost", match_level_features=[],
            point_level_features=["is_surface_hard", "is_break_point"],
        )
        cols = m._point_read_cols()
        assert "surface" in cols          # source of the one-hot
        assert "is_break_point" in cols   # raw, read directly


class TestRunnerBranchEmit:
    """A6: per-branch held-out metrics at the endpoint."""

    def _model(self, *, first_in_feats=(), constant_win_second=True):
        return TwoLevelServeModel(
            model_type="xgboost",
            first_in_match_features=[], first_in_point_features=list(first_in_feats),
            win_first_match_features=[], win_first_point_features=[],
            win_second_match_features=[], win_second_point_features=[],
        )

    def test_emits_every_component_key_even_when_unscorable(self):
        """`avg_metrics` is built from the FIRST fold's key set, so a fold that
        omits these would KeyError on the next one. Constant branches are a
        legitimate production shape — `two_level_flat` ships a featureless first_in
        arm, and an all-empty two-level model is the recovery-test arm — so
        they emit NaN rather than raising."""
        from mvp.projection.iid.runner import _two_level_branch_metrics

        model = self._model()
        test_df = _matches(6).unique(subset=["match_uid"], keep="first")
        out = _two_level_branch_metrics(
            model, test_df, fold_test_points=_points(6),
            preloaded_match_features=None,
        )
        for comp in (WIN_FIRST, WIN_SECOND):
            for key in ("log_loss", "brier_score", "roc_auc", "calibration_error"):
                assert f"branch_{comp}_{key}" in out
        assert "branch_first_in_rate_wmse" in out
        # unfitted win branches -> NaN, not absent, not an exception
        assert np.isnan(out[f"branch_{WIN_FIRST}_log_loss"])

    def test_first_in_wmse_matches_the_fs_scorer(self, tmp_path):
        """The endpoint readout and the selection metric must be the same
        quantity, or the comparison the plan exists for is meaningless."""
        from mvp.projection.iid.runner import _first_in_rate_wmse

        sel = _selector(tmp_path, _config(
            tmp_path, metric="branch_rate_wmse", component=FIRST_IN,
            fixed={WIN_FIRST: [MIRROR_SPEC]}, point_pool=[], name="parity",
        ))
        fold = sel._chain_folds[0]
        model = self._model()
        raw = pl.read_parquet(sel.points_path).filter(
            pl.col("match_uid").is_in(fold.test_df["match_uid"].to_list())
        )
        assert _first_in_rate_wmse(model, fold.test_df, raw) == pytest.approx(
            sel._score_first_in(model, fold), rel=1e-12
        )

    def test_single_level_metrics_unchanged(self):
        """The new emit is gated on TwoLevelServeModel; a score_state model
        still reports `point_*` and nothing else new."""
        import inspect

        from mvp.projection.iid import runner

        from mvp.projection.iid.serve_model import ScoreStateChainServeModel

        # The load-bearing fact, and the reason A6 was needed: a two-level
        # model is NOT a ScoreStateChainServeModel, so the existing gate skips
        # it. If that ever became a subclass the `elif` would be dead and
        # every two-level run would silently emit `point_*` instead.
        assert not issubclass(TwoLevelServeModel, ScoreStateChainServeModel)
        src = inspect.getsource(runner.IIDProjectionRunner.run)
        assert "isinstance(serve_model, ScoreStateChainServeModel)" in src
        assert "elif isinstance(serve_model, TwoLevelServeModel)" in src
