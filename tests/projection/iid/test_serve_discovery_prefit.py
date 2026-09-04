"""Serve FS wall-time, Phase 0/1 (plan 2026-09-03-serve-fs-wall-time).

Phase 1 fits the two FIXED components of a two-level component run once per
fold and hands each candidate a private copy. The contract is bitwise
equivalence with the per-candidate refit, under the production scorer params
(subsample/colsample < 1, seeded hist) where row and column order ARE model
identity. Phase 0 exposes per-phase fit timings on the [diag] line.

Three component runs are exercised, because each prefits a different class
mix: win_second (a real win_first branch + intercept first_in), win_first
(a `_ConstantBranch` win_second + a first_in with a fitted regressor), and
first_in (BOTH win branches real). Fixture: synthetic two-sided matches, a
points frame with both servers, and the MirroringFakeEngine from the
swap-side tests. No B:/ reads.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from textwrap import dedent

import numpy as np
import polars as pl
import pytest

from mvp.projection.iid.serve_discovery import ServeDiscoverySelector
from mvp.projection.iid.serve_model import (
    ScoreStateChainServeModel,
    neutral_score_state,
)
from mvp.projection.iid.two_level_serve_model import (
    COMPONENTS,
    FIRST_IN,
    WIN_FIRST,
    WIN_SECOND,
    FirstServeInModel,
    TwoLevelServeModel,
    _ConstantBranch,
)

from tests.projection.iid.test_branch_metrics import _round1_ranking
from tests.projection.iid.test_serve_discovery_swap_side import (
    DIFF_SPEC,
    MATCHUP_SPEC,
    MIRROR_SPEC,
    MirroringFakeEngine,
)

# The plan's four required params from the live one-level run
# (fs_runs/20260903_srv_one_level): subsample, colsample_bytree, random_state,
# tree_method. Tree count and depth are scaled down for test speed;
# subsample/colsample < 1 is the point — a shifted row or column would change
# the fitted model, not just round it.
_SCORER = dedent("""
    scoring_model:
      type: xgboost
      params:
        n_estimators: 8
        max_depth: 2
        learning_rate: 0.05
        subsample: 0.8
        colsample_bytree: 0.8
        min_child_weight: 5
        tree_method: hist
        random_state: 42
        n_jobs: 1
""")

# (selected component, first_in feats, win_first feats, win_second feats)
SHAPES = {
    "win_second": (WIN_SECOND, [], [MIRROR_SPEC], []),
    "win_first": (WIN_FIRST, [MIRROR_SPEC], [], []),
    "first_in": (FIRST_IN, [], [MIRROR_SPEC], [DIFF_SPEC]),
    # A held arm carrying a spec that is NOT in the candidate pool. Every other
    # shape draws its fixed features from the pool, which is why the
    # materialization bug survived this file: `load_specs` was the pool plus
    # its swap partners, so a fixed spec outside the pool was never loaded and
    # the first prefit raised KeyError.
    "fixed_outside_pool": (FIRST_IN, [], [MATCHUP_SPEC], []),
}


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
                "surface": "hard",
                "circuit": "tour",
                "draw_type": "singles",
                "player_set1_games": 6, "player_set2_games": 4, "player_set3_games": 6,
                "player_set4_games": None, "player_set5_games": None,
                "opp_set1_games": 4, "opp_set2_games": 6, "opp_set3_games": 3,
                "opp_set4_games": None, "opp_set5_games": None,
            })
    return pl.DataFrame(rows)


def _points(n_matches: int, per_match: int = 48) -> pl.DataFrame:
    """Both players serve; outcomes depend on the server so a fit has signal."""
    rng = np.random.default_rng(11)
    rows = []
    for i in range(n_matches):
        a, b = 1000 + i, 2000 + i
        for k in range(per_match):
            server, returner = (a, b) if (k // 4) % 2 == 0 else (b, a)
            p = 0.58 if server == a else 0.66
            rows.append({
                "match_uid": f"m{i:03d}",
                "server_id": server,
                "returner_id": returner,
                "serve": 1 if rng.random() < 0.62 else 2,
                "point_won_by_server": bool(rng.random() < p),
                "is_break_point": bool(k % 7 == 3),
                "surface": "hard",
                "best_of": 3,
            })
    return pl.DataFrame(rows)


def _two_level_config(tmp_path, shape, point_pool=None) -> str:
    component, fi, w1, w2 = shape
    # first_in refuses STATE-DERIVABLE point candidates, so a run() that
    # selects it needs match-constant ones instead (the surface one-hots).
    point_pool = point_pool if point_pool is not None else ["is_break_point"]
    path = tmp_path / "fs.yaml"
    path.write_text(dedent(f"""
        data:
          date_range:
            start: 2024-01-01
            end: 2024-12-31
          filters:
            circuit: [tour]
            draw_type: singles
        validation:
          type: walk_forward
          n_splits: 2
          min_train_size: 10
          test_size: 5
        # Point-grain splits, used by `run()`'s base-matrix build. The defaults
        # want 60k point rows; the fixture has ~1.9k.
        point_validation:
          type: walk_forward
          n_splits: 2
          min_train_size: 200
          test_size: 100
        metric: iid_match_win_log_loss
        serve_component: {component}
        serve_model:
          type: two_level
          model_type: xgboost
          first_in_match_features: {fi}
          first_in_point_features: []
          win_first_match_features: {w1}
          win_first_point_features: []
          win_second_match_features: {w2}
          win_second_point_features: []
        features:
          candidate_match_level_features: [{MIRROR_SPEC}, {DIFF_SPEC}]
          candidate_point_level_features: {point_pool}
    """) + _SCORER)
    return str(path)


def _make_selector(
    tmp_path, config_path, *, match_pool=None, engine=None,
    prepare=True, checkpoint=False,
) -> ServeDiscoverySelector:
    n = 40
    matches_path = tmp_path / "matches.parquet"
    _matches(n).write_parquet(matches_path)
    points_path = tmp_path / "points.parquet"
    _points(n).write_parquet(points_path)
    sel = ServeDiscoverySelector(
        config_path=config_path,
        matches_path=matches_path,
        points_path=points_path,
        cache_dir=tmp_path / "cache",
        # The per-round history is written beside the checkpoint, so a test
        # that reads the round-1 ranking has to ask for one.
        checkpoint_path=(tmp_path / "cp.json") if checkpoint else None,
    )
    if not prepare:
        return sel
    sel._prepare_match_data(
        match_pool=list(match_pool) if match_pool else [MIRROR_SPEC, DIFF_SPEC],
        engine=engine if engine is not None else MirroringFakeEngine(),
        cache_key="k",
    )
    return sel


@pytest.fixture(params=list(SHAPES), ids=list(SHAPES))
def shape(request):
    return SHAPES[request.param]


@pytest.fixture
def selector(tmp_path, shape):
    """A two-level component run, prepared through the real chain-fold build
    (which is where the prefit cache is built)."""
    return _make_selector(tmp_path, _two_level_config(tmp_path, shape))


def _candidates(component):
    cands = [([DIFF_SPEC], []), ([MIRROR_SPEC], [])]
    if component != FIRST_IN:
        # first_in refuses state-derivable point candidates by design
        cands.append(([], ["is_break_point"]))
    return cands


class TestPrefitEquivalence:
    def test_prefit_cache_holds_the_fixed_components(self, selector, shape):
        component = shape[0]
        assert selector._prefit_fixed is not None
        assert set(selector._prefit_fixed) == set(range(len(selector._chain_folds)))
        for fold_cache in selector._prefit_fixed.values():
            assert set(fold_cache) == set(COMPONENTS) - {component}
            for fitted in fold_cache.values():
                assert "fit" in fitted.fit_timings  # it was fitted

    def test_prefit_class_mix_matches_the_shape(self, selector, shape):
        component, fi, w1, w2 = shape
        cache = selector._prefit_fixed[0]
        expect = {
            FIRST_IN: (FirstServeInModel, bool(fi)),
            WIN_FIRST: (ScoreStateChainServeModel if w1 else _ConstantBranch, bool(w1)),
            WIN_SECOND: (ScoreStateChainServeModel if w2 else _ConstantBranch, bool(w2)),
        }
        for name, (cls, has_model) in expect.items():
            if name == component:
                continue
            fitted = cache[name]
            assert isinstance(fitted, cls)
            if cls is not _ConstantBranch:
                assert (fitted._model is not None) == has_model

    def test_prefit_scores_equal_the_per_candidate_refit(self, selector, shape):
        """Bitwise: same fold frames, same params, same seed => same booster."""
        for match_level, point_level in _candidates(shape[0]):
            with_prefit = selector._score_cv_chain(match_level, point_level)
            cache = selector._prefit_fixed
            selector._prefit_fixed = None  # today's path: every component refit
            try:
                without = selector._score_cv_chain(match_level, point_level)
            finally:
                selector._prefit_fixed = cache
            assert np.isfinite(with_prefit)
            assert with_prefit == without, (match_level, point_level)

    def test_candidate_model_fits_only_the_selected_component(self, selector, shape):
        component = shape[0]
        params = selector._scoring_params()
        model = selector._build_candidate_model([DIFF_SPEC], [], params)
        assert isinstance(model, TwoLevelServeModel)
        selector._attach_prefit(model, 0)
        assert model._prefit == set(COMPONENTS) - {component}
        # private copies, not the cached objects
        for name, cached in selector._prefit_fixed[0].items():
            assert model.components()[name] is not cached
        fold = selector._chain_folds[0]
        model.fit(fold.train_df, preloaded_match_features=fold.feats,
                  preloaded_points=fold.points)
        # only the selected component's time is this call's time
        assert model.fit_timings == model.components()[component].fit_timings


@pytest.mark.parametrize("shape", ["win_second"], indirect=True)
class TestSingleLevel:
    def test_single_level_run_has_no_prefit(self, tmp_path, shape):
        cfg = tmp_path / "ol.yaml"
        cfg.write_text(dedent(f"""
            data:
              date_range:
                start: 2024-01-01
                end: 2024-12-31
              filters:
                circuit: [tour]
                draw_type: singles
            validation:
              type: walk_forward
              n_splits: 2
              min_train_size: 10
              test_size: 5
            metric: iid_match_win_log_loss
            features:
              candidate_match_level_features: [{MIRROR_SPEC}, {DIFF_SPEC}]
              candidate_point_level_features: [is_break_point]
        """) + _SCORER)
        sel = _make_selector(tmp_path, str(cfg))
        assert sel._prefit_fixed is None
        assert np.isfinite(sel._score_cv_chain([MIRROR_SPEC], []))
        # one-level records all five fit phases
        means = sel._take_phase_means()
        assert {"load", "join", "derive", "matrix", "fit"} <= set(means)


@pytest.mark.parametrize("shape", ["win_second"], indirect=True)
class TestScoringParams:
    def test_single_derivation_site_matches_config(self, selector):
        assert selector._scoring_params() == selector.config.scoring_model.params

    def test_n_jobs_defaults_to_one_for_xgboost(self, selector):
        saved = dict(selector.config.scoring_model.params)
        try:
            selector.config.scoring_model.params.pop("n_jobs")
            assert selector._scoring_params()["n_jobs"] == 1
        finally:
            selector.config.scoring_model.params.clear()
            selector.config.scoring_model.params.update(saved)

    def test_prefit_used_the_candidate_params(self, selector):
        fitted = selector._prefit_fixed[0][WIN_FIRST]
        assert fitted.params == selector._scoring_params()


class TestThreadIsolation:
    def test_concurrent_candidates_match_sequential(self, selector, shape):
        cands = _candidates(shape[0])
        sequential = [selector._score_cv_chain(m, p) for m, p in cands]
        with ThreadPoolExecutor(max_workers=3) as ex:
            concurrent = list(ex.map(lambda c: selector._score_cv_chain(*c), cands))
        assert concurrent == sequential


@pytest.mark.parametrize("shape", ["win_second"], indirect=True)
class TestClosureLocals:
    def test_predict_state_fn_leaves_nothing_on_the_instance(self, selector):
        fitted = selector._prefit_fixed[0][WIN_FIRST]
        fold = selector._chain_folds[0]
        p_a_fn, p_b_fn = fitted.predict_state_fn(fold.test_df)
        for attr in ("_X_match_A", "_X_match_B", "_point_constants"):
            assert attr not in vars(fitted)
        # the closures still work: they own the arrays now
        assert len(p_a_fn(neutral_score_state())) == len(fold.test_df)

    def test_shared_fitted_model_survives_two_frames(self, selector):
        """Two predict_state_fn calls on one fitted model must not clobber
        each other — the arrays are per call, not per instance."""
        fitted = selector._prefit_fixed[0][WIN_FIRST]
        f0, f1 = selector._chain_folds[0].test_df, selector._chain_folds[1].test_df
        a0, _ = fitted.predict_state_fn(f0)
        a1, _ = fitted.predict_state_fn(f1)
        assert len(a0(neutral_score_state())) == len(f0)
        assert len(a1(neutral_score_state())) == len(f1)


@pytest.mark.parametrize("shape", ["win_second"], indirect=True)
class TestPhaseTimings:
    def test_fit_timings_and_diag_means(self, selector):
        selector._take_phase_means()  # reset whatever the fixture accumulated
        selector._score_cv_chain([DIFF_SPEC], [])
        means = selector._take_phase_means()
        assert {"fit", "predict", "dp", "score"} <= set(means)
        assert all(v >= 0.0 for v in means.values())
        assert selector._take_phase_means() == {}  # reset on read
        line = ServeDiscoverySelector._format_phases(means)
        assert line.startswith("phases(s/cand-fold): ") and "fit=" in line
        assert ServeDiscoverySelector._format_phases({}) == "phases=n/a"

    def test_component_timings_have_the_registry_keys(self, selector):
        fitted = selector._prefit_fixed[0][WIN_FIRST]
        assert set(fitted.fit_timings) == {"load", "join", "derive", "matrix", "fit"}
        assert isinstance(selector._prefit_fixed[0][FIRST_IN].fit_timings.get("fit"), float)
        cb = _ConstantBranch(serve_branch=2)
        cb.fit(selector._chain_folds[0].train_df, preloaded_points=selector._chain_folds[0].points)
        assert "fit" in cb.fit_timings


@pytest.mark.parametrize("shape", ["win_second"], indirect=True)
class TestPickleCompat:
    def test_old_two_level_artifact_refits_after_setstate(self, selector):
        """An artifact pickled before `_prefit`/`fit_timings` existed must
        still fit and count clips (the same gap offset_clipped_count sat in)."""
        model = TwoLevelServeModel(
            model_type="xgboost",
            first_in_match_features=[], win_first_match_features=[],
            win_first_point_features=[], win_second_match_features=[],
            win_second_point_features=[],
        )
        state = dict(model.__dict__)
        for k in ("_prefit", "fit_timings", "offset_clipped_count"):
            state.pop(k)
        revived = TwoLevelServeModel.__new__(TwoLevelServeModel)
        revived.__setstate__(state)
        assert revived._prefit == set()
        assert revived.fit_timings == {}
        assert revived.offset_clipped_count == 0
        assert set(revived.components()) == set(COMPONENTS)
        # and it really refits: all three (feature-less) components
        fold = selector._chain_folds[0]
        revived.fit(fold.train_df, preloaded_match_features=fold.feats,
                    preloaded_points=fold.points)
        assert revived.fit_timings.get("fit", 0.0) >= 0.0
        assert all("fit" in c.fit_timings for c in revived.components().values())
        p_a, p_b = revived.predict(fold.test_df)
        assert len(p_a) == len(p_b) == len(fold.test_df)


class TestFixedArmSpecsOutsideThePool:
    """`match_pool` is what FS SEARCHES; the fits READ the union of the three
    components' match features.

    A two_level component run holds the other two arms fixed at sets that come
    from `serve_model`, not from the candidate pool. Those were only ever
    materialized because the pool happened to be a superset of them, so
    trimming first_in's pool to server-own specs left every fixed win_first
    diff missing and `_build_prefit_fixed` raised KeyError on the first fold.
    """

    SHAPE = (FIRST_IN, [], [MATCHUP_SPEC], [])

    def _build(self, tmp_path, point_pool=None, **kw):
        engine = MirroringFakeEngine()
        cfg = _two_level_config(tmp_path, self.SHAPE, point_pool=point_pool)
        sel = _make_selector(tmp_path, cfg, engine=engine, **kw)
        return sel, engine

    def test_prefit_builds_with_the_fixed_spec_outside_the_pool(self, tmp_path):
        # Reaching the assertions at all is the regression: _prepare_match_data
        # -> _build_chain_folds -> _build_prefit_fixed used to raise.
        sel, _ = self._build(tmp_path)
        assert sel._prefit_fixed is not None
        assert set(sel._prefit_fixed[0]) == set(COMPONENTS) - {FIRST_IN}
        assert np.isfinite(sel._score_cv_chain([MIRROR_SPEC], []))

    def test_fixed_spec_and_its_swap_partner_are_materialized(self, tmp_path):
        sel, engine = self._build(tmp_path)
        requested = engine.requested[-1]
        assert MATCHUP_SPEC in requested
        # register_matchup leaves mirror=True, so the B-serves side reads the
        # opp_ column. Taking swap partners over match_pool alone would have
        # missed it and the run would have died on win_second instead.
        assert "opp_svc_elo_matchup" in requested
        cols = sel._match_features_both_sides.columns
        assert MATCHUP_SPEC in cols
        assert "opp_svc_elo_matchup" in cols

    def test_the_out_of_pool_spec_is_never_a_candidate(self, tmp_path, monkeypatch):
        """Widening the frame must not widen the SEARCH. Asserted on the
        round-1 ranking, which is the observable, rather than on the candidate
        list the ranking is built from."""
        sel, _ = self._build(
            tmp_path, prepare=False, checkpoint=True,
            point_pool=["is_surface_hard", "is_surface_clay"],
        )
        monkeypatch.setattr(
            ServeDiscoverySelector, "_pre_cache_all",
            lambda self, **kw: (MirroringFakeEngine(), "k"),
        )
        result = sel.run()
        ranked = _round1_ranking(sel)
        assert ranked, "no round-1 ranking recorded"
        assert MATCHUP_SPEC not in ranked
        assert MIRROR_SPEC in ranked
        assert MATCHUP_SPEC not in result.selected_match_level

    def test_extra_columns_do_not_perturb_a_score(self, tmp_path):
        """Materializing more than the pool changes fold residency, not
        results. Same config both sides — only the frame width differs.

        A property test, NOT a guard on the union: its shape draws both fixed
        arms from the pool, so it passes with the fix reverted. The union guard
        is test_prefit_builds_with_the_fixed_spec_outside_the_pool, which
        cannot even construct its selector under revert.
        """
        shape = SHAPES["first_in"]          # fixed arms are pool members here
        cfg = _two_level_config(tmp_path, shape)
        narrow = _make_selector(tmp_path, cfg, match_pool=[MIRROR_SPEC, DIFF_SPEC])
        wide = _make_selector(
            tmp_path, cfg, match_pool=[MIRROR_SPEC, DIFF_SPEC, MATCHUP_SPEC],
        )
        assert MATCHUP_SPEC not in narrow._match_features_both_sides.columns
        assert MATCHUP_SPEC in wide._match_features_both_sides.columns
        a = narrow._score_cv_chain([MIRROR_SPEC], [])
        b = wide._score_cv_chain([MIRROR_SPEC], [])
        assert np.isfinite(a)
        assert a == b
