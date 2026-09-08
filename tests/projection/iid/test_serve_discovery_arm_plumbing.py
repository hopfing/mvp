"""Serve FS #113: the arm threaded through candidate build, prefit and scoring.

Every place the selector builds, prefits, attaches or scores a candidate takes
the arm as an explicit argument instead of reading `config.serve_component`,
and the prefit cache is keyed by arm before fold. A component run passes no arm
anywhere and must behave exactly as it did before — that equivalence is what
these tests pin, so the joint loop (#115) can add a second arm without touching
these functions again.

Fixture is the prefit file's (`test_serve_discovery_prefit`): synthetic
two-sided matches, a points frame with both servers, the MirroringFakeEngine.
The shape is `win_second` — a component run whose held arms are a real
win_first branch and an intercept first_in. No B:/ reads.
"""

import math

import pytest

from mvp.projection.iid.serve_discovery import DiscoveryResult, FSRoundResult
from mvp.projection.iid.two_level_serve_model import (
    COMPONENTS,
    FIRST_IN,
    WIN_FIRST,
    WIN_SECOND,
    TwoLevelServeModel,
    _ConstantBranch,
)
from tests.projection.iid.test_serve_discovery_prefit import (
    SHAPES,
    _make_selector,
    _two_level_config,
)
from tests.projection.iid.test_serve_discovery_swap_side import (
    DIFF_SPEC,
    MIRROR_SPEC,
)


@pytest.fixture
def selector(tmp_path):
    """A `win_second` component run, prepared through the real chain-fold
    build — which is where the prefit cache is built."""
    return _make_selector(tmp_path, _two_level_config(tmp_path, SHAPES["win_second"]))


class TestRecordDefaults:
    """The two records carry the arm, and a component run leaves it unset."""

    def test_round_result_arm_defaults_to_none(self):
        record = FSRoundResult(
            round_idx=0,
            feature_added=None,
            grain="base",
            score=0.5,
            delta=0.0,
        )
        assert record.arm is None

    def test_discovery_result_selected_by_arm_defaults_to_none(self):
        result = DiscoveryResult(
            selected_match_level=[],
            selected_point_level=[],
            rounds=[],
            n_train_rows=0,
        )
        assert result.selected_by_arm is None


class TestPrefitKeyedByArm:
    """`_prefit_fixed[arm][fold]`. A component run has exactly one arm key."""

    def test_component_run_caches_exactly_its_own_arm(self, selector):
        cache = selector._prefit_fixed
        assert cache is not None
        assert set(cache) == {WIN_SECOND}
        assert set(cache[WIN_SECOND]) == set(range(len(selector._chain_folds)))
        for fold_cache in cache[WIN_SECOND].values():
            assert set(fold_cache) == {FIRST_IN, WIN_FIRST}

    def test_explicit_arms_build_a_cache_each(self, selector):
        selector._build_prefit_fixed(arms=[WIN_FIRST, WIN_SECOND])
        cache = selector._prefit_fixed
        assert set(cache) == {WIN_FIRST, WIN_SECOND}
        for arm, by_fold in cache.items():
            assert set(by_fold) == set(range(len(selector._chain_folds)))
            for fold_cache in by_fold.values():
                # each arm holds the OTHER two — never itself, which is the
                # one the candidate loop is there to fit
                assert set(fold_cache) == set(COMPONENTS) - {arm}

    def test_attaching_the_arm_matches_the_default(self, selector):
        params = selector._scoring_params()
        default = selector._build_candidate_model([DIFF_SPEC], [], params)
        selector._attach_prefit(default, 0)
        explicit = selector._build_candidate_model([DIFF_SPEC], [], params)
        selector._attach_prefit(explicit, 0, arm=WIN_SECOND)
        assert default._prefit == set(COMPONENTS) - {WIN_SECOND}
        assert explicit._prefit == default._prefit

    def test_attaching_an_uncached_arm_is_a_no_op(self, selector):
        # this run prefit win_second only; win_first has no cache to attach
        model = selector._build_candidate_model(
            [DIFF_SPEC], [], selector._scoring_params(), arm=WIN_FIRST,
        )
        selector._attach_prefit(model, 0, arm=WIN_FIRST)
        assert model._prefit == set()


class TestBuildCandidateModelArm:
    """`arm` says where the candidate lists go; everything else comes from
    `_joint_selected` first and the config second."""

    def test_arm_places_the_candidate_and_holds_the_rest(self, selector):
        model = selector._build_candidate_model(
            [DIFF_SPEC], [], selector._scoring_params(), arm=WIN_FIRST,
        )
        assert isinstance(model, TwoLevelServeModel)
        components = model.components()
        assert components[WIN_FIRST].match_level_features == [DIFF_SPEC]
        # win_second is empty in this shape's config and stays that way, which
        # for a win branch means the constant branch, not an unfitted scorer
        assert isinstance(components[WIN_SECOND], _ConstantBranch)
        assert components[FIRST_IN].match_level_features == []

    def test_joint_selected_reaches_the_other_arms(self, selector):
        # the config gives win_first MIRROR_SPEC; a joint run's current
        # selection for that arm must win over it
        selector._joint_selected = {WIN_FIRST: ([DIFF_SPEC], [])}
        model = selector._build_candidate_model(
            [MIRROR_SPEC], [], selector._scoring_params(), arm=WIN_SECOND,
        )
        components = model.components()
        assert components[WIN_SECOND].match_level_features == [MIRROR_SPEC]
        assert components[WIN_FIRST].match_level_features == [DIFF_SPEC]

    def test_the_candidate_wins_over_joint_selected_for_its_own_arm(self, selector):
        # the arm under search is being perturbed, so its entry in
        # `_joint_selected` is last round's selection, not this candidate
        selector._joint_selected = {WIN_SECOND: ([MIRROR_SPEC], [])}
        model = selector._build_candidate_model(
            [DIFF_SPEC], [], selector._scoring_params(), arm=WIN_SECOND,
        )
        assert model.components()[WIN_SECOND].match_level_features == [DIFF_SPEC]

    def test_first_in_still_refuses_state_derivable_points(self, selector):
        # the guard reads the ARM now, not serve_component — this run's
        # component is win_second, which accepts is_break_point
        with pytest.raises(ValueError, match="state-derivable"):
            selector._build_candidate_model(
                [], ["is_break_point"], selector._scoring_params(), arm=FIRST_IN,
            )


class TestScoringThreadsTheArm:
    """Naming this run's own arm changes nothing about the score."""

    def test_chain_score_is_unchanged_when_the_arm_is_named(self, selector):
        default = selector._score_cv_chain_detailed([DIFF_SPEC], [])
        explicit = selector._score_cv_chain_detailed([DIFF_SPEC], [], arm=WIN_SECOND)
        assert math.isfinite(default[0])
        assert explicit == default

    def test_match_grain_forwards_the_arm_to_the_chain(self, selector):
        forwarded = selector._score_cv_match_grain_detailed(
            [DIFF_SPEC], [], arm=WIN_SECOND,
        )
        assert forwarded == selector._score_cv_chain_detailed(
            [DIFF_SPEC], [], arm=WIN_SECOND,
        )
