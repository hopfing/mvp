"""Joint serve FS, seam 1: the `joint_selection` config block (#112, spec #111).

The three serve components are called ARMS here, as in the spec. This file
observes the config seam only -- `ServeDiscoveryConfig.from_yaml` and the
module-level `substitute_arm_lists` -- plus the promotion helper's output. No
selector, no data, no engine: #113 is what makes a joint run select anything.

`TestPromotionUnchanged` is the refactor's guard rail. Its expected blocks were
captured by running `to_iid_projection_config_dict` on the pre-refactor code
(commit 4ffb8cd) for every shape in the prefit table, so a component run's
emitted `serve_model` is pinned byte for byte across the move onto the shared
arm-substitution helper.
"""

import re
from textwrap import dedent

import pytest
import yaml

from mvp.projection.iid.config import (
    ServeDiscoveryConfig,
    ServeModelConfig,
    substitute_arm_lists,
)
from mvp.projection.iid.metric_registry import chain_metric_names
from tests.projection.iid.test_serve_discovery_prefit import SHAPES, _two_level_config
from tests.projection.iid.test_serve_discovery_swap_side import DIFF_SPEC, MIRROR_SPEC

CHAIN_METRIC = "iid_match_win_log_loss"

# Everything a ServeDiscoveryConfig needs and this file never varies. The parts
# under test are appended as dumped YAML rather than f-string-substituted: a
# substitution inside a `dedent` block destroys its common prefix, and
# `sort_keys=False` is what keeps arm order (the tie-break order) intact.
_BASE = dedent("""
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
""")

_TWO_LEVEL = {
    "type": "two_level",
    "model_type": "xgboost",
    "first_in_match_features": [MIRROR_SPEC],
    "first_in_point_features": [],
    "win_first_match_features": [DIFF_SPEC],
    "win_first_point_features": ["is_break_point"],
    "win_second_match_features": [],
    "win_second_point_features": [],
}

_UNSET = object()


def _yaml(
    *,
    arms=_UNSET,
    metric=CHAIN_METRIC,
    serve_component=_UNSET,
    serve_model=_UNSET,
    features=_UNSET,
) -> str:
    """A serve discovery YAML document. `_UNSET` omits the key entirely;
    `None` writes it as an explicit null."""
    blocks: dict = {}
    if metric is not _UNSET:
        blocks["metric"] = metric
    if serve_component is not _UNSET:
        blocks["serve_component"] = serve_component
    blocks["serve_model"] = _TWO_LEVEL if serve_model is _UNSET else serve_model
    if features is not _UNSET:
        blocks["features"] = features
    if arms is not _UNSET:
        blocks["joint_selection"] = {"arms": arms}
    return _BASE + yaml.safe_dump(blocks, sort_keys=False)


def _two_arms() -> dict:
    return {"win_first": {}, "win_second": {}}


class TestJointSelectionValidation:
    """One test per rule, each pinning the exact message an operator sees."""

    def test_joint_and_serve_component_are_mutually_exclusive(self):
        msg = "joint_selection and serve_component are mutually exclusive"
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(
                _yaml(arms=_two_arms(), serve_component="win_first")
            )

    @pytest.mark.parametrize(
        "serve_model",
        [None, {"type": "score_state"}],
        ids=["missing", "score_state"],
    )
    def test_joint_requires_a_two_level_serve_model(self, serve_model):
        msg = "joint_selection requires serve_model.type == 'two_level'"
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(
                _yaml(arms=_two_arms(), serve_model=serve_model)
            )

    def test_unknown_arm_name_is_refused(self):
        msg = (
            "joint_selection.arms.win_third: not one of "
            "first_in / win_first / win_second"
        )
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(
                _yaml(arms={"win_first": {}, "win_third": {}})
            )

    def test_a_single_arm_is_refused(self):
        msg = (
            "joint_selection needs at least two arms; use serve_component "
            "for a single arm"
        )
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(_yaml(arms={"win_first": {}}))

    def test_non_chain_metric_is_refused(self):
        msg = (
            "metric 'brier_score' is point-grain; joint_selection scores every "
            "arm on one chain metric (one of: "
            f"{', '.join(sorted(chain_metric_names()))})"
        )
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(
                _yaml(arms=_two_arms(), metric="brier_score")
            )

    def test_the_default_metric_is_refused(self):
        """`metric` defaults to a point-grain metric, so a joint config that
        omits it is refused by the same rule rather than scoring arms on
        numbers that are not comparable."""
        msg = "metric 'log_loss' is point-grain; joint_selection scores every arm"
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(_yaml(arms=_two_arms(), metric=_UNSET))

    @pytest.mark.parametrize(
        "field", ["base_match_level_features", "base_point_level_features"]
    )
    def test_shared_base_lists_are_refused(self, field):
        msg = (
            "joint_selection: features.base_match_level_features / "
            "base_point_level_features must be empty; pin per arm via "
            "serve_model.<arm>_match_features / <arm>_point_features"
        )
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(
                _yaml(arms=_two_arms(), features={field: [MIRROR_SPEC]})
            )

    @pytest.mark.parametrize(
        "field",
        ["candidate_match_level_features", "candidate_point_level_features"],
    )
    def test_shared_candidate_lists_are_refused(self, field):
        msg = (
            "joint_selection: features.candidate_match_level_features / "
            "candidate_point_level_features must be empty; list candidates "
            "per arm under joint_selection.arms.<arm>"
        )
        with pytest.raises(ValueError, match=re.escape(msg)):
            ServeDiscoveryConfig.from_yaml(
                _yaml(arms=_two_arms(), features={field: [MIRROR_SPEC]})
            )


class TestJointSelectionAccepts:
    def test_two_arm_block_parses(self):
        cfg = ServeDiscoveryConfig.from_yaml(_yaml(arms=_two_arms()))
        assert cfg.joint_arms() == ["win_first", "win_second"]

    def test_three_arm_block_keeps_config_order(self):
        """Arm order is the round's tie-break order, so it is the config's
        order and not sorted, insertion-sorted, or the COMPONENTS order."""
        arms = {"win_second": {}, "first_in": {}, "win_first": {}}
        cfg = ServeDiscoveryConfig.from_yaml(_yaml(arms=arms))
        assert cfg.joint_arms() == ["win_second", "first_in", "win_first"]

    def test_empty_arm_lists_and_absent_caps_are_accepted(self):
        cfg = ServeDiscoveryConfig.from_yaml(_yaml(arms=_two_arms()))
        arm = cfg.joint_selection.arms["win_first"]
        assert arm.candidate_match_level_features == []
        assert arm.candidate_point_level_features == []
        assert arm.max_features is None

    def test_per_arm_lists_and_caps_round_trip(self):
        arms = {
            "win_first": {
                "candidate_match_level_features": [MIRROR_SPEC],
                "candidate_point_level_features": ["is_break_point"],
                "max_features": 3,
            },
            "win_second": {"candidate_match_level_features": [DIFF_SPEC]},
        }
        cfg = ServeDiscoveryConfig.from_yaml(_yaml(arms=arms))
        w1 = cfg.joint_selection.arms["win_first"]
        assert w1.candidate_match_level_features == [MIRROR_SPEC]
        assert w1.candidate_point_level_features == ["is_break_point"]
        assert w1.max_features == 3
        assert cfg.joint_selection.arms["win_second"].max_features is None

    def test_explicit_null_serve_component_is_accepted(self):
        cfg = ServeDiscoveryConfig.from_yaml(
            _yaml(arms=_two_arms(), serve_component=None)
        )
        assert cfg.serve_component is None
        assert cfg.joint_arms() == ["win_first", "win_second"]

    def test_a_component_config_has_no_joint_arms(self):
        cfg = ServeDiscoveryConfig.from_yaml(
            _yaml(serve_component="win_first", metric=CHAIN_METRIC)
        )
        assert cfg.joint_selection is None
        assert cfg.joint_arms() == []


class TestSubstituteArmLists:
    def _model(self) -> ServeModelConfig:
        return ServeModelConfig(**_TWO_LEVEL)

    def test_returns_a_deep_copy(self):
        base = self._model()
        out = substitute_arm_lists(base, {"win_second": ([MIRROR_SPEC], [])})
        assert out is not base
        out.win_second_match_features.append(DIFF_SPEC)
        out.first_in_match_features.append(DIFF_SPEC)
        assert base.win_second_match_features == []
        assert base.first_in_match_features == [MIRROR_SPEC]

    def test_only_the_named_arms_are_replaced(self):
        base = self._model()
        out = substitute_arm_lists(
            base,
            {
                "win_first": ([MIRROR_SPEC], ["is_tiebreak"]),
                "win_second": ([DIFF_SPEC], []),
            },
        )
        assert out.win_first_match_features == [MIRROR_SPEC]
        assert out.win_first_point_features == ["is_tiebreak"]
        assert out.win_second_match_features == [DIFF_SPEC]
        # the held arm and every non-arm field survive untouched
        assert out.first_in_match_features == [MIRROR_SPEC]
        assert out.first_in_point_features == []
        untouched = {
            k: v for k, v in out.model_dump().items()
            if not k.startswith(("win_first_", "win_second_"))
        }
        expected = {
            k: v for k, v in base.model_dump().items()
            if not k.startswith(("win_first_", "win_second_"))
        }
        assert untouched == expected

    def test_an_empty_pair_clears_an_arm(self):
        out = substitute_arm_lists(self._model(), {"win_first": ([], [])})
        assert out.win_first_match_features == []
        assert out.win_first_point_features == []

    def test_unknown_arm_raises(self):
        msg = (
            "unknown serve arm 'serve_component'; expected one of "
            "first_in / win_first / win_second"
        )
        with pytest.raises(ValueError, match=re.escape(msg)):
            substitute_arm_lists(
                self._model(), {"serve_component": ([MIRROR_SPEC], [])}
            )

    def test_no_substitution_is_still_a_copy(self):
        base = self._model()
        out = substitute_arm_lists(base, {})
        assert out is not base
        assert out.model_dump() == base.model_dump()


# Captured from `to_iid_projection_config_dict(...)["serve_model"]` on the
# pre-refactor source (4ffb8cd), one entry per prefit shape. Do not regenerate:
# a diff here means a component run's promoted model changed.
_SEL_MATCH = [MIRROR_SPEC, DIFF_SPEC]
_SEL_POINT = ["is_break_point"]

SNAPSHOT = {
    "win_second": {
        "calib_intercept": 0.0,
        "calib_slope": 1.0,
        "clip_max": 0.9,
        "clip_min": 0.3,
        "feature_columns": [],
        "first_in_match_features": [],
        "first_in_params": {},
        "first_in_point_features": [],
        "gap_shrink": 1.0,
        "match_level_columns": [],
        "match_level_features": [],
        "model_type": "xgboost",
        "params": {"n_estimators": 8},
        "point_level_features": [],
        "posterior_draws": 200,
        "posterior_seed": 0,
        "regressor": {"params": {}, "type": "ridge"},
        "surface_circuit_offset": {},
        "type": "two_level",
        "win_first_match_features": [MIRROR_SPEC],
        "win_first_point_features": [],
        "win_second_match_features": [MIRROR_SPEC, DIFF_SPEC],
        "win_second_point_features": ["is_break_point"],
        "window": 90,
    },
    "win_first": {
        "calib_intercept": 0.0,
        "calib_slope": 1.0,
        "clip_max": 0.9,
        "clip_min": 0.3,
        "feature_columns": [],
        "first_in_match_features": [MIRROR_SPEC],
        "first_in_params": {},
        "first_in_point_features": [],
        "gap_shrink": 1.0,
        "match_level_columns": [],
        "match_level_features": [],
        "model_type": "xgboost",
        "params": {"n_estimators": 8},
        "point_level_features": [],
        "posterior_draws": 200,
        "posterior_seed": 0,
        "regressor": {"params": {}, "type": "ridge"},
        "surface_circuit_offset": {},
        "type": "two_level",
        "win_first_match_features": [MIRROR_SPEC, DIFF_SPEC],
        "win_first_point_features": ["is_break_point"],
        "win_second_match_features": [],
        "win_second_point_features": [],
        "window": 90,
    },
    "first_in": {
        "calib_intercept": 0.0,
        "calib_slope": 1.0,
        "clip_max": 0.9,
        "clip_min": 0.3,
        "feature_columns": [],
        "first_in_match_features": [MIRROR_SPEC, DIFF_SPEC],
        "first_in_params": {},
        "first_in_point_features": ["is_break_point"],
        "gap_shrink": 1.0,
        "match_level_columns": [],
        "match_level_features": [],
        "model_type": "xgboost",
        "params": {"n_estimators": 8},
        "point_level_features": [],
        "posterior_draws": 200,
        "posterior_seed": 0,
        "regressor": {"params": {}, "type": "ridge"},
        "surface_circuit_offset": {},
        "type": "two_level",
        "win_first_match_features": [MIRROR_SPEC],
        "win_first_point_features": [],
        "win_second_match_features": [DIFF_SPEC],
        "win_second_point_features": [],
        "window": 90,
    },
    "fixed_outside_pool": {
        "calib_intercept": 0.0,
        "calib_slope": 1.0,
        "clip_max": 0.9,
        "clip_min": 0.3,
        "feature_columns": [],
        "first_in_match_features": [MIRROR_SPEC, DIFF_SPEC],
        "first_in_params": {},
        "first_in_point_features": ["is_break_point"],
        "gap_shrink": 1.0,
        "match_level_columns": [],
        "match_level_features": [],
        "model_type": "xgboost",
        "params": {"n_estimators": 8},
        "point_level_features": [],
        "posterior_draws": 200,
        "posterior_seed": 0,
        "regressor": {"params": {}, "type": "ridge"},
        "surface_circuit_offset": {},
        "type": "two_level",
        "win_first_match_features": ["player_svc_elo_matchup"],
        "win_first_point_features": [],
        "win_second_match_features": [],
        "win_second_point_features": [],
        "window": 90,
    },
}


# The COMPONENT shapes only. `SHAPES` also carries the joint shape (#115),
# whose first slot is a tuple of searched arms; it promotes through
# `selected_by_arm`, which is `TestJointPromotion`'s subject below, and has no
# single-component snapshot to be unchanged against.
COMPONENT_SHAPES = [n for n, s in SHAPES.items() if not isinstance(s[0], tuple)]


class TestPromotionUnchanged:
    """Every component shape promotes byte-identically across the refactor."""

    @pytest.mark.parametrize("name", COMPONENT_SHAPES, ids=COMPONENT_SHAPES)
    def test_promoted_serve_model_matches_the_snapshot(self, tmp_path, name):
        cfg = ServeDiscoveryConfig.from_file(
            _two_level_config(tmp_path, SHAPES[name])
        )
        emitted = cfg.to_iid_projection_config_dict(
            _SEL_MATCH, _SEL_POINT,
            model_type="xgboost", model_params={"n_estimators": 8},
        )
        assert emitted["serve_model"] == SNAPSHOT[name]

    def test_snapshot_covers_every_shape(self):
        assert set(SNAPSHOT) == set(COMPONENT_SHAPES)


class TestJointPromotion:
    """The promotion helper's joint entry point. The selector that produces
    `selected_by_arm` lands in #113; this pins the plumbing it will use."""

    def _cfg(self) -> ServeDiscoveryConfig:
        return ServeDiscoveryConfig.from_yaml(_yaml(arms=_two_arms()))

    def test_every_searched_arm_lands_and_the_held_arm_survives(self):
        emitted = self._cfg().to_iid_projection_config_dict(
            [], [],
            model_type="xgboost", model_params={},
            selected_by_arm={
                "win_first": ([MIRROR_SPEC], ["is_break_point"]),
                "win_second": ([DIFF_SPEC], []),
            },
        )
        sm = emitted["serve_model"]
        assert sm["type"] == "two_level"
        assert sm["win_first_match_features"] == [MIRROR_SPEC]
        assert sm["win_first_point_features"] == ["is_break_point"]
        assert sm["win_second_match_features"] == [DIFF_SPEC]
        # held verbatim from the config, not emptied
        assert sm["first_in_match_features"] == [MIRROR_SPEC]

    def test_description_names_the_searched_arms(self):
        emitted = self._cfg().to_iid_projection_config_dict(
            [], [],
            model_type="xgboost", model_params={},
            selected_by_arm={"win_first": ([], []), "win_second": ([], [])},
        )
        assert (
            f"[joint(win_first,win_second) selected on {CHAIN_METRIC}]"
            in emitted["description"]
        )
