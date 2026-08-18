"""Tests for the two-level serve estimator.

The composition is arithmetic, so most of these pin identities rather than
behaviour. The ones that matter operationally are the branch filter surviving
`preloaded_points` (the path FS drives) and the fingerprint separating configs
that differ only in a component's feature list.
"""

import numpy as np
import polars as pl
import pytest

from mvp.common.config_hash import compute_iid_fingerprint
from mvp.projection.iid.config import ServeDiscoveryConfig, ServeModelConfig
from mvp.projection.iid.serve_model import (
    ScoreStateChainServeModel,
    build_serve_model,
)
from mvp.projection.iid.two_level_serve_model import (
    COMPONENTS,
    FIRST_IN,
    WIN_FIRST,
    WIN_SECOND,
    TwoLevelServeModel,
)


def _cfg(**over):
    base = dict(
        type="two_level",
        first_in_match_features=["player_age_diff"],
        win_first_match_features=["player_age_diff"],
        win_second_match_features=["player_age_diff"],
    )
    base.update(over)
    return ServeModelConfig(**base)


class TestComposition:
    """p = fi*w1 + (1-fi)*w2, and the identities that follow from it."""

    @staticmethod
    def _raw(fi, w1, w2):
        return TwoLevelServeModel._compose_raw(
            np.array([fi]), np.array([w1]), np.array([w2])
        )[0]

    def test_equal_branches_collapse_to_that_value(self):
        # The degenerate recovery, in arithmetic form: when both branches agree,
        # the composite is that value for ANY first-serve rate. This is why a
        # feature-blind, state-blind two-level model reproduces the one-level p
        # exactly (verified end-to-end at 0.00e+00), and it must not drift.
        for fi in (0.0, 0.37, 0.5, 0.618, 1.0):
            assert self._raw(fi, 0.61, 0.61) == pytest.approx(0.61, abs=1e-15)

    def test_endpoints_select_a_branch(self):
        assert self._raw(1.0, 0.69, 0.50) == pytest.approx(0.69, abs=1e-15)
        assert self._raw(0.0, 0.69, 0.50) == pytest.approx(0.50, abs=1e-15)

    def test_reproduces_the_measured_corpus_identity(self):
        # P(1st in)=0.6182, P(win|1st)=0.6904, P(win|2nd)=0.4971 compose to the
        # observed marginal 0.616579 — the arithmetic the whole spec rests on.
        #
        # Tolerance is set by the INPUTS, not by the identity: those are the
        # reported 4-decimal component values, so the composite can only agree
        # to the precision they carry (~2e-5 here). At full precision on the
        # corpus the identity closed at 0.00e+00, which is what
        # `test_equal_branches_collapse_to_that_value` pins exactly.
        got = self._raw(0.6182, 0.6904, 0.4971)
        assert got == pytest.approx(0.616579, abs=1e-4)

    def test_is_monotone_in_the_first_serve_rate(self):
        # With a stronger first serve than second, more first serves in must
        # weakly raise p. A sign error here would invert the whole model.
        vals = [self._raw(fi, 0.70, 0.50) for fi in np.linspace(0, 1, 11)]
        assert all(b > a for a, b in zip(vals, vals[1:], strict=False))


class TestFactory:
    def test_builds_a_two_level_estimator(self):
        est = build_serve_model(_cfg())
        assert isinstance(est, TwoLevelServeModel)
        assert est.is_state_aware is True

    def test_branches_are_one_implementation_split_by_a_row_filter(self):
        est = build_serve_model(_cfg())
        assert isinstance(est._win_first, ScoreStateChainServeModel)
        assert isinstance(est._win_second, ScoreStateChainServeModel)
        assert est._win_first.serve_branch == 1
        assert est._win_second.serve_branch == 2

    def test_first_in_takes_no_score_state(self):
        est = build_serve_model(_cfg())
        assert not hasattr(est._first_in, "predict_state_fn")

    def test_empty_component_sets_raise(self):
        with pytest.raises(ValueError, match="two_level"):
            build_serve_model(ServeModelConfig(type="two_level"))

    def test_components_tuple_is_the_addressable_set(self):
        assert COMPONENTS == (FIRST_IN, WIN_FIRST, WIN_SECOND)


class TestServeBranchFilter:
    """The filter must survive the path FS actually drives."""

    def test_rejects_a_bad_branch_value(self):
        with pytest.raises(ValueError, match="serve_branch"):
            ScoreStateChainServeModel(
                model_type="logistic", match_level_features=["x"],
                point_level_features=[], serve_branch=3,
            )

    def _model(self, branch):
        return ScoreStateChainServeModel(
            model_type="logistic", match_level_features=["x"],
            point_level_features=[], serve_branch=branch,
        )

    def test_filters_a_preloaded_frame(self):
        # The trap: FS passes `preloaded_points`, so a filter attached to the
        # parquet read alone would leave both branches training on all rows —
        # a wrong model that raises nothing and scores plausibly.
        pts = pl.DataFrame({"serve": [1, 2, 1, 2, 2], "v": [1, 2, 3, 4, 5]})
        assert self._model(1)._apply_serve_branch(pts)["v"].to_list() == [1, 3]
        assert self._model(2)._apply_serve_branch(pts)["v"].to_list() == [2, 4, 5]

    def test_none_is_a_passthrough(self):
        pts = pl.DataFrame({"serve": [1, 2], "v": [1, 2]})
        assert self._model(None)._apply_serve_branch(pts).height == 2

    def test_missing_serve_column_raises_rather_than_silently_passing(self):
        with pytest.raises(ValueError, match="serve"):
            self._model(1)._apply_serve_branch(pl.DataFrame({"v": [1]}))

    def test_fit_applies_the_filter_to_preloaded_points(self):
        """The wiring, not the helper.

        `test_filters_a_preloaded_frame` above proves `_apply_serve_branch`
        works; it does NOT prove `fit` calls it. Removing the call from `fit`
        leaves that test green while FS — which always passes
        `preloaded_points` — silently trains both branches on every row.

        Exercised by handing a serve==2 model only serve==1 rows: with the
        filter wired, nothing survives and `fit` raises. Without it, fit walks
        on into feature work with the wrong rows.
        """
        df = pl.DataFrame({"match_uid": ["m1"]})
        only_first = pl.DataFrame({
            "match_uid": ["m1", "m1"], "serve": [1, 1],
            "point_won_by_server": [True, False],
        })
        model = self._model(2)
        with pytest.raises(ValueError, match="no points rows"):
            model.fit(df, preloaded_points=only_first)

    def test_fit_keeps_matching_preloaded_rows(self):
        # The converse: the filter must not reject rows that DO belong to the
        # branch. Failing past the row check means the filter kept them.
        df = pl.DataFrame({"match_uid": ["m1"]})
        only_first = pl.DataFrame({
            "match_uid": ["m1", "m1"], "serve": [1, 1],
            "point_won_by_server": [True, False],
        })
        model = self._model(1)
        with pytest.raises(Exception) as exc:
            model.fit(df, preloaded_points=only_first)
        assert "no points rows" not in str(exc.value)

    def test_post_pickle_default_is_none(self):
        # A joblib written before serve_branch existed must restore as
        # "train on every point", not AttributeError on first read.
        m = ScoreStateChainServeModel(
            model_type="logistic", match_level_features=["x"],
            point_level_features=[],
        )
        state = dict(m.__dict__)
        state.pop("serve_branch")
        restored = ScoreStateChainServeModel.__new__(ScoreStateChainServeModel)
        restored.__setstate__(state)
        assert restored.serve_branch is None


class TestFingerprint:
    """Two-level configs differing only in a component list are different models."""

    def _fp(self, cfg):
        from mvp.projection.iid.config import IIDProjectionConfig

        full = IIDProjectionConfig(
            data={
                "date_range": {"start": "2023-01-01", "end": "2026-01-01"},
                "filters": {"draw_type": "singles"},
            },
            features={"include": ["player_age_diff"]},
            serve_model=cfg,
        )
        return compute_iid_fingerprint(full, config_path=None)

    @pytest.mark.parametrize(
        "field",
        ["first_in_match_features", "win_first_match_features",
         "win_first_point_features", "win_second_match_features",
         "win_second_point_features"],
    )
    def test_each_component_list_reaches_the_hash(self, field):
        # The silent-overwrite hazard: an unregistered field lets two configs
        # share one projection_evaluations/<fp>/ and the second clobbers the
        # first's artifacts with nothing raised.
        a = _cfg()
        b = _cfg(**{field: [*getattr(a, field), "is_tour"]})
        assert self._fp(a) != self._fp(b), f"{field} does not reach the hash"

    def test_identical_configs_agree(self):
        assert self._fp(_cfg()) == self._fp(_cfg())


class TestPromotedConfig:
    """FS must promote the model it selected against, not a single-level one."""

    def _disco(self, component):
        from mvp.projection.iid.config import ServeDiscoveryConfig

        return ServeDiscoveryConfig(
            data={"date_range": {"start": "2023-01-01", "end": "2026-01-01"},
                  "filters": {"draw_type": "singles"}},
            metric="iid_crps_total_games",
            serve_component=component,
            serve_model=_cfg(
                first_in_match_features=["player_age_diff"],
                win_first_match_features=["player_glicko_rd_diff"],
                win_first_point_features=["sets_won_asymmetry"],
                win_second_match_features=["player_height_diff"],
                win_second_point_features=["set_score_asymmetry"],
            ),
        )

    def test_single_level_run_still_emits_score_state(self):
        from mvp.projection.iid.config import ServeDiscoveryConfig

        cfg = ServeDiscoveryConfig(
            data={"date_range": {"start": "2023-01-01", "end": "2026-01-01"},
                  "filters": {"draw_type": "singles"}},
        )
        out = cfg.to_iid_projection_config_dict(["player_age_diff"], ["is_tiebreak"])
        assert out["serve_model"]["type"] == "score_state"

    def test_two_level_run_emits_two_level(self):
        out = self._disco("win_first").to_iid_projection_config_dict(
            ["player_svc_elo_matchup"], ["is_break_point"],
        )
        assert out["serve_model"]["type"] == "two_level"

    def test_selected_lists_land_on_the_named_component(self):
        out = self._disco("win_second").to_iid_projection_config_dict(
            ["player_svc_elo_matchup"], ["is_break_point"],
        )
        sm = out["serve_model"]
        assert sm["win_second_match_features"] == ["player_svc_elo_matchup"]
        assert sm["win_second_point_features"] == ["is_break_point"]

    def test_non_selected_components_are_carried_forward(self):
        # The silent-loss case: promoting only the selected component would
        # drop the other two, emitting a config that runs but is not the model
        # FS scored.
        out = self._disco("win_second").to_iid_projection_config_dict(
            ["player_svc_elo_matchup"], ["is_break_point"],
        )
        sm = out["serve_model"]
        assert sm["first_in_match_features"] == ["player_age_diff"]
        assert sm["win_first_match_features"] == ["player_glicko_rd_diff"]
        assert sm["win_first_point_features"] == ["sets_won_asymmetry"]

    def test_first_in_takes_only_the_match_list(self):
        out = self._disco("first_in").to_iid_projection_config_dict(
            ["player_svc_elo_matchup"], [],
        )
        assert out["serve_model"]["first_in_match_features"] == ["player_svc_elo_matchup"]

    def test_include_covers_every_component_not_just_the_selected_one(self):
        # The engine must compute the non-selected components' features too, or
        # the promoted config loads and then fails at predict on a missing col.
        out = self._disco("win_second").to_iid_projection_config_dict(
            ["player_svc_elo_matchup"], ["is_break_point"],
        )
        inc = set(out["features"]["include"])
        for spec in ("player_age_diff", "player_glicko_rd_diff",
                     "player_svc_elo_matchup"):
            assert spec in inc, f"{spec} missing from features.include"

    def test_component_without_serve_model_raises(self):
        from mvp.projection.iid.config import ServeDiscoveryConfig

        cfg = ServeDiscoveryConfig(
            data={"date_range": {"start": "2023-01-01", "end": "2026-01-01"},
                  "filters": {"draw_type": "singles"}},
            serve_component="win_first",
        )
        with pytest.raises(ValueError, match="serve_model"):
            cfg.to_iid_projection_config_dict(["player_age_diff"], [])


class TestFirstInPointFeatureBoundary:
    """first_in takes match-constant point features, refuses state-derivable ones.

    The narrowing is only safe if the boundary is EXACTLY the set the win
    branches route on. These pin that, because a boundary that drifts either
    starves first_in of surface (its only route to it) or silently accepts a
    feature that has no ScoreState to be evaluated at.
    """

    def _selector_cfg(self):
        from mvp.projection.iid.config import ServeDiscoveryConfig

        return ServeDiscoveryConfig(
            data={"date_range": {"start": "2023-01-01", "end": "2026-01-01"},
                  "filters": {"draw_type": "singles"}},
            metric="iid_crps_total_games",
            serve_component="first_in",
            serve_model=_cfg(),
        )

    def _build(self, point_level):
        from mvp.projection.iid.serve_discovery import ServeDiscoverySelector

        sel = ServeDiscoverySelector.__new__(ServeDiscoverySelector)
        sel.config = self._selector_cfg()
        sel.points_path = None
        sel.matches_path = None
        sel.cache_dir = None
        sel._engine = None
        return sel._build_candidate_model(["player_age_diff"], point_level, {})

    def test_surface_flags_are_accepted(self):
        est = self._build(["is_surface_hard", "is_surface_clay"])
        assert est.first_in_point_features == ["is_surface_hard", "is_surface_clay"]

    def test_state_derivable_is_refused(self):
        with pytest.raises(ValueError, match="state-derivable"):
            self._build(["is_break_point"])

    def test_the_boundary_is_the_win_branches_own_set(self):
        # Not a restated list: the refusal keys off _STATE_DERIVABLE itself, so
        # a feature the win branches treat as state must be refused here, and a
        # feature they treat as match-constant must be accepted.
        derivable = ScoreStateChainServeModel._STATE_DERIVABLE
        assert "is_break_point" in derivable
        assert "sets_won_asymmetry" in derivable
        for surface in ("is_surface_hard", "is_surface_clay", "is_surface_grass"):
            assert surface not in derivable, (
                f"{surface} became state-derivable; first_in would now refuse "
                "its only route to surface"
            )

    def test_mixed_list_refuses_and_names_only_the_bad_ones(self):
        with pytest.raises(ValueError) as exc:
            self._build(["is_surface_hard", "sets_won_asymmetry"])
        assert "sets_won_asymmetry" in str(exc.value)
        assert "is_surface_hard" not in str(exc.value)


class TestPromotedConfigIncludeList:
    """`features.include` must contain only specs the engine can resolve.

    The emitter used to pair every `player_X` with an `opp_X` unconditionally.
    That is merely wasteful for registry-backed diffs (the engine can compute
    `opp_X_diff`, and nothing reads it â€” a diff's swap value is the negation of
    the player value). It is FATAL for transform outputs, which register explicit
    column names: only `player_vs_opp_style_resid_flat_diff` exists, so the
    invented `opp_` twin falls through to `registry.get(base_name)` and raises
    KeyError, making the promoted config unrunnable.
    """

    def _emit(self, match, point=()):
        cfg = ServeDiscoveryConfig(
            data={"date_range": {"start": "2023-01-01", "end": "2026-01-01"}},
            metric="iid_crps_spread",
        )
        return cfg.to_iid_projection_config_dict(
            selected_match_level=list(match),
            selected_point_level=list(point),
            model_type="xgboost",
        )

    def test_transform_output_diff_gets_no_invented_opp_twin(self):
        inc = self._emit(["player_vs_opp_style_resid_flat_diff"])["features"]["include"]
        assert "player_vs_opp_style_resid_flat_diff" in inc
        # No such column is registered or produced by any transform.
        assert "opp_vs_opp_style_resid_flat_diff" not in inc

    def test_mirror_feature_does_get_its_opp_column(self):
        # predict_state_fn reads opp_X at the swap side for mirror features, so
        # this one MUST be present or the projection raises ColumnNotFoundError.
        inc = self._emit(["player_glicko_rd"])["features"]["include"]
        assert "opp_glicko_rd" in inc

    def test_params_survive_onto_the_opp_spec(self):
        inc = self._emit(["player_surface_matches(days=30)"])["features"]["include"]
        assert "opp_surface_matches(days=30)" in inc

    def test_include_matches_swap_side_opp_specs(self):
        """The emitter and the FS materializer must agree on which opp_ columns
        exist, or a config is promoted naming columns the run never built."""
        from mvp.projection.iid.serve_model import swap_side_opp_specs

        match = [
            "player_elo_surface_indoor_diff",
            "player_glicko_rd",
            "player_vs_opp_style_resid_flat_diff",
            "opp_surface_matches(days=30)",
        ]
        inc = self._emit(match)["features"]["include"]
        assert set(inc) == set(match) | set(swap_side_opp_specs(match))
