"""Tests for the two-level serve estimator.

The composition is arithmetic, so most of these pin identities rather than
behaviour. The ones that matter operationally are the branch filter surviving
`preloaded_points` (the path FS drives) and the fingerprint separating configs
that differ only in a component's feature list.
"""

import pickle

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

    def test_all_empty_builds_with_every_component_degenerate(self):
        """The feature-blind fit must be constructible.

        It is what validation-ladder step 2 (degenerate recovery) is DEFINED as,
        and it is the round-0 state of any component-wise FS starting from
        nothing — round 1 leaves the two non-selected components empty. This
        used to raise, which made both unreachable.
        """
        est = build_serve_model(ServeModelConfig(type="two_level"))
        assert isinstance(est, TwoLevelServeModel)
        # Each win branch degrades to its own training rate rather than a model.
        assert not isinstance(est._win_first, ScoreStateChainServeModel)
        assert not isinstance(est._win_second, ScoreStateChainServeModel)
        assert est._win_first.serve_branch == 1
        assert est._win_second.serve_branch == 2
        # first_in already had this path — an empty set is intercept-only.
        assert est._first_in.match_level_features == []

    def test_one_populated_component_leaves_the_others_degenerate(self):
        """Round 1 of a component-wise FS: one feature, two empty components."""
        est = build_serve_model(
            ServeModelConfig(type="two_level", win_first_match_features=["a"])
        )
        assert isinstance(est._win_first, ScoreStateChainServeModel)
        assert not isinstance(est._win_second, ScoreStateChainServeModel)

    def test_a_degenerate_branch_is_constant_across_states(self):
        est = build_serve_model(ServeModelConfig(type="two_level"))
        est._win_first._rate = 0.7
        fn_a, fn_b = est._win_first.predict_state_fn(
            pl.DataFrame({"match_uid": ["m1", "m2"]})
        )
        # No state dependence and no side dependence: the branch carries a rate.
        assert fn_a(object()).tolist() == [0.7, 0.7]
        assert fn_b(object()).tolist() == [0.7, 0.7]

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
    `opp_X_diff`, and nothing reads it — a diff's swap value is the negation of
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

    def test_include_matches_the_partner_resolver(self):
        """The emitter must name exactly what `_match_feature_values` reads."""
        from mvp.projection.iid.serve_model import swap_side_partner_specs

        match = [
            "player_elo_surface_indoor_diff",
            "player_glicko_rd",
            "player_vs_opp_style_resid_flat_diff",
            "opp_surface_matches(days=30)",
        ]
        inc = self._emit(match)["features"]["include"]
        assert set(inc) == set(match) | set(swap_side_partner_specs(match))

    def test_opp_selected_mirror_gets_its_player_partner(self):
        """The swap side of an `opp_`-prefixed mirror reads the `player_` column.

        `_match_feature_values` (serve_model.py:875-876) resolves a `returner_`
        mirror column to `opp_` when A serves and `player_` when B serves. So a
        selection FS made as `opp_surface_matches(days=30)` — which it did, via
        the shortlist's composite-side expansion — needs both.

        This passes without the fix only by accident: the engine computes every
        feature player-side and derives `opp_` by mirroring, so the player-side
        column exists whether or not anything asked for it.
        """
        inc = self._emit(["opp_surface_matches(days=30)"])["features"]["include"]
        assert "opp_surface_matches(days=30)" in inc
        assert "player_surface_matches(days=30)" in inc

    def test_diffs_still_get_no_partner_in_either_direction(self):
        for spec in ("player_elo_surface_indoor_diff",
                     "player_vs_opp_style_resid_flat_diff"):
            inc = self._emit([spec])["features"]["include"]
            assert inc == [spec], inc


class TestConstantBranchFit:
    """`_ConstantBranch` stands in for an unselected win branch. It shares no
    base class with the score-state model, so it needs the branch filter as a
    module-level function rather than a method — the call that was missing.
    """

    @staticmethod
    def _points():
        return pl.DataFrame({
            "match_uid": ["m1"] * 6,
            "serve": [1, 1, 1, 2, 2, 2],
            "point_won_by_server": [True, True, False, True, False, False],
        })

    def test_it_fits_the_rate_of_its_own_branch(self):
        from mvp.projection.iid.two_level_serve_model import _ConstantBranch

        for branch, expected in ((1, 2 / 3), (2, 1 / 3)):
            b = _ConstantBranch(serve_branch=branch)
            b.fit(pl.DataFrame({"match_uid": ["m1"]}), preloaded_points=self._points())
            assert b._rate == pytest.approx(expected), f"branch {branch}"

    def test_the_branches_differ(self):
        """If the filter were dropped, both branches would fit the pooled rate
        and the fallback would be silently wrong rather than absent."""
        from mvp.projection.iid.two_level_serve_model import _ConstantBranch

        rates = []
        for branch in (1, 2):
            b = _ConstantBranch(serve_branch=branch)
            b.fit(pl.DataFrame({"match_uid": ["m1"]}), preloaded_points=self._points())
            rates.append(b._rate)
        assert rates[0] != rates[1]

    def test_predict_returns_the_fitted_rate(self):
        from mvp.projection.iid.two_level_serve_model import _ConstantBranch

        b = _ConstantBranch(serve_branch=1)
        b.fit(pl.DataFrame({"match_uid": ["m1"]}), preloaded_points=self._points())
        df = pl.DataFrame({"match_uid": ["m1", "m1"]})
        first, second = b.predict_state_fn(df)
        assert first(None).tolist() == pytest.approx([2 / 3, 2 / 3])

    def test_an_empty_branch_raises(self):
        """No points on the branch means no rate to fall back to — better than
        silently returning 0.5."""
        from mvp.projection.iid.two_level_serve_model import _ConstantBranch

        only_first = self._points().filter(pl.col("serve") == 1)
        b = _ConstantBranch(serve_branch=2)
        with pytest.raises(ValueError, match="no training points on this branch"):
            b.fit(pl.DataFrame({"match_uid": ["m1"]}), preloaded_points=only_first)


class TestPickleRoundTrip:
    """A two-level model must survive `joblib.dump` — `_save_artifact` pickles
    `projector.serve_model` on every non-cached `iid-project` / `iid-backtest`.

    `_engine` holds the global FeatureRegistry, whose `register_diff` /
    `register_sum` / `register_matchup` closures pickle cannot resolve by
    qualified name. `ScoreStateChainServeModel` scrubs it in `__getstate__`, so
    both win branches were already safe; `FirstServeInModel` did not, and was
    the single unscrubbed reference that made EVERY two-level model unsavable.
    Latent until the first two-level backtest — `_engine` is set in `__init__`
    whether or not the arm holds features, so an empty first_in fails too.
    """

    @staticmethod
    def _engine_with_registry_closures():
        """Stand-in for a FeatureEngine: holds a closure pickle cannot name."""
        def _outer():
            def _diff():
                return None
            return _diff

        class _FakeEngine:
            def __init__(self):
                self.registry = {"x_diff": _outer()}

        return _FakeEngine()

    def _model(self, **overrides):
        from mvp.projection.iid.two_level_serve_model import TwoLevelServeModel

        kwargs = dict(
            model_type="xgboost",
            first_in_match_features=[],
            first_in_point_features=[],
            win_first_match_features=[],
            win_first_point_features=[],
            win_second_match_features=[],
            win_second_point_features=[],
            engine=self._engine_with_registry_closures(),
        )
        kwargs.update(overrides)
        return TwoLevelServeModel(**kwargs)

    def test_the_bare_engine_is_the_thing_pickle_chokes_on(self):
        """Guards the test itself: if the stand-in were picklable, the round-trip
        assertions below would pass for the wrong reason."""
        with pytest.raises(Exception):
            pickle.dumps(self._engine_with_registry_closures())

    def test_round_trips_with_every_component_empty(self):
        restored = pickle.loads(pickle.dumps(self._model()))
        assert restored._first_in._engine is None

    def test_round_trips_with_first_in_holding_features(self):
        m = self._model(
            first_in_match_features=["player_svc_first_serve_in_pct(days=730)"],
        )
        restored = pickle.loads(pickle.dumps(m))
        assert restored._first_in._engine is None
        # The feature list itself must survive — nulling the engine must not
        # take the configuration with it.
        assert restored._first_in.match_level_features == [
            "player_svc_first_serve_in_pct(days=730)"
        ]

    def test_round_trips_with_all_three_arms_holding_features(self):
        m = self._model(
            first_in_match_features=["player_svc_first_serve_in_pct(days=730)"],
            win_first_match_features=["player_glicko_rd_diff"],
            win_second_match_features=["player_elo_surface_indoor"],
        )
        restored = pickle.loads(pickle.dumps(m))
        assert restored._first_in._engine is None
        assert restored._win_first._engine is None
        assert restored._win_second._engine is None

    def test_first_serve_in_base_rate_survives(self):
        """The fallback `f` is what an unfitted arm predicts, so losing it on
        save would silently change every projection made from the artifact."""
        m = self._model()
        m._first_in._base_rate = 0.6176
        restored = pickle.loads(pickle.dumps(m))
        assert restored._first_in._base_rate == pytest.approx(0.6176)


class TestTwoLevelEmitterIncludeList:
    """The two-level branch of the emitter must use the same partner resolver.

    It used to pair `player_X` with `opp_X` inline and unconditionally, skipping
    the `is_diff` check the one-level branch gets from
    `swap_side_partner_specs`. `TestEmitterIncludeList` above never caught it
    because its `_emit` leaves `serve_component` unset, so every one of those
    tests exercises the one-level path only.

    The cost was that all four Phase-B configs were unrunnable:
    `opp_vs_opp_style_resid_flat_diff` is in no registry and produced by no
    transform, so `_resolve_dependencies` raises KeyError on it.
    """

    W1 = ["opp_ret_elo_surface_indoor_matchup", "player_glicko_rd_diff"]
    W2 = ["player_elo_surface_indoor", "player_vs_opp_style_resid_flat_diff"]

    def _emit(self, selected, component="first_in"):
        cfg = ServeDiscoveryConfig(
            data={"date_range": {"start": "2023-01-01", "end": "2026-01-01"}},
            metric="iid_crps_spread",
            serve_component=component,
            serve_model={
                "type": "two_level",
                "model_type": "xgboost",
                "first_in_match_features": [],
                "first_in_point_features": [],
                "win_first_match_features": list(self.W1),
                "win_first_point_features": [],
                "win_second_match_features": list(self.W2),
                "win_second_point_features": [],
            },
        )
        return cfg.to_iid_projection_config_dict(
            selected_match_level=list(selected),
            selected_point_level=[],
            model_type="xgboost",
        )["features"]["include"]

    def test_transform_output_diff_gets_no_invented_opp_twin(self):
        """The crash. Carried in from `win_second`, not from this run's pick."""
        inc = self._emit(["player_tourn_svc_df_pct"])
        assert "player_vs_opp_style_resid_flat_diff" in inc
        assert "opp_vs_opp_style_resid_flat_diff" not in inc

    def test_registry_diffs_get_no_opp_twin_either(self):
        inc = self._emit(["player_tourn_svc_df_pct"])
        assert "player_glicko_rd_diff" in inc
        assert "opp_glicko_rd_diff" not in inc

    def test_every_component_is_declared_not_just_the_selected_one(self):
        """The non-selected components are carried forward and read at predict
        time, so an include list built from this run's picks alone emits a config
        that loads and then fails on a missing column."""
        inc = self._emit(["player_tourn_svc_df_pct"])
        for spec in self.W1 + self.W2 + ["player_tourn_svc_df_pct"]:
            assert spec in inc, spec

    def test_mirror_features_still_get_their_partner(self):
        inc = self._emit(["player_surface_matches(days=30)"])
        assert "opp_surface_matches(days=30)" in inc
        # `opp_`-prefixed selections need the player-side column too.
        assert "player_ret_elo_surface_indoor_matchup" in inc

    def test_include_matches_the_partner_resolver(self):
        """Same invariant the one-level branch is held to, over all components."""
        from mvp.projection.iid.serve_model import swap_side_partner_specs

        picked = ["player_tourn_svc_df_pct", "player_surface_matches(days=30)"]
        inc = self._emit(picked)
        allspecs = picked + self.W1 + self.W2
        assert set(inc) == set(allspecs) | set(swap_side_partner_specs(allspecs))

    def test_no_duplicates(self):
        inc = self._emit(["player_glicko_rd_diff"])   # also in W1
        assert len(inc) == len(set(inc))


class TestFirstInPerspectiveSwap:
    """`_first_in_for` must read the PARTNER column for the B-serving side.

    The swap applied only the negation half of the rule, so mirrored features
    kept A's values when B served. Every spec in the serve_base_first_in pool
    is mirrored (108 of 108), which made the two sides come back IDENTICAL --
    indistinguishable from a legitimately symmetric match rather than visibly
    wrong. `_score_first_in` then stacked A's prediction against B's realised
    first-serve rate on half the held-out rows.

    The defect was a CALLER not applying the rule, not the rule being wrong, so
    the tests that pin it have to go through `_first_in_for`. The two that call
    `match_feature_matrix` directly are unit tests of the rule and pass on the
    broken code — they are labelled as such rather than trusted to catch a
    regression here.
    """

    class _FirstCol:
        """Stub regressor: the prediction IS the first design column, so a
        test can read back exactly which value reached each side."""

        def predict(self, X):
            return np.asarray(X)[:, 0]

    def _model(self, spec):
        from mvp.projection.iid.two_level_serve_model import FirstServeInModel

        fi = FirstServeInModel("xgboost", [spec])
        fi._cols, fi._is_diff = fi._resolve_cols()
        fi._model = self._FirstCol()
        est = TwoLevelServeModel.__new__(TwoLevelServeModel)
        est._first_in = fi
        return est

    def test_mirrored_feature_reads_partner_column_on_swap(self):
        est = self._model("player_svc_first_serve_in_pct(days=180)")
        df = pl.DataFrame({
            "player_svc_first_serve_in_pct_180d": [0.80],
            "opp_svc_first_serve_in_pct_180d": [0.40],
        })
        a, b = est._first_in_for(df)
        assert a[0] == pytest.approx(0.80)
        assert b[0] == pytest.approx(0.40), (
            "B's side read A's column — the mirrored half of the swap is gone"
        )

    def test_swapping_the_frame_swaps_the_sides(self):
        # The property that does not depend on the stub: relabelling who is
        # player_ and who is opp_ must exchange the two outputs.
        est = self._model("player_svc_first_serve_in_pct(days=180)")
        df = pl.DataFrame({
            "player_svc_first_serve_in_pct_180d": [0.80, 0.55],
            "opp_svc_first_serve_in_pct_180d": [0.40, 0.70],
        })
        flipped = df.rename({
            "player_svc_first_serve_in_pct_180d": "_tmp",
            "opp_svc_first_serve_in_pct_180d": "player_svc_first_serve_in_pct_180d",
        }).rename({"_tmp": "opp_svc_first_serve_in_pct_180d"})
        a, b = est._first_in_for(df)
        a2, b2 = est._first_in_for(flipped)
        np.testing.assert_allclose(a, b2)
        np.testing.assert_allclose(b, a2)

    def test_diff_feature_negates_instead_of_reading_a_partner(self):
        # Rule-level unit test: passes on the broken caller by construction.
        from mvp.projection.iid.serve_model import match_feature_matrix

        cols, is_diff = ["server_age_diff"], [True]
        df = pl.DataFrame({"player_age_diff": [3.0]})
        assert match_feature_matrix(df, cols, is_diff, swap=False)[0, 0] == 3.0
        assert match_feature_matrix(df, cols, is_diff, swap=True)[0, 0] == -3.0

    def test_inference_name_frame_resolves_the_partner_side(self):
        # Rule-level unit test; the caller-level pin is the test below.
        # The FS scorer hands over a frame already in server_/returner_ names,
        # where the partner of server_x is returner_x rather than opp_x.
        from mvp.projection.iid.serve_model import match_feature_matrix

        cols, is_diff = ["server_svc_ace_pct"], [False]
        df = pl.DataFrame({"server_svc_ace_pct": [0.11], "returner_svc_ace_pct": [0.07]})
        assert match_feature_matrix(df, cols, is_diff, swap=False)[0, 0] == pytest.approx(0.11)
        assert match_feature_matrix(df, cols, is_diff, swap=True)[0, 0] == pytest.approx(0.07)

    def test_inference_name_frame_swaps_through_the_caller(self):
        # The FS scorer hands `_first_in_for` a frame already in inference
        # names, so the partner of server_x is returner_x. Distinct from the
        # rule-level test above: this one regresses if the caller stops
        # applying the rule, which is exactly what happened.
        est = self._model("player_svc_first_serve_in_pct(days=180)")
        df = pl.DataFrame({
            "server_svc_first_serve_in_pct_180d": [0.80],
            "returner_svc_first_serve_in_pct_180d": [0.40],
        })
        a, b = est._first_in_for(df)
        assert a[0] == pytest.approx(0.80)
        assert b[0] == pytest.approx(0.40)
