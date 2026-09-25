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


# --- arm offset ----------------------------------------------------------------

_W1 = "player_chain_w1_logit(model=src_chain)"
_W2 = "player_chain_w2_logit(model=src_chain)"
_UNCOVERED = "m007"


class _ArmEngine:
    """MirroringFakeEngine for plain specs; the two arm specs resolve to their
    engine column names, with the source uncovered on one match."""

    def __init__(self) -> None:
        from tests.projection.iid.test_serve_discovery_swap_side import (
            MirroringFakeEngine,
        )

        self._plain = MirroringFakeEngine()
        self.requested: list[list[str]] = []

    def load_features_numpy(self, feature_specs, base_df, cache_key):
        import polars as pl

        from mvp.projection.iid.config import arm_offset_column

        self.requested.append(list(feature_specs))
        arm = [s for s in feature_specs if "(model=" in s]
        out = self._plain.load_features_numpy(
            [s for s in feature_specs if s not in arm], base_df, cache_key,
        )
        for spec in arm:
            col = arm_offset_column(spec)
            player_col = "player_" + col.split("_", 1)[1]
            value = (
                pl.when(pl.col("match_uid") == _UNCOVERED).then(None)
                .otherwise(pl.col("player_id").cast(pl.Float64) / 2000.0 - 0.5)
            )
            if player_col not in out.columns:
                out = out.with_columns(value.alias(player_col))
            if col.startswith("opp_") and col not in out.columns:
                lookup = out.select("match_uid", "player_id", player_col).rename(
                    {"player_id": "_lk", player_col: col}
                )
                out = out.join(
                    lookup, left_on=["match_uid", "opp_id"],
                    right_on=["match_uid", "_lk"], how="left",
                )
        return out


def _with_arm_offset(path: str, arm_offset: dict) -> str:
    import yaml

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    data["serve_model"]["arm_offset"] = arm_offset
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f)
    return path


@pytest.fixture
def arm_frame(monkeypatch):
    """Every (match, server) of the fixture OOF for the source."""
    from datetime import date

    import polars as pl

    from mvp.model.features import prior

    rows = [
        (f"m{i:03d}", server)
        for i in range(40) for server in (1000 + i, 2000 + i)
    ]
    frame = pl.DataFrame(
        {"match_uid": [r[0] for r in rows], "server_id": [r[1] for r in rows]}
    ).with_columns(pl.lit(date(2023, 12, 31)).alias("arm_train_end"))
    monkeypatch.setattr(prior, "arm_frame", lambda m: (None, frame))
    return frame


@pytest.fixture
def arm_selector(tmp_path, arm_frame):
    """A win_second component run whose held win_first arm and searched
    win_second arm both start from a source chain's arm output."""
    path = _with_arm_offset(
        _two_level_config(tmp_path, SHAPES["win_second"]),
        {"win_first": _W1, "win_second": _W2},
    )
    return _make_selector(tmp_path, path, engine=_ArmEngine())


class TestArmOffsetPlumbing:
    def test_fixed_specs_include_the_offsets_and_the_load_carries_partners(
        self, arm_selector,
    ):
        fixed = arm_selector._fixed_arm_match_specs()
        assert _W1 in fixed and _W2 in fixed
        cols = arm_selector._match_features_both_sides.columns
        assert "opp_chain_w1_logit_src_chain" in cols
        assert "opp_chain_w2_logit_src_chain" in cols

    def test_an_uncovered_match_is_absent_from_every_fold(self, arm_selector):
        assert _UNCOVERED not in arm_selector._match_df["match_uid"].to_list()
        for fold in arm_selector._chain_folds:
            assert _UNCOVERED not in fold.train_df["match_uid"].to_list()
            assert _UNCOVERED not in fold.test_df["match_uid"].to_list()

    def test_the_candidate_arm_carries_its_offset_after_substitution(
        self, arm_selector,
    ):
        params = arm_selector._scoring_params()
        model = arm_selector._build_candidate_model([DIFF_SPEC], [], params)
        searched = model.components()[WIN_SECOND]
        assert searched.offset_spec == _W2
        assert searched.match_level_features == [DIFF_SPEC, _W2]
        empty = arm_selector._build_candidate_model([], [], params)
        assert empty.components()[WIN_SECOND].match_level_features == [_W2]

    def test_the_prefit_held_arm_is_fitted_with_its_offset(self, arm_selector):
        held = arm_selector._prefit_fixed[WIN_SECOND][0][WIN_FIRST]
        assert held.offset_spec == _W1
        assert held._offset_model is not None

    def test_each_fold_carries_a_prefit_offset_per_win_arm(self, arm_selector):
        for fold in arm_selector._chain_folds:
            assert set(fold.arm_offsets) == {WIN_FIRST, WIN_SECOND}

    def test_the_searched_arm_is_handed_its_folds_prefit_offset(self, arm_selector):
        params = arm_selector._scoring_params()
        model = arm_selector._build_candidate_model([DIFF_SPEC], [], params)
        arm_selector._attach_prefit(model, 1)
        assert (
            model.components()[WIN_SECOND]._prefit_offset
            is arm_selector._chain_folds[1].arm_offsets[WIN_SECOND]
        )

    def test_a_candidate_scores_finitely_with_offsets(self, arm_selector):
        score = arm_selector._score_cv_chain_detailed([DIFF_SPEC], [])[0]
        assert math.isfinite(score)


class TestArmSourceRegeneration:
    def _selector(self, tmp_path, monkeypatch, ready: bool):
        from mvp.model import prior_promotion
        from mvp.model.features import prior

        path = _with_arm_offset(
            _two_level_config(tmp_path, SHAPES["win_second"]), {"win_first": _W1},
        )
        sel = _make_selector(tmp_path, path, prepare=False)
        # The arm source, and a base it consumes only as a plain prior: a
        # single-level projection, which never writes arm files.
        source = prior.PriorSource(
            model="src_chain", config_path=tmp_path / "src_chain.yaml",
            fp="f" * 12, eval_dir=tmp_path / "pe", kind="projection",
            via_arms=True,
        )
        base = prior.PriorSource(
            model="plain_base", config_path=tmp_path / "plain_base.yaml",
            fp="e" * 12, eval_dir=tmp_path / "pe_base", kind="projection",
        )
        by_model = {"src_chain": source, "plain_base": base}
        calls = {"regenerated": [], "cleared": 0, "order": None, "via_arms": None}

        def order(stems, via_arms=()):
            calls["order"] = list(stems)
            calls["via_arms"] = set(via_arms)
            return [base, source]

        monkeypatch.setattr(prior_promotion, "dependency_order", order)
        monkeypatch.setattr(
            prior_promotion, "regenerate_prior",
            lambda s: calls["regenerated"].append(s.model),
        )
        monkeypatch.setattr(
            prior_promotion, "_clear_prior_caches",
            lambda: calls.__setitem__("cleared", calls["cleared"] + 1),
        )
        monkeypatch.setattr(
            prior, "resolve_prior", lambda m, *a, **kw: by_model[m],
        )
        monkeypatch.setattr(prior, "prior_artifacts_ready", lambda s: True)
        monkeypatch.setattr(
            prior, "serve_arms_ready", lambda s: ready if s is source else False,
        )
        return sel, calls

    def test_a_source_without_its_arm_store_is_regenerated(
        self, tmp_path, monkeypatch,
    ):
        sel, calls = self._selector(tmp_path, monkeypatch, ready=False)
        sel._ensure_arm_sources()
        assert calls["order"] == ["src_chain"]
        assert calls["via_arms"] == {"src_chain"}
        assert calls["regenerated"] == ["src_chain"]
        assert calls["cleared"] == 1

    def test_a_plain_prior_base_is_not_asked_for_arm_files(
        self, tmp_path, monkeypatch,
    ):
        """Asked, a single-level base would be regenerated on every run and
        still never have them."""
        sel, calls = self._selector(tmp_path, monkeypatch, ready=True)
        sel._ensure_arm_sources()
        assert "plain_base" not in calls["regenerated"]

    def test_a_ready_source_is_left_alone(self, tmp_path, monkeypatch):
        sel, calls = self._selector(tmp_path, monkeypatch, ready=True)
        sel._ensure_arm_sources()
        assert calls["regenerated"] == []

    def test_a_run_without_arm_sources_resolves_nothing(self, selector, monkeypatch):
        from mvp.model import prior_promotion

        def boom(stems):
            raise AssertionError("resolved a source for a run naming none")

        monkeypatch.setattr(prior_promotion, "dependency_order", boom)
        selector._ensure_arm_sources()


class TestPointGrainFilterSkipsTheOffsetKey:
    def test_the_base_matrix_builds_with_the_offset_filter_present(
        self, tmp_path, arm_frame,
    ):
        path = _with_arm_offset(
            _two_level_config(tmp_path, SHAPES["win_second"]), {"win_first": _W1},
        )
        sel = _make_selector(tmp_path, path, prepare=False)
        assert "player_chain_w1_logit_src_chain" in sel.config.data.filters
        base_df, _ = sel._build_base_matrix(
            _ArmEngine(), "k", base_match=[], base_point=[], candidate_point=[],
        )
        assert base_df.height > 0
