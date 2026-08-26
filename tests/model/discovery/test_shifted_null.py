"""Tests for the shifted-candidate null (FS-protocol redesign item 3)."""

import importlib
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import yaml

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.fast_selection import FastForwardSelector
from mvp.model.discovery.shifted_null import (
    FamilyNullVerdict,
    _FoldScorer,
    _append_verdict,
    _side_gather_map,
    composite_gain,
    load_verdicts,
    make_family_acceptance,
    make_family_refiner,
    max_null,
    max_null_p,
    negative_control_floor,
    null_verdicts_path,
    rebuild_parent_specs,
    refine_family,
    resolve_rebuild,
    run_family_nulls,
)

FAMILY_SPECS = [
    "player_win_pct(days=90)",
    "opp_win_pct(days=90)",
    "player_win_pct_diff(days=90)",
]
BASE_SPEC = "player_ranking_points_diff"


@pytest.fixture(autouse=True)
def ensure_features_registered(isolated_registry):
    import mvp.model.features.ranking
    import mvp.model.features.serve
    import mvp.model.features.win_rate

    importlib.reload(mvp.model.features.ranking)
    importlib.reload(mvp.model.features.serve)
    importlib.reload(mvp.model.features.win_rate)


@pytest.fixture
def sample_matches(tmp_path: Path) -> Path:
    n = 300
    rng = np.random.RandomState(42)
    df = pl.DataFrame(
        {
            "match_uid": [f"M{i}" for i in range(n)],
            "player_id": [f"P{i % 10}" for i in range(n)],
            "opp_id": [f"P{(i + 5) % 10}" for i in range(n)],
            "effective_match_date": [
                f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(n)
            ],
            "won": [bool(x) for x in rng.randint(0, 2, n)],
            "player_rankings_points": rng.randint(100, 2000, n).tolist(),
            "opp_rankings_points": rng.randint(100, 2000, n).tolist(),
            "circuit": ["tour" for _ in range(n)],
        }
    ).with_columns(pl.col("effective_match_date").str.to_datetime())
    path = tmp_path / "matches.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def offset_config(tmp_path: Path) -> Path:
    config_dict = {
        "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
        "model": {"type": "xgboost"},
        "validation": {
            "type": "walk_forward",
            "n_splits": 2,
            "min_train_size": 50,
            "test_size": 25,
        },
        "discovery": {
            "metric": "log_loss",
            "direction": "minimize",
            "features": {"base": [BASE_SPEC]},
        },
        "offset": {"feature": BASE_SPEC},
    }
    path = tmp_path / "offset.yaml"
    with open(path, "w") as f:
        yaml.dump(config_dict, f)
    return path


def _fast(config_path: Path, matches: Path, cache: Path) -> FastForwardSelector:
    fast = FastForwardSelector(
        config=DiscoveryConfig.from_file(config_path),
        all_feature_specs=[BASE_SPEC, *FAMILY_SPECS],
        matches_path=matches,
        cache_dir=cache,
    )
    fast.precompute()
    return fast


class TestResolveRebuild:
    def test_side_and_diff_members(self):
        cols = {
            "player_win_pct_90d": 0,
            "opp_win_pct_90d": 1,
            "player_win_pct_diff_90d": 2,
        }
        plan = resolve_rebuild("win_pct", FAMILY_SPECS, cols)

        assert plan.missing == []
        assert plan.shift_cols == ["opp_win_pct_90d", "player_win_pct_90d"]
        diff = next(m for m in plan.members if m.kind == "combine")
        assert diff.col == "player_win_pct_diff_90d"
        assert (diff.left, diff.right, diff.sign) == (
            "player_win_pct_90d", "opp_win_pct_90d", -1,
        )

    def test_matchup_shifts_own_stat_keeps_cross_stat_real(self):
        cols = {
            "player_svc_first_serve_win_pct_matchup_30d": 0,
            "player_svc_first_serve_win_pct_30d": 1,
            "opp_ret_first_serve_win_pct_30d": 2,
        }
        plan = resolve_rebuild(
            "svc_first_serve_win_pct",
            ["player_svc_first_serve_win_pct_matchup(days=30)"],
            cols,
        )

        assert plan.missing == []
        (member,) = plan.members
        assert (member.left, member.right, member.sign) == (
            "player_svc_first_serve_win_pct_30d",
            "opp_ret_first_serve_win_pct_30d",
            -1,
        )
        # only the family's own stat is shifted; the cross-stat dep stays real
        assert plan.shift_cols == ["player_svc_first_serve_win_pct_30d"]

    def test_parent_specs_for_combiner_members(self):
        specs = rebuild_parent_specs([
            "player_win_pct_diff(days=90)", "win_pct_sum", "player_win_pct(days=90)",
        ])
        assert specs == {
            "player_win_pct(days=90)", "opp_win_pct(days=90)",
            "player_win_pct", "opp_win_pct",
        }

    def test_matchup_without_declared_parents_is_shifted_as_itself(self):
        # a matchup column whose registry entry has no depends_on: not a
        # reason to leave the family untestable — it is shifted as its own
        # per-side column and flagged as approximated on the plan
        col_to_idx = {"player_ranking_points_diff": 0, "player_nodeps_matchup": 1}
        plan = resolve_rebuild(
            "nodeps", ["player_nodeps_matchup"], col_to_idx,
        )
        assert plan.missing == []
        assert plan.shift_cols == ["player_nodeps_matchup"]
        assert plan.approximated == ["player_nodeps_matchup"]
        assert plan.members[0].kind == "side"

    def test_missing_parent_is_reported_not_guessed(self):
        plan = resolve_rebuild(
            "win_pct", ["player_win_pct_diff(days=90)"],
            {"player_win_pct_diff_90d": 0},
        )
        assert set(plan.missing) == {"player_win_pct_90d", "opp_win_pct_90d"}


class TestSideGatherMap:
    def test_circular_shift_within_train_rows_only(self):
        ids = np.array(["A", "A", "A", "B", "B", "C"])
        dates = np.array([1, 2, 3, 1, 2, 1])
        uids = np.array([f"m{i}" for i in range(6)])
        train_idx = np.arange(5)  # row 5 (C) outside the train window

        src = _side_gather_map(ids, dates, uids, train_idx, {"A": 1, "B": 1})

        # A's rows (0,1,2 by date): each takes the previous match's value
        assert list(src[:3]) == [2, 0, 1]
        # B's rows (3,4): swapped
        assert list(src[3:5]) == [4, 3]
        # outside the train window: identity
        assert src[5] == 5

    def test_single_match_sequences_stay_real(self):
        ids = np.array(["A", "B"])
        src = _side_gather_map(
            ids, np.array([1, 1]), np.array(["m0", "m1"]),
            np.arange(2), {"A": 1, "B": 1},
        )
        assert list(src) == [0, 1]


class TestAcceptanceBars:
    def test_composite_gain_gates_on_fold_agreement(self):
        gains = [0.1, -0.2, 0.3]
        assert composite_gain(gains, 2) == pytest.approx(np.mean(gains))
        assert composite_gain(gains, 3) == float("-inf")
        assert composite_gain([], 1) == float("-inf")

    def test_max_null_is_best_fake_per_replicate(self):
        neg = float("-inf")
        verdicts = [
            FamilyNullVerdict("a", 0.5, 0.01, null_composites=[0.001, neg, 0.003]),
            FamilyNullVerdict("b", 0.5, 0.02, null_composites=[0.004, 0.002, neg]),
            FamilyNullVerdict("u", None, reason="untestable"),
        ]
        assert max_null(verdicts) == [0.004, 0.002, 0.003]
        assert max_null([]) == []

    def test_max_null_p_puts_multiplicity_in_the_statistic(self):
        # A family at its own-null floor (beats all 20 of its own fakes) is
        # still rejected when other families' fakes beat it: the per-family
        # p-value cannot get under q/n for any feasible K, the max-null can.
        maxes = [0.002] * 20
        assert max_null_p(0.001, maxes) == pytest.approx(21 / 21)
        assert max_null_p(0.003, maxes) == pytest.approx(1 / 21)
        assert max_null_p(0.003, [0.005] + [0.002] * 19) == pytest.approx(2 / 21)
        assert max_null_p(float("-inf"), maxes) == pytest.approx(1.0)
        assert max_null_p(0.5, []) == pytest.approx(1.0)

    def test_max_null_all_neg_inf_loses_magnitude_discrimination(self):
        # Every fake failed the fold-agreement gate: the bar is "cleared the
        # gate at all", the size of the gain no longer matters. Documented
        # degenerate case; the gate counts these replicates (see the
        # gate test below).
        neg = float("-inf")
        maxes = [neg] * 20
        assert max_null_p(1e-9, maxes) == pytest.approx(1 / 21)
        assert max_null_p(0.5, maxes) == pytest.approx(1 / 21)
        assert max_null_p(neg, maxes) == pytest.approx(1.0)

    def test_null_verdicts_path_derives_from_checkpoint(self, tmp_path):
        ck = tmp_path / "discovery_checkpoint_residual_families.json"
        assert null_verdicts_path(ck, 3) == (
            tmp_path / "null_verdicts_residual_families_r3.jsonl"
        )
        assert null_verdicts_path(None, 3) is None

    def test_verdict_log_round_trips_and_filters_on_key(self, tmp_path):
        neg = float("-inf")
        path = tmp_path / "null_verdicts_x_r2.jsonl"
        key = {"accepted": ["base"], "k": 3, "seed": 1, "min_agree": 2}
        other = {**key, "seed": 2}
        v1 = FamilyNullVerdict(
            "a", 0.25, 0.01, observed_fold_gains=[0.02, neg],
            null_composites=[0.001, neg, 0.002],
        )
        v2 = FamilyNullVerdict("u", None, reason="unresolvable: x")
        v3 = FamilyNullVerdict("b", 0.5, 0.0, null_composites=[0.0] * 3)
        with open(path, "w", encoding="utf-8") as fh:
            _append_verdict(fh, v1, key)
            _append_verdict(fh, v2, key)
            _append_verdict(fh, v3, other)  # another draw: must be ignored

        got = load_verdicts(path, key)
        assert set(got) == {"a", "u"}
        assert got["a"] == v1  # -inf survives the JSON round trip
        assert got["u"] == v2
        assert load_verdicts(path, other) == {"b": v3}
        assert load_verdicts(tmp_path / "missing.jsonl", key) == {}
        assert load_verdicts(None, key) == {}

    def test_negative_control_floor_needs_a_pool(self):
        few = {f"f{i}": 0.001 * i for i in range(20)}
        assert negative_control_floor(few, min_pool=40) is None
        many = {f"f{i}": 0.001 * i for i in range(100)}
        floor = negative_control_floor(many, min_pool=40)
        # bottom half is f0..f49; 95th percentile sits near its top
        assert floor is not None
        assert 0.045 <= floor <= 0.05


class TestFoldScorerConsistency:
    def test_matches_fast_selection_scorer_metrics(
        self, offset_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """The null path's fold fit must reproduce the FS scorer's fold
        metrics exactly — gains and null gains are only comparable to the
        round's observed scores if both go through the same arithmetic."""
        fast = _fast(offset_config, sample_matches, tmp_path / "cache")
        specs = [BASE_SPEC, *FAMILY_SPECS]

        scorer = fast.create_scorer("log_loss")
        scorer(specs)
        expected = scorer.last_fold_metrics

        from mvp.model.engine import get_feature_columns

        fold_scorer = _FoldScorer(fast, "log_loss")
        col_indices = np.array(
            [fast.col_to_idx[c] for c in get_feature_columns(specs)]
        )
        got = [
            fold_scorer.score_fold(f, col_indices)
            for f in range(len(fast.folds))
        ]

        assert len(got) == len(expected)
        np.testing.assert_allclose(got, expected, rtol=1e-6)


class TestRunFamilyNulls:
    def test_verdict_mechanics(
        self, offset_config: Path, sample_matches: Path, tmp_path: Path
    ):
        fast = _fast(offset_config, sample_matches, tmp_path / "cache")
        k = 3

        (verdict,) = run_family_nulls(
            fast,
            "log_loss",
            accepted_specs=[BASE_SPEC],
            families={"win_pct": FAMILY_SPECS},
            k=k,
            seed=7,
        )

        assert verdict.family == "win_pct"
        assert verdict.reason is None
        assert len(verdict.observed_fold_gains) == len(fast.folds)
        assert len(verdict.null_composites) == k
        assert verdict.p_value is not None
        assert 1 / (k + 1) <= verdict.p_value <= 1.0

    def test_untestable_family_flagged(
        self, offset_config: Path, sample_matches: Path, tmp_path: Path
    ):
        fast = _fast(offset_config, sample_matches, tmp_path / "cache")
        verdicts = run_family_nulls(
            fast,
            "log_loss",
            accepted_specs=[BASE_SPEC],
            families={"ctx_tier": ["is_grand_slam"]},
            k=2,
            seed=7,
        )
        (verdict,) = verdicts
        assert verdict.p_value is None
        assert "unresolvable" in verdict.reason

    def test_parallel_matches_serial(
        self, offset_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Threaded family processing reproduces the serial verdicts
        exactly: offsets are drawn up front per (fold, replicate), and the
        fold scorer carries no per-call shared state."""
        fast = _fast(offset_config, sample_matches, tmp_path / "cache")
        families = {
            "win_pct_side": [FAMILY_SPECS[0], FAMILY_SPECS[1]],
            "win_pct_diff": [FAMILY_SPECS[2]],
        }
        kwargs = dict(
            accepted_specs=[BASE_SPEC], families=families, k=3, seed=11,
            n_jobs=1,
        )
        serial = run_family_nulls(fast, "log_loss", workers=1, **kwargs)
        threaded = run_family_nulls(fast, "log_loss", workers=2, **kwargs)

        assert [v.family for v in threaded] == [v.family for v in serial]
        for s, t in zip(serial, threaded):
            assert t.p_value == s.p_value
            assert t.observed_fold_gains == s.observed_fold_gains
            assert t.null_composites == s.null_composites

    def test_verdict_checkpoint_resumes_identically(
        self, offset_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """A run cut after the first family, resumed from its verdict log,
        returns exactly what an uninterrupted run returns; a log written
        under another key is ignored rather than reused."""
        fast = _fast(offset_config, sample_matches, tmp_path / "cache")
        families = {
            "win_pct_diff": [FAMILY_SPECS[2]],
            "win_pct_side": [FAMILY_SPECS[0], FAMILY_SPECS[1]],
        }
        kwargs = dict(
            accepted_specs=[BASE_SPEC], families=families, k=3, seed=5,
            n_jobs=1,
        )
        clean = run_family_nulls(fast, "log_loss", **kwargs)

        log = tmp_path / "null_verdicts_unit_r2.jsonl"
        run_family_nulls(fast, "log_loss", checkpoint_path=log, **kwargs)
        lines = log.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        # "interrupted after the first family"
        log.write_text(lines[0] + "\n", encoding="utf-8")

        resumed = run_family_nulls(fast, "log_loss", checkpoint_path=log, **kwargs)
        assert resumed == clean
        assert len(log.read_text(encoding="utf-8").splitlines()) == 2

        # a different seed is a different draw: nothing restored, all recomputed
        other = run_family_nulls(
            fast, "log_loss", checkpoint_path=log, **{**kwargs, "seed": 6}
        )
        assert [v.family for v in other] == [v.family for v in clean]
        assert len(log.read_text(encoding="utf-8").splitlines()) == 4

        # a narrower tested set (top_m) restores only its own families: the
        # other family's stored verdict must not enter this call's max-null
        narrow = run_family_nulls(
            fast, "log_loss", checkpoint_path=log,
            **{**kwargs, "families": {"win_pct_diff": families["win_pct_diff"]}},
        )
        assert [v.family for v in narrow] == ["win_pct_diff"]
        assert narrow[0] == clean[0]

    def test_refine_family_end_to_end(
        self, offset_config: Path, sample_matches: Path, tmp_path: Path
    ):
        """Members tested one at a time as one-column candidates; the diff
        member's parents are shifted because the plan is keyed by the real
        family id, so nothing comes back untestable."""
        fast = _fast(offset_config, sample_matches, tmp_path / "cache")

        class Cfg(_GateCfg):
            k = 3
            alpha = 1.0  # accept anything testable: exercises the loop, not the bar

        kept, info = refine_family(
            fast, "log_loss", [BASE_SPEC], "win_pct", list(FAMILY_SPECS), Cfg(),
            n_jobs=1,
        )
        assert info["resolved"] == "members"
        assert 1 <= len(kept) <= 3 and set(kept) <= set(FAMILY_SPECS)
        assert info["passes"][0]["untestable"] == []
        assert len(info["passes"][0]["max_null"]) == 3


class _GateCfg:
    k = 20
    alpha = 0.10
    min_agree = None
    min_control_pool = 4
    seed = 3
    top_m = None
    max_members = 3


class TestRefineFamily:
    """Within-family pick with an injected null runner — engine-free."""

    MEMBERS = ["m1", "m2", "m3"]

    def _nulls(self, obs_by_pass, floor_by_pass):
        calls = []

        def fake(fast, metric, accepted, fams, **kwargs):
            i = len(calls)
            calls.append({"accepted": list(accepted), "cands": sorted(fams), **kwargs})
            obs = obs_by_pass[i]
            # m3's fakes are the round's best fake in every replicate
            return [
                FamilyNullVerdict(
                    family=m, p_value=1 / 21, observed_composite=obs[m],
                    null_composites=[floor_by_pass[i] if m == "m3" else 0.0] * 20,
                )
                for m in sorted(fams)
            ]

        return fake, calls

    def test_picks_until_nothing_clears(self):
        fake, calls = self._nulls(
            obs_by_pass=[{"m1": 0.01, "m2": 0.005, "m3": 0.0},
                         {"m2": 0.002, "m3": 0.0}],
            floor_by_pass=[0.006, 0.003],
        )
        kept, info = refine_family(
            None, "log_loss", ["base"], "fam", self.MEMBERS, _GateCfg(),
            seed=5, workers=2, n_jobs=4, _null_fn=fake,
        )
        assert kept == ["m1"]
        assert info["resolved"] == "members"
        assert [p["picked"] for p in info["passes"]] == ["m1", None]
        # pass 2 conditions on the pick, tests the rest, fresh seed
        assert calls[0]["accepted"] == ["base"] and calls[0]["cands"] == self.MEMBERS
        assert calls[1]["accepted"] == ["base", "m1"] and calls[1]["cands"] == ["m2", "m3"]
        assert calls[0]["seed"] == 5 and calls[1]["seed"] == 6
        # members are rebuilt as their family's columns, not as their own
        assert calls[0]["rebuild_ids"] == {m: "fam" for m in self.MEMBERS}
        assert calls[0]["workers"] == 2 and calls[0]["n_jobs"] == 4

    def test_block_kept_when_no_member_clears_alone(self):
        fake, _ = self._nulls(
            obs_by_pass=[{"m1": 0.001, "m2": 0.001, "m3": 0.0}],
            floor_by_pass=[0.002],
        )
        kept, info = refine_family(
            None, "log_loss", ["base"], "fam", self.MEMBERS, _GateCfg(), _null_fn=fake,
        )
        assert kept == self.MEMBERS
        assert info["resolved"] == "block"
        assert len(info["passes"]) == 1

    def test_cap_stops_the_pick(self):
        class Cfg(_GateCfg):
            max_members = 1

        fake, calls = self._nulls(
            obs_by_pass=[{"m1": 0.01, "m2": 0.009, "m3": 0.0}],
            floor_by_pass=[0.001],
        )
        kept, _ = refine_family(
            None, "log_loss", ["base"], "fam", self.MEMBERS, Cfg(), _null_fn=fake,
        )
        assert kept == ["m1"] and len(calls) == 1

    def test_refiner_excludes_the_family_and_expands_kept_members(self):
        families = {"fam": ["m1", "m2"], "other": ["o1", "o2"]}
        seen = {}

        def fake_refine(fast, metric, accepted, family, members, cfg, **kwargs):
            seen.update(accepted=accepted, family=family, members=members, **kwargs)
            return ["m2"], {"resolved": "members"}

        refine = make_family_refiner(
            None, "log_loss", families, _GateCfg(), workers=3, n_jobs=4,
            _refine_fn=fake_refine,
        )
        families["other"] = ["o2"]  # an earlier round's pick, shared dict
        kept, _ = refine("fam", ["base_col", "other", "fam"])
        assert kept == ["m2"]
        assert seen["accepted"] == ["base_col", "o2"]
        assert seen["family"] == "fam" and seen["members"] == ["m1", "m2"]
        assert seen["seed"] == _GateCfg.seed + 1000 + 3
        assert seen["workers"] == 3 and seen["n_jobs"] == 4


class TestMakeFamilyAcceptance:
    """Gate logic with an injected null runner — engine-free."""

    # 10 families; f1 clears the max-null but has a sub-floor gain.
    GAINS = {
        "f0": 0.10, "f1": 0.001, "f2": 0.06, "f3": 0.05, "f4": 0.04,
        "f5": 0.03, "f6": 0.003, "f7": 0.002, "f8": 0.0, "f9": -0.001,
    }
    # Observed composites: f0 and f1 pass the fold-agreement gate, the rest
    # collapse to -inf. Nulls: 20 per family; f5's replicate 0 is the round's
    # best fake at 0.0015, which beats f1 once (p = 2/21) and f0 never.
    COMPOSITES = {"f0": 0.10, "f1": 0.001}

    # Replicates in which EVERY family's fake fails the fold-agreement gate
    # (max-null -inf); empty by default, set by the degenerate-case test.
    DEAD_REPLICATES: tuple[int, ...] = ()

    def _nulls(self, fam: str) -> list[float]:
        base = 0.0005 if fam == "f0" else 0.0002
        nulls = [base] * _GateCfg.k
        if fam == "f5":
            nulls[0] = 0.0015
        for j in self.DEAD_REPLICATES:
            nulls[j] = float("-inf")
        return nulls

    def _call(self, cfg=None):
        best_metric = 0.7
        scores = {f: best_metric - g for f, g in self.GAINS.items()}
        families = {f: [f"{f}_col"] for f in self.GAINS}
        calls: dict = {}

        def fake_nulls(fast, metric, accepted, fams, **kwargs):
            calls["accepted"] = accepted
            calls["tested"] = sorted(fams)
            calls["kwargs"] = kwargs
            return [
                FamilyNullVerdict(
                    family=f, p_value=1 / 21,
                    observed_composite=self.COMPOSITES.get(f, float("-inf")),
                    null_composites=self._nulls(f),
                )
                for f in sorted(fams)
            ]

        gate = make_family_acceptance(
            fast=None, metric="log_loss", families=families,
            cfg=cfg or _GateCfg(), _null_fn=fake_nulls, workers=3, n_jobs=4,
        )
        eligible, info = gate(["base_col", "f0"], best_metric, scores, {})
        return eligible, info, calls

    def test_two_bars_intersect(self):
        eligible, info, _ = self._call()
        # max-null: replicate 0 is f5's 0.0015, the other 19 are 0.0005.
        assert info["max_null"][0] == pytest.approx(0.0015)
        assert info["max_null"][1] == pytest.approx(0.0005)
        assert info["p_max"]["f0"] == pytest.approx(1 / 21)
        assert info["p_max"]["f1"] == pytest.approx(2 / 21)
        assert info["p_max"]["f2"] == pytest.approx(1.0)  # -inf composite
        # own-null p is recorded but does not decide: every family is at its
        # own floor here, only two clear the best fake.
        assert set(info["p_own"]) == set(self.GAINS)
        assert info["bar_a"] == ["f0", "f1"]
        # the negative-control floor (95th pct of the bottom-half gains,
        # ~0.0028) then removes f1, whose gain is 0.001.
        assert 0.002 < info["control_floor"] < 0.004
        assert eligible == {"f0"}
        assert info["max_null_neg_inf"] == 0

    def test_dead_replicates_are_counted(self, caplog):
        # Replicates 0 and 3: every fake fails the gate, max-null is -inf.
        # f5's best fake sat in replicate 0, so f1 now beats every max
        # (p = 1/21) on gate survival alone — the loss of magnitude
        # discrimination the counter and warning exist for.
        self.DEAD_REPLICATES = (0, 3)
        try:
            with caplog.at_level("WARNING"):
                _, info, _ = self._call()
        finally:
            self.DEAD_REPLICATES = ()
        assert info["max_null_neg_inf"] == 2
        assert info["max_null"][0] == float("-inf")
        assert info["p_max"]["f1"] == pytest.approx(1 / 21)
        assert "2/20 replicates" in caplog.text

    def test_floor_fallback_below_control_pool(self):
        class Cfg(_GateCfg):
            min_control_pool = 40

        eligible, info, _ = self._call(cfg=Cfg())
        assert info["control_floor"] is None
        assert eligible == {"f0", "f1"}  # bar (a) alone

    def test_top_m_caps_and_specs_expand(self):
        class Cfg(_GateCfg):
            top_m = 3

        _, info, calls = self._call(cfg=Cfg())
        # top 3 by observed gain; the rest are ineligible this round
        assert calls["tested"] == ["f0", "f2", "f3"]
        assert info["tested"] == 3
        # selected ids expand: family ids to member specs, seeds pass through
        assert calls["accepted"] == ["base_col", "f0_col"]
        # fresh nulls each round: seed varies with the selected count
        assert calls["kwargs"]["seed"] == _GateCfg.seed + 2
        # the null runner's thread budget passes through the gate
        assert calls["kwargs"]["workers"] == 3
        assert calls["kwargs"]["n_jobs"] == 4
        # no FS checkpoint -> no verdict log
        assert calls["kwargs"]["checkpoint_path"] is None

    def test_verdict_log_path_follows_the_round(self, tmp_path):
        calls: dict = {}

        def fake_nulls(fast, metric, accepted, fams, **kwargs):
            calls["kwargs"] = kwargs
            return []

        gate = make_family_acceptance(
            fast=None, metric="log_loss", families={"f0": ["f0_col"]},
            cfg=_GateCfg(), _null_fn=fake_nulls,
            checkpoint_path=tmp_path / "discovery_checkpoint_unit.json",
        )
        gate(["base_col", "f0"], 0.7, {"f0": 0.6}, {})
        # two selected (seed + one family) -> this is round 3
        assert calls["kwargs"]["checkpoint_path"] == (
            tmp_path / "null_verdicts_unit_r3.jsonl"
        )
