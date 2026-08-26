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
    _side_gather_map,
    bh_accept,
    composite_gain,
    make_family_acceptance,
    negative_control_floor,
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

    def test_bh_accept(self):
        assert bh_accept({}) == set()
        # n=3, q=0.10: thresholds 0.033/0.067/0.10
        accepted = bh_accept({"a": 0.001, "b": 0.02, "c": 0.9}, q=0.10)
        assert accepted == {"a", "b"}

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


class _GateCfg:
    k = 5
    q = 0.10
    min_agree = None
    min_control_pool = 4
    seed = 3
    top_m = None


class TestMakeFamilyAcceptance:
    """Gate logic with an injected null runner — engine-free."""

    # 10 families; f1 has a strong p-value but a sub-floor gain.
    GAINS = {
        "f0": 0.10, "f1": 0.001, "f2": 0.06, "f3": 0.05, "f4": 0.04,
        "f5": 0.03, "f6": 0.003, "f7": 0.002, "f8": 0.0, "f9": -0.001,
    }
    P_VALUES = {"f0": 0.01, "f1": 0.02}  # everything else 0.9

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
                FamilyNullVerdict(family=f, p_value=self.P_VALUES.get(f, 0.9))
                for f in sorted(fams)
            ]

        gate = make_family_acceptance(
            fast=None, metric="log_loss", families=families,
            cfg=cfg or _GateCfg(), _null_fn=fake_nulls,
        )
        eligible, info = gate(["base_col", "f0"], best_metric, scores, {})
        return eligible, info, calls

    def test_two_bars_intersect(self):
        eligible, info, _ = self._call()
        # BH at q=0.10 over 10 p-values accepts f0 (0.01) and f1 (0.02); the
        # negative-control floor (95th pct of the bottom-half gains, ~0.0028)
        # then removes f1, whose gain is 0.001.
        assert info["bar_a"] == ["f0", "f1"]
        assert 0.002 < info["control_floor"] < 0.004
        assert eligible == {"f0"}

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
