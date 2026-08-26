"""Tests for family-unit forward selection (FS-protocol redesign item 2)."""

import json

import pytest

from mvp.model.discovery.config import DiscoveryConfig
from mvp.model.discovery.selection import FeatureSelector

FAMILIES = {
    "fam_a": ["a1", "a2"],
    "fam_b": ["b1"],
    "fam_noise": ["n1", "n2"],
}
ALL_COLS = ["a1", "a2", "b1", "n1", "n2"]


def make_scorer(calls: list | None = None):
    """Column-keyed scorer, lower is better; stamps last_fold_metrics like
    the real fast-selection scorer closure does."""

    def scorer(cols: list[str]) -> float:
        if calls is not None:
            calls.append(list(cols))
        s = 1.0
        if "a1" in cols:
            s -= 0.15
        if "a2" in cols:
            s -= 0.15
        if "b1" in cols:
            s -= 0.1
        if "n1" in cols:
            s += 0.05
        if "n2" in cols:
            s += 0.05
        if "base_col" in cols:
            s -= 0.2
        scorer.last_fold_metrics = [s, s]
        return s

    scorer.last_fold_metrics = []
    return scorer


class TestFamilyForwardSelection:
    def _selector(self, scorer, **kwargs) -> FeatureSelector:
        return FeatureSelector(
            scorer=scorer,
            all_features=ALL_COLS,
            method="forward",
            direction="minimize",
            families=FAMILIES,
            **kwargs,
        )

    def test_accepts_families_whole_and_expands_columns(self):
        result = self._selector(make_scorer()).run()

        # fam_a (-0.3 joint) first, fam_b (-0.1) second, fam_noise rejected
        assert result.selected_families == ["fam_a", "fam_b"]
        assert result.selected_features == ["a1", "a2", "b1"]
        assert result.excluded_features == ["fam_noise"]
        assert result.history[0]["feature"] == "fam_a"
        assert result.history[1]["feature"] == "fam_b"

    def test_scorer_receives_expanded_member_columns(self):
        calls: list[list[str]] = []
        self._selector(make_scorer(calls)).run()

        # every candidate eval passes the family's members jointly
        assert ["a1", "a2"] in calls
        # after fam_a is accepted, candidates stack on its expanded columns
        assert ["a1", "a2", "b1"] in calls

    def test_base_columns_stay_pinned_and_out_of_families(self):
        calls: list[list[str]] = []
        selector = FeatureSelector(
            scorer=make_scorer(calls),
            all_features=ALL_COLS,
            method="forward",
            direction="minimize",
            families=FAMILIES,
            base_features=["base_col"],
        )
        result = selector.run()

        assert calls[0] == ["base_col"]  # base scored as-is
        assert ["base_col", "a1", "a2"] in calls
        assert result.selected_features[:1] == ["base_col"]
        # base column is not a family id
        assert "base_col" not in result.selected_families

    def test_history_records_family_ids_and_fold_metrics(self, tmp_path):
        selector = self._selector(make_scorer())
        selector.forward_selection(
            checkpoint_path=tmp_path / "discovery_checkpoint_unit.json",
        )

        hist = tmp_path / "fs_history_unit.jsonl"
        lines = [json.loads(ln) for ln in hist.read_text().splitlines() if ln.strip()]
        first = lines[0]
        assert first["feature"] == "fam_a"
        assert {r[0] for r in first["ranking"]} == set(FAMILIES)
        # per-candidate per-fold metrics captured off the scorer side channel,
        # consistent with the round ranking's mean scores
        ranking = dict(tuple(r) for r in first["ranking"])
        for fam, folds in first["fold_metrics"].items():
            assert folds == [ranking[fam], ranking[fam]]
        assert set(first["fold_metrics"]) == set(FAMILIES)

    def test_parallel_candidates_keep_fold_metrics(self, tmp_path, caplog):
        """With a scorer that returns fold metrics alongside the score, the
        family-mode candidate loop runs on threads and every candidate's
        fold metrics still land in the history record."""
        scorer = make_scorer()

        def score_with_folds(cols: list[str]) -> tuple[float, list[float]]:
            s = scorer(cols)
            return s, [s, s]

        scorer.score_with_folds = score_with_folds
        selector = self._selector(scorer, forward_max_workers=2)
        with caplog.at_level("INFO"):
            selector.forward_selection(
                checkpoint_path=tmp_path / "discovery_checkpoint_unit.json",
            )

        assert "forcing serial" not in caplog.text
        hist = tmp_path / "fs_history_unit.jsonl"
        lines = [json.loads(ln) for ln in hist.read_text().splitlines() if ln.strip()]
        first = lines[0]
        ranking = dict(tuple(r) for r in first["ranking"])
        assert set(first["fold_metrics"]) == set(FAMILIES)
        for fam, folds in first["fold_metrics"].items():
            assert folds == [ranking[fam], ranking[fam]]

    def test_side_channel_only_scorer_forces_serial(self, tmp_path, caplog):
        selector = self._selector(make_scorer(), forward_max_workers=2)
        with caplog.at_level("INFO"):
            selector.forward_selection(
                checkpoint_path=tmp_path / "discovery_checkpoint_unit.json",
            )
        assert "forcing serial candidate loop (was x2)" in caplog.text

    def test_feature_mode_unchanged(self, tmp_path):
        def plain_scorer(cols: list[str]) -> float:  # no side channel at all
            return 1.0 - 0.1 * ("a1" in cols)

        selector = FeatureSelector(
            scorer=plain_scorer,
            all_features=["a1", "b1"],
            method="forward",
            direction="minimize",
        )
        result = selector.forward_selection(
            checkpoint_path=tmp_path / "discovery_checkpoint_unit.json",
        )

        assert result.selected_families == []
        assert result.selected_features == ["a1"]
        hist = tmp_path / "fs_history_unit.jsonl"
        lines = [json.loads(ln) for ln in hist.read_text().splitlines() if ln.strip()]
        assert lines[0]["fold_metrics"] == {}


class TestWithinFamilyPick:
    """refine_fn hook: kept members replace the block for later rounds, the
    result, the history, and — via the checkpoint — a resume."""

    @staticmethod
    def _first_member(family, selected):
        return [FAMILIES[family][0]], {"resolved": "members"}

    def _run(self, refine_fn, tmp_path, fams):
        selector = FeatureSelector(
            scorer=make_scorer(), all_features=ALL_COLS, method="forward",
            direction="minimize", families=fams, refine_fn=refine_fn,
        )
        return selector.forward_selection(
            checkpoint_path=tmp_path / "discovery_checkpoint_unit.json",
        )

    def test_kept_members_replace_the_block(self, tmp_path):
        calls = []

        def refine(family, selected):
            calls.append((family, list(selected)))
            return self._first_member(family, selected)

        fams = {k: list(v) for k, v in FAMILIES.items()}
        result = self._run(refine, tmp_path, fams)

        # fam_a (a1+a2 = 0.70) accepted, reduced to a1 (0.85); fam_b then
        # adds b1 (0.75); fam_noise cannot improve on that -> stop.
        assert calls[0] == ("fam_a", ["fam_a"])
        assert calls[1] == ("fam_b", ["fam_a", "fam_b"])
        assert result.selected_families == ["fam_a", "fam_b"]
        assert result.selected_features == ["a1", "b1"]
        assert fams["fam_a"] == ["a1"]  # shared dict, not a copy
        lines = [
            json.loads(ln)
            for ln in (tmp_path / "fs_history_unit.jsonl").read_text().splitlines()
            if ln.strip()
        ]
        first = lines[0]
        assert first["kept"] == ["a1"]
        assert first["block_metric"] == pytest.approx(0.70)
        assert first["metric"] == pytest.approx(0.85)  # re-scored on a1 alone
        assert first["refinement"] == {"resolved": "members"}

    def test_resume_restores_kept_members(self, tmp_path):
        fams = {k: list(v) for k, v in FAMILIES.items()}
        self._run(self._first_member, tmp_path, fams)
        cp = json.loads((tmp_path / "discovery_checkpoint_unit.json").read_text())
        assert cp["refined_families"] == {"fam_a": ["a1"], "fam_b": ["b1"]}

        # a fresh selector (fresh full-member dict) resuming from that
        # checkpoint must expand the accepted families to the kept members
        fresh = {k: list(v) for k, v in FAMILIES.items()}
        result = self._run(self._first_member, tmp_path, fresh)
        assert fresh["fam_a"] == ["a1"]
        assert result.selected_features == ["a1", "b1"]


class TestAcceptanceGate:
    """Two-bar gate mechanics at the loop level (stub acceptance_fn)."""

    def _run(self, acceptance_fn, tmp_path=None):
        selector = FeatureSelector(
            scorer=make_scorer(),
            all_features=ALL_COLS,
            method="forward",
            direction="minimize",
            families=FAMILIES,
            min_delta=0.5,  # would block everything; the gate must replace it
            acceptance_fn=acceptance_fn,
        )
        if tmp_path is not None:
            return selector.forward_selection(
                checkpoint_path=tmp_path / "discovery_checkpoint_unit.json",
            )
        return selector.run()

    def test_winner_restricted_to_eligible_and_min_delta_replaced(self):
        # fam_a scores best but is never eligible; fam_b's 0.1 improvement is
        # far below min_delta=0.5 yet accepted — the gate replaces the delta.
        def gate(selected, best_metric, scores, fold_scores):
            return {"fam_b"}, {"eligible": ["fam_b"]}

        result = self._run(gate)
        assert result.selected_families == ["fam_b"]

    def test_stops_when_nothing_eligible(self):
        calls: list[list[str]] = []

        def gate(selected, best_metric, scores, fold_scores):
            calls.append(sorted(scores))
            return set(), {"eligible": []}

        result = self._run(gate)
        assert result.selected_families == []
        assert len(calls) == 1  # stopped in round 1
        assert calls[0] == sorted(FAMILIES)

    def test_gate_info_lands_in_history(self, tmp_path):
        def gate(selected, best_metric, scores, fold_scores):
            return {"fam_a"}, {"eligible": ["fam_a"], "control_floor": 0.01}

        self._run(gate, tmp_path=tmp_path)
        hist = tmp_path / "fs_history_unit.jsonl"
        lines = [
            json.loads(ln) for ln in hist.read_text().splitlines() if ln.strip()
        ]
        assert lines[0]["action"] == "add"
        assert lines[0]["acceptance"]["control_floor"] == 0.01
        assert lines[-1]["action"] == "stop"
        assert "two-bar" in lines[-1]["reason"]


class TestFamilyAcceptanceConfig:
    def _cfg(self) -> dict:
        return {
            "data": {"date_range": {"start": "2024-01-01", "end": "2024-12-31"}},
            "model": {"type": "xgboost"},
            "validation": {"type": "walk_forward", "n_splits": 2},
            "discovery": {"family_acceptance": {"k": 5}},
        }

    def test_requires_family_unit(self):
        with pytest.raises(ValueError, match="selection_unit"):
            DiscoveryConfig.model_validate(self._cfg())

    def test_accepted_with_family_unit(self):
        cfg = self._cfg()
        cfg["discovery"]["selection_unit"] = "family"
        parsed = DiscoveryConfig.model_validate(cfg)
        assert parsed.discovery.family_acceptance.k == 5
        assert parsed.discovery.family_acceptance.alpha == 0.10
        assert parsed.discovery.family_acceptance.max_members == 3
