"""Serve FS #115: one forward selection across several arms at once.

A joint run scores every `(arm, candidate)` pair of every ACTIVE arm on the
same chain metric each round and gives the winner to the arm it was scored in,
so the metric decides which arm a feature belongs to instead of the order the
operator happened to run the component runs in. Arms not listed are held at
their `serve_model` lists, exactly as a component run holds its other two.

Fixture is the prefit file's `SHAPES["joint"]`: synthetic two-sided matches, a
points frame with both servers, the MirroringFakeEngine, and `win_first` /
`win_second` searched from empty with `first_in` held at MIRROR_SPEC. No B:/
reads. The chain scorer is replaced throughout -- what is under test is the
loop, not the number it is handed -- and every replacement records the
`(arm, match_list, point_list)` it was asked for, which is how "this arm was
not scored this round" is asserted.
"""

import json
from datetime import datetime

import pytest
import yaml

from mvp.model.discovery.checkpoint import SelectionCheckpoint, save_checkpoint
from mvp.model.discovery.selection import _fs_history_path, _fs_progress_path
from mvp.projection.iid.serve_discovery import ServeDiscoverySelector
from mvp.projection.iid.two_level_serve_model import (
    FIRST_IN,
    WIN_FIRST,
    WIN_SECOND,
)
from tests.projection.iid.test_serve_discovery_prefit import (
    SHAPES,
    _make_selector,
    _two_level_config,
)
from tests.projection.iid.test_serve_discovery_swap_side import (
    DIFF_SPEC,
    MIRROR_SPEC,
    MirroringFakeEngine,
)

BP_SPEC = "is_break_point"
# The point candidate first_in is allowed: it is match-constant, so it has
# something to be evaluated at on a branch fit at (match, server) grain.
SURFACE_SPEC = "is_surface_hard"


def _joint_config(tmp_path, *, arms=None, max_features=None, min_delta=None) -> str:
    """`SHAPES["joint"]` as yaml, with the per-arm blocks optionally rewritten.

    `arms` replaces the whole `joint_selection.arms` mapping, so a test can
    give one arm a cap, a shorter pool, or a third arm, without a second copy
    of the fixture config.
    """
    path = _two_level_config(tmp_path, SHAPES["joint"])
    cfg = yaml.safe_load(open(path, encoding="utf-8"))
    if arms is not None:
        cfg["joint_selection"]["arms"] = arms
    if max_features is not None:
        cfg.setdefault("features", {})["max_features"] = max_features
    if min_delta is not None:
        cfg["min_delta"] = min_delta
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh, sort_keys=False)
    return path


def _arm(match=None, point=None, max_features=None) -> dict:
    block = {
        "candidate_match_level_features": (
            [MIRROR_SPEC, DIFF_SPEC] if match is None else list(match)
        ),
        # Never left empty: an empty per-arm list expands to the shared default
        # pool, which is hundreds of specs and no fixture at all.
        "candidate_point_level_features": [BP_SPEC] if point is None else list(point),
    }
    if max_features is not None:
        block["max_features"] = max_features
    return block


def _selector(tmp_path, monkeypatch, config_path):
    """A selector on `config_path` whose match-feature caching is faked."""
    sel = _make_selector(tmp_path, config_path, prepare=False, checkpoint=True)
    monkeypatch.setattr(
        ServeDiscoverySelector, "_pre_cache_all",
        lambda self, **kw: (MirroringFakeEngine(), "k"),
    )
    return sel


def _constant_scorer(sel, score: float = 0.55):
    """Replace the chain scorer with one constant number.

    Every `(arm, candidate)` pair therefore ties exactly, which is the only
    way to see the tie-break the loop is supposed to apply. Returns the list
    of `(arm, match_list, point_list)` the loop asked for, in call order.
    """
    calls: list[tuple] = []

    def scorer(match_level, point_level, arm=None):
        calls.append((arm, list(match_level), list(point_level)))
        return score, ()

    sel._score_cv_match_grain_detailed = scorer
    return calls


def _preferring_scorer(sel):
    """A scorer with a preference: DIFF_SPEC, on as many arms as will take it.

    The score falls slightly with the MODEL's total list length, so every
    round still improves on the last and the run does not halt after one, and
    falls much further for each ARM whose match list carries `DIFF_SPEC`, so
    putting that one feature on a second arm beats anything else on offer.
    The perturbed arm's lists come from the call; the other searched arms'
    come from `_joint_selected`, which is the live selection the loop keeps.
    """
    calls: list[tuple] = []

    def scorer(match_level, point_level, arm=None):
        calls.append((arm, list(match_level), list(point_level)))
        n_total = len(match_level) + len(point_level)
        n_diff = 1 if DIFF_SPEC in match_level else 0
        for other, (sel_match, sel_point) in sel._joint_selected.items():
            if other == arm:
                continue
            n_total += len(sel_match) + len(sel_point)
            n_diff += 1 if DIFF_SPEC in sel_match else 0
        return 0.60 - 0.001 * n_total - 0.05 * n_diff, ()

    sel._score_cv_match_grain_detailed = scorer
    return calls


def _history(sel) -> list[dict]:
    path = _fs_history_path(sel.checkpoint_path)
    return [json.loads(ln) for ln in path.read_text(encoding="utf-8").splitlines()]


def _added(result) -> list[tuple]:
    """`(round, feature, arm)` for each accepted round, in order."""
    return [
        (r.round_idx, r.feature_added, r.arm)
        for r in result.rounds if r.feature_added is not None
    ]


class TestJointSelection:
    def test_two_arms_are_selected_into_and_emitted_with_the_held_arm(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=2))
        _preferring_scorer(sel)
        result = sel.run()

        # The same feature earns a place on both arms, across two rounds.
        assert _added(result) == [
            (1, DIFF_SPEC, WIN_FIRST), (2, DIFF_SPEC, WIN_SECOND),
        ]
        assert result.selected_by_arm == {
            WIN_FIRST: ([DIFF_SPEC], []),
            WIN_SECOND: ([DIFF_SPEC], []),
        }
        # The flat lists are the DEDUPLICATED union: one feature, two arms.
        assert result.selected_match_level == [DIFF_SPEC]
        assert result.selected_point_level == []

        emitted = sel.config.to_iid_projection_config_dict(
            selected_match_level=result.selected_match_level,
            selected_point_level=result.selected_point_level,
            model_type=sel.config.scoring_model.type,
            model_params=sel.config.scoring_model.params,
            selected_by_arm=result.selected_by_arm,
        )
        block = emitted["serve_model"]
        assert block["type"] == "two_level"
        assert block["win_first_match_features"] == [DIFF_SPEC]
        assert block["win_second_match_features"] == [DIFF_SPEC]
        # The held arm is carried through verbatim, features and all.
        assert block["first_in_match_features"] == [MIRROR_SPEC]
        assert block["first_in_point_features"] == []
        assert (
            "[joint(win_first,win_second) selected on iid_match_win_log_loss]"
            in emitted["description"]
        )

    def test_three_arms_run_end_to_end(self, tmp_path, monkeypatch):
        path = _joint_config(
            tmp_path,
            arms={
                WIN_FIRST: _arm(),
                WIN_SECOND: _arm(),
                # first_in is fit at match grain with no ScoreState, so its
                # point pool has to be the match-constant one-hots.
                FIRST_IN: _arm(point=[SURFACE_SPEC]),
            },
            # The run-wide cap counts PINNED features too, and first_in brings
            # one: four buys the three rounds this asserts on.
            max_features=4,
        )
        sel = _selector(tmp_path, monkeypatch, path)
        _preferring_scorer(sel)
        result = sel.run()

        assert [arm for _r, _f, arm in _added(result)] == [
            WIN_FIRST, WIN_SECOND, FIRST_IN,
        ]
        assert result.selected_by_arm == {
            WIN_FIRST: ([DIFF_SPEC], []),
            WIN_SECOND: ([DIFF_SPEC], []),
            # first_in was pinned at MIRROR_SPEC and keeps it.
            FIRST_IN: ([MIRROR_SPEC, DIFF_SPEC], []),
        }
        emitted = sel.config.to_iid_projection_config_dict(
            selected_match_level=result.selected_match_level,
            selected_point_level=result.selected_point_level,
            model_type=sel.config.scoring_model.type,
            model_params=sel.config.scoring_model.params,
            selected_by_arm=result.selected_by_arm,
        )
        assert emitted["serve_model"]["first_in_match_features"] == [
            MIRROR_SPEC, DIFF_SPEC,
        ]
        assert (
            "[joint(win_first,win_second,first_in) selected on"
            in emitted["description"]
        )

    def test_the_round_one_ranking_holds_one_row_per_arm_and_candidate(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=2))
        _preferring_scorer(sel)
        sel.run()

        rows = _history(sel)[0]["ranking"]
        # 2 arms x (2 match + 1 point) candidates, each scored on its own arm.
        assert len(rows) == 6
        pairs = {(feat, arm) for feat, _score, arm in rows}
        assert (DIFF_SPEC, WIN_FIRST) in pairs
        assert (DIFF_SPEC, WIN_SECOND) in pairs

    def test_an_exact_tie_across_arms_goes_to_the_first_arm_in_config_order(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=1))
        _constant_scorer(sel)
        result = sel.run()

        rec = _history(sel)[0]
        # Guard on the FIXTURE: the round has to be decidable more than one
        # way, or arrival order would satisfy these assertions too.
        assert len({arm for _f, _s, arm in rec["ranking"]}) == 2
        assert rec["arm"] == WIN_FIRST
        # Within the winning arm, the alphabetically first candidate.
        assert rec["feature"] == BP_SPEC
        assert result.selected_by_arm[WIN_FIRST] == ([], [BP_SPEC])
        assert result.selected_by_arm[WIN_SECOND] == ([], [])

    def test_the_held_arm_stays_at_its_configured_list(self, tmp_path, monkeypatch):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path))
        sel._prepare_match_data(
            match_pool=[MIRROR_SPEC, DIFF_SPEC],
            engine=MirroringFakeEngine(), cache_key="k",
        )
        model = sel._build_candidate_model(
            [DIFF_SPEC], [], sel._scoring_params(), arm=WIN_SECOND,
        )
        components = model.components()
        assert components[WIN_SECOND].match_level_features == [DIFF_SPEC]
        assert components[FIRST_IN].match_level_features == [MIRROR_SPEC]


    def test_the_baseline_scores_the_first_non_empty_arm(self, tmp_path, monkeypatch):
        """Review finding on #115: with win_first empty and win_second pinned,
        the baseline must score win_second's pinned set, not short-circuit on
        the first arm's empty lists and start from worst_score."""
        import math

        path = _joint_config(tmp_path, max_features=1)
        cfg = yaml.safe_load(open(path, encoding="utf-8"))
        cfg["serve_model"]["win_second_match_features"] = [DIFF_SPEC]
        with open(path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(cfg, fh, sort_keys=False)
        sel = _selector(tmp_path, monkeypatch, path)
        calls = _constant_scorer(sel)
        result = sel.run()
        assert calls[0] == (WIN_SECOND, [DIFF_SPEC], [])
        assert math.isfinite(result.rounds[0].score)
        assert result.rounds[0].selected_match_level == [DIFF_SPEC]


class TestCaps:
    def test_a_capped_arm_submits_nothing_after_it_fills(self, tmp_path, monkeypatch):
        path = _joint_config(tmp_path, arms={
            WIN_FIRST: _arm(max_features=1), WIN_SECOND: _arm(),
        })
        sel = _selector(tmp_path, monkeypatch, path)
        calls = _preferring_scorer(sel)
        result = sel.run()

        # win_first wins round 1, hits its cap, and is never fitted or scored
        # again: exactly its three round-1 candidates.
        assert sum(1 for arm, _m, _p in calls if arm == WIN_FIRST) == 3
        assert sum(1 for arm, _m, _p in calls if arm == WIN_SECOND) > 3
        assert result.selected_by_arm[WIN_FIRST] == ([DIFF_SPEC], [])
        assert len(result.selected_by_arm[WIN_SECOND][0]) > 0

    def test_the_total_cap_ends_the_run_across_arms(self, tmp_path, monkeypatch):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=2))
        _preferring_scorer(sel)
        result = sel.run()

        selected = result.selected_by_arm
        assert sum(len(m) + len(p) for m, p in selected.values()) == 2
        # ... and it is two arms' worth, not one arm twice.
        assert {arm for _r, _f, arm in _added(result)} == {WIN_FIRST, WIN_SECOND}

    def test_an_exhausted_pool_takes_its_arm_out_of_later_rounds(
        self, tmp_path, monkeypatch,
    ):
        path = _joint_config(tmp_path, arms={
            WIN_FIRST: _arm(match=[DIFF_SPEC]),   # two candidates in total
            WIN_SECOND: _arm(),
        })
        sel = _selector(tmp_path, monkeypatch, path)
        calls = _preferring_scorer(sel)
        result = sel.run()

        # win_first took both of its candidates, so it has nothing left to
        # submit and the tail of the run is win_second alone.
        first_match, first_point = result.selected_by_arm[WIN_FIRST]
        assert set(first_match + first_point) == {DIFF_SPEC, BP_SPEC}
        arms_seen = [arm for arm, _m, _p in calls]
        after_last = arms_seen[len(arms_seen) - arms_seen[::-1].index(WIN_FIRST):]
        assert after_last, "the run ended on win_first's own last candidate"
        assert set(after_last) == {WIN_SECOND}


class TestRecords:
    def test_the_add_record_carries_the_arm_and_three_field_rows(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=2))
        _preferring_scorer(sel)
        sel.run()

        rec = _history(sel)[0]
        assert rec["action"] == "add" and rec["round"] == 1
        assert rec["arm"] == WIN_FIRST
        assert all(len(row) == 3 for row in rec["ranking"])
        assert {row[2] for row in rec["ranking"]} == {WIN_FIRST, WIN_SECOND}

    def test_the_stop_record_names_the_best_arm(self, tmp_path, monkeypatch):
        # DIFF_SPEC only on win_first's pool, so once it is taken every
        # remaining candidate ties and the round's winner is win_first's.
        path = _joint_config(
            tmp_path,
            arms={WIN_FIRST: _arm(), WIN_SECOND: _arm(match=[MIRROR_SPEC])},
            min_delta=10.0,
        )
        sel = _selector(tmp_path, monkeypatch, path)
        _preferring_scorer(sel)
        sel.run()

        stop = _history(sel)[-1]
        assert stop["action"] == "stop"
        assert stop["best_arm"] == WIN_FIRST
        assert stop["best_candidate"] == BP_SPEC
        assert all(len(row) == 3 for row in stop["ranking"])
        assert {row[2] for row in stop["ranking"]} == {WIN_FIRST, WIN_SECOND}

    def test_the_progress_file_is_grouped_by_arm(self, tmp_path, monkeypatch):
        path = _joint_config(
            tmp_path,
            arms={
                WIN_FIRST: _arm(),
                WIN_SECOND: _arm(),
                FIRST_IN: _arm(point=[SURFACE_SPEC]),
            },
            # first_in's pinned feature counts against the run-wide cap, so
            # three is what buys a round for each win arm.
            max_features=3,
        )
        sel = _selector(tmp_path, monkeypatch, path)
        _preferring_scorer(sel)
        sel.run()

        text = _fs_progress_path(sel.checkpoint_path).read_text(encoding="utf-8")
        assert text.index("win_first:") < text.index("win_second:")
        assert text.index("win_second:") < text.index("first_in:")
        # Each arm's own selections sit under its own header ...
        win_first, win_second = text.split("win_second:")
        assert f"1. {DIFF_SPEC} [match]" in win_first
        assert f"2. {DIFF_SPEC} [match]" in win_second
        # ... and a searched arm's PINNED features are its base lines.
        assert f"   base. {MIRROR_SPEC} [match]" in text.split("first_in:")[1]


class TestResumeNotYet:
    """Deleted by #116, which is where joint resume gets built."""

    def test_a_joint_config_with_a_checkpoint_refuses_to_resume(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=1))
        _constant_scorer(sel)
        now = datetime.now()
        save_checkpoint(sel.checkpoint_path, SelectionCheckpoint(
            run_name="joint", started_at=now, updated_at=now,
            completed_rounds=[], current_round=1, total_candidates=0,
            current_round_scores={}, best_metric=0.6, direction="minimize",
            max_features=1, chain_shrink="fixed",
        ))
        with pytest.raises(NotImplementedError, match="joint resume: #116"):
            sel.run()
