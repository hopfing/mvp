"""Serve FS #116: a joint run resumes from its own checkpoint.

A joint checkpoint says which ARM took each completed round and which
`(arm, grain, candidate)` each partial score belongs to. That is what a resume
needs: the completed rounds have to replay into the arms that won them, and a
round interrupted half-way has to finish without paying again for the pairs it
already scored.

The two checkpoint shapes are not interchangeable and neither can be read as
the other, so a config of one kind meeting a checkpoint of the other is refused
in both directions rather than replayed into the wrong arms — silently, which
is the only way it could go wrong.

Fixture is the joint file's: `SHAPES["joint"]`, the MirroringFakeEngine, and a
replaced chain scorer, so no B:/ reads. `checkpoint_interval = 1` throughout —
every scored pair leaves a checkpoint, which is what makes "interrupted after
exactly two pairs" a thing a test can arrange.
"""

from datetime import datetime

import pytest

from mvp.model.discovery.checkpoint import (
    SelectionCheckpoint,
    load_checkpoint,
    save_checkpoint,
)
from mvp.projection.iid.two_level_serve_model import WIN_FIRST, WIN_SECOND
from tests.projection.iid.test_serve_discovery_joint import (
    BP_SPEC,
    _arm,
    _constant_scorer,
    _joint_config,
    _preferring_scorer,
    _selector,
)
from tests.projection.iid.test_serve_discovery_prefit import (
    SHAPES,
    _two_level_config,
)
from tests.projection.iid.test_serve_discovery_swap_side import (
    DIFF_SPEC,
    MIRROR_SPEC,
)


def _resumable(tmp_path, monkeypatch, config_path):
    """A joint selector that checkpoints after every scored pair.

    The production default is every 25, which on a fixture round of five pairs
    would checkpoint nothing at all.
    """
    sel = _selector(tmp_path, monkeypatch, config_path)
    sel.checkpoint_interval = 1
    return sel


def _interrupting_scorer(sel, *, at_call: int = 3):
    """`_preferring_scorer`, cut short part-way through round 2.

    Round 2 is recognised from the LIVE selection rather than from a running
    call count: an arm's list grows only when a round ends, so the first call
    made while some arm already carries a feature is round 2's first, whatever
    the round before it cost. `KeyboardInterrupt` is what an operator's Ctrl-C
    raises, and it is raised before the inner scorer is reached, so the pair it
    lands on is neither scored nor recorded — the checkpoint on disk holds
    exactly the pairs that completed.
    """
    calls = _preferring_scorer(sel)
    inner = sel._score_cv_match_grain_detailed
    seen = {"n": 0}

    def scorer(match_level, point_level, arm=None):
        if any(m or p for m, p in sel._joint_selected.values()):
            seen["n"] += 1
            if seen["n"] == at_call:
                raise KeyboardInterrupt("interrupted mid-round")
        return inner(match_level, point_level, arm=arm)

    sel._score_cv_match_grain_detailed = scorer
    return calls


def _write_checkpoint(path, **overrides) -> None:
    """A checkpoint carrying every field `SelectionCheckpoint` does not default.

    Hand-written rather than produced by a run: the point of these is to be a
    shape the run under test did not write, which is exactly what no run of it
    can produce.
    """
    now = datetime.now()
    fields = dict(
        run_name="fs",
        started_at=now,
        updated_at=now,
        completed_rounds=[],
        current_round=1,
        total_candidates=3,
        current_round_scores={},
        best_metric=0.6,
        direction="minimize",
        max_features=2,
    )
    fields.update(overrides)
    save_checkpoint(path, SelectionCheckpoint(**fields))


class TestMidRoundCheckpoint:
    def test_the_checkpoint_left_mid_round_names_arms_on_both_halves(
        self, tmp_path, monkeypatch,
    ):
        sel = _resumable(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=2))
        _interrupting_scorer(sel)
        with pytest.raises(KeyboardInterrupt):
            sel.run()

        cp = load_checkpoint(sel.checkpoint_path)
        assert cp.current_round == 2
        # The completed half: one entry per selected feature, each saying which
        # arm took it.
        assert cp.completed_rounds
        assert all("arm" in entry for entry in cp.completed_rounds)
        assert cp.completed_rounds[0]["arm"] == WIN_FIRST
        # The partial half: `arm|grain|name`, because the same feature is
        # offered to every arm that lists it and one arm's score is not another's.
        assert cp.current_round_scores
        assert all(key.count("|") == 2 for key in cp.current_round_scores)

    def test_resuming_finishes_the_round_without_rescoring_what_it_holds(
        self, tmp_path, monkeypatch,
    ):
        path = _joint_config(tmp_path, max_features=2)
        sel = _resumable(tmp_path, monkeypatch, path)
        _interrupting_scorer(sel)
        with pytest.raises(KeyboardInterrupt):
            sel.run()
        held = load_checkpoint(sel.checkpoint_path).current_round_scores
        # Guard on the FIXTURE: the interrupted round got through part of
        # win_first, so "win_first is not scored again" has something to say.
        assert {key.split("|")[0] for key in held} == {WIN_FIRST}

        resumed = _resumable(tmp_path, monkeypatch, path)
        calls = _preferring_scorer(resumed)
        result = resumed.run()

        # Round 2 finished, and the run reached the run-wide cap.
        assert result.selected_by_arm == {
            WIN_FIRST: ([DIFF_SPEC], []),
            WIN_SECOND: ([DIFF_SPEC], []),
        }
        # Only the pairs the checkpoint did NOT hold were scored: win_first's
        # two came back from disk, win_second's three were the round's work.
        assert calls == [
            (WIN_SECOND, [MIRROR_SPEC], []),
            (WIN_SECOND, [DIFF_SPEC], []),
            (WIN_SECOND, [], [BP_SPEC]),
        ]
        assert not resumed.checkpoint_path.exists()

    def test_a_round_won_by_a_later_arm_replays_into_that_arm(
        self, tmp_path, monkeypatch,
    ):
        """Round 1 goes to win_second here, so a replay that ignored the
        entry's arm and used the first state would show: win_first would come
        back carrying a feature it never won."""
        path = _joint_config(
            tmp_path,
            arms={WIN_FIRST: _arm(match=[MIRROR_SPEC]), WIN_SECOND: _arm()},
            max_features=2,
        )
        sel = _resumable(tmp_path, monkeypatch, path)
        _interrupting_scorer(sel)
        with pytest.raises(KeyboardInterrupt):
            sel.run()
        cp = load_checkpoint(sel.checkpoint_path)
        assert cp.completed_rounds[0] == {
            "feature": DIFF_SPEC, "grain": "match",
            "score": cp.best_metric, "arm": WIN_SECOND,
        }

        resumed = _resumable(tmp_path, monkeypatch, path)
        _preferring_scorer(resumed)
        result = resumed.run()

        assert result.selected_by_arm[WIN_SECOND][0] == [DIFF_SPEC]
        assert DIFF_SPEC not in result.selected_by_arm[WIN_FIRST][0]


class TestCrossKindResume:
    def test_a_component_checkpoint_under_a_joint_config_is_refused(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=2))
        _constant_scorer(sel)
        _write_checkpoint(
            sel.checkpoint_path,
            completed_rounds=[
                {"feature": DIFF_SPEC, "grain": "match", "score": 0.59},
            ],
            current_round=2,
            current_round_scores={MIRROR_SPEC: 0.6},
        )
        with pytest.raises(
            ValueError, match="is from a component run; use --fresh",
        ):
            sel.run()

    def test_a_joint_checkpoint_under_a_component_config_is_refused(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(
            tmp_path, monkeypatch, _two_level_config(tmp_path, SHAPES["win_second"]),
        )
        _constant_scorer(sel)
        _write_checkpoint(
            sel.checkpoint_path,
            completed_rounds=[
                {
                    "feature": DIFF_SPEC, "grain": "match",
                    "score": 0.59, "arm": WIN_SECOND,
                },
            ],
            current_round=2,
            current_round_scores={f"{WIN_SECOND}|point|{BP_SPEC}": 0.6},
        )
        with pytest.raises(ValueError, match="is from a joint run; use --fresh"):
            sel.run()

    def test_an_empty_checkpoint_answers_to_either_kind(self, tmp_path, monkeypatch):
        """Nothing completed and nothing scored contradicts neither shape, so
        it resumes — a run interrupted before its first pair finished is not a
        cross-kind resume, it is a run that has done nothing yet."""
        sel = _selector(tmp_path, monkeypatch, _joint_config(tmp_path, max_features=1))
        calls = _constant_scorer(sel)
        _write_checkpoint(sel.checkpoint_path, max_features=1)

        result = sel.run()

        # Round 1 ran from the checkpoint's best_metric, every pair tied, and
        # the tie went to the first arm in config order.
        assert calls
        assert result.selected_by_arm == {
            WIN_FIRST: ([], [BP_SPEC]), WIN_SECOND: ([], []),
        }
        assert not sel.checkpoint_path.exists()
