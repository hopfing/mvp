"""A chain FS run scores each candidate at its own train-fitted shrink (#109).

`chain_shrink` selects the scorer per run. Under `fixed` the chain scorer is
the one that shipped before this existed, proven here against an in-process
oracle that runs the pre-change call sequence by hand. Under `proxy` or `grid`
the gap shrink is fitted on each fold's TRAIN side, set on that candidate's
model for the evaluation, and the test fold is scored at it.

Fixture: the prefit tests' synthetic two-sided matches, their mirroring fake
engine and their `_make_selector`, so a candidate is scored at the same seam
the prefit equivalence tests use. No B:/ reads.
"""

from pathlib import Path
from textwrap import dedent

import numpy as np
import pytest
import yaml

from mvp.projection.iid.calibration import fit_gap_shrink, score_at_shrink
from mvp.projection.iid.metric_registry import score_chain
from mvp.projection.iid.serve_discovery import ServeDiscoverySelector
from mvp.projection.iid.stateful_chain import match_distribution_from_state_fn
from tests.projection.iid.test_serve_discovery_prefit import (
    SHAPES,
    _make_selector,
    _two_level_config,
)
from tests.projection.iid.test_serve_discovery_swap_side import (
    DIFF_SPEC,
    MirroringFakeEngine,
)

# The win_second shape's match candidate every score here is taken on.
KNOWN_CANDIDATE = ([DIFF_SPEC], [])

# Captured before any patching, so a spy can call the real thing without
# chaining onto whatever spy a previous run in the same test installed.
_RECORD = ServeDiscoverySelector._record_candidate


def _config_path(
    tmp_path,
    extra: str = "",
    *,
    name: str = "fs",
    gap_shrink: float | None = None,
    max_features: int | None = None,
) -> str:
    """The prefit fixture's win_second yaml, with `extra` appended."""
    text = Path(_two_level_config(tmp_path, SHAPES["win_second"])).read_text()
    text += dedent(extra)
    if gap_shrink is not None or max_features is not None:
        cfg = yaml.safe_load(text)
        if gap_shrink is not None:
            cfg["serve_model"]["gap_shrink"] = gap_shrink
        if max_features is not None:
            cfg["features"]["max_features"] = max_features
        text = yaml.safe_dump(cfg, sort_keys=False)
    path = tmp_path / f"{name}.yaml"
    path.write_text(text)
    return str(path)


def _selector(tmp_path, extra: str = "", **kwargs):
    prepare = kwargs.pop("prepare", True)
    checkpoint = kwargs.pop("checkpoint", False)
    return _make_selector(
        tmp_path,
        _config_path(tmp_path, extra, **kwargs),
        prepare=prepare,
        checkpoint=checkpoint,
    )


def _fitted_candidate(selector, fold_idx, fold, match_level, point_level):
    """The candidate model for one fold, built and fitted as the scorer does."""
    model = selector._build_candidate_model(
        match_level, point_level, selector._scoring_params(),
    )
    selector._attach_prefit(model, fold_idx)
    model.fit(
        fold.train_df,
        preloaded_match_features=fold.feats,
        preloaded_points=fold.points,
    )
    return model


def _oracle_score(selector, match_level, point_level) -> float:
    """The chain scorer's exact call sequence from before #109.

    Deliberately does not touch the calibration module: it is the independent
    source of truth the fixed setting is held to, so it must not be able to
    agree with the code under test by sharing it. The model is scored at
    whatever gap scale it was configured with, which is what `fixed` means.
    """
    fold_scores: list[float] = []
    for fold_idx, fold in enumerate(selector._chain_folds):
        model = _fitted_candidate(
            selector, fold_idx, fold, match_level, point_level,
        )
        test_df = fold.test_df
        p_a_fn, p_b_fn = model.predict_state_fn(test_df)
        p_a, p_b = model.predict(test_df)
        dist = match_distribution_from_state_fn(
            p_a_fn, p_b_fn, p_a, p_b,
            test_df["best_of"].to_numpy().astype(np.int64),
        )
        fold_scores.append(
            score_chain(
                selector.config.metric, dist,
                test_df["_target_games_a"].to_numpy().astype(np.float64),
                test_df["_target_games_b"].to_numpy().astype(np.float64),
                total_lines=list(selector.config.metrics.total_lines),
                spread_lines=list(selector.config.metrics.spread_lines),
                y_won=test_df["won"].to_numpy().astype(np.int64),
            )
        )
    return float(np.mean(fold_scores))


def _by_hand(selector, match_level, point_level, method):
    """The fitter applied by hand: fit the candidate per fold, fit the shrink
    on the train side, score the test side at it."""
    objective = selector._chain_objective()
    scores: list[float] = []
    shrinks: list[float] = []
    for fold_idx, fold in enumerate(selector._chain_folds):
        model = _fitted_candidate(
            selector, fold_idx, fold, match_level, point_level,
        )
        fit = fit_gap_shrink(
            model, fold.train_df, fold.points,
            method=method, objective=objective,
        )
        scores.append(score_at_shrink(model, fold.test_df, objective, fit.shrink))
        shrinks.append(fit.shrink)
    return float(np.mean(scores)), shrinks


def _run_and_capture(selector, monkeypatch):
    """Drive the real round loop, handing back the round's two maps.

    The maps are locals of `run()`, so they are taken from the recorder both
    loop paths file through — the objects themselves, not a copy the spy
    assembled.
    """
    seen: dict[str, dict] = {}

    def spy(self, cand, score, shrinks, round_scores, round_shrinks):
        _RECORD(self, cand, score, shrinks, round_scores, round_shrinks)
        seen["scores"], seen["shrinks"] = round_scores, round_shrinks

    monkeypatch.setattr(ServeDiscoverySelector, "_record_candidate", spy)
    monkeypatch.setattr(
        ServeDiscoverySelector, "_pre_cache_all",
        lambda self, **kw: (MirroringFakeEngine(), "k"),
    )
    selector.run()
    assert seen, "the round loop scored no candidate"
    return seen["scores"], seen["shrinks"]


class TestFixedIsTodaysScorer:
    """The default has to be a no-op by test, not by claim."""

    def test_the_field_absent_scores_as_the_pre_change_sequence(self, tmp_path):
        selector = _selector(tmp_path)
        assert selector.config.chain_shrink == "fixed"

        score, shrinks = selector._score_cv_chain_detailed(*KNOWN_CANDIDATE)

        assert score == _oracle_score(selector, *KNOWN_CANDIDATE)
        assert shrinks == ()

    def test_fixed_in_the_yaml_scores_as_the_pre_change_sequence(self, tmp_path):
        selector = _selector(tmp_path, "chain_shrink: fixed\n")
        score, _ = selector._score_cv_chain_detailed(*KNOWN_CANDIDATE)
        assert score == _oracle_score(selector, *KNOWN_CANDIDATE)

    def test_fixed_scores_at_the_configured_gap_shrink(self, tmp_path):
        """`fixed` means the model's own gap scale, not 1.0. A run whose yaml
        sets 0.8 must be ranked on the model it configured."""
        selector = _selector(tmp_path, gap_shrink=0.8)
        assert selector.config.serve_model.gap_shrink == 0.8

        score, _ = selector._score_cv_chain_detailed(*KNOWN_CANDIDATE)

        assert score == _oracle_score(selector, *KNOWN_CANDIDATE)
        at_one, _ = _selector(
            tmp_path, name="unshrunk",
        )._score_cv_chain_detailed(*KNOWN_CANDIDATE)
        assert score != at_one, "0.8 scored the same as 1.0"


class TestFittedShrink:
    @pytest.mark.parametrize("method", ["proxy", "grid"])
    def test_score_equals_the_fitter_applied_by_hand(self, tmp_path, method):
        selector = _selector(tmp_path, f"chain_shrink: {method}\n")
        expected, expected_shrinks = _by_hand(selector, *KNOWN_CANDIDATE, method)

        score, shrinks = selector._score_cv_chain_detailed(*KNOWN_CANDIDATE)

        assert score == pytest.approx(expected, abs=1e-12)
        assert list(shrinks) == pytest.approx(expected_shrinks, abs=1e-12)
        assert len(shrinks) == len(selector._chain_folds)
        # Guard against a fit that quietly did nothing: a shrink of 1.0 on
        # every fold would make this test agree with the fixed scorer and
        # still pass.
        assert any(s != 1.0 for s in shrinks)


class TestProgressPostfix:
    def test_carries_the_mean_shrink_when_one_was_fitted(self):
        postfix = ServeDiscoverySelector._progress_postfix(
            0.5, "melo", "match", {"melo": [1.2, 1.4]},
        )
        assert postfix == {
            "best": "0.500000", "feat": "melo[match]", "shrink": "1.30",
        }

    def test_carries_no_shrink_under_a_fixed_run(self):
        """A fixed run fits none, so its map is empty and its line is the line
        it has always been."""
        postfix = ServeDiscoverySelector._progress_postfix(
            0.5, "melo", "match", {},
        )
        assert postfix == {"best": "0.500000", "feat": "melo[match]"}


class TestTheRoundLoop:
    ONE_ROUND = {"extra": "chain_shrink: proxy\n", "max_features": 1}

    def test_every_scored_candidate_gets_a_shrink_per_fold(
        self, tmp_path, monkeypatch,
    ):
        selector = _selector(
            tmp_path, prepare=False, checkpoint=True, **self.ONE_ROUND,
        )

        scores, shrinks = _run_and_capture(selector, monkeypatch)

        assert scores
        assert set(shrinks) == set(scores)
        n_folds = len(selector._chain_folds)
        assert all(len(v) == n_folds for v in shrinks.values())

    def test_two_parallel_candidates_match_sequential(
        self, tmp_path, monkeypatch,
    ):
        """The parallel loop is a different code path, not just a faster one:
        it hands the score and the shrinks back through a future."""
        sequential = _selector(
            tmp_path, prepare=False, checkpoint=True, name="seq",
            **self.ONE_ROUND,
        )
        seq_scores, seq_shrinks = _run_and_capture(sequential, monkeypatch)

        parallel = _selector(
            tmp_path, prepare=False, checkpoint=True, name="par",
            extra="chain_shrink: proxy\nn_parallel_candidates: 2\n",
            max_features=1,
        )
        par_scores, par_shrinks = _run_and_capture(parallel, monkeypatch)

        assert parallel.config.n_parallel_candidates == 2
        assert par_scores == seq_scores
        assert par_shrinks == seq_shrinks
