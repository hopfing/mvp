"""The serve FS round winner is chosen AFTER the round, from the full ranking
(#114).

`run()` keeps a running best while it scores, but that best is set by whichever
candidate ARRIVES first carrying a winning score — and under
`n_parallel_candidates > 1` arrival order is whatever the thread pool happens
to produce. Two candidates that tie exactly would then resolve differently from
run to run and machine to machine. `_pick_round_winner` recomputes the winner
from the whole score map once the round has finished: by score under the
metric's direction, then by name. The streaming best survives only as the
progress bar's postfix.

Fixture: the two-level prefit fixture on `SHAPES["win_second"]`, so no B:/
reads. The maximise case rewrites the fixture's `metric:` to a maximised one.
"""

import json
import math

import pytest
import yaml

from mvp.model.discovery.selection import _fs_history_path
from mvp.projection.iid.metric_registry import is_minimize
from mvp.projection.iid.serve_discovery import ServeDiscoverySelector
from tests.projection.iid.test_serve_discovery_prefit import (
    SHAPES,
    _make_selector,
    _two_level_config,
)
from tests.projection.iid.test_serve_discovery_swap_side import MirroringFakeEngine

MINIMISED = "iid_match_win_log_loss"  # the fixture's own metric
MAXIMISED = "iid_match_win_auc"


def _by_name(name: str) -> tuple:
    """The `order` key `run()` passes: name only."""
    return (name,)


def _zulu_first(name: str) -> tuple:
    """An `order` key that outranks the alphabet, with the name still last —
    so a test using it proves the callable decided, not the name fallback."""
    return (0 if name == "zulu" else 1, name)


def _selector(tmp_path, *, metric=None, max_features=None, **kw):
    """The prefit `win_second` fixture, optionally re-metriced / capped."""
    path = _two_level_config(tmp_path, SHAPES["win_second"])
    if metric is not None or max_features is not None:
        cfg = yaml.safe_load(open(path, encoding="utf-8"))
        if metric is not None:
            cfg["metric"] = metric
        if max_features is not None:
            cfg["features"]["max_features"] = max_features
        with open(path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(cfg, fh, sort_keys=False)
    return _make_selector(tmp_path, path, **kw)


@pytest.fixture
def minimising(tmp_path):
    """Nothing here scores, so the expensive prep is skipped."""
    return _selector(tmp_path, prepare=False)


@pytest.fixture
def maximising(tmp_path):
    return _selector(tmp_path, metric=MAXIMISED, prepare=False)


class TestPickRoundWinner:
    def test_the_fixtures_point_in_opposite_directions(self, minimising, maximising):
        """Guard: the two cases below only differ if the metrics do."""
        assert is_minimize(minimising.config.metric)
        assert not is_minimize(maximising.config.metric)

    def test_a_tie_resolves_by_name_when_the_metric_is_minimised(self, minimising):
        # "zulu" is inserted first, so an arrival-order winner would take it.
        scores = {"zulu": 0.55, "bravo": 0.60, "alpha": 0.55}
        assert minimising._pick_round_winner(
            scores, 0.70, order=_by_name,
        ) == ("alpha", 0.55)

    def test_a_tie_resolves_by_name_when_the_metric_is_maximised(self, maximising):
        scores = {"zulu": 0.74, "bravo": 0.70, "alpha": 0.74}
        assert maximising._pick_round_winner(
            scores, 0.65, order=_by_name,
        ) == ("alpha", 0.74)

    def test_a_non_finite_score_never_wins(self, minimising):
        # -inf sorts ahead of every real score under a minimised metric and nan
        # compares false against everything: both have to be dropped BEFORE the
        # sort, not ranked and then rejected.
        scores = {
            "aaa_neg": -math.inf, "aab_nan": math.nan,
            "real": 0.55, "zzz_pos": math.inf,
        }
        assert minimising._pick_round_winner(
            scores, 0.70, order=_by_name,
        ) == ("real", 0.55)

    def test_a_non_finite_score_never_wins_when_maximised(self, maximising):
        scores = {"aaa_pos": math.inf, "aab_nan": math.nan, "real": 0.74}
        assert maximising._pick_round_winner(
            scores, 0.65, order=_by_name,
        ) == ("real", 0.74)

    def test_nothing_better_than_the_incumbent_returns_no_winner(self, minimising):
        # 0.60 only ties the incumbent — improvement is strict.
        assert minimising._pick_round_winner(
            {"a": 0.61, "b": 0.60}, 0.60, order=_by_name,
        ) == (None, 0.60)

    def test_every_score_non_finite_returns_no_winner(self, minimising):
        assert minimising._pick_round_winner(
            {"a": math.nan, "b": math.inf}, 0.60, order=_by_name,
        ) == (None, 0.60)
        # Round 1 has no baseline; a non-finite score must not beat the
        # sentinel either.
        assert minimising._pick_round_winner(
            {"a": math.nan}, math.inf, order=_by_name,
        ) == (None, math.inf)
        assert minimising._pick_round_winner(
            {}, 0.60, order=_by_name,
        ) == (None, 0.60)

    def test_the_order_callable_breaks_ties_before_the_name(self, minimising):
        scores = {"alpha": 0.55, "zulu": 0.55}
        assert minimising._pick_round_winner(
            scores, 0.70, order=_zulu_first,
        ) == ("zulu", 0.55)
        # Same scores, same insertion order: only the callable changed.
        assert minimising._pick_round_winner(
            scores, 0.70, order=_by_name,
        ) == ("alpha", 0.55)

    def test_the_order_callable_does_not_outrank_the_score(self, minimising):
        """Score first, `order` only within a tie."""
        assert minimising._pick_round_winner(
            {"alpha": 0.60, "zulu": 0.55}, 0.70, order=_zulu_first,
        ) == ("zulu", 0.55)
        assert minimising._pick_round_winner(
            {"alpha": 0.55, "zulu": 0.60}, 0.70, order=_zulu_first,
        ) == ("alpha", 0.55)


class TestRunUsesThePostRoundWinner:
    """End to end through `run()`, on a round every candidate ties: the winner
    must be the ranking's alphabetical first, not its first-scored entry."""

    def test_a_round_of_identical_scores_takes_the_first_name(
        self, tmp_path, monkeypatch,
    ):
        sel = _selector(
            tmp_path, max_features=1, prepare=False, checkpoint=True,
        )
        monkeypatch.setattr(
            ServeDiscoverySelector, "_pre_cache_all",
            lambda self, **kw: (MirroringFakeEngine(), "k"),
        )
        monkeypatch.setattr(
            ServeDiscoverySelector, "_score_cv_match_grain_detailed",
            lambda self, match_level, point_level: (0.55, ()),
        )
        result = sel.run()

        history = _fs_history_path(sel.checkpoint_path)
        rec = json.loads(history.read_text(encoding="utf-8").splitlines()[0])
        ranked = [f for f, _ in rec["ranking"]]
        assert rec["action"] == "add" and rec["round"] == 1
        assert len(ranked) > 1
        # Guard on the FIXTURE: the round has to be decidable two ways, or the
        # assertions below would pass under arrival-order selection too.
        assert ranked[0] != min(ranked), ranked
        assert rec["feature"] == min(ranked)
        assert rec["grain"] == "point"  # min(ranked) is the point candidate
        assert rec["metric"] == 0.55
        assert result.selected_match_level == []
        assert result.selected_point_level == [min(ranked)]
