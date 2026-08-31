"""The chain-metric registry: the match-win metric and score_chain's contract.

The rest of the registry (CRPS, cal errors) is exercised through
test_metrics.py; this file pins the iid_match_win_log_loss entry added so
serve FS can select features FOR match-win (plan
2026-08-31-winner-tuned-projection).
"""

from types import SimpleNamespace

import numpy as np
import pytest

from mvp.projection.iid.metric_registry import (
    direction_of,
    is_chain_metric,
    score_chain,
    validate_metric_name,
)


def _dist(p_match_win_a):
    return SimpleNamespace(p_match_win_a=np.asarray(p_match_win_a, dtype=np.float64))


_GAMES = np.zeros(3)  # unused by the match-win scorer, required by the signature


class TestMatchWinLogLoss:
    def test_registered_as_chain_minimize(self):
        name = "iid_match_win_log_loss"
        assert validate_metric_name(name) == name
        assert is_chain_metric("iid_match_win_log_loss")
        assert direction_of("iid_match_win_log_loss") == "minimize"

    def test_hand_computed_value(self):
        # p = [0.8, 0.3, 0.5], y = [1, 0, 1]
        p = np.array([0.8, 0.3, 0.5])
        y = np.array([1, 0, 1])
        expected = -np.mean([np.log(0.8), np.log(0.7), np.log(0.5)])
        score = score_chain(
            "iid_match_win_log_loss", _dist(p), _GAMES, _GAMES, y_won=y,
        )
        assert score == pytest.approx(expected, abs=1e-12)

    def test_requires_y_won(self):
        """Never a silent games-derived proxy: winners can lose the games
        count, so the winner label is mandatory."""
        with pytest.raises(ValueError, match="y_won"):
            score_chain("iid_match_win_log_loss", _dist([0.5]), _GAMES, _GAMES)

    def test_ranks_aligned_above_anti_aligned(self):
        """The FS ordering this metric exists for: a candidate whose chain
        probability agrees with outcomes must score better (lower) than one
        that opposes them."""
        y = np.array([1, 1, 0, 0])
        aligned = _dist([0.7, 0.65, 0.35, 0.3])
        anti = _dist([0.3, 0.35, 0.65, 0.7])
        s_aligned = score_chain(
            "iid_match_win_log_loss", aligned, _GAMES, _GAMES, y_won=y,
        )
        s_anti = score_chain(
            "iid_match_win_log_loss", anti, _GAMES, _GAMES, y_won=y,
        )
        assert s_aligned < s_anti

    def test_extreme_probs_are_clipped(self):
        score = score_chain(
            "iid_match_win_log_loss", _dist([1.0, 0.0]), _GAMES[:2], _GAMES[:2],
            y_won=np.array([0, 1]),
        )
        assert np.isfinite(score)


class TestWidenedKwarg:
    def test_existing_chain_scorers_absorb_y_won(self):
        """score_chain now always forwards y_won; the games/lines scorers must
        be indifferent to it."""
        pmf = np.zeros((2, 5))
        pmf[0, 2] = 1.0
        pmf[1, 3] = 1.0
        dist = SimpleNamespace(total_games_pmf=pmf)
        y_a = np.array([1.0, 2.0])
        y_b = np.array([1.0, 1.0])
        with_y = score_chain(
            "iid_crps_total_games", dist, y_a, y_b, y_won=np.array([1, 0]),
        )
        without_y = score_chain("iid_crps_total_games", dist, y_a, y_b)
        assert with_y == without_y
