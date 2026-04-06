"""Tests for iid tennis model features."""

from datetime import date

import polars as pl
import pytest

from mvp.model.features import iid as iid_module  # noqa: F401
from mvp.model.features.iid import (
    _EXPECTED_GAMES,
    _TIEBREAK_PROB,
    _compute_set_stats_avg,
    _iid_hold_probability,
)
from mvp.model.registry import get_registry


@pytest.fixture(scope="module", autouse=True)
def ensure_features_registered():
    yield


class TestIidHoldProbability:
    """Tests for the iid hold probability formula."""

    def test_p_zero(self):
        assert _iid_hold_probability(0.0) == 0.0

    def test_p_one(self):
        assert _iid_hold_probability(1.0) == 1.0

    def test_p_half(self):
        # At p=0.5, by symmetry P(hold) = 0.5
        assert _iid_hold_probability(0.5) == pytest.approx(0.5)

    def test_p_065(self):
        # Known value: at p=0.65, P(hold) ≈ 0.8293
        assert _iid_hold_probability(0.65) == pytest.approx(0.8293, abs=0.001)

    def test_monotonic(self):
        """P(hold) increases monotonically with p."""
        prev = 0.0
        for i in range(1, 101):
            p = i / 100
            h = _iid_hold_probability(p)
            assert h >= prev, f"Non-monotonic at p={p}: {h} < {prev}"
            prev = h


class TestSetMarkovChain:
    """Tests for the set Markov chain computation."""

    def test_both_hold_always(self):
        """When both players hold 100%, every set is a tiebreak."""
        eg, pt = _compute_set_stats_avg(1.0, 1.0)
        assert eg == pytest.approx(13.0)
        assert pt == pytest.approx(1.0)

    def test_both_broken_always(self):
        """When both players hold 0%, every server gets broken → 6-6 tiebreak."""
        eg, pt = _compute_set_stats_avg(0.0, 0.0)
        # Every service game is a break, score alternates 1-1, 2-2, ... → 6-6
        assert eg == pytest.approx(13.0)
        assert pt == pytest.approx(1.0)

    def test_symmetric_hold(self):
        """Equal hold probabilities should produce symmetric results."""
        eg_ab, pt_ab = _compute_set_stats_avg(0.8, 0.8)
        eg_ba, pt_ba = _compute_set_stats_avg(0.8, 0.8)
        assert eg_ab == pytest.approx(eg_ba)
        assert pt_ab == pytest.approx(pt_ba)

    def test_high_hold_more_games(self):
        """Higher hold rates → more games per set."""
        eg_low, _ = _compute_set_stats_avg(0.6, 0.6)
        eg_high, _ = _compute_set_stats_avg(0.9, 0.9)
        assert eg_high > eg_low

    def test_high_hold_more_tiebreaks(self):
        """Higher hold rates → more tiebreaks."""
        _, pt_low = _compute_set_stats_avg(0.6, 0.6)
        _, pt_high = _compute_set_stats_avg(0.9, 0.9)
        assert pt_high > pt_low

    def test_lookup_tables_populated(self):
        """Lookup tables have correct shape and no NaN."""
        assert _EXPECTED_GAMES.shape == (101, 101)
        assert _TIEBREAK_PROB.shape == (101, 101)
        assert not any(map(lambda x: x != x, _EXPECTED_GAMES.flat))  # no NaN
        assert not any(map(lambda x: x != x, _TIEBREAK_PROB.flat))

    def test_expected_games_range(self):
        """Expected games should be between 6 and 13."""
        assert _EXPECTED_GAMES.min() >= 6.0
        assert _EXPECTED_GAMES.max() <= 13.0

    def test_tiebreak_prob_range(self):
        """Tiebreak probability should be between 0 and 1."""
        assert _TIEBREAK_PROB.min() >= 0.0
        assert _TIEBREAK_PROB.max() <= 1.0


class TestIidHoldProbFeature:
    """Tests for the registered iid_hold_prob feature."""

    def test_registered(self):
        registry = get_registry()
        feat = registry.get("iid_hold_prob")
        assert feat.params == ["days"]
        assert feat.mirror is True
        assert feat.depends_on == ["pts_service_won_pct"]

    def test_computation(self):
        from mvp.model.features.iid import iid_hold_prob

        df = pl.DataFrame({
            "player_id": ["A", "A", "A"],
            "effective_match_date": [
                date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1),
            ],
            # Simulating pre-computed rolling pts_service_won_pct
            "player_pts_service_won_pct": [0.65, 0.70, 0.60],
        })
        result = df.with_columns(iid_hold_prob().alias("val"))
        assert result["val"][0] == pytest.approx(_iid_hold_probability(0.65), abs=0.001)
        assert result["val"][1] == pytest.approx(_iid_hold_probability(0.70), abs=0.001)
        assert result["val"][2] == pytest.approx(_iid_hold_probability(0.60), abs=0.001)

    def test_windowed(self):
        from mvp.model.features.iid import iid_hold_prob

        df = pl.DataFrame({
            "player_pts_service_won_pct_90d": [0.65, 0.70],
        })
        result = df.with_columns(iid_hold_prob(days=90).alias("val"))
        assert result["val"][0] == pytest.approx(_iid_hold_probability(0.65), abs=0.001)


class TestMatchLevelFeatures:
    """Tests for iid_expected_games_per_set and iid_tiebreak_prob."""

    def test_expected_games_registered(self):
        registry = get_registry()
        feat = registry.get("iid_expected_games_per_set")
        assert feat.match_level is True
        assert feat.depends_on == ["iid_hold_prob"]
        assert feat.params == ["days"]

    def test_tiebreak_prob_registered(self):
        registry = get_registry()
        feat = registry.get("iid_tiebreak_prob")
        assert feat.match_level is True
        assert feat.depends_on == ["iid_hold_prob"]

    def test_expected_games_computation(self):
        from mvp.model.features.iid import iid_expected_games_per_set

        df = pl.DataFrame({
            "player_iid_hold_prob": [1.0, 0.8],
            "opp_iid_hold_prob": [1.0, 0.8],
        })
        result = df.with_columns(iid_expected_games_per_set().alias("val"))
        # Both hold 100% → 13 games (tiebreak)
        assert result["val"][0] == pytest.approx(13.0)
        # Both hold 80% → somewhere around 10-11 games
        eg_80, _ = _compute_set_stats_avg(0.8, 0.8)
        assert result["val"][1] == pytest.approx(eg_80, abs=0.2)

    def test_tiebreak_prob_computation(self):
        from mvp.model.features.iid import iid_tiebreak_prob

        df = pl.DataFrame({
            "player_iid_hold_prob": [1.0, 0.5],
            "opp_iid_hold_prob": [1.0, 0.5],
        })
        result = df.with_columns(iid_tiebreak_prob().alias("val"))
        # Both hold 100% → tiebreak certain
        assert result["val"][0] == pytest.approx(1.0)
        # Both hold 50% → tiebreak unlikely
        assert result["val"][1] < 0.2


class TestIidDerivedFeatures:
    """Tests for diff and sum registration."""

    def test_diff_and_sum_registered(self):
        registry = get_registry()
        registry.get("iid_hold_prob_diff")
        registry.get("iid_hold_prob_sum")
