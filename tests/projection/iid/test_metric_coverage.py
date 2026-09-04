"""Coverage added by plan 2026-09-04-serve-fs-metric-coverage.

Each test pins a property the plan exists for, not the mere presence of a
name: the reliability metrics have to catch what the cal metrics miss, the
rate AUC has to equal what a point-grain expansion would give, and the
full-range switch has to leave the classification path untouched.
"""

import numpy as np
import pytest
from sklearn.metrics import roc_auc_score

from mvp.model.metrics import METRIC_MIN_DELTA, compute_metrics
from mvp.projection.iid.metrics import (
    first_in_metrics,
    match_win_auc,
    set_count_cal,
    spread_cal_errs,
    spread_reliability,
    total_cal_errs,
    total_reliability,
)
from mvp.projection.iid.metric_registry import (
    METRICS,
    SERVE_METRIC_MIN_DELTA,
    _POINT_GRAIN_DELEGATED,
    default_serve_min_delta,
)


class TestFullRange:
    """D1: the p >= 0.50 mask de-duplicates mirrored rows, and branch rows
    have no mirror."""

    def test_default_is_unchanged_for_the_classification_path(self):
        rng = np.random.default_rng(0)
        p = rng.uniform(0.2, 0.95, 400)
        y = (rng.uniform(size=400) < p).astype(int)
        base = compute_metrics(y, p)
        again = compute_metrics(y, p, full_range=False)
        for k in base:
            assert base[k] == again[k], k

    def test_full_range_changes_a_population_centred_at_half(self):
        """win_second sits at 0.4968, so the default mask drops about half its
        rows. If this ever stops differing, the switch has stopped working."""
        rng = np.random.default_rng(1)
        p = rng.uniform(0.35, 0.65, 2000)          # a win_second-shaped spread
        y = (rng.uniform(size=2000) < p).astype(int)
        masked = compute_metrics(y, p)["calibration_error"]
        full = compute_metrics(y, p, full_range=True)["calibration_error"]
        assert masked != full

    def test_all_four_masked_outputs_respond(self):
        """compute_metrics calls four functions off _bucket_errors; a flag
        threaded to one would leave the other three truncated."""
        # The worst bucket must sit BELOW 0.5, or the two max-over-buckets
        # metrics are unchanged by construction and the test proves nothing.
        # 0.45 predicted against 0.05 actual is both the worst absolute gap
        # and the worst overconfident one, and the default mask never sees it.
        p = np.r_[np.full(200, 0.45), np.full(200, 0.60)]
        y = np.r_[np.r_[np.ones(10), np.zeros(190)],
                  np.r_[np.ones(130), np.zeros(70)]].astype(int)
        a = compute_metrics(y, p)
        b = compute_metrics(y, p, full_range=True)
        for k in ("calibration_error", "calibration_error_max",
                  "overconfidence_max", "signed_calibration"):
            assert a[k] != b[k], k


class TestFirstInMetrics:
    def _sample(self, seed, k=30):
        rng = np.random.default_rng(seed)
        n = rng.integers(40, 220, size=k).astype(float)
        p = np.round(rng.uniform(0.42, 0.82, size=k), 2)     # rounding => ties
        n_in = rng.binomial(n.astype(int), p)
        return p, n_in / n, n, n_in

    @pytest.mark.parametrize("seed", [0, 1, 2, 3])
    def test_rate_auc_equals_the_point_grain_expansion(self, seed):
        """B3's whole claim: the aggregate weighted AUC is the AUC you would
        get by expanding each row into its individual serve outcomes."""
        p, y, n, n_in = self._sample(seed)
        got = first_in_metrics(p, y, n)["rate_roc_auc"]
        yy = np.concatenate(
            [np.r_[np.ones(int(i)), np.zeros(int(t - i))] for i, t in zip(n_in, n)]
        )
        pp = np.concatenate([np.full(int(t), q) for q, t in zip(p, n)])
        assert got == pytest.approx(roc_auc_score(yy, pp), abs=1e-12)

    def test_base_rate_only_candidate_scores_half(self):
        """Ties must group by DISTINCT p across the frame. A round-1 candidate
        that fits the base rate gives every row the same p; a per-row tie rule
        would score that as skill."""
        n = np.full(40, 90.0)
        p = np.full(40, 0.617)
        rng = np.random.default_rng(5)
        y = rng.binomial(n.astype(int), 0.617) / n
        assert first_in_metrics(p, y, n)["rate_roc_auc"] == pytest.approx(0.5)

    def test_signed_bias_and_wmse_split_the_same_residual(self):
        p, y, n, _ = self._sample(7)
        m = first_in_metrics(p, y, n)
        w = n / n.sum()
        assert m["rate_signed_bias"] == pytest.approx(float(np.sum(w * (p - y))))
        assert m["rate_wmse"] == pytest.approx(float(np.sum(w * (p - y) ** 2)))

    def test_bias_is_directional_where_wmse_is_not(self):
        """Two candidates with identical wMSE and opposite bias are the case
        wMSE alone cannot tell apart."""
        n = np.full(50, 100.0)
        y = np.full(50, 0.62)
        hi = first_in_metrics(y + 0.05, y, n)
        lo = first_in_metrics(y - 0.05, y, n)
        assert hi["rate_wmse"] == pytest.approx(lo["rate_wmse"])
        assert hi["rate_signed_bias"] == pytest.approx(-lo["rate_signed_bias"])
        assert hi["rate_signed_bias"] > 0 > lo["rate_signed_bias"]

    def test_sparse_buckets_are_dropped_and_counted(self):
        """B2's floor: a bucket with a handful of rows has an unbiased but
        very noisy gap. Dropping it trades coverage for a steadier number —
        it is NOT protection from a lone row dominating, which the
        summed-n cross-bucket weighting already prevents."""
        n = np.full(60, 100.0)
        p = np.r_[np.full(59, 0.62), [0.05]]       # 1 row alone in its bucket
        y = np.r_[np.full(59, 0.62), [0.95]]       # and wildly off
        m = first_in_metrics(p, y, n, min_bucket=20)
        assert m["rate_calibration_dropped"] == 1.0
        assert m["rate_calibration_error"] == pytest.approx(0.0)
        # Without the floor the lone row contributes, but only in proportion
        # to its sample: buckets combine weighted by summed n, so one row in
        # sixty moves the number by ~1.7% of its 0.90 gap, not by 0.90. The
        # floor is variance hygiene, not protection from domination.
        loose = first_in_metrics(p, y, n, min_bucket=1)
        assert loose["rate_calibration_error"] == pytest.approx(0.90 / 60, rel=1e-9)

    def test_empty_input_returns_empty(self):
        assert first_in_metrics(np.array([]), np.array([]), np.array([])) == {}


class _FakeDist:
    """Minimal stand-in exposing only what the line metrics read."""

    def __init__(self, p_over, p_cover=None):
        self._p_over = np.asarray(p_over, dtype=np.float64)
        self._p_cover = np.asarray(
            p_cover if p_cover is not None else p_over, dtype=np.float64
        )

    def p_over_total(self, line):
        return self._p_over

    def p_a_spread_cover(self, line):
        return self._p_cover


class TestReliability:
    def test_reliability_catches_offsetting_error_that_cal_misses(self):
        """C2's reason for existing. Half the population is predicted 0.30 and
        realises 0.60; the other half is predicted 0.70 and realises 0.40. The
        net bias is zero and `total_cal_errs` reports ~0, while every single
        match is badly miscalibrated."""
        n = 400
        p = np.r_[np.full(n, 0.30), np.full(n, 0.70)]
        # observed totals chosen so actual_over matches the intended rates
        actual = np.r_[
            (np.arange(n) < int(0.60 * n)).astype(float),
            (np.arange(n) < int(0.40 * n)).astype(float),
        ]
        games_a = np.where(actual > 0, 20.0, 10.0)
        games_b = np.zeros_like(games_a)
        dist = _FakeDist(p)
        line = 15.0
        net = total_cal_errs(dist, games_a, games_b, [line])[0]
        binned = total_reliability(dist, games_a, games_b, [line])
        assert net == pytest.approx(0.0, abs=1e-9)
        assert binned > 0.25

    def test_spread_reliability_mirrors_it(self):
        n = 300
        p = np.r_[np.full(n, 0.25), np.full(n, 0.75)]
        actual = np.r_[
            (np.arange(n) < int(0.75 * n)).astype(float),
            (np.arange(n) < int(0.25 * n)).astype(float),
        ]
        games_a = np.where(actual > 0, 8.0, 0.0)
        games_b = np.zeros_like(games_a)
        dist = _FakeDist(p, p)
        assert spread_cal_errs(dist, games_a, games_b, [4.0])[0] == pytest.approx(
            0.0, abs=1e-9
        )
        assert spread_reliability(dist, games_a, games_b, [4.0]) > 0.4


class TestMatchWinAuc:
    def test_matches_sklearn(self):
        rng = np.random.default_rng(3)
        p = rng.uniform(0, 1, 500)
        y = (rng.uniform(size=500) < p).astype(int)
        assert match_win_auc(p, y) == pytest.approx(roc_auc_score(y, p))

    def test_single_class_returns_half_rather_than_raising(self):
        assert match_win_auc(np.array([0.4, 0.6]), np.array([1, 1])) == 0.5


class TestMinDeltaTable:
    def test_every_metric_is_covered_exactly_once(self):
        assert set(SERVE_METRIC_MIN_DELTA) | _POINT_GRAIN_DELEGATED == set(METRICS)
        assert not (set(SERVE_METRIC_MIN_DELTA) & _POINT_GRAIN_DELEGATED)

    def test_delegated_names_resolve_to_the_classification_table(self):
        for n in sorted(_POINT_GRAIN_DELEGATED):
            assert default_serve_min_delta(n) == METRIC_MIN_DELTA[n]

    def test_unmapped_name_names_the_serve_table_not_the_other_one(self):
        """An absence-triggered fallback would raise from `default_min_delta`
        and send the reader to METRIC_MIN_DELTA in the wrong module."""
        with pytest.raises(ValueError, match="SERVE_METRIC_MIN_DELTA"):
            default_serve_min_delta("branch_not_a_metric")

    def test_thresholds_track_metric_magnitude(self):
        """The point of the table: CRPS near 3.4 cannot share a threshold with
        a squared residual on a rate."""
        assert (
            SERVE_METRIC_MIN_DELTA["branch_rate_wmse"]
            < SERVE_METRIC_MIN_DELTA["iid_match_win_log_loss"]
            < SERVE_METRIC_MIN_DELTA["iid_crps_total_games"]
        )


class TestSetCountCal:
    class _D:
        def __init__(self, straight):
            self.set_outcome_probs = {(2, 0): np.asarray(straight)}

    def _df(self, sets_played):
        """One row per match, `sets_played` sets recorded on each."""
        import polars as pl

        cols = {}
        for i in range(1, 6):
            cols[f"player_set{i}_games"] = [
                6.0 if i <= k else None for k in sets_played
            ]
        return pl.DataFrame(cols)

    def test_perfect_prediction_scores_zero(self):
        n = 20
        df = self._df([2] * n)
        out = set_count_cal(self._D(np.ones(n)), np.full(n, 3.0), df)
        assert out["iid_set_count_cal"] == pytest.approx(0.0)
        assert out["set_count_straight_actual_bo3"] == pytest.approx(1.0)

    def test_miscalibration_is_measured_and_signed(self):
        """abs() hides the direction in the objective, so the signed key has
        to carry it — a sign flip in the residual would otherwise be
        invisible."""
        n = 100
        df = self._df([2] * 30 + [3] * 70)      # 30% straight in reality
        out = set_count_cal(self._D(np.full(n, 0.80)), np.full(n, 3.0), df)
        assert out["set_count_straight_actual_bo3"] == pytest.approx(0.30)
        assert out["set_count_straight_pred_bo3"] == pytest.approx(0.80)
        assert out["set_count_straight_bias_bo3"] == pytest.approx(0.50)
        assert out["iid_set_count_cal"] == pytest.approx(0.50)
        # under-prediction gives the opposite sign, same objective value
        under = set_count_cal(self._D(np.zeros(n)), np.full(n, 3.0), df)
        assert under["set_count_straight_bias_bo3"] == pytest.approx(-0.30)
        assert under["iid_set_count_cal"] == pytest.approx(0.30)

    def test_bo3_and_bo5_are_scored_separately(self):
        """Their supports differ (2-or-3 sets against 3-4-5), so pooling would
        compare distributions over different outcome spaces. The objective is
        the mean of the per-support errors."""
        n = 100
        best_of = np.r_[np.full(50, 3.0), np.full(50, 5.0)]
        # bo3 rows: all straight (2 sets). bo5 rows: none straight (4 sets).
        df = self._df([2] * 50 + [4] * 50)
        out = set_count_cal(self._D(np.full(n, 0.60)), best_of, df)
        assert out["set_count_straight_actual_bo3"] == pytest.approx(1.0)
        assert out["set_count_straight_actual_bo5"] == pytest.approx(0.0)
        assert out["set_count_straight_bias_bo3"] == pytest.approx(-0.40)
        assert out["set_count_straight_bias_bo5"] == pytest.approx(0.60)
        # mean of |−0.40| and |+0.60|, not the pooled 0.10 they would cancel to
        assert out["iid_set_count_cal"] == pytest.approx(0.50)

    def test_absent_support_is_omitted_rather_than_zero(self):
        n = 10
        df = self._df([2] * n)
        out = set_count_cal(self._D(np.ones(n)), np.full(n, 3.0), df)
        assert "set_count_straight_actual_bo5" not in out
