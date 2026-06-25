"""Tests for the early-stopping primitives (watch carve + embargo, guard, feval)."""

from datetime import date

import numpy as np

from mvp.model.early_stopping import (
    carve_watch,
    make_xgb_feval,
    make_xgb_feval_dtrain,
    watch_bounds,
    watch_tail_ok,
)


class _MockDMatrix:
    def __init__(self, labels):
        self._labels = np.asarray(labels, dtype=np.float64)

    def get_label(self):
        return self._labels


class TestCarveWatch:
    def test_embargo_excludes_the_gap(self):
        # test_start 2025-06-01, G=7 -> the week before test is embargoed out.
        ts = date(2025, 6, 1)
        dates = np.array([
            np.datetime64(date(2025, 5, 30)),  # in the 7d gap -> EXCLUDED
            np.datetime64(date(2025, 5, 15)),  # in the watch window -> included
            np.datetime64(date(2025, 1, 1)),   # before the watch -> EXCLUDED
        ], dtype="datetime64[D]")
        mask = carve_watch(dates, ts, watch_months=2.0, gap_days=7)
        assert mask.tolist() == [False, True, False]

    def test_watch_window_bounds_half_open(self):
        # watch = [test_start - G - V, test_start - G); V=2mo~=61d, G=7d.
        ts = date(2025, 6, 1)
        watch_end = date(2025, 5, 25)    # ts - 7d
        watch_start = date(2025, 3, 25)  # watch_end - 61d
        dates = np.array([
            np.datetime64(watch_start),         # >= start  -> in
            np.datetime64(date(2025, 5, 24)),   # < end     -> in
            np.datetime64(watch_end),           # == end    -> OUT (half-open)
            np.datetime64(date(2025, 3, 24)),   # < start   -> OUT
        ], dtype="datetime64[D]")
        assert carve_watch(dates, ts, 2.0, 7).tolist() == [True, True, False, False]

    def test_test_period_rows_never_in_watch(self):
        ts = date(2025, 6, 1)
        dates = np.array([
            np.datetime64(date(2025, 6, 1)), np.datetime64(date(2025, 6, 20)),
        ], dtype="datetime64[D]")
        assert not carve_watch(dates, ts, 2.0, 7).any()


class TestWatchTailOk:
    def test_floor_on_estimated_tail(self):
        # tail ~= 20% of watch; floor 100 -> need >= 500 watch rows.
        assert watch_tail_ok(500, 100, 0.2)
        assert not watch_tail_ok(499, 100, 0.2)
        assert not watch_tail_ok(100, 100, 0.2)


class TestFeval:
    def test_minimize_metric_lower_for_better_preds(self):
        feval, lower_better = make_xgb_feval("log_loss")
        assert lower_better is True
        y = np.array([1, 0, 1, 0])
        good = np.array([0.9, 0.1, 0.85, 0.15])
        bad = np.array([0.45, 0.55, 0.4, 0.6])
        assert feval(y, good) < feval(y, bad)

    def test_maximize_metric_is_negated_to_lower_better(self):
        # accuracy is a MAXIMIZE metric -> feval negates it so lower is better.
        feval, lower_better = make_xgb_feval("accuracy")
        assert lower_better is True
        y = np.array([1, 0, 1, 0])
        good = np.array([0.9, 0.1, 0.85, 0.15])  # all correct
        bad = np.array([0.4, 0.6, 0.45, 0.55])   # all wrong
        assert feval(y, good) < feval(y, bad)
        assert feval(y, good) < 0  # -accuracy(good) is negative


class TestWatchBounds:
    def test_bounds_match_carve(self):
        ws, we = watch_bounds(date(2025, 6, 1), 2.0, 7)
        assert we == date(2025, 5, 25)   # test_start - 7d
        assert ws == date(2025, 3, 25)   # watch_end - 61d


class TestFevalDtrain:
    def test_mtl_form_lower_for_better_preds(self):
        feval = make_xgb_feval_dtrain("log_loss")
        y = _MockDMatrix(np.array([1, 0, 1, 0]))
        good_predt = np.array([[3.0], [-3.0], [2.5], [-2.5]])  # confident + correct
        flat_predt = np.array([[0.0], [0.0], [0.0], [0.0]])    # all p=0.5
        name, good_val = feval(good_predt, y)
        _, bad_val = feval(flat_predt, y)
        assert name.startswith("es_")
        assert good_val < bad_val
