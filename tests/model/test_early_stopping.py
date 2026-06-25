"""Tests for the early-stopping primitives (watch carve + embargo, guard, feval)."""

from datetime import date

import numpy as np

from mvp.model.early_stopping import (
    EarlyStoppingConfig,
    carve_watch,
    make_xgb_feval,
    make_xgb_feval_dtrain,
    two_stage_fit,
    watch_bounds,
    watch_tail_ok,
)


class _MockDMatrix:
    def __init__(self, labels):
        self._labels = np.asarray(labels, dtype=np.float64)

    def get_label(self):
        return self._labels


class _MockESModel:
    """Records the round count it was built with + whether it got an eval_set;
    simulates early stopping by reporting best_iteration when monitored."""

    def __init__(self, n_rounds):
        self.n_rounds = n_rounds
        self.got_eval_set = False
        self.best_iteration = None

    def fit(self, X, y, sample_weight=None, eval_set=None,
            early_stopping_rounds=None, eval_metric=None):
        self.got_eval_set = eval_set is not None
        if eval_set is not None:
            self.best_iteration = 42


def _dates_with_watch(n_before, n_watch, test_start):
    ws, we = watch_bounds(test_start, 2.0, 7)
    before = [np.datetime64(ws) - np.timedelta64(i + 1, "D") for i in range(n_before)]
    inwatch = [np.datetime64(ws) + np.timedelta64(i % 30, "D") for i in range(n_watch)]
    return np.array(before + inwatch, dtype="datetime64[D]")


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


class TestTwoStageFit:
    def test_happy_path_refits_full_train_at_best_iteration(self):
        ts = date(2025, 7, 1)
        dates = _dates_with_watch(400, 600, ts)   # 600 watch rows -> guard passes
        X = np.zeros((1000, 2))
        y = np.zeros(1000, dtype=int)
        built: list[_MockESModel] = []

        def factory(n):
            m = _MockESModel(n)
            built.append(m)
            return m

        model, best = two_stage_fit(
            factory, X, y, None, dates, ts, EarlyStoppingConfig(), "log_loss")

        assert best == 42
        assert built[0].n_rounds == 3000 and built[0].got_eval_set      # Stage 1: ceiling + watch
        assert built[1].n_rounds == 43 and not built[1].got_eval_set    # Stage 2: best+1, full train
        assert model is built[1]

    def test_small_watch_falls_back_to_fixed_rounds(self):
        ts = date(2025, 7, 1)
        dates = _dates_with_watch(400, 50, ts)    # 50 watch rows -> guard fails
        X = np.zeros((450, 2))
        y = np.zeros(450, dtype=int)
        built: list[_MockESModel] = []

        def factory(n):
            m = _MockESModel(n)
            built.append(m)
            return m

        model, best = two_stage_fit(
            factory, X, y, None, dates, ts, EarlyStoppingConfig(), "log_loss")

        assert best is None
        assert len(built) == 1
        assert built[0].n_rounds == 300 and not built[0].got_eval_set   # fixed-rounds fallback
        assert model is built[0]
