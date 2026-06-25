"""Leakage-safe XGBoost early stopping primitives.

The watch-slice carving (with the test-adjacency embargo), the watch-size guard
(the tail-metric needs a non-trivial tail population), and the metric->feval
adapter (early-stop on the run's actual objective, not a logloss proxy).

Spec: mvp-docs/specs/2026-06-24-xgboost-early-stopping.md. The two-stage fit
(Stage 1 on train-minus-watch to find best_iteration, Stage 2 refit on full
train) and the runner/tuning wiring consume these primitives.
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

import numpy as np

from mvp.model.metrics import metric_direction

logger = logging.getLogger(__name__)


@dataclass
class EarlyStoppingConfig:
    """Config knobs (spec §7). Off by default — opt-in per run."""

    enabled: bool = False
    watch_months: float = 2.0   # V — watch-slice length
    gap_days: int = 7           # G — embargo between watch-end and test-start
    min_watch_tail: int = 100   # floor on the watch's tail population (§4)
    patience: int = 50          # early_stopping_rounds
    ceiling: int = 3000         # n_estimators ceiling when early stopping owns rounds
    fallback_rounds: int = 300  # fixed rounds when the watch is too small (§4 fallback)

    # The metric is a tail statistic at this upper fraction (beta_tail_score and
    # friends look at the worst-predicted tail); used to estimate the watch's
    # effective tail population for the guard.
    tail_fraction: float = 0.2


def carve_watch(
    dates: np.ndarray,
    test_start: date,
    watch_months: float,
    gap_days: int,
) -> np.ndarray:
    """Boolean mask over TRAIN rows selecting the early-stop watch set.

    Watch window = ``[test_start - G - V, test_start - G)``: the recent tail of
    train, held back by the embargo ``G`` days from the test boundary. The gap
    purges the player-trajectory serial correlation with the test window (a
    player in the watch tail reappearing in test would make the watch an
    optimistically-easy proxy — spec §1, MLE-1). The complement of this mask is
    ``train-minus-watch`` (Stage-1 train).

    ``dates`` is the per-train-row effective_match_date (anything numpy can read
    as ``datetime64[D]``); ``test_start`` is the fold's test boundary.
    """
    watch_start, watch_end = watch_bounds(test_start, watch_months, gap_days)
    dates = np.asarray(dates, dtype="datetime64[D]")
    ws = np.datetime64(watch_start, "D")
    we = np.datetime64(watch_end, "D")
    return (dates >= ws) & (dates < we)


def watch_bounds(
    test_start: date, watch_months: float, gap_days: int
) -> tuple[date, date]:
    """``(watch_start, watch_end)`` for the carve — exposed so the runner can log
    the per-fold watch date range (spec §4 requires the fallback rate / watch be
    visible). ``watch_months`` is converted as ``round(V*30.4)`` days; calendar-
    exact month boundaries are intentionally not used — the weekly tournament
    cycle, not the ±2-day month variance, is the binding constraint for the
    embargo, and a fixed day count keeps the window length stable across folds.
    """
    from datetime import timedelta

    watch_end = test_start - timedelta(days=int(gap_days))
    watch_start = watch_end - timedelta(days=int(round(watch_months * 30.4)))
    return watch_start, watch_end


def watch_tail_ok(
    n_watch: int, min_watch_tail: int, tail_fraction: float = 0.2
) -> bool:
    """Whether the watch set has enough TAIL population for a stable tail-metric.

    The metric (e.g. beta_tail_score) is computed on the worst-predicted
    ``tail_fraction`` of the watch; on a small watch that's a handful of obs and
    the metric is noise -> early stopping halts at a random round (spec §4,
    MLE-2). Guard on the estimated tail count, not the raw watch size. Callers
    fall back to fixed rounds (and log) when this is False.
    """
    return int(n_watch * tail_fraction) >= int(min_watch_tail)


def make_xgb_feval(
    metric: str, lambda_over: float | None = None,
) -> tuple[Callable[[np.ndarray, np.ndarray], float], bool]:
    """Adapt a prob-scoring metric to an XGBoost sklearn ``eval_metric`` callable.

    Returns ``(feval, lower_is_better)``. The callable has the XGBoost sklearn
    signature ``(y_true, y_pred) -> float`` and ALWAYS returns a lower-is-better
    value (maximize metrics are negated), so early stopping minimizes it
    regardless of direction-flag handling across XGBoost versions. Early stopping
    then optimizes the SAME objective the run selects on — no logloss proxy
    (spec §3).

    ``_make_metric_fn`` is imported inside the function: discovery imports this
    module (model layer), so a module-level import of discovery would be circular.
    """
    from mvp.model.discovery.fast_selection import _make_metric_fn

    metric_fn = _make_metric_fn(metric, lambda_over)
    maximize = metric_direction(metric) == "maximize"

    def feval(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        v = float(metric_fn(np.asarray(y_true), np.asarray(y_pred)))
        return -v if maximize else v

    # The 2nd element is DOCUMENTARY for the caller: XGBClassifier's custom
    # eval_metric callable takes no direction flag, and the negation above already
    # makes `feval` lower-is-better, so XGB minimizes it correctly. The wiring must
    # NOT also set maximize on the estimator — the negation does all the work.
    return feval, True


def make_xgb_feval_dtrain(
    metric: str, lambda_over: float | None = None,
) -> Callable[[np.ndarray, object], tuple[str, float]]:
    """The ``xgb.train`` ``custom_metric`` form of `make_xgb_feval`, for the
    `XGBoostMTLModel` path.

    Signature ``(predt, dtrain) -> (name, value)``. `predt` is the raw-margin
    matrix ``[n, num_target]``; the PRIMARY head (col 0, sigmoid'd) is the
    prediction and the primary label column the target — mirroring
    `_mtl_primary_logloss_eval`. `value` is always lower-is-better (maximize
    metrics negated); pair with ``maximize=False`` in ``xgb.train``.
    """
    from mvp.model.discovery.fast_selection import _make_metric_fn
    from mvp.model.models import _sigmoid

    metric_fn = _make_metric_fn(metric, lambda_over)
    maximize = metric_direction(metric) == "maximize"
    name = f"es_{metric}"

    def feval(predt: np.ndarray, dtrain: object) -> tuple[str, float]:
        predt = np.asarray(predt)
        label = dtrain.get_label()  # type: ignore[attr-defined]
        if predt.ndim == 2:
            p = _sigmoid(predt[:, 0])
            y = np.asarray(label).reshape(predt.shape)[:, 0]
        else:
            p = _sigmoid(predt)
            y = np.asarray(label)
        v = float(metric_fn(np.asarray(y), np.asarray(p)))
        return name, (-v if maximize else v)

    return feval


def two_stage_fit(
    model_factory: Callable[[int], Any],
    X: np.ndarray,
    y: np.ndarray,
    sample_weight: np.ndarray | None,
    dates: np.ndarray,
    test_start: date,
    cfg: EarlyStoppingConfig,
    metric: str,
    lambda_over: float | None = None,
    is_mtl: bool = False,
) -> tuple[Any, int | None]:
    """Leakage-safe two-stage early-stopping fit (spec §1-2). Returns
    ``(fitted_model, best_iteration)``.

    ``model_factory(n_rounds)`` builds a fresh, unfit model trained to exactly
    ``n_rounds`` boosting rounds. ``y`` / ``sample_weight`` are whatever the
    model's fit expects (1D for the single-task model, 2D ``y`` for MTL).
    ``dates`` are the per-row effective_match_date; ``test_start`` the fold's
    test boundary.

    Stage 1 fits on train-minus-watch with the watch as the early-stop monitor
    (on ``metric`` via the feval) to find ``best_iteration``; Stage 2 refits on
    the FULL train at that round count (so the watch data is in the deployed
    model). If the watch is too small for a stable tail metric, or no improving
    round is found, falls back to a single fixed-rounds fit — and logs it (§4).
    """
    watch = carve_watch(dates, test_start, cfg.watch_months, cfg.gap_days)
    n_watch = int(watch.sum())
    ws, we = watch_bounds(test_start, cfg.watch_months, cfg.gap_days)

    if not watch_tail_ok(n_watch, cfg.min_watch_tail, cfg.tail_fraction):
        logger.warning(
            "early-stop FALLBACK: watch %s..%s has %d rows (est. tail < floor %d) "
            "-> fixed %d rounds", ws, we, n_watch, cfg.min_watch_tail,
            cfg.fallback_rounds,
        )
        model = model_factory(cfg.fallback_rounds)
        model.fit(X, y, sample_weight=sample_weight)
        return model, None

    sub = ~watch
    feval = (
        make_xgb_feval_dtrain(metric, lambda_over) if is_mtl
        else make_xgb_feval(metric, lambda_over)[0]
    )
    w_sub = None if sample_weight is None else sample_weight[sub]
    m1 = model_factory(cfg.ceiling)
    m1.fit(
        X[sub], y[sub], sample_weight=w_sub,
        eval_set=[(X[watch], y[watch])],
        early_stopping_rounds=cfg.patience,
        eval_metric=feval,
    )
    best_it = m1.best_iteration

    if best_it is None or int(best_it) < 1:
        logger.warning(
            "early-stop FALLBACK: no improving round (best_iteration=%r) on watch "
            "%s..%s -> keeping the ceiling-trained Stage-1 model", best_it, ws, we,
        )
        return m1, (None if best_it is None else int(best_it))

    # Stage 2: refit on FULL train at best_iteration+1 rounds (best_iteration is
    # 0-indexed -> round count). The watch only set the count; the deployed model
    # trains on all data (spec §2, MLE-3: no scale-up heuristic).
    m2 = model_factory(int(best_it) + 1)
    # No eval_set here, intentionally and REQUIRED: it makes Stage 2 a plain
    # fixed-rounds fit. Passing one would reactivate the wrapper's default
    # eval_metric="logloss" as the stopping metric (MLE review finding 1).
    m2.fit(X, y, sample_weight=sample_weight)
    logger.info(
        "early-stop: best_iteration=%d (watch %s..%s, %d rows) -> refit full train",
        int(best_it), ws, we, n_watch,
    )
    return m2, int(best_it)
