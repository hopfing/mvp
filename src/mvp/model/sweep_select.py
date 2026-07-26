"""Pick a set of tuning trials to evaluate downstream.

Lifted from scripts/frozen_backtest_sweep.py so the classification script and
`iid-sweep` share one implementation.

The default is `select_diverse`, deliberately. Tune leaderboards are usually a
plateau rather than a peak — many trials tie on the objective while behaving
differently downstream — so spreading over the hyperparameter space finds more
distinct model behavior than taking the top N of a metric that can't separate
them. `select_top` is there for when you do want the leaderboard's own ordering.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
from optuna.distributions import CategoricalDistribution

from mvp.model.tuning import _MAXIMIZE_METRICS


def norm_value(dist: Any, v: Any) -> float:
    """Sampled value -> [0,1] by its search distribution (log-aware)."""
    if isinstance(dist, CategoricalDistribution):
        k = len(dist.choices)
        return dist.choices.index(v) / (k - 1) if k > 1 else 0.0
    low, high = dist.low, dist.high
    if getattr(dist, "log", False):
        if v <= 0 or low <= 0:
            return 0.0
        return (math.log(v) - math.log(low)) / (math.log(high) - math.log(low))
    if high == low:
        return 0.0
    return (v - low) / (high - low)


def select_diverse(trials: list, n: int) -> list:
    """Maximin (farthest-point) selection over the trials' hyperparameters.

    Deterministic: seed = trial nearest the centroid, farthest-point thereafter.
    Metric-agnostic by design — it answers "which N trials are most unlike each
    other", not "which N scored best".
    """
    if n >= len(trials):
        return list(trials)
    shared = set(trials[0].distributions)
    for t in trials[1:]:
        shared &= set(t.distributions)
    cols = sorted(shared)
    if not cols:
        return list(trials[:n])
    dist_of = {c: trials[0].distributions[c] for c in cols}
    M = np.array([[norm_value(dist_of[c], t.params[c]) for c in cols] for t in trials])
    seed = int(np.argmin(((M - M.mean(axis=0)) ** 2).sum(axis=1)))
    selected = [seed]
    mind = np.sqrt(((M - M[seed]) ** 2).sum(axis=1))
    while len(selected) < n:
        mind[selected] = -np.inf
        pick = int(np.argmax(mind))
        selected.append(pick)
        mind = np.minimum(mind, np.sqrt(((M - M[pick]) ** 2).sum(axis=1)))
    return [trials[i] for i in selected]


def select_top(trials: list, metric: str, n: int) -> list:
    """Top-N by `metric`, read from each trial's user_attrs.

    `metric` is used verbatim — pass `holdout_<m>` for a classification study,
    a bare name for IID (which has no holdout block).
    """
    maximize = _resolve_direction(metric)
    worst = float("-inf") if maximize else float("inf")

    def key(t):
        v = t.user_attrs.get(metric, worst)
        return -v if maximize else v

    return sorted(trials, key=key)[:n]


def _resolve_direction(metric: str) -> bool:
    """True if higher is better, looking through any holdout_ prefix."""
    bare = metric
    for prefix in ("holdout_cal_", "holdout_"):
        if bare.startswith(prefix):
            bare = bare[len(prefix):]
            break
    return bare in _MAXIMIZE_METRICS


def missing_metric_trials(trials: list, metric: str) -> int:
    """How many trials lack `metric` — a top-N sort over these is arbitrary.

    Worth surfacing rather than silently ranking every trial at the sentinel.
    """
    return sum(1 for t in trials if metric not in t.user_attrs)
