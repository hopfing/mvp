"""Single source of truth for IID FS metric names, direction, and scorers.

Adding a metric is one entry in METRICS — config validation, FS dispatch,
direction handling, and the runner's aggregate math all derive from here.

Scope: covers the score-state serve FS path (ServeDiscoveryConfig) plus the
chain-calibration aggregate math shared with the projection runner and the
older matchup-serve discovery dispatch. Point metrics ("log_loss",
"brier_score", etc.) declare grain/direction only; they are scored by
``mvp.model.metrics.compute_metrics`` from a point classifier and have no
chain scorer here.
"""

from dataclasses import dataclass
from typing import Any, Callable, Literal

import numpy as np

# Primitives live in metrics.py (foundational module); the registry layers
# direction/grain/dispatch metadata on top and re-exports the helpers so
# consumers that want "all metric-stuff in one place" can import from here.
from mvp.projection.iid.metrics import (
    crps_discrete_pmf,
    spread_cal_errs,
    total_cal_errs,
)

Grain = Literal["point", "chain"]
Direction = Literal["minimize", "maximize"]
ChainScorer = Callable[..., float]


@dataclass(frozen=True)
class MetricSpec:
    name: str
    grain: Grain
    direction: Direction
    chain_scorer: ChainScorer | None = None  # required iff grain == "chain"


def _score_iid_crps_total_games(dist, y_a, y_b, **_):
    obs_total = (y_a + y_b).astype(np.int64)
    return crps_discrete_pmf(obs_total, dist.total_games_pmf)


def _score_iid_crps_spread(dist, y_a, y_b, **_):
    obs_spread = (y_a - y_b).astype(np.int64)
    obs_idx = obs_spread + dist.spread_offset
    obs_idx = np.clip(obs_idx, 0, dist.spread_pmf.shape[1] - 1)
    return crps_discrete_pmf(obs_idx, dist.spread_pmf)


def _score_iid_total_cal(dist, y_a, y_b, *, total_lines=None, **_):
    if not total_lines:
        raise ValueError("iid_total_cal requires non-empty total_lines")
    return float(sum(total_cal_errs(dist, y_a, y_b, total_lines)))


def _score_iid_total_cal_max(dist, y_a, y_b, *, total_lines=None, **_):
    if not total_lines:
        raise ValueError("iid_total_cal_max requires non-empty total_lines")
    return float(max(total_cal_errs(dist, y_a, y_b, total_lines)))


def _score_iid_spread_cal(dist, y_a, y_b, *, spread_lines=None, **_):
    if not spread_lines:
        raise ValueError("iid_spread_cal requires non-empty spread_lines")
    return float(sum(spread_cal_errs(dist, y_a, y_b, spread_lines)))


def _score_iid_spread_cal_max(dist, y_a, y_b, *, spread_lines=None, **_):
    if not spread_lines:
        raise ValueError("iid_spread_cal_max requires non-empty spread_lines")
    return float(max(spread_cal_errs(dist, y_a, y_b, spread_lines)))


def _score_mae(dist, y_a, y_b, **_):
    return float(np.mean(np.abs(y_a - dist.expected_games_a)))


def _score_rmse(dist, y_a, y_b, **_):
    return float(np.sqrt(np.mean((y_a - dist.expected_games_a) ** 2)))


METRICS: dict[str, MetricSpec] = {
    spec.name: spec for spec in [
        MetricSpec("log_loss",          "point", "minimize"),
        MetricSpec("brier_score",       "point", "minimize"),
        MetricSpec("roc_auc",           "point", "maximize"),
        MetricSpec("calibration_error", "point", "minimize"),
        MetricSpec("iid_crps_total_games", "chain", "minimize", _score_iid_crps_total_games),
        MetricSpec("iid_crps_spread",      "chain", "minimize", _score_iid_crps_spread),
        MetricSpec("iid_total_cal",        "chain", "minimize", _score_iid_total_cal),
        MetricSpec("iid_total_cal_max",    "chain", "minimize", _score_iid_total_cal_max),
        MetricSpec("iid_spread_cal",       "chain", "minimize", _score_iid_spread_cal),
        MetricSpec("iid_spread_cal_max",   "chain", "minimize", _score_iid_spread_cal_max),
        MetricSpec("mae",  "chain", "minimize", _score_mae),
        MetricSpec("rmse", "chain", "minimize", _score_rmse),
    ]
}


def is_chain_metric(name: str) -> bool:
    return METRICS[name].grain == "chain"


def is_point_metric(name: str) -> bool:
    return METRICS[name].grain == "point"


def is_minimize(name: str) -> bool:
    return METRICS[name].direction == "minimize"


def direction_of(name: str) -> Direction:
    return METRICS[name].direction


def chain_metric_names() -> set[str]:
    return {n for n, s in METRICS.items() if s.grain == "chain"}


def point_metric_names() -> set[str]:
    return {n for n, s in METRICS.items() if s.grain == "point"}


def worst_score(name: str) -> float:
    """Sentinel score that any real value beats under this metric's direction."""
    return float("inf") if is_minimize(name) else float("-inf")


def score_chain(
    name: str,
    dist: Any,
    y_games_a: np.ndarray,
    y_games_b: np.ndarray,
    *,
    total_lines: list[float] | None = None,
    spread_lines: list[float] | None = None,
) -> float:
    """Score a MatchDistribution against observed games for a chain-grain metric."""
    spec = METRICS.get(name)
    if spec is None or spec.chain_scorer is None:
        raise ValueError(f"Unknown chain metric: {name}")
    return spec.chain_scorer(
        dist, y_games_a, y_games_b,
        total_lines=total_lines, spread_lines=spread_lines,
    )


def validate_metric_name(name: str) -> str:
    """Pydantic-friendly validator: raise on unknown name, else return it."""
    if name not in METRICS:
        raise ValueError(
            f"Unknown metric '{name}'. Valid: {sorted(METRICS.keys())}"
        )
    return name
