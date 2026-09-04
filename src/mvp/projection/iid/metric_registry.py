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
    match_win_log_loss,
    spread_cal_errs,
    total_cal_errs,
)

Grain = Literal["point", "chain", "branch"]
Direction = Literal["minimize", "maximize"]
ChainScorer = Callable[..., float]


@dataclass(frozen=True)
class MetricSpec:
    name: str
    grain: Grain
    direction: Direction
    chain_scorer: ChainScorer | None = None  # required iff grain == "chain"
    # grain == "branch" only: the key `mvp.model.metrics.compute_metrics`
    # returns for this metric. Carried rather than derived by stripping the
    # `branch_` prefix at the call site, so the mapping has one home.
    # `None` for the first_in rate metric, which compute_metrics cannot score.
    base_metric: str | None = None


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


def _score_iid_match_win_log_loss(dist, y_a, y_b, *, y_won=None, **_):
    if y_won is None:
        raise ValueError(
            "iid_match_win_log_loss requires y_won -- the match winner cannot "
            "be derived from game counts (winners can lose the games count)"
        )
    return match_win_log_loss(dist.p_match_win_a, y_won)


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
        # Match-win through the chain. Distinct from point-grain "log_loss"
        # (the serve model's point-level loss): this scores dist.p_match_win_a
        # against the actual winner, so serve FS can select FOR match-win.
        MetricSpec(
            "iid_match_win_log_loss", "chain", "minimize",
            _score_iid_match_win_log_loss,
        ),
        MetricSpec("mae",  "chain", "minimize", _score_mae),
        MetricSpec("rmse", "chain", "minimize", _score_rmse),
        # Per-branch selection (plan: 2026-09-03-per-branch-selection-metrics).
        # A `serve_component` run scores the branch it is selecting for on that
        # branch's OWN target instead of the composed chain: win_first on
        # `point_won_by_server` over `serve == 1` rows, win_second over
        # `serve == 2`, first_in on the first-serve-in rate. The chain metrics
        # above are untouched and remain the default route; `metric:` picks.
        MetricSpec("branch_log_loss", "branch", "minimize",
                   base_metric="log_loss"),
        MetricSpec("branch_brier", "branch", "minimize",
                   base_metric="brier_score"),
        MetricSpec("branch_roc_auc", "branch", "maximize",
                   base_metric="roc_auc"),
        MetricSpec("branch_calibration_error", "branch", "minimize",
                   base_metric="calibration_error"),
        # first_in only: weighted MSE on the (match, server) first-serve rate,
        # weights = service points played. NOT a point-grain Brier on the
        # broadcast rate -- the two rank identically
        # (Brier = wMSE + sum n_g r_g(1-r_g)/N, the second term irreducible and
        # model-independent) but the broadcast's dominant constant would leave
        # the entire model-dependent range in the fourth decimal, and min_delta
        # is an absolute threshold.
        MetricSpec("branch_rate_wmse", "branch", "minimize"),
    ]
}


def is_chain_metric(name: str) -> bool:
    return METRICS[name].grain == "chain"


def is_point_metric(name: str) -> bool:
    return METRICS[name].grain == "point"


def is_branch_metric(name: str) -> bool:
    """Scored on the selected serve component's own target, not the chain."""
    return METRICS[name].grain == "branch"


def grain_of(name: str) -> Grain:
    return METRICS[name].grain


def base_metric_of(name: str) -> str | None:
    """The `compute_metrics` key a branch metric scores, if any."""
    return METRICS[name].base_metric


def needs_match_grain_prep(name: str) -> bool:
    """Does this metric need the match-grain frame, folds and two-sided
    feature frame that `_prepare_match_data` builds?

    True for chain metrics (they run the chain over match-grain test rows) AND
    for branch metrics (their branches read match-level features and their
    held-out rows come from the same match-grain folds). False for point
    metrics, which score the point matrix directly. Distinct from
    `is_chain_metric`, which answers "does scoring go through the chain".
    """
    return METRICS[name].grain in ("chain", "branch")


def branch_rate_metrics() -> set[str]:
    return {n for n, s in METRICS.items() if s.grain == "branch" and s.base_metric is None}


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
    y_won: np.ndarray | None = None,
) -> float:
    """Score a MatchDistribution against observed outcomes for a chain-grain
    metric. `y_won` is required by the match-win metric only; the games/lines
    scorers absorb it via their kwargs catch-all."""
    spec = METRICS.get(name)
    if spec is None or spec.chain_scorer is None:
        raise ValueError(f"Unknown chain metric: {name}")
    return spec.chain_scorer(
        dist, y_games_a, y_games_b,
        total_lines=total_lines, spread_lines=spread_lines, y_won=y_won,
    )


def validate_metric_name(name: str) -> str:
    """Pydantic-friendly validator: raise on unknown name, else return it."""
    if name not in METRICS:
        raise ValueError(
            f"Unknown metric '{name}'. Valid: {sorted(METRICS.keys())}"
        )
    return name
