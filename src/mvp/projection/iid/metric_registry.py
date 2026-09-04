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
from mvp.model.metrics import METRIC_MIN_DELTA as _CLASSIFICATION_MIN_DELTA
from mvp.model.metrics import default_min_delta as _classification_default_min_delta
from mvp.projection.iid.metrics import (
    crps_discrete_pmf,
    match_win_auc,
    match_win_log_loss,
    set_count_cal,
    spread_cal_errs,
    spread_reliability,
    total_cal_errs,
    total_reliability,
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


def _score_iid_match_win_auc(dist, y_a, y_b, *, y_won=None, **_):
    if y_won is None:
        raise ValueError("iid_match_win_auc requires y_won")
    return match_win_auc(dist.p_match_win_a, y_won)


def _score_iid_total_reliability(dist, y_a, y_b, *, total_lines=None, **_):
    if not total_lines:
        raise ValueError("iid_total_reliability requires non-empty total_lines")
    return total_reliability(dist, y_a, y_b, total_lines)


def _score_iid_spread_reliability(dist, y_a, y_b, *, spread_lines=None, **_):
    if not spread_lines:
        raise ValueError("iid_spread_reliability requires non-empty spread_lines")
    return spread_reliability(dist, y_a, y_b, spread_lines)


def _score_iid_set_count_cal(dist, y_a, y_b, *, test_df=None, **_):
    if test_df is None:
        raise ValueError(
            "iid_set_count_cal requires test_df -- sets played cannot be "
            "derived from game counts"
        )
    best_of = test_df["best_of"].to_numpy()
    out = set_count_cal(dist, best_of, test_df)
    if "iid_set_count_cal" not in out:
        raise ValueError("iid_set_count_cal: no bo3 or bo5 rows to score")
    return out["iid_set_count_cal"]


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
        MetricSpec("iid_match_win_auc", "chain", "maximize",
                   _score_iid_match_win_auc),
        # Binned counterparts to iid_total_cal / iid_spread_cal, which reduce
        # each line to a NET bias and so cancel offsetting error. Both kept:
        # net bias and reliability are different questions.
        MetricSpec("iid_total_reliability", "chain", "minimize",
                   _score_iid_total_reliability),
        MetricSpec("iid_spread_reliability", "chain", "minimize",
                   _score_iid_spread_reliability),
        # Needs test_df: sets played is not recoverable from game counts.
        # Its sibling `iid_tiebreak_win_cal` is a runner diagnostic only --
        # scoring it needs `t_ab`, which the FS path never computes (it builds
        # a MatchDistribution, and t_ab lives on ProjectionOutput).
        MetricSpec("iid_set_count_cal", "chain", "minimize",
                   _score_iid_set_count_cal),
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
        # first_in, splitting what wMSE bundles. base_metric stays None:
        # compute_metrics cannot score a rate target, `first_in_metrics` does.
        MetricSpec("branch_rate_calibration_error", "branch", "minimize"),
        MetricSpec("branch_rate_roc_auc", "branch", "maximize"),
        MetricSpec("branch_rate_log_loss", "branch", "minimize"),
    ]
}


# Scale-appropriate min_delta per metric. `min_delta` is an ABSOLUTE
# improvement threshold, so one flat value makes a run halt earlier or later
# purely as a function of which `metric:` a config names -- CRPS runs near 3.4
# while `branch_rate_wmse` is a squared residual on a [0, 1] rate.
#
# Same rule as the classification side's METRIC_MIN_DELTA: anchored on
# log_loss = 1e-4 at its ~0.60 scale (ratio ~1.7e-4 per unit of metric),
# scaled by each metric's typical magnitude. The `~` comment on each entry is
# that magnitude. "observed" means read off a real run; "unobserved" means
# derived from the metric's definition and range, and worth a sanity check the
# first time it is actually optimized.
#
# The four POINT-grain names are deliberately absent -- see
# _POINT_GRAIN_DELEGATED below.
SERVE_METRIC_MIN_DELTA: dict[str, float] = {
    # --- chain, observed. Thresholds are magnitude x 1.7e-4 rounded to two
    #     significant figures. The scheme is a coarse scaling, not a precise
    #     value: its job is to stop CRPS at ~3.5 sharing a threshold with a
    #     squared residual at ~0.0035, and a 15% error in a magnitude will not
    #     change which candidate a run accepts. Source is the 6-fold
    #     2022-range srv_two_level_flat_mlog_t51 projection
    #     (B:/projection_evaluations/bb4e5f9534a2). ------------------------
    "iid_match_win_log_loss":     1e-4,   # ~0.60   observed
    "iid_match_win_auc":          1.3e-4, # ~0.74   observed (0.7385)
    "iid_crps_total_games":       5.9e-4, # ~3.46   observed
    "iid_crps_spread":            4.6e-4, # ~2.72   observed
    "mae":                        5.1e-4, # ~2.98   observed
    "rmse":                       6.6e-4, # ~3.88   observed
    "iid_total_cal":              1.8e-4, # ~1.08   observed (sum over 8 lines)
    "iid_total_cal_max":          2.5e-5, # ~0.15   observed
    "iid_spread_cal":             9.6e-5, # ~0.57   observed (sum over 10 lines)
    "iid_spread_cal_max":         1.3e-5, # ~0.08   observed
    # --- chain, unobserved -------------------------------------------------
    "iid_total_reliability":      2.0e-5, # ~0.12   unobserved: mean per-line
                                          #         binned |gap|, so it sits
                                          #         near cal_max, not cal
    "iid_spread_reliability":     1.0e-5, # ~0.06   unobserved, same shape
    "iid_set_count_cal":          1.0e-5, # ~0.05   unobserved: |pred-actual|
                                          #         on a probability
    # --- branch, all unobserved: no archived serve run has ever selected on
    #     a branch metric, so none of these has a measured magnitude --------
    "branch_log_loss":            1e-4,   # ~0.60   unobserved
    "branch_brier":               3.5e-5, # ~0.21   unobserved
    "branch_roc_auc":             1e-4,   # ~0.60   unobserved
    "branch_calibration_error":   5.0e-6, # ~0.03   unobserved
    "branch_rate_wmse":           6.0e-7, # ~0.0035 unobserved: squared
                                          #         residual on a rate, so
                                          #         orders below the rest
    "branch_rate_calibration_error": 5.0e-6,  # ~0.03  unobserved
    "branch_rate_roc_auc":        1e-4,   # ~0.60   unobserved
    "branch_rate_log_loss":       1.1e-4, # ~0.66   unobserved: Bernoulli
                                          #         entropy at the 0.6163
                                          #         first-in base rate
}

# These four carry the bare classification names and are scored by
# `mvp.model.metrics.compute_metrics` on the serve model's point rows. Rather
# than restate values, the resolver delegates them to METRIC_MIN_DELTA.
#
# The delegation is by NAME IDENTITY, never "absent here, so fall back":
# `default_min_delta` raises for an unmapped name with a message naming
# METRIC_MIN_DELTA in mvp/model/metrics.py, so an absence-triggered fallback
# would send a genuinely missing SERVE entry to the wrong table in the wrong
# module.
#
# Borrowing those anchors is an argument, not a measurement. The point
# population's base rate is 0.6163 -- (0.6901*4,878,376 + 0.4968*3,015,479) /
# 7,893,855, essentially the 0.616579 blend already cited in
# two_level_serve_model.py -- so it sits near the classification log_loss
# anchor's own ~0.60 scale. No archived run has ever fitted a point-grain
# serve model on one of these, so treat it as unobserved. Exposure is narrow:
# log_loss and roc_auc both resolve to 1e-4, identical to the old flat
# default, so delegating those changes nothing. brier_score (3e-5) and
# calibration_error (5e-6) are where the assumption has teeth.
_POINT_GRAIN_DELEGATED = frozenset(
    {"log_loss", "brier_score", "roc_auc", "calibration_error"}
)

# Two assertions, not one. The second alone would stop catching a rename or
# removal on the classification side, which is exactly what delegation makes
# possible.
assert _POINT_GRAIN_DELEGATED <= set(_CLASSIFICATION_MIN_DELTA), (
    "point-grain names delegated to METRIC_MIN_DELTA are missing from it: "
    f"{sorted(_POINT_GRAIN_DELEGATED - set(_CLASSIFICATION_MIN_DELTA))}"
)
assert set(SERVE_METRIC_MIN_DELTA) | _POINT_GRAIN_DELEGATED == set(METRICS), (
    "SERVE_METRIC_MIN_DELTA out of sync with METRICS: missing "
    f"{sorted(set(METRICS) - set(SERVE_METRIC_MIN_DELTA) - _POINT_GRAIN_DELEGATED)}, "
    f"extra {sorted(set(SERVE_METRIC_MIN_DELTA) - set(METRICS))}"
)


def default_serve_min_delta(name: str) -> float:
    """Scale-appropriate min_delta for a serve-FS metric."""
    if name in _POINT_GRAIN_DELEGATED:
        return _classification_default_min_delta(name)
    try:
        return SERVE_METRIC_MIN_DELTA[name]
    except KeyError:
        raise ValueError(
            f"No default min_delta for serve metric {name!r}; add it to "
            "SERVE_METRIC_MIN_DELTA in metric_registry.py."
        ) from None


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
    """Branch metrics scored on the first_in RATE target, not a point binary.

    Identified by carrying no `base_metric`: `compute_metrics` cannot score a
    weighted rate, so these route through `first_in_metrics` instead. The
    config guard uses this both ways — a rate metric on a win arm, or a
    classification metric on first_in, is refused rather than silently scoring
    the wrong quantity under the configured name.
    """
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
    test_df: Any = None,
) -> float:
    """Score a MatchDistribution against observed outcomes for a chain-grain
    metric. `y_won` is required by the match-win metrics only and `test_df` by
    `iid_set_count_cal` only; every other scorer absorbs them via its kwargs
    catch-all."""
    spec = METRICS.get(name)
    if spec is None or spec.chain_scorer is None:
        raise ValueError(f"Unknown chain metric: {name}")
    return spec.chain_scorer(
        dist, y_games_a, y_games_b,
        total_lines=total_lines, spread_lines=spread_lines, y_won=y_won,
        test_df=test_df,
    )


def validate_metric_name(name: str) -> str:
    """Pydantic-friendly validator: raise on unknown name, else return it."""
    if name not in METRICS:
        raise ValueError(
            f"Unknown metric '{name}'. Valid: {sorted(METRICS.keys())}"
        )
    return name
