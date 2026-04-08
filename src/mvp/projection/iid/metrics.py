"""Distributional and classification-bridge metrics for the IID projector.

Three metric families are computed in one place so a single mlflow run is
directly comparable to:
    - the production classifier (via classification metrics on match-win prob)
    - the existing per-player game regression (via MAE/RMSE on expected games)
    - other distributional projectors (via CRPS and per-line calibration)
"""

import numpy as np

from mvp.model.metrics import compute_metrics
from mvp.projection.iid.projector import ProjectionOutput
from mvp.projection.metrics import compute_regression_metrics


def crps_discrete_pmf(obs_idx: np.ndarray, pmf: np.ndarray) -> float:
    """Continuous Ranked Probability Score for a discrete pmf.

    For a discrete cumulative distribution F over integer indices,
        CRPS_match = sum_j (F(j) - 1[obs <= j])^2
    Returns the mean CRPS across matches.

    Args:
        obs_idx: shape (N,) integer observed indices, in [0, K).
        pmf: shape (N, K) per-match discrete probability mass function.
    """
    if pmf.shape[0] == 0:
        return 0.0
    cdf = np.cumsum(pmf, axis=1)
    n_cols = pmf.shape[1]
    cols = np.arange(n_cols, dtype=np.int64)[None, :]
    heaviside = (obs_idx[:, None] <= cols).astype(np.float64)
    per_match = ((cdf - heaviside) ** 2).sum(axis=1)
    return float(per_match.mean())


def compute_iid_metrics(
    out: ProjectionOutput,
    y_won: np.ndarray,
    y_games_a: np.ndarray,
    y_games_b: np.ndarray,
    *,
    total_lines: list[float] | None = None,
    spread_lines: list[float] | None = None,
    include_classification: bool = True,
    include_regression: bool = True,
) -> dict[str, float]:
    """Compute classification, regression, and distributional metrics for an IID projection.

    Args:
        out: ProjectionOutput from `TennisProjector.project`.
        y_won: shape (N,) integer 0/1, did player A (the row's player) win the match.
        y_games_a: shape (N,) games won by player A.
        y_games_b: shape (N,) games won by player B.
        total_lines: Total games O/U lines for calibration metrics.
        spread_lines: A's game spread lines for calibration metrics.
        include_classification: Whether to compute log_loss/brier/calibration on match-win prob.
        include_regression: Whether to compute MAE/RMSE on expected games for player A.
    """
    metrics: dict[str, float] = {}
    dist = out.distribution

    if include_classification:
        metrics.update(compute_metrics(y_won.astype(np.int64), dist.p_match_win_a))

    if include_regression:
        reg_metrics = compute_regression_metrics(
            y_games_a.astype(np.float64), dist.expected_games_a,
        )
        metrics.update(reg_metrics)

    obs_total = (y_games_a + y_games_b).astype(np.int64)
    metrics["iid_crps_total_games"] = crps_discrete_pmf(obs_total, dist.total_games_pmf)

    obs_spread_int = (y_games_a - y_games_b).astype(np.int64)
    obs_spread_idx = obs_spread_int + dist.spread_offset
    obs_spread_idx = np.clip(obs_spread_idx, 0, dist.spread_pmf.shape[1] - 1)
    metrics["iid_crps_spread"] = crps_discrete_pmf(obs_spread_idx, dist.spread_pmf)

    if total_lines:
        for line in total_lines:
            p_over = dist.p_over_total(line)
            actual_over = (obs_total > line).astype(np.float64)
            mean_p = float(p_over.mean())
            actual_rate = float(actual_over.mean())
            metrics[f"iid_line_total_{line}_pred"] = mean_p
            metrics[f"iid_line_total_{line}_actual"] = actual_rate
            metrics[f"iid_line_total_{line}_err"] = abs(mean_p - actual_rate)

    if spread_lines:
        obs_spread = obs_spread_int.astype(np.float64)
        for line in spread_lines:
            p_cover = dist.p_a_spread_cover(line)
            actual_cover = (obs_spread > line).astype(np.float64)
            mean_p = float(p_cover.mean())
            actual_rate = float(actual_cover.mean())
            metrics[f"iid_line_spread_{line}_pred"] = mean_p
            metrics[f"iid_line_spread_{line}_actual"] = actual_rate
            metrics[f"iid_line_spread_{line}_err"] = abs(mean_p - actual_rate)

    return metrics
