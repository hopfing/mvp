"""Distributional and classification-bridge metrics for the IID projector.

Three metric families are computed in one place so a single mlflow run is
directly comparable to:
    - the production classifier (via classification metrics on match-win prob)
    - the existing per-player game regression (via MAE/RMSE on expected games)
    - other distributional projectors (via CRPS and per-line calibration)
"""

import numpy as np
import polars as pl

from mvp.model.metrics import compute_metrics
from mvp.projection.iid.chain import SET_SCORE_LABELS, set_score_distribution
from mvp.projection.iid.projector import ProjectionOutput
from mvp.projection.iid.serve_model import SERVE_PROB_MAX, SERVE_PROB_MIN
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
    metrics["signed_total_bias"] = float(
        dist.expected_total_games.mean() - obs_total.mean()
    )

    obs_spread_int = (y_games_a - y_games_b).astype(np.int64)
    obs_spread_idx = obs_spread_int + dist.spread_offset
    obs_spread_idx = np.clip(obs_spread_idx, 0, dist.spread_pmf.shape[1] - 1)
    metrics["iid_crps_spread"] = crps_discrete_pmf(obs_spread_idx, dist.spread_pmf)
    metrics["signed_spread_bias"] = float(
        dist.expected_spread.mean() - obs_spread_int.mean()
    )

    if total_lines:
        for line in total_lines:
            p_over = dist.p_over_total(line)
            actual_over = (obs_total > line).astype(np.float64)
            mean_p = float(p_over.mean())
            actual_rate = float(actual_over.mean())
            metrics[f"iid_line_total_{line}_pred"] = mean_p
            metrics[f"iid_line_total_{line}_actual"] = actual_rate
            metrics[f"iid_line_total_{line}_signed"] = mean_p - actual_rate

    if spread_lines:
        obs_spread = obs_spread_int.astype(np.float64)
        for line in spread_lines:
            p_cover = dist.p_a_spread_cover(line)
            actual_cover = (obs_spread > line).astype(np.float64)
            mean_p = float(p_cover.mean())
            actual_rate = float(actual_cover.mean())
            metrics[f"iid_line_spread_{line}_pred"] = mean_p
            metrics[f"iid_line_spread_{line}_actual"] = actual_rate
            metrics[f"iid_line_spread_{line}_signed"] = mean_p - actual_rate

    return metrics


def compute_serve_diagnostics(
    out: ProjectionOutput,
    test_df: pl.DataFrame,
    *,
    clip_min: float = SERVE_PROB_MIN,
    clip_max: float = SERVE_PROB_MAX,
) -> dict[str, float]:
    """Serve-level residual diagnostics: bias, MAE, and clipping rates.

    Compares predicted serve point win probs (from the serve model) against
    the actual per-match serve rates to diagnose whether systematic O/U bias
    originates at the serve-probability level or in the chain math.
    """
    won_a = test_df["pts_service_pts_won"].to_numpy().astype(np.float64)
    played_a = test_df["pts_service_pts_played"].to_numpy().astype(np.float64)
    won_b = test_df["opp_pts_service_pts_won"].to_numpy().astype(np.float64)
    played_b = test_df["opp_pts_service_pts_played"].to_numpy().astype(np.float64)

    with np.errstate(divide="ignore", invalid="ignore"):
        actual_a = np.where(played_a > 0, won_a / played_a, np.nan)
        actual_b = np.where(played_b > 0, won_b / played_b, np.nan)

    p_a = out.p_a_serve_win
    p_b = out.p_b_serve_win

    resid_a = p_a - actual_a
    resid_b = p_b - actual_b
    valid_a = np.isfinite(resid_a)
    valid_b = np.isfinite(resid_b)

    metrics: dict[str, float] = {}
    resid_all = np.concatenate([resid_a[valid_a], resid_b[valid_b]])
    if len(resid_all) > 0:
        metrics["serve_bias"] = float(np.mean(resid_all))
        metrics["serve_mae"] = float(np.mean(np.abs(resid_all)))

    # Clipping: how many predictions sit exactly at the bounds?
    n_predictions = len(p_a) + len(p_b)
    n_clipped_low = int(np.sum(p_a == clip_min) + np.sum(p_b == clip_min))
    n_clipped_high = int(np.sum(p_a == clip_max) + np.sum(p_b == clip_max))
    metrics["serve_clip_min"] = float(clip_min)
    metrics["serve_clip_max"] = float(clip_max)
    metrics["serve_n_clipped_low"] = float(n_clipped_low)
    metrics["serve_n_clipped_high"] = float(n_clipped_high)
    metrics["serve_pct_clipped"] = (
        float((n_clipped_low + n_clipped_high) / n_predictions)
        if n_predictions > 0
        else 0.0
    )
    # Raw prediction extrema (pre any external post-processing) — useful for
    # telling whether the chosen bounds are active or merely vestigial.
    if len(p_a) > 0 or len(p_b) > 0:
        all_p = np.concatenate([p_a, p_b])
        metrics["serve_p_min"] = float(np.min(all_p))
        metrics["serve_p_max"] = float(np.max(all_p))

    return metrics


# Map (player_games, opp_games) → index in SET_SCORE_LABELS.
_SET_SCORE_INDEX: dict[tuple[int, int], int] = {
    tuple(int(x) for x in label.split("-")): i
    for i, label in enumerate(SET_SCORE_LABELS)
}

# Tight sets: 7-5, 5-7, 7-6, 6-7 (indices 5, 12, 6, 13)
_TIGHT_INDICES = [
    i for i, label in enumerate(SET_SCORE_LABELS)
    if label in ("7-5", "5-7", "7-6", "6-7")
]
# Blowout sets: 6-0, 0-6, 6-1, 1-6 (indices 0, 7, 1, 8)
_BLOWOUT_INDICES = [
    i for i, label in enumerate(SET_SCORE_LABELS)
    if label in ("6-0", "0-6", "6-1", "1-6")
]


def compute_hold_diagnostics(
    out: ProjectionOutput,
    test_df: pl.DataFrame,
) -> dict[str, float]:
    """Layer 1 chain diagnostics: predicted vs actual hold rates.

    Compares the IID-derived hold probability ``h = p_service_game_win(p)``
    against the actual per-match hold rate computed from service game stats.
    """
    gp_a = test_df["svc_games_played"].to_numpy().astype(np.float64)
    bp_faced_a = test_df["svc_bp_faced"].to_numpy().astype(np.float64)
    bp_saved_a = test_df["svc_bp_saved"].to_numpy().astype(np.float64)

    gp_b = test_df["opp_svc_games_played"].to_numpy().astype(np.float64)
    bp_faced_b = test_df["opp_svc_bp_faced"].to_numpy().astype(np.float64)
    bp_saved_b = test_df["opp_svc_bp_saved"].to_numpy().astype(np.float64)

    with np.errstate(divide="ignore", invalid="ignore"):
        holds_a = gp_a - (bp_faced_a - bp_saved_a)
        actual_hold_a = np.where(gp_a > 0, holds_a / gp_a, np.nan)
        holds_b = gp_b - (bp_faced_b - bp_saved_b)
        actual_hold_b = np.where(gp_b > 0, holds_b / gp_b, np.nan)

    resid_a = out.h_a - actual_hold_a
    resid_b = out.h_b - actual_hold_b
    valid_a = np.isfinite(resid_a)
    valid_b = np.isfinite(resid_b)

    metrics: dict[str, float] = {}
    resid_all = np.concatenate([resid_a[valid_a], resid_b[valid_b]])
    if len(resid_all) > 0:
        metrics["hold_bias"] = float(np.mean(resid_all))
        metrics["hold_mae"] = float(np.mean(np.abs(resid_all)))

    return metrics


def compute_set_score_diagnostics(
    out: ProjectionOutput,
    test_df: pl.DataFrame,
) -> dict[str, float]:
    """Layer 2 chain diagnostics: predicted vs actual set score frequencies.

    Recomputes the per-match set score PMF from the chain and compares
    against actual set score frequencies extracted from set game columns.
    Reports bias for tight sets (7-5, 5-7, 7-6, 6-7) and blowout sets
    (6-0, 0-6, 6-1, 1-6).
    """
    n_scores = len(SET_SCORE_LABELS)

    # Predicted: per-match (N, 14) set score PMF.
    pred_pmf = set_score_distribution(out.h_a, out.h_b, out.t_ab)

    # Actual: extract per-set scores and build a frequency histogram.
    actual_counts = np.zeros(n_scores, dtype=np.float64)
    total_sets = 0
    for i in range(1, 6):
        pg_col = f"player_set{i}_games"
        og_col = f"opp_set{i}_games"
        pg = test_df[pg_col].to_numpy().astype(np.float64)
        og = test_df[og_col].to_numpy().astype(np.float64)
        valid = np.isfinite(pg) & np.isfinite(og)
        for j in np.where(valid)[0]:
            pg_int, og_int = int(pg[j]), int(og[j])
            idx = _SET_SCORE_INDEX.get((pg_int, og_int))
            if idx is not None:
                actual_counts[idx] += 1
                total_sets += 1

    if total_sets == 0:
        return {}

    actual_freq = actual_counts / total_sets

    # Predicted frequency: average the per-match PMFs, weighted by
    # number of sets each match actually played (so matches with more
    # sets contribute proportionally).
    sets_per_match = np.zeros(len(out.h_a), dtype=np.float64)
    for i in range(1, 6):
        pg = test_df[f"player_set{i}_games"].to_numpy().astype(np.float64)
        sets_per_match += np.isfinite(pg).astype(np.float64)
    weights = sets_per_match / sets_per_match.sum()
    pred_freq = (pred_pmf * weights[:, None]).sum(axis=0)

    metrics: dict[str, float] = {}
    metrics["set_score_bias_tight"] = float(
        pred_freq[_TIGHT_INDICES].sum() - actual_freq[_TIGHT_INDICES].sum()
    )
    metrics["set_score_bias_blowout"] = float(
        pred_freq[_BLOWOUT_INDICES].sum() - actual_freq[_BLOWOUT_INDICES].sum()
    )

    return metrics


def compute_tiebreak_diagnostics(
    out: ProjectionOutput,
    test_df: pl.DataFrame,
) -> dict[str, float]:
    """Layer 3 chain diagnostics: predicted vs actual tiebreak frequency.

    Compares the predicted probability of a tiebreak set (from the set score
    PMF) against the actual tiebreak rate observed in the data.
    """
    # Actual tiebreak count: a tiebreak occurred if the tiebreak score column
    # is non-null for that set.
    actual_tb = 0
    total_sets = 0
    for i in range(1, 6):
        pg = test_df[f"player_set{i}_games"].to_numpy().astype(np.float64)
        tb = test_df[f"player_set{i}_tiebreak"].to_numpy().astype(np.float64)
        set_played = np.isfinite(pg)
        total_sets += int(set_played.sum())
        actual_tb += int((set_played & np.isfinite(tb)).sum())

    if total_sets == 0:
        return {}

    actual_rate = actual_tb / total_sets

    # Predicted tiebreak rate: indices 6 ("7-6") and 13 ("6-7") in the set PMF.
    pred_pmf = set_score_distribution(out.h_a, out.h_b, out.t_ab)

    sets_per_match = np.zeros(len(out.h_a), dtype=np.float64)
    for i in range(1, 6):
        pg = test_df[f"player_set{i}_games"].to_numpy().astype(np.float64)
        sets_per_match += np.isfinite(pg).astype(np.float64)
    weights = sets_per_match / sets_per_match.sum()

    pred_freq = (pred_pmf * weights[:, None]).sum(axis=0)
    pred_rate = float(pred_freq[6] + pred_freq[13])  # 7-6 + 6-7

    metrics: dict[str, float] = {}
    metrics["tiebreak_rate_pred"] = pred_rate
    metrics["tiebreak_rate_actual"] = actual_rate
    metrics["tiebreak_rate_bias"] = pred_rate - actual_rate

    return metrics
