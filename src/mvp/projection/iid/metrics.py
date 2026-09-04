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
from mvp.projection.iid.chain import SET_SCORE_LABELS
from mvp.projection.iid.projector import ProjectionOutput
from mvp.projection.iid.serve_model import SERVE_PROB_MAX, SERVE_PROB_MIN
from mvp.projection.metrics import compute_regression_metrics


def total_cal_errs(
    dist,
    y_games_a: np.ndarray,
    y_games_b: np.ndarray,
    total_lines: list[float],
) -> list[float]:
    """Per-line absolute calibration errors for total-games O/U lines.

    Shared between the projection runner's per-fold metric emit and FS
    chain-scoring so the aggregate values cannot diverge.
    """
    obs_total = (y_games_a + y_games_b).astype(np.int64)
    errs: list[float] = []
    for line in total_lines:
        p_over = dist.p_over_total(line)
        actual_over = (obs_total > line).astype(np.float64)
        errs.append(abs(float(p_over.mean()) - float(actual_over.mean())))
    return errs


def spread_cal_errs(
    dist,
    y_games_a: np.ndarray,
    y_games_b: np.ndarray,
    spread_lines: list[float],
) -> list[float]:
    """Per-line absolute calibration errors for A's game-spread lines."""
    obs_spread = (y_games_a - y_games_b).astype(np.float64)
    errs: list[float] = []
    for line in spread_lines:
        p_cover = dist.p_a_spread_cover(line)
        actual_cover = (obs_spread > line).astype(np.float64)
        errs.append(abs(float(p_cover.mean()) - float(actual_cover.mean())))
    return errs


def _binned_reliability(p: np.ndarray, actual: np.ndarray) -> float:
    """Count-weighted mean |gap| over fixed 0.05 buckets of predicted p.

    The `*_cal_errs` pair above reduces each line to
    `abs(p.mean() - actual.mean())`, which answers NET BIAS and is blind to
    offsetting error: a candidate trading under-prediction on short matches
    for over-prediction on long ones scores identically. This bins first, so
    those cancel nowhere.

    Fixed width over [0, 1], matching `_bucket_errors` in
    `mvp/model/metrics.py`. Deliberately not the equal-count quartile scheme
    in `diagnostics.py`, which answers a different question.
    """
    if p.size == 0:
        return 0.0
    edges = np.asarray([round(0.05 * i, 2) for i in range(21)], dtype=np.float64)
    idx = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, len(edges) - 2)
    gaps: list[float] = []
    weights: list[int] = []
    for b in range(len(edges) - 1):
        m = idx == b
        n = int(m.sum())
        if n == 0:
            continue
        gaps.append(abs(float(p[m].mean()) - float(actual[m].mean())))
        weights.append(n)
    if not gaps:
        return 0.0
    return float(np.average(gaps, weights=weights))


def total_reliability(
    dist,
    y_games_a: np.ndarray,
    y_games_b: np.ndarray,
    total_lines: list[float],
) -> float:
    """Mean over lines of the binned reliability of the total-games O/U."""
    obs_total = (y_games_a + y_games_b).astype(np.int64)
    vals = [
        _binned_reliability(dist.p_over_total(line),
                            (obs_total > line).astype(np.float64))
        for line in total_lines
    ]
    return float(np.mean(vals)) if vals else 0.0


def spread_reliability(
    dist,
    y_games_a: np.ndarray,
    y_games_b: np.ndarray,
    spread_lines: list[float],
) -> float:
    """Mean over lines of the binned reliability of A's game-spread cover."""
    obs_spread = (y_games_a - y_games_b).astype(np.float64)
    vals = [
        _binned_reliability(dist.p_a_spread_cover(line),
                            (obs_spread > line).astype(np.float64))
        for line in spread_lines
    ]
    return float(np.mean(vals)) if vals else 0.0


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


def match_win_log_loss(p_match_win_a: np.ndarray, y_won: np.ndarray) -> float:
    """Log loss of the chain's match-win probability against the actual winner.

    One primitive for both the FS chain scorer (metric_registry's
    `iid_match_win_log_loss`) and the runner's per-fold emit, so the metric a
    serve FS selects on and the objective `mvp tune` reads from projection
    metrics are the same number under the same name.
    """
    p = np.clip(np.asarray(p_match_win_a, dtype=np.float64), 1e-15, 1 - 1e-15)
    y = np.asarray(y_won, dtype=np.float64)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def match_win_auc(p_match_win_a: np.ndarray, y_won: np.ndarray) -> float:
    """ROC AUC of the chain's match-win probability against the actual winner.

    The ranking counterpart to `match_win_log_loss`, on the same two arrays.
    It earns its own name rather than riding along as the generic `roc_auc`
    key because a projection's fold rows are nested-Platt-calibrated before
    they become `player_prior_logit` (`mvp/model/features/prior.py`). Platt is
    monotone: it repairs miscalibration and cannot repair ranking. Selecting
    on log loss alone, which mixes the two, can therefore promote a candidate
    whose calibration the downstream fit would have supplied for free over one
    that ranks better and cannot be rescued.
    """
    from sklearn.metrics import roc_auc_score

    y = np.asarray(y_won)
    if y.size == 0 or len(np.unique(y)) < 2:
        return 0.5
    return float(roc_auc_score(y, np.asarray(p_match_win_a, dtype=np.float64)))


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
    # Always emitted, under the registry's name: the serve FS promotes its
    # `metric` as `metrics.objective`, and the tune reads that key from here.
    metrics["iid_match_win_log_loss"] = match_win_log_loss(dist.p_match_win_a, y_won)

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

    # Aggregate per-line calibration errors via the shared helpers so FS
    # scoring and runner emit cannot diverge.
    if total_lines:
        cal_errs = total_cal_errs(dist, y_games_a, y_games_b, total_lines)
        metrics["iid_total_cal"] = float(sum(cal_errs))
        metrics["iid_total_cal_max"] = float(max(cal_errs))

    if spread_lines:
        cal_errs = spread_cal_errs(dist, y_games_a, y_games_b, spread_lines)
        metrics["iid_spread_cal"] = float(sum(cal_errs))
        metrics["iid_spread_cal_max"] = float(max(cal_errs))

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


def first_in_metrics(
    p: np.ndarray, y: np.ndarray, n: np.ndarray, *, min_bucket: int = 20,
) -> dict[str, float]:
    """Every first_in number, from the aggregated (match, server) frame.

    `p` predicted first-serve-in rate, `y` observed rate, `n` service points
    played. One implementation because the FS scorer and the runner's emit
    both need these: two copies of the weighting is how the number a run
    SELECTS on and the number it REPORTS come to disagree.

    `wmse` is the pre-existing metric; the other four split what it bundles.
    A model that orders servers well but sits systematically high scores the
    same as one that is centred but orders poorly, and those call for
    different next features.
    """
    p = np.asarray(p, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    n = np.asarray(n, dtype=np.float64)
    if p.size == 0 or n.sum() <= 0:
        return {}
    w = n / n.sum()
    resid = p - y

    out: dict[str, float] = {
        "rate_wmse": float(np.sum(w * resid**2)),
        "rate_signed_bias": float(np.sum(w * resid)),
    }

    pc = np.clip(p, 1e-15, 1 - 1e-15)
    out["rate_log_loss"] = float(
        -np.sum(w * (y * np.log(pc) + (1 - y) * np.log(1 - pc)))
    )

    # Calibration: fixed 0.05 buckets, gap = weighted mean p - weighted mean y
    # within the bucket, buckets combined weighted by their summed n. Buckets
    # under `min_bucket` rows are dropped from BOTH sums.
    #
    # What the floor does and does not do: because buckets are combined by
    # summed n, a sparse bucket is ALREADY down-weighted in proportion to its
    # sample -- one row among sixty moves the result by ~1.7%, not more. So
    # the floor is variance hygiene, not protection against a lone row
    # dominating: a bucket with a handful of rows has an unbiased but very
    # noisy gap, and dropping it trades a little coverage for a steadier
    # number. 20 sits between the sparse tails (2-57 rows on a measured
    # holdout) and the populated centre (2,400-2,900). `dropped` is returned
    # so a fold that loses real support is visible rather than silent.
    edges = np.asarray([round(0.05 * i, 2) for i in range(21)], dtype=np.float64)
    idx = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, len(edges) - 2)
    gaps: list[float] = []
    bw: list[float] = []
    dropped = 0
    for b in range(len(edges) - 1):
        m = idx == b
        cnt = int(m.sum())
        if cnt == 0:
            continue
        if cnt < min_bucket:
            dropped += cnt
            continue
        nb = n[m]
        gaps.append(abs(float(np.average(p[m], weights=nb))
                        - float(np.average(y[m], weights=nb))))
        bw.append(float(nb.sum()))
    out["rate_calibration_error"] = (
        float(np.average(gaps, weights=bw)) if gaps else 0.0
    )
    out["rate_calibration_dropped"] = float(dropped)

    # Weighted AUC over the aggregate: row i carries n_in positives and
    # n - n_in negatives, all at the same score p_i. Equal to the point-grain
    # AUC of the broadcast rate, without expanding to points. Ties are grouped
    # by DISTINCT p value across the whole frame, not row by row: a round-1
    # base-rate-only candidate gives every row an identical p, and a per-row
    # rule would score that as skill instead of returning 0.5.
    pos = y * n
    neg = n - pos
    order = np.argsort(p, kind="mergesort")
    p_s, pos_s, neg_s = p[order], pos[order], neg[order]
    bounds = np.flatnonzero(np.diff(p_s)) + 1
    groups = np.split(np.arange(p_s.size), bounds)
    conc = 0.0
    tie = 0.0
    seen_neg = 0.0
    for g in groups:
        gp, gn = pos_s[g].sum(), neg_s[g].sum()
        conc += gp * seen_neg
        tie += gp * gn
        seen_neg += gn
    denom = pos.sum() * neg.sum()
    out["rate_roc_auc"] = (
        float((conc + 0.5 * tie) / denom) if denom > 0 else 0.5
    )
    return out


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

    # Predicted: per-match (N, 14) set score PMF, produced by the projector.
    # Not rebuilt here from `out.h_a`/`h_b`/`t_ab` — under a distributional
    # serve model those are a reduction over draws, and the pmf of the reduced
    # `p` is not the mixture over the posterior. See `ProjectionOutput`.
    pred_pmf = out.set_score_pmf

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
    sets_per_match = np.zeros(pred_pmf.shape[0], dtype=np.float64)
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


def _sets_played(test_df: pl.DataFrame) -> np.ndarray:
    played = np.zeros(len(test_df), dtype=np.float64)
    for i in range(1, 6):
        pg = test_df[f"player_set{i}_games"].to_numpy().astype(np.float64)
        played += np.isfinite(pg).astype(np.float64)
    return played


def set_count_cal(
    dist, best_of: np.ndarray, test_df: pl.DataFrame,
) -> dict[str, float]:
    """Calibration of the MATCH-level set count: straight sets vs not.

    `chain_p_straight` ships as a `chain_shape` feature with nothing checking
    it. `compute_set_score_diagnostics` scores INDIVIDUAL set scores (6-0,
    7-6…), a different grain from how many sets the match took.

    Takes `dist` rather than a ProjectionOutput so the FS chain scorer can
    call it: that path builds a MatchDistribution and never a full
    ProjectionOutput. Predicted is the same `set_outcome_probs` reduction
    `shape_scalars` uses for `chain_p_straight`, so this scores the number the
    feature actually carries.

    bo3 and bo5 are scored separately — their supports differ (2 or 3 sets
    against 3, 4 or 5), so pooling compares distributions over different
    outcome spaces.
    """
    n = len(test_df)
    p_straight = np.zeros(n, dtype=np.float64)
    for k in ((2, 0), (0, 2), (3, 0), (0, 3)):
        vec = dist.set_outcome_probs.get(k)
        if vec is not None:
            p_straight = p_straight + vec

    played = _sets_played(test_df)
    bo = np.asarray(best_of, dtype=np.float64)
    metrics: dict[str, float] = {}
    errs: list[float] = []
    for n_best, straight in ((3.0, 2.0), (5.0, 3.0)):
        m = bo == n_best
        if not m.any():
            continue
        actual = float((played[m] == straight).mean())
        pred = float(p_straight[m].mean())
        tag = f"bo{int(n_best)}"
        metrics[f"set_count_straight_pred_{tag}"] = pred
        metrics[f"set_count_straight_actual_{tag}"] = actual
        metrics[f"set_count_straight_bias_{tag}"] = pred - actual
        errs.append(abs(pred - actual))
    if errs:
        metrics["iid_set_count_cal"] = float(np.mean(errs))
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
    pred_pmf = out.set_score_pmf

    sets_per_match = np.zeros(pred_pmf.shape[0], dtype=np.float64)
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

    # WHO wins a breaker, not whether one happens. `chain_tb_edge` ships as a
    # feature off `t_ab` and nothing scored it; the rate above answers
    # occurrence only. Same two columns the occurrence check uses: a non-null
    # tiebreak score means the set went 7-6, so `player_games == 7` is exactly
    # the player having taken it. Deliberately not `opp_set{i}_games`, which
    # this function has never required.
    t_ab = np.asarray(out.t_ab, dtype=np.float64)
    tb_pred_sum = 0.0
    tb_actual_sum = 0.0
    n_tb = 0
    for i in range(1, 6):
        pg = test_df[f"player_set{i}_games"].to_numpy().astype(np.float64)
        tb = test_df[f"player_set{i}_tiebreak"].to_numpy().astype(np.float64)
        was_tb = np.isfinite(pg) & np.isfinite(tb)
        if not was_tb.any():
            continue
        n_tb += int(was_tb.sum())
        tb_pred_sum += float(t_ab[was_tb].sum())
        tb_actual_sum += float((pg[was_tb] == 7).sum())
    if n_tb:
        pred_win = tb_pred_sum / n_tb
        actual_win = tb_actual_sum / n_tb
        metrics["tiebreak_win_pred"] = pred_win
        metrics["tiebreak_win_actual"] = actual_win
        metrics["iid_tiebreak_win_cal"] = abs(pred_win - actual_win)

    return metrics
