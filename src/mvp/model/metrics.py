"""Metrics calculation for experiments."""


import numpy as np
from scipy.special import betainc as _betainc
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
    roc_curve,
)

# Tail-emphasizing threshold grid for threshold_weighted_brier. Excludes the
# central [0.40, 0.60] band so near-coinflip calibration carries no weight,
# leaving a proper (cost-weighted) scoring rule focused on confident calls.
_TWBRIER_THRESHOLDS = np.array([
    0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40,
    0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95,
])

# compute_metrics keys where a higher value is better. Single source of truth
# for optimization direction so HP tuning and feature selection always agree;
# every other compute_metrics key is minimize. Tuning unions this with its own
# projection/IID extras (r_squared, point_* variants).
MAXIMIZE_METRICS = frozenset({
    "accuracy", "roc_auc", "weighted_concordance", "partial_auc_tail",
})


def metric_direction(name: str) -> str:
    """Return "maximize" or "minimize" for a metric name (default minimize)."""
    return "maximize" if name in MAXIMIZE_METRICS else "minimize"


def _bucket_errors(
    y_true: np.ndarray, y_prob: np.ndarray, signed: bool
) -> tuple[list[float], list[int]]:
    """Per-bucket calibration errors and counts for probabilities >= 0.50.

    Returns parallel lists of (errors, counts) — one entry per non-empty bucket.
    """
    mask = y_prob >= 0.50
    y_true_filtered = y_true[mask]
    y_prob_filtered = y_prob[mask]

    if len(y_true_filtered) == 0:
        return [], []

    bucket_edges = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00]
    errors: list[float] = []
    weights: list[int] = []

    for i in range(len(bucket_edges) - 1):
        low, high = bucket_edges[i], bucket_edges[i + 1]
        if i == len(bucket_edges) - 2:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered <= high)
        else:
            bucket_mask = (y_prob_filtered >= low) & (y_prob_filtered < high)

        if not bucket_mask.any():
            continue

        predicted_mean = float(np.mean(y_prob_filtered[bucket_mask]))
        actual = float(np.mean(y_true_filtered[bucket_mask]))
        n = int(bucket_mask.sum())
        error = actual - predicted_mean if signed else abs(predicted_mean - actual)

        errors.append(error)
        weights.append(n)

    return errors, weights


def compute_calibration_error(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute weighted mean calibration error for probabilities >= 0.50."""
    errors, weights = _bucket_errors(y_true, y_prob, signed=False)
    if not errors:
        return 0.0
    return float(np.average(errors, weights=weights))


def compute_signed_calibration(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute signed calibration for probabilities >= 0.50.

    Positive = underconfident (actual win rate > predicted).
    Negative = overconfident (actual win rate < predicted).
    """
    errors, weights = _bucket_errors(y_true, y_prob, signed=True)
    if not errors:
        return 0.0
    return float(np.average(errors, weights=weights))


def compute_calibration_error_max(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Worst-bucket calibration error for probabilities >= 0.50.

    Tuning target for flattening the worst-offending bucket rather than the
    weighted average, which can hide a wildly miscalibrated bucket behind
    well-calibrated ones.
    """
    errors, _ = _bucket_errors(y_true, y_prob, signed=False)
    if not errors:
        return 0.0
    return float(max(errors))


def compute_overconfidence_max(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Worst overconfident-bucket magnitude for probabilities >= 0.50.

    Returns the largest amount by which any bucket's predicted mean exceeds
    its actual win rate (i.e. the worst overconfidence). 0 if every bucket
    is underconfident. Asymmetric counterpart to calibration_error_max that
    penalizes only the side of miscalibration that loses real money.
    """
    errors, _ = _bucket_errors(y_true, y_prob, signed=True)
    if not errors:
        return 0.0
    return float(max(0.0, -min(errors)))


def compute_error_rate_80plus(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Compute error rate for predictions at 80%+ confidence."""
    y_pred = (y_prob >= 0.5).astype(int)
    is_error = y_pred != y_true
    tier_mask = y_prob >= 0.80
    tier_total = int(tier_mask.sum())
    if tier_total == 0:
        return 0.0
    tier_errors = int((tier_mask & is_error).sum())
    return tier_errors / tier_total


def compute_asymmetric_logloss(
    y_true: np.ndarray, y_prob: np.ndarray, lambda_over: float = 2.0
) -> float:
    """Asymmetric log-loss with overconfident-side penalty weighted by lambda_over.

    Mirrors the training objective in `XGBoostModel._asymmetric_logloss` so
    HP tuning can target the same loss surface the model is trained against.
    Overconfident = predicted prob > actual outcome.
    """
    # Cast to float64 before clipping: in float32 the upper bound 1 - 1e-15
    # rounds to exactly 1.0, so a prediction at 1.0 survives the clip and
    # log(1 - p) = log(0) (divide-by-zero RuntimeWarning + inf in the mean).
    p = np.clip(np.asarray(y_prob, dtype=np.float64), 1e-15, 1 - 1e-15)
    base = -(y_true * np.log(p) + (1 - y_true) * np.log(1 - p))
    weight = np.where(p > y_true, lambda_over, 1.0)
    return float(np.mean(base * weight))


def compute_beta_tail_score(
    y_true: np.ndarray, y_prob: np.ndarray, a: float = 0.5, b: float = 0.5
) -> float:
    """Beta-family proper scoring loss with a tail-concentrated weight (lower better).

    By the Schervish representation, every proper binary scoring rule is a
    mixture of cost-weighted threshold losses against a weight measure over the
    threshold c. A Beta(a, b) measure with a, b < 1 is U-shaped — it
    concentrates weight near c = 0 and c = 1, so the resulting *proper* loss is
    dominated by how well confident predictions (p near 0 or 1) are scored and
    gives ~zero weight to the near-0.5 bulk. a = b = 1 recovers half the Brier
    score; a = b = 0.5 (default, arcsine) is a mild tail emphasis; smaller
    a = b sharpens it.

    Closed form under the normalized Beta(a,b) weight (I_x = regularized
    incomplete beta). The 1/B(a,b) normalization collapses the
    B(a,b+1)/B(a,b) and B(a+1,b)/B(a,b) prefactors to b/(a+b) and a/(a+b):
        y = 1:  b/(a+b) * (1 - I_p(a, b+1))
        y = 0:  a/(a+b) * I_p(a+1, b)

    Proper by construction (positive-weighted mixture of proper threshold
    scores). Unlike weighting log-loss/Brier by a function of p — which is
    improper and rewards overconfidence — this cannot be gamed by pushing
    predictions toward the tails.
    """
    p = np.clip(y_prob, 1e-15, 1 - 1e-15)
    loss_pos = (b / (a + b)) * (1.0 - _betainc(a, b + 1.0, p))
    loss_neg = (a / (a + b)) * _betainc(a + 1.0, b, p)
    loss = np.where(np.asarray(y_true) == 1, loss_pos, loss_neg)
    return float(np.mean(loss))


def compute_threshold_weighted_brier(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    thresholds: np.ndarray | None = None,
) -> float:
    """Threshold-weighted Brier over a tail grid (lower better).

    Discrete cousin of compute_beta_tail_score: averages the cost-weighted
    elementary threshold loss
        s_t(p, y) = (1 - t)  if y = 1 and p <= t
                  =      t   if y = 0 and p >  t
                  =      0   otherwise
    over a grid of thresholds that excludes the central [0.40, 0.60] band. Each
    elementary score is proper for the event {Y = 1} at threshold t, and a
    positive-weighted mixture of proper scores is proper — so this targets tail
    calibration + discrimination without the impropriety of weighting a score
    by a function of the forecast. (The naive (1{p>t} - y)^2 mixture integrates
    to linear loss, which is improper; the cost-weighted form above integrates
    to half the Brier score under a uniform grid.)
    """
    t = _TWBRIER_THRESHOLDS if thresholds is None else np.asarray(thresholds, float)
    p = np.asarray(y_prob, dtype=float)[:, None]
    y = np.asarray(y_true)[:, None]
    tt = t[None, :]
    s = np.where(
        (y == 1) & (p <= tt),
        1.0 - tt,
        np.where((y == 0) & (p > tt), tt, 0.0),
    )
    return float(s.mean())


def compute_restricted_logloss(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    tau: float = 0.15,
    target_coverage: float = 0.35,
    lambda_cov: float = 3.0,
) -> float:
    """Log loss on confident predictions only, with a coverage guard (lower better).

    Scores log loss over the subset |p - 0.5| > tau (the region where bets are
    placed), then adds ``lambda_cov * max(0, target_coverage - coverage)``
    where coverage is the fraction of predictions in that subset. Without the
    guard the optimizer could win by collapsing predictions toward 0.5 to empty
    the scored set; the penalty makes under-coverage cost more than any
    achievable confident-region log loss.

    DIAGNOSTIC-GRADE: ``target_coverage`` and ``lambda_cov`` are data-dependent
    design constants — set so that zero coverage (penalty ~1.05) clearly
    exceeds plausible confident-region log loss (~0.3-0.6). Prefer
    beta_tail_score as the primary tail objective.
    """
    p = np.clip(y_prob, 1e-15, 1 - 1e-15)
    mask = np.abs(np.asarray(y_prob, dtype=float) - 0.5) > tau
    coverage = float(mask.mean())
    ll = float(log_loss(y_true[mask], p[mask], labels=[0, 1])) if mask.any() else 0.0
    return ll + lambda_cov * max(0.0, target_coverage - coverage)


def compute_weighted_concordance(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Confidence-weighted concordance / weighted Somers' D (higher better).

    Somers' D = 2*AUC - 1, but each (positive, negative) pair is weighted by
    (|p_pos - 0.5| + |p_neg - 0.5|) / 2, so concordant orderings among confident
    predictions count more and near-0.5 pairs count ~nothing. A pure ranking
    metric (no calibration signal) — suited to the voter / discrimination path.
    Range [-1, 1]; 0 = no weighted skill. O(n log n) via searchsorted; ties in
    p contribute 0 (neither concordant nor discordant).
    """
    y = np.asarray(y_true)
    prob = np.asarray(y_prob, dtype=float)
    c = np.abs(prob - 0.5)
    pos = y == 1
    neg = ~pos
    p_pos, c_pos = prob[pos], c[pos]
    p_neg, c_neg = prob[neg], c[neg]
    n_pos, n_neg = p_pos.size, p_neg.size
    if n_pos == 0 or n_neg == 0:
        return 0.0

    p_neg_s = np.sort(p_neg)
    neg_below = np.searchsorted(p_neg_s, p_pos, side="left")
    neg_above = n_neg - np.searchsorted(p_neg_s, p_pos, side="right")
    p_pos_s = np.sort(p_pos)
    pos_above = n_pos - np.searchsorted(p_pos_s, p_neg, side="right")
    pos_below = np.searchsorted(p_pos_s, p_neg, side="left")

    # Concordant: positive ranked above negative (p_pos > p_neg). Each pair's
    # weight (c_pos + c_neg)/2 splits across the two per-side sums.
    conc = 0.5 * np.sum(c_pos * neg_below) + 0.5 * np.sum(c_neg * pos_above)
    disc = 0.5 * np.sum(c_pos * neg_above) + 0.5 * np.sum(c_neg * pos_below)
    denom = 0.5 * (n_neg * np.sum(c_pos) + n_pos * np.sum(c_neg))
    if denom <= 0:
        return 0.0
    return float((conc - disc) / denom)


def _standardized_partial_auc(
    y_true: np.ndarray, y_prob: np.ndarray, beta: float
) -> float:
    """McClish-standardized partial AUC over FPR in [0, beta].

    The high-specificity corner — the regime of the model's most confident
    positive calls. Returns 0.5 at chance, -> 1.0 ideal.
    """
    if len(np.unique(y_true)) < 2:
        return 0.5
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    # roc_curve can emit duplicate fpr values (score ties → vertical steps).
    # Collapse each to its top-of-step (max tpr, the last point in the group
    # since both arrays are non-decreasing) so fpr is strictly increasing and
    # np.interp at beta is well-defined.
    last_of_group = np.concatenate([np.diff(fpr) > 0, [True]])
    fpr, tpr = fpr[last_of_group], tpr[last_of_group]
    tpr_beta = float(np.interp(beta, fpr, tpr))
    keep = fpr <= beta
    fpr_c = np.concatenate([fpr[keep], [beta]])
    tpr_c = np.concatenate([tpr[keep], [tpr_beta]])
    pauc = float(np.trapezoid(tpr_c, fpr_c))
    pauc_min = beta * beta / 2.0  # area under the chance diagonal over [0, beta]
    pauc_max = beta               # perfect classifier
    if pauc_max <= pauc_min:
        return 0.5
    return 0.5 * (1.0 + (pauc - pauc_min) / (pauc_max - pauc_min))


def compute_partial_auc_tail(
    y_true: np.ndarray, y_prob: np.ndarray, beta: float = 0.2
) -> float:
    """Two-corner tail partial AUC (higher better).

    Averages the standardized partial AUC of the high-specificity corner
    (FPR <= beta) and the high-sensitivity corner (the flipped problem), so it
    rewards correct ranking among the model's most confident calls on *both*
    sides. Ranking metric (no calibration signal) — voter / discrimination
    path. Fallback for weighted_concordance when the confident tail is sparse.
    Range ~[0.5, 1].
    """
    y = np.asarray(y_true)
    prob = np.asarray(y_prob, dtype=float)
    hi_spec = _standardized_partial_auc(y, prob, beta)
    hi_sens = _standardized_partial_auc(1 - y, 1.0 - prob, beta)
    return 0.5 * (hi_spec + hi_sens)


def compute_metrics(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    threshold: float = 0.5,
    lambda_over: float | None = None,
) -> dict[str, float]:
    """Compute classification metrics.

    Args:
        y_true: True binary labels.
        y_prob: Predicted probabilities for positive class.
        threshold: Classification threshold for accuracy.
        lambda_over: Override for asymmetric_logloss's overconfidence penalty.
            When None, uses compute_asymmetric_logloss's default (2.0). Callers
            with a YAML-configured `model.params.lambda_over` should pass it
            through so the tune metric mirrors the training objective.

    Returns:
        Dictionary of metric name -> value.
    """
    y_pred = (y_prob >= threshold).astype(int)

    # Clip probabilities to avoid log(0)
    y_prob_clipped = np.clip(y_prob, 1e-15, 1 - 1e-15)

    asym_kwargs = {"lambda_over": lambda_over} if lambda_over is not None else {}

    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "log_loss": float(log_loss(y_true, y_prob_clipped)),
        "brier_score": float(brier_score_loss(y_true, y_prob)),
        "roc_auc": float(roc_auc_score(y_true, y_prob)),
        "calibration_error": compute_calibration_error(y_true, y_prob),
        "calibration_error_max": compute_calibration_error_max(y_true, y_prob),
        "overconfidence_max": compute_overconfidence_max(y_true, y_prob),
        "signed_calibration": compute_signed_calibration(y_true, y_prob),
        "error_rate_80plus": compute_error_rate_80plus(y_true, y_prob),
        "asymmetric_logloss": compute_asymmetric_logloss(y_true, y_prob, **asym_kwargs),
        # Tail-sensitive objectives (see each compute_* docstring). Lead /
        # calibration-sizing path: beta_tail_score, threshold_weighted_brier,
        # restricted_logloss (lower = better).
        # Voter / discrimination path: weighted_concordance, partial_auc_tail
        # (higher = better — listed in tuning._MAXIMIZE_METRICS).
        "beta_tail_score": compute_beta_tail_score(y_true, y_prob),
        "beta_tail_score_sharp": compute_beta_tail_score(y_true, y_prob, a=0.25, b=0.25),
        "threshold_weighted_brier": compute_threshold_weighted_brier(y_true, y_prob),
        "restricted_logloss": compute_restricted_logloss(y_true, y_prob),
        "weighted_concordance": compute_weighted_concordance(y_true, y_prob),
        "partial_auc_tail": compute_partial_auc_tail(y_true, y_prob),
    }
