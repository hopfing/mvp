"""Review Optuna tuning study results."""

import logging
import statistics

import optuna

from mvp.model.metrics import CALIBRATION_INVARIANT_METRICS
from mvp.model.tuning import (
    _MAXIMIZE_METRICS,
    _OBJECTIVE_FRAME_CAL,
    _decode_params,
)

logger = logging.getLogger(__name__)

# Below this many outer folds the 1-SE rule is not just noisy but algebraically
# degenerate — see `_robust_pick`.
_MIN_ROBUST_FOLDS = 3
# A within-1-SE band covering most of the leaderboard means per-fold noise
# dominates the between-trial spread; the "pick" is then meaningless. Only
# assessed once there are enough trials for the fraction to say anything.
_ROBUST_MIN_TRIALS = 20
_ROBUST_MAX_BAND_FRACTION = 0.5


def _yaml_value(v: object) -> str:
    """Render a param value as a YAML-safe scalar for copy-paste into a config.

    None -> null, bools -> lowercase, lists -> flow sequence; everything else
    via str(). Avoids the bare ``None`` (parsed as the string "None") and
    string-encoded ``hidden_layers`` that otherwise reach the model untouched.
    """
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _is_new_pipeline_study(trials: list[optuna.trial.FrozenTrial]) -> bool:
    """Return True if the study was tuned by the post-decoupling-refactor pipeline.

    New-pipeline trials carry `_tuning_mode` in {"raw", "calibrated"} — the search
    frame, set by `HyperparamTuner._objective`. Legacy studies (pre-refactor) lack
    the attr entirely; their metrics were Platt-calibrated IN-SAMPLE during search
    (search conflated with calibrator fit) and aren't comparable, so `tune-review`
    refuses to display them.
    """
    return any(
        t.user_attrs.get("_tuning_mode") in ("raw", "calibrated") for t in trials
    )


def _to_ranked(metric: str, use_cal: bool) -> str:
    """Map a bare metric name to the user-attr key tune-review ranks on.

    Classification studies from the Phase-2 tuner carry deployment-frame
    ``holdout_cal_*`` metrics (global Platt fit on the tuning OOF, applied to
    the held-out block). When ``use_cal`` is set, probability-scale metrics rank
    on those so the ordering reflects what deployment actually scores.
    Calibration-invariant ranking metrics — and IID/projection studies, which
    never fit Platt — rank on the raw ``holdout_*`` key. Already-prefixed names
    (``holdout_`` or ``holdout_cal_``) pass through unchanged.
    """
    if metric.startswith(("holdout_cal_", "holdout_")):
        return metric
    if use_cal and metric not in CALIBRATION_INVARIANT_METRICS:
        return f"holdout_cal_{metric}"
    return f"holdout_{metric}"


def _objective_key(
    study: optuna.Study, trials: list[optuna.trial.FrozenTrial]
) -> str | None:
    """The user-attr key holding the value Optuna actually optimized, or None.

    Two uses, neither of them ranking: labelling the param-importance target
    (importance is computed against this, not the leaderboard's sort key), and
    recovering the default sort metric for studies tuned before the
    `objective_metrics` stamp existed. It is NOT a selection metric — every
    trial was optimized against it, so its best value is winner's-curse
    inflated and the leaderboard ranks the held-out block instead.

    Studies tuned since the `objective_metrics` stamp carry the name outright.
    Older studies don't, so recover it: take the metric attr whose value equals
    `values[0]` on EVERY trial. Two attrs tying everywhere is ambiguous, so that
    returns None — no column beats a guessed one.
    """
    if len(study.directions) != 1:
        return None  # multi-objective: `values[0]` isn't the whole objective
    names = study.user_attrs.get("objective_metrics")
    if isinstance(names, list) and len(names) == 1:
        bare = names[0]
        key = (
            f"cal_{bare}"
            if study.user_attrs.get("objective_frame") == _OBJECTIVE_FRAME_CAL
            and bare not in CALIBRATION_INVARIANT_METRICS
            else bare
        )
        if any(key in t.user_attrs for t in trials):
            return key
    candidates: set[str] | None = None
    for t in trials:
        if not t.values:
            continue
        value = t.values[0]
        matches = {
            k for k, v in t.user_attrs.items()
            if isinstance(v, float) and v == value
        }
        candidates = matches if candidates is None else candidates & matches
        if not candidates:
            return None
    if candidates and len(candidates) == 1:
        return next(iter(candidates))
    return None


def _study_frame(
    trials: list[optuna.trial.FrozenTrial],
) -> tuple[bool, bool, bool, bool, int]:
    """(is_iid, is_projection, use_cal, cal_mixed, n_cal) for a set of trials.

    Deployment-frame reporting: classification studies from the Phase-2 tuner
    carry `holdout_cal_*` metrics (raw search objective, calibrated held-out
    block). We switch to the calibrated view only when EVERY trial has them.
    Mixing calibrated and raw rows in one ranking would sort raw-only trials
    (pre-Phase-2 in a resumed study, or a trial whose OOF was single-class so
    the reporting calibrator couldn't fit) to ±inf and truncate them out of the
    top-N — the same silent apples-vs-oranges failure `_is_new_pipeline_study`
    refuses. A mixed study shows the raw view with a note until it's refreshed.
    """
    first_ua = trials[0].user_attrs
    is_iid = "iid_crps_total_games" in first_ua
    is_projection = "mae" in first_ua and "log_loss" not in first_ua
    is_classification = not is_iid and not is_projection
    n_cal = sum(
        any(k.startswith("holdout_cal_") for k in t.user_attrs) for t in trials
    )
    use_cal = is_classification and n_cal == len(trials) and n_cal > 0
    cal_mixed = is_classification and 0 < n_cal < len(trials)
    return is_iid, is_projection, use_cal, cal_mixed, n_cal


def _objective_sort_metrics(
    study: optuna.Study, trials: list[optuna.trial.FrozenTrial], fallback: str
) -> list[str]:
    """Bare metric name(s) to rank on when the caller didn't name one.

    The study's own objective — ranking by the held-out measurement of what was
    actually optimized. A hardcoded default instead meant a brier-objective
    study was ranked by a metric it never optimized unless you passed `--sort`
    yourself. `fallback` covers studies whose objective can't be determined
    (tuned before the `objective_metrics` stamp, with an ambiguous value).
    """
    names = study.user_attrs.get("objective_metrics")
    if isinstance(names, list) and names:
        return [str(m) for m in names]
    key = _objective_key(study, trials)
    if key:
        return [key[len("cal_"):] if key.startswith("cal_") else key]
    return [fallback]


def _direction_key(metric: str) -> str:
    """The bare metric a (possibly holdout-prefixed) key inherits direction from."""
    for prefix in ("holdout_cal_", "holdout_"):
        if metric.startswith(prefix):
            return metric[len(prefix):]
    return metric


def resolve_sort_keys(
    study: optuna.Study,
    trials: list[optuna.trial.FrozenTrial],
    sort_by: list[str] | None = None,
) -> list[str]:
    """The user-attr key(s) to rank on: the study's objective unless overridden.

    Bare names are auto-prefixed to the holdout key — Optuna optimizes in-fold
    per trial, but ranking ACROSS trials uses the held-out measurement
    (calibrated for classification when available, raw otherwise). IID studies
    have no holdout block (the tuner never passes outer_folds to
    IIDProjectionRunner), so their keys stay bare.

    Shared with the frozen backtest sweep so `--select topn` and `tune-review`
    cannot drift into different orderings.
    """
    is_iid, is_projection, use_cal, _, _ = _study_frame(trials)
    if sort_by is None:
        fallback = (
            "iid_crps_total_games" if is_iid
            else "mae" if is_projection
            else "log_loss"
        )
        sort_by = _objective_sort_metrics(study, trials, fallback)
    if is_iid:
        return list(sort_by)
    return [_to_ranked(m, use_cal) for m in sort_by]


def sort_trials(
    trials: list[optuna.trial.FrozenTrial], sort_keys: list[str]
) -> list[optuna.trial.FrozenTrial]:
    """Trials best-first on `sort_keys`. Maximize-direction metrics are negated
    so one ascending sort serves both directions; a trial missing a key sorts
    last rather than winning on a sentinel."""
    def key(t: optuna.trial.FrozenTrial) -> tuple:
        return tuple(
            -t.user_attrs.get(m, float("-inf"))
            if _direction_key(m) in _MAXIMIZE_METRICS
            else t.user_attrs.get(m, float("inf"))
            for m in sort_keys
        )

    return sorted(trials, key=key)


# Short labels for the metrics that have an abbreviated leaderboard column;
# anything else is referred to by its bare metric name.
_METRIC_LABELS = {
    "log_loss": "LL",
    "brier_score": "brier",
    "roc_auc": "AUC",
    "accuracy": "acc",
}


def _metric_label(bare: str) -> str:
    """Leaderboard-column label for a bare metric name."""
    return _METRIC_LABELS.get(bare, bare)


def _fold_spread_from(ua: dict, fold_key: str, metric: str) -> str:
    """Min..max of `metric` across the per-fold dicts stored under `fold_key`."""
    folds = ua.get(fold_key)
    if not folds:
        return ""
    vals = [
        f[metric] for f in folds if isinstance(f, dict) and f.get(metric) is not None
    ]
    if len(vals) < 2:
        return ""
    return f"[{min(vals):.4f}..{max(vals):.4f}] over {len(vals)} folds"


def _fold_spread(ua: dict, metric: str, use_cal: bool) -> str:
    """Min..max dispersion of ``metric`` across the K outer-block folds, or ""
    if fewer than two folds carry it. Uses the calibrated per-fold metrics when
    ``use_cal`` is set, matching the leaderboard's calibrated columns."""
    key = "holdout_fold_metrics_calibrated" if use_cal else "holdout_fold_metrics"
    folds = ua.get(key)
    if not folds:
        return ""
    vals = [
        f[metric] for f in folds if isinstance(f, dict) and f.get(metric) is not None
    ]
    if len(vals) < 2:
        return ""
    return f"[{min(vals):.4f}..{max(vals):.4f}] over {len(vals)} folds"


def _robust_pick(
    sorted_trials: list[optuna.trial.FrozenTrial],
    sort_metric: str,
    outer_folds: int | None = None,
) -> tuple[int | None, int, str]:
    """1-SE rule (anti-winner's-curse): among trials within one cross-fold standard
    error of the best mean on ``sort_metric``, return the one with the lowest
    cross-fold variance (most stable across the outer folds), the size of that
    within-1-SE band, and a note when the rule was suppressed as uninformative.

    Guards against crowning an argmax that's inside the noise. Returns
    (None, 0, "") if per-fold outer-block metrics aren't available (needs >=2
    folds). The SE is naive per-fold and the folds are correlated, so the band
    is not a calibrated confidence statement in either direction — which is why
    the two guards below are on measured degeneracy rather than on trusting it.

    Suppressed, with a note, in two cases — both only when a pick would
    otherwise have been shown, since a band of one says nothing either way:

    * Fewer than `_MIN_ROBUST_FOLDS` outer folds. At K=2 the statistics are
      algebraically degenerate: variance collapses to ``(a - b)**2 / 2`` and the
      SE to ``|a - b| / 2``, so "lowest cross-fold variance" is exactly "the two
      numbers that happen to sit closest together" — not stability in any
      multi-point sense.
    * The band covers more than `_ROBUST_MAX_BAND_FRACTION` of a study of at
      least `_ROBUST_MIN_TRIALS` trials. Then per-fold noise dominates the
      between-trial spread and "within noise" describes the whole leaderboard,
      so naming one member of it implies a distinction the data doesn't carry.
      Fold count alone wouldn't catch this: it can recur at K=3 or K=4.
    """
    if sort_metric.startswith("holdout_cal_"):
        bare = sort_metric[len("holdout_cal_"):]
        fold_key = "holdout_fold_metrics_calibrated"
    elif sort_metric.startswith("holdout_"):
        bare = sort_metric[len("holdout_"):]
        fold_key = "holdout_fold_metrics"
    else:
        return None, 0, ""
    maximize = bare in _MAXIMIZE_METRICS

    stats: list[tuple[int, float, float, float]] = []  # (number, mean, se, var)
    fold_counts: list[int] = []
    for t in sorted_trials:
        folds = t.user_attrs.get(fold_key)
        if not folds:
            continue
        vals = [
            f[bare] for f in folds if isinstance(f, dict) and f.get(bare) is not None
        ]
        if len(vals) < 2:
            continue
        mean = statistics.fmean(vals)
        var = statistics.variance(vals)
        se = statistics.stdev(vals) / (len(vals) ** 0.5)
        stats.append((t.number, mean, se, var))
        fold_counts.append(len(vals))
    if not stats:
        return None, 0, ""
    best = (
        max(stats, key=lambda s: s[1]) if maximize else min(stats, key=lambda s: s[1])
    )
    best_mean, best_se = best[1], best[2]
    if maximize:
        band = [s for s in stats if s[1] >= best_mean - best_se]
    else:
        band = [s for s in stats if s[1] <= best_mean + best_se]

    if len(band) > 1:
        # The study records the held-out block size; fall back to what the
        # per-fold attrs actually carry for studies predating that attr.
        k = outer_folds if outer_folds is not None else min(fold_counts)
        if k < _MIN_ROBUST_FOLDS:
            return None, 0, (
                f"1-SE pick off: {k} outer folds — SE rests on {k} points, "
                "variance tie-break is just their gap"
            )
        if (
            len(stats) >= _ROBUST_MIN_TRIALS
            and len(band) / len(stats) > _ROBUST_MAX_BAND_FRACTION
        ):
            return None, 0, (
                f"1-SE pick off: {len(band)}/{len(stats)} trials within 1 SE — "
                "leaderboard doesn't separate trials"
            )

    pick = min(band, key=lambda s: s[3])  # lowest cross-fold variance
    return pick[0], len(band), ""


def format_leaderboard(
    study: optuna.Study,
    sort_by: list[str] | None = None,
    top_n: int = 15,
) -> list[str]:
    """Format a leaderboard of trials sorted by the specified metric(s).

    Returns a list of formatted lines.
    """
    trials = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]

    if not trials:
        return ["No completed trials found."]

    pinned = study.user_attrs.get("pinned_params") or {}

    # Refuse legacy (pre-decoupling-refactor) studies. Their metrics were
    # Platt-calibrated during tuning, which conflated HP search with calibrator
    # fit. The new pipeline tunes raw discrimination and post-hoc calibrates
    # via `mvp model`. Mixing the two would silently rank apples against
    # oranges; force a fresh study instead.
    if not _is_new_pipeline_study(trials):
        return [
            "This study was tuned before the calibration-decoupling refactor.",
            "Trial metrics are post-Platt and not comparable to raw-mode tuning.",
            "",
            "Delete the study DB and start fresh:",
            "  rm <data_root>/tuning/<config_name>.db",
            "  poetry run py -m mvp tune <config>",
        ]

    is_iid, is_projection, use_cal, cal_mixed, n_cal = _study_frame(trials)
    # Defaults to the study's own objective; `--sort` overrides.
    sort_by = resolve_sort_keys(study, trials, sort_by)

    # Confirm holdout metrics exist for the requested sort metric(s). Studies
    # tuned with holdout_folds=0 won't have them; the tuner sets holdout_folds to
    # `outer_folds` (>=1), so this catches misconfigured studies only.
    has_any_holdout = any(
        any(k.startswith("holdout_") for k in t.user_attrs) for t in trials
    )
    if not has_any_holdout and any(m.startswith("holdout_") for m in sort_by):
        return [
            "No holdout metrics found on any trial in this study.",
            "Studies must be tuned with holdout_folds >= 1 (the default for",
            "`mvp tune`). Start a fresh study and try again.",
        ]

    trials = sort_trials(trials, sort_by)
    # 1-SE robust pick (single-metric sorts): identify it over the full sorted set
    # before truncation, so it isn't lost to top_n.
    robust_number, robust_band, robust_note = (
        _robust_pick(trials, sort_by[0], study.user_attrs.get("outer_folds"))
        if len(sort_by) == 1 else (None, 0, "")
    )
    # Captured before truncation: the mixed-vintage note counts trials in the
    # STUDY, not the ones that survived top_n.
    n_complete = len(trials)
    trials = trials[:top_n]

    # Map each trial's Optuna id (`trial.number`, assigned at creation across
    # ALL states) to a crash-immune sequence position: its rank among terminal
    # (complete + pruned) trials in creation order. A failed or zombie trial
    # consumes an Optuna number but not a `--n-trials` batch slot, so `seq` —
    # not `trial.number` — is what lines up with the user's batch boundaries.
    # This matches the header's `Total trials` (complete + pruned) count.
    _terminal = (optuna.trial.TrialState.COMPLETE, optuna.trial.TrialState.PRUNED)
    seq_by_number = {
        t.number: i + 1
        for i, t in enumerate(
            sorted(
                (t for t in study.trials if t.state in _terminal),
                key=lambda t: t.number,
            )
        )
    }

    # The per-trial reference line reports the raw->calibrated gap and the outer-
    # fold spread for the metric actually being RANKED on. It used to be pinned to
    # log_loss regardless of `--sort`, so a brier-sorted leaderboard carried two
    # numbers about a metric nobody asked for and none about the one it ranked by.
    # Multi-metric sorts key off the primary. Calibration-invariant metrics get no
    # raw->cal delta — Platt doesn't move them, so it would print as zero.
    ref_metric = _direction_key(sort_by[0])
    ref_label = _metric_label(ref_metric)
    ref_invariant = ref_metric in CALIBRATION_INVARIANT_METRICS

    lines: list[str] = []
    sort_label = ", ".join(sort_by)
    lines.append(f"TOP {top_n} TRIALS (sorted by {sort_label})")

    # Bullets are for things that change how you read the table — not for
    # restating what the column labels already say. The sort line names the
    # ranked key and every field labels its own frame (`raw LL=`), so the frame
    # needs no note. The search objective appears nowhere here at all: it is
    # winner's-curse inflated by every trial that was optimized against it, so
    # it cannot be ranked on and informs nothing at selection time. It belongs
    # in `mvp tune`, where "is the search still moving?" is a live question.
    notes: list[str] = []
    if cal_mixed:
        notes.append(
            f"only {n_cal}/{n_complete} trials have deployment-frame metrics; "
            "showing raw until all do"
        )
    if robust_note:
        notes.append(robust_note)
    elif robust_number is not None and robust_band > 1:
        notes.append(
            f"1-SE pick: trial {robust_number} — lowest fold variance of "
            f"{robust_band} within 1 SE (marked below)"
        )
    lines.extend(f"  · {n}" for n in notes)
    lines.append("=" * 100)

    for i, trial in enumerate(trials):
        ua = trial.user_attrs
        duration = ua.get("duration_s", 0.0)
        seq = seq_by_number.get(trial.number)
        seq_str = f"{seq}" if seq is not None else "?"
        seq_tag = f"[seq {seq_str}]"
        trial_id = f"trial {trial.number}"

        if is_iid:
            crps_total = ua.get("iid_crps_total_games", float("nan"))
            crps_spread = ua.get("iid_crps_spread", float("nan"))
            ll = ua.get("log_loss", float("nan"))
            mae = ua.get("mae", float("nan"))
            lines.append(
                f"  {i + 1:>2}. {seq_tag:<9}  CRPS_total={crps_total:.4f}"
                f"  CRPS_spread={crps_spread:.4f}"
                f"  MAE={mae:.4f}  LL={ll:.4f}  ({duration:.0f}s · {trial_id})"
            )
            # The per-fold breakdown is persisted per trial but was never shown.
            # On a flat plateau the fold spread is often wider than the gap
            # between adjacent trials, which is the thing worth seeing.
            spread = _fold_spread_from(ua, "fold_metrics", sort_by[0])
            if spread:
                lines.append(f"      {sort_by[0]} per fold: {spread}")
            shown = {"iid_crps_total_games", "iid_crps_spread", "mae", "log_loss"}
        elif is_projection:
            mae = ua.get("mae", float("nan"))
            rmse = ua.get("rmse", float("nan"))
            r2 = ua.get("r_squared", float("nan"))
            crps = ua.get("crps")
            crps_str = f"  CRPS={crps:.4f}" if crps is not None else ""
            lines.append(
                f"  {i + 1:>2}. {seq_tag:<9}  MAE={mae:.4f}  RMSE={rmse:.4f}"
                f"  R²={r2:.4f}{crps_str}  ({duration:.0f}s · {trial_id})"
            )
            shown = {"mae", "rmse", "r_squared", "crps"}
        elif use_cal:
            # Classification, deployment-frame view: probability-scale columns
            # come from the calibrated held-out block; AUC stays raw (invariant).
            # A second line shows the raw→calibrated gap for the ranked metric (the
            # "looks better in tuning than it deploys" delta) and its outer spread.
            ll = ua.get("holdout_cal_log_loss", float("nan"))
            brier = ua.get("holdout_cal_brier_score", float("nan"))
            auc = ua.get("holdout_roc_auc", float("nan"))
            acc = ua.get("holdout_cal_accuracy", float("nan"))
            cal = ua.get("holdout_cal_calibration_error", float("nan"))
            cal_max = ua.get("holdout_cal_calibration_error_max", float("nan"))
            oc_max = ua.get("holdout_cal_overconfidence_max", float("nan"))
            scal = ua.get("holdout_cal_signed_calibration", float("nan"))
            err80 = ua.get("holdout_cal_error_rate_80plus", float("nan"))
            lines.append(
                f"  {i + 1:>2}. {seq_tag:<9}  LL={ll:.5f}  brier={brier:.5f}  "
                f"AUC={auc:.5f}  acc={acc:.5f}  cal={cal * 100:.2f}%  "
                f"cal_max={cal_max * 100:.2f}%  oc_max={oc_max * 100:.2f}%  "
                f"scal={scal * 100:+.2f}%  err80={err80 * 100:.1f}%  "
                f"({duration:.0f}s · {trial_id})"
            )
            raw_key = f"holdout_{ref_metric}"
            cal_key = _to_ranked(ref_metric, use_cal)
            ref_parts: list[str] = []
            if not ref_invariant and raw_key in ua and cal_key in ua:
                raw_val, cal_val = ua[raw_key], ua[cal_key]
                ref_parts.append(
                    f"raw {ref_label}={raw_val:.4f} (cal Δ{cal_val - raw_val:+.4f})"
                )
            spread = _fold_spread(ua, ref_metric, use_cal=True)
            if spread:
                ref_parts.append(f"outer {ref_label} {spread}")
            if ref_parts:
                lines.append("      " + "  ·  ".join(ref_parts))
            shown = {
                "holdout_cal_log_loss", "holdout_cal_brier_score",
                "holdout_roc_auc", "holdout_cal_accuracy",
                "holdout_cal_calibration_error", "holdout_cal_calibration_error_max",
                "holdout_cal_overconfidence_max", "holdout_cal_signed_calibration",
                "holdout_cal_error_rate_80plus", raw_key, cal_key,
            }
        else:
            # Classification, raw view (pre-Phase-2 studies with no calibrated
            # holdout metrics). One row per trial showing every standard holdout
            # metric. The user picks a metric NAME — they don't pick in-fold vs
            # holdout. In-fold is Optuna's internal signal during a trial;
            # holdout is the ranking signal across trials.
            ll = ua.get("holdout_log_loss", float("nan"))
            brier = ua.get("holdout_brier_score", float("nan"))
            auc = ua.get("holdout_roc_auc", float("nan"))
            acc = ua.get("holdout_accuracy", float("nan"))
            cal = ua.get("holdout_calibration_error", float("nan"))
            cal_max = ua.get("holdout_calibration_error_max", float("nan"))
            oc_max = ua.get("holdout_overconfidence_max", float("nan"))
            scal = ua.get("holdout_signed_calibration", float("nan"))
            err80 = ua.get("holdout_error_rate_80plus", float("nan"))
            lines.append(
                f"  {i + 1:>2}. {seq_tag:<9}  LL={ll:.5f}  brier={brier:.5f}  "
                f"AUC={auc:.5f}  acc={acc:.5f}  cal={cal * 100:.2f}%  "
                f"cal_max={cal_max * 100:.2f}%  oc_max={oc_max * 100:.2f}%  "
                f"scal={scal * 100:+.2f}%  err80={err80 * 100:.1f}%  "
                f"({duration:.0f}s · {trial_id})"
            )
            shown = {
                "holdout_log_loss", "holdout_brier_score", "holdout_roc_auc",
                "holdout_accuracy", "holdout_calibration_error",
                "holdout_calibration_error_max", "holdout_overconfidence_max",
                "holdout_signed_calibration", "holdout_error_rate_80plus",
            }

        if trial.number == robust_number and robust_band > 1:
            lines.append(
                "      ◆ 1-SE robust pick (most stable across the outer folds)"
            )

        extra = [m for m in sort_by if m not in shown]
        if extra:
            extra_str = "  ".join(
                f"{m}={ua.get(m, float('nan')):.4f}" for m in extra
            )
            lines.append(f"      sort: {extra_str}")

        merged = _decode_params({**trial.params, **pinned})
        for k, v in sorted(merged.items()):
            suffix = "  # pinned" if k in pinned else ""
            lines.append(f"      {k}: {_yaml_value(v)}{suffix}")

    return lines


def format_param_importance(
    study: optuna.Study,
    metric_index: int = 0,
) -> list[str]:
    """Format parameter importance ranking.

    Returns a list of formatted lines.
    """
    trials = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]

    if len(trials) < 2:
        return ["Not enough completed trials for param importance (need >= 2)."]

    lines: list[str] = []
    lines.append("PARAM IMPORTANCE")
    # Importance is computed against the value Optuna optimized, which is NOT
    # the leaderboard's sort metric (that one ranks the held-out block, and is
    # `--sort`-controlled). Two different targets in one report, so name this one.
    obj_key = _objective_key(study, trials) if metric_index == 0 else None
    target = obj_key or f"values[{metric_index}]"
    lines.append(f"  · target: {target} (tuning objective, not the sort metric)")
    lines.append("=" * 60)

    try:
        importance = optuna.importance.get_param_importances(
            study, target=lambda t: t.values[metric_index] if t.values else float("inf"),
        )
        for param, score in importance.items():
            bar = "#" * int(score * 40)
            lines.append(f"  {param:>25}: {score:.3f}  {bar}")
    except Exception as e:
        lines.append(f"  Could not compute importance: {e}")

    return lines


def format_best_trial(study: optuna.Study) -> list[str]:
    """Format details of the best trial.

    Returns a list of formatted lines.
    """
    trials = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    if not trials:
        return ["No completed trials found."]

    pinned = study.user_attrs.get("pinned_params") or {}

    lines: list[str] = []
    lines.append("BEST TRIAL")
    lines.append("=" * 60)

    try:
        best = study.best_trial
    except ValueError:
        # Multi-objective — show Pareto front
        pareto = study.best_trials
        lines.append(f"Multi-objective: {len(pareto)} Pareto-optimal trials")
        for i, trial in enumerate(pareto):
            ua = trial.user_attrs
            metrics_str = ", ".join(
                f"{k}={v:.4f}" if isinstance(v, float) else f"{k}={v}"
                for k, v in sorted(ua.items())
                if k != "duration_s"
            )
            lines.append(f"  {i + 1}. {metrics_str}")
            merged = _decode_params({**trial.params, **pinned})
            params_str = ", ".join(
                f"{k}={_yaml_value(v)}{'(pinned)' if k in pinned else ''}"
                for k, v in sorted(merged.items())
            )
            lines.append(f"     {params_str}")
        return lines

    ua = best.user_attrs
    lines.append(f"  Trial: {best.number}")

    lines.append("  Metrics:")
    for k, v in sorted(ua.items()):
        if k == "duration_s":
            continue
        if isinstance(v, float):
            lines.append(f"    {k}: {v:.4f}")

    lines.append("  Params:")
    merged = _decode_params({**best.params, **pinned})
    for k, v in sorted(merged.items()):
        suffix = "  # pinned" if k in pinned else ""
        lines.append(f"    {k}: {_yaml_value(v)}{suffix}")

    return lines


