"""Review Optuna tuning study results."""

import logging

import optuna

from mvp.model.tuning import _MAXIMIZE_METRICS, _decode_params

logger = logging.getLogger(__name__)


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


def _is_raw_mode_study(trials: list[optuna.trial.FrozenTrial]) -> bool:
    """Return True if the study was tuned post-decoupling refactor.

    Raw-mode studies have `_tuning_mode == "raw"` set on every trial by
    `HyperparamTuner._objective`. Legacy studies (pre-refactor) lack the
    attr entirely; their metrics are Platt-calibrated and not comparable
    to raw-mode trials, so `tune-review` refuses to display them.
    """
    # Check the most recent completed trial: if a study was started pre-refactor
    # and resumed post-refactor, segregation is impossible — fail loud.
    return any(t.user_attrs.get("_tuning_mode") == "raw" for t in trials)


def _to_holdout(metric: str) -> str:
    """Prefix a bare metric name with ``holdout_`` for ranking.

    Optuna optimizes against an in-fold measurement during each trial (the
    only signal available inside the trial); tune-review ranks across
    completed trials using the holdout measurement (the generalization-
    honest version of the same metric). The user shouldn't have to think
    about that split — they pass a metric name, we always rank by the
    holdout version of it. Already-prefixed names pass through unchanged
    so power users can override if needed.
    """
    if metric.startswith("holdout_"):
        return metric
    return f"holdout_{metric}"


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
    if not _is_raw_mode_study(trials):
        return [
            "This study was tuned before the calibration-decoupling refactor.",
            "Trial metrics are post-Platt and not comparable to raw-mode tuning.",
            "",
            "Delete the study DB and start fresh:",
            "  rm <data_root>/tuning/<config_name>.db",
            "  poetry run py -m mvp tune <config>",
        ]

    # Detect config type from first trial's metrics
    first_ua = trials[0].user_attrs
    is_iid = "iid_crps_total_games" in first_ua
    is_projection = "mae" in first_ua and "log_loss" not in first_ua

    # Default sort: the honest (holdout) metric for this config family.
    # When the user passes a bare metric name (e.g. `--sort log_loss`), we
    # auto-prefix it to its holdout version — Optuna optimizes in-fold per
    # trial, but ranking across completed trials should always use the
    # holdout measurement of that same metric.
    if sort_by is None:
        if is_iid:
            sort_by = [_to_holdout("iid_crps_total_games")]
        elif is_projection:
            sort_by = [_to_holdout("mae")]
        else:
            sort_by = [_to_holdout("log_loss")]
    else:
        sort_by = [_to_holdout(m) for m in sort_by]

    # Confirm holdout metrics exist for the requested sort metric(s). Studies
    # tuned with holdout_folds=0 won't have them; the runner always uses
    # holdout_folds=1 for tuning, so this catches misconfigured studies only.
    has_any_holdout = any(
        any(k.startswith("holdout_") for k in t.user_attrs) for t in trials
    )
    if not has_any_holdout and any(m.startswith("holdout_") for m in sort_by):
        return [
            "No holdout metrics found on any trial in this study.",
            "Studies must be tuned with holdout_folds >= 1 (the default for",
            "`mvp tune`). Start a fresh study and try again.",
        ]

    # Sort by the requested metrics — flip sign for maximize-direction metrics
    # so ascending sort puts the best trial first. Holdout-prefixed metrics
    # inherit their underlying metric's direction.
    def _direction_key(m: str) -> str:
        return m[len("holdout_"):] if m.startswith("holdout_") else m

    def sort_key(t: optuna.trial.FrozenTrial) -> tuple:
        return tuple(
            -t.user_attrs.get(m, float("-inf")) if _direction_key(m) in _MAXIMIZE_METRICS
            else t.user_attrs.get(m, float("inf"))
            for m in sort_by
        )

    trials.sort(key=sort_key)
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

    lines: list[str] = []
    sort_label = ", ".join(sort_by)
    lines.append(
        f"TOP {top_n} TRIALS (raw discrimination, sorted by {sort_label})"
    )
    lines.append(
        "Metrics below reflect uncalibrated predictor quality. Calibration "
        "is applied separately by `mvp model` at training time."
    )
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
        else:
            # Classification leaderboard: one row per trial showing every
            # standard holdout metric. The user picks a metric NAME — they
            # don't pick in-fold vs holdout. In-fold is Optuna's internal
            # signal during a trial; holdout is the ranking signal across
            # trials. Both live in user_attrs but the leaderboard only
            # surfaces holdout because that's the selection-relevant view.
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
                f"  {i + 1:>2}. {seq_tag:<9}  LL={ll:.4f}  brier={brier:.4f}  "
                f"AUC={auc:.4f}  acc={acc:.4f}  cal={cal * 100:.2f}%  "
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


