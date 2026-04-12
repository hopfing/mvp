"""Review Optuna tuning study results."""

import logging

import optuna

logger = logging.getLogger(__name__)


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

    # Detect projection vs classification from first trial's metrics
    first_ua = trials[0].user_attrs
    is_projection = "mae" in first_ua and "log_loss" not in first_ua

    if sort_by is None:
        sort_by = ["mae"] if is_projection else ["log_loss"]

    # Sort by the requested metrics (ascending for all — lower is better)
    def sort_key(t: optuna.trial.FrozenTrial) -> tuple:
        return tuple(t.user_attrs.get(m, float("inf")) for m in sort_by)

    trials.sort(key=sort_key)
    trials = trials[:top_n]

    lines: list[str] = []
    sort_label = ", ".join(sort_by)
    lines.append(f"TOP {top_n} TRIALS (sorted by {sort_label})")
    lines.append("=" * 100)

    for i, trial in enumerate(trials):
        ua = trial.user_attrs
        duration = ua.get("duration_s", 0.0)

        if is_projection:
            mae = ua.get("mae", float("nan"))
            rmse = ua.get("rmse", float("nan"))
            r2 = ua.get("r_squared", float("nan"))
            crps = ua.get("crps")
            crps_str = f"  CRPS={crps:.4f}" if crps is not None else ""
            lines.append(
                f"  {i + 1:>2}. MAE={mae:.4f}  RMSE={rmse:.4f}"
                f"  R²={r2:.4f}{crps_str}  ({duration:.0f}s)"
            )
        else:
            ll = ua.get("log_loss", float("nan"))
            cal = ua.get("calibration_error", float("nan"))
            scal = ua.get("signed_calibration")
            err80 = ua.get("error_rate_80plus", float("nan"))

            scal_str = f"  scal={scal * 100:+.2f}%" if scal is not None else ""
            lines.append(
                f"  {i + 1:>2}. LL={ll:.4f}  cal={cal * 100:.2f}%{scal_str}"
                f"  err80={err80 * 100:.1f}%  ({duration:.0f}s)"
            )

        for k, v in sorted(trial.params.items()):
            lines.append(f"      {k}: {v}")

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
            params_str = ", ".join(
                f"{k}={v}" for k, v in sorted(trial.params.items())
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
    for k, v in sorted(best.params.items()):
        lines.append(f"    {k}: {v}")

    return lines


