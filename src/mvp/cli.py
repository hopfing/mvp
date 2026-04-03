"""Unified CLI entry point."""


import argparse
import logging
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

warnings.filterwarnings("ignore", message="All-NaN slice encountered")
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn.utils.parallel")

import polars as pl

from mvp.common.base_job import get_data_root, get_local_data_root


@dataclass
class BookConfig:
    """Configuration for a sportsbook integration."""

    code: str           # "dk", "br", "mgm", "b365"
    label: str          # "DK", "BR", "MGM", "B365"
    display_name: str   # "DraftKings", "BetRivers", "BetMGM", "Bet365"
    domain: str         # "draftkings", "betrivers", "betmgm", "bet365"
    event_id_col: str   # "dk_event_id", etc.
    stage_rel: str      # "stage/draftkings/moneyline.parquet"
    aliases_rel: str    # "draftkings/player_aliases.yaml"
    matcher_class: str  # "DraftKingsOddsMatcher"


BOOK_REGISTRY: list[BookConfig] = [
    BookConfig("dk", "DK", "DraftKings", "draftkings", "dk_event_id",
               "stage/draftkings/moneyline.parquet", "draftkings/player_aliases.yaml",
               "DraftKingsOddsMatcher"),
    BookConfig("br", "BR", "BetRivers", "betrivers", "br_event_id",
               "stage/betrivers/moneyline.parquet", "betrivers/player_aliases.yaml",
               "BetRiversOddsMatcher"),
    BookConfig("mgm", "MGM", "BetMGM", "betmgm", "mgm_event_id",
               "stage/betmgm/moneyline.parquet", "betmgm/player_aliases.yaml",
               "BetMGMOddsMatcher"),
    BookConfig("b365", "B365", "Bet365", "bet365", "b365_event_id",
               "stage/bet365/moneyline.parquet", "bet365/player_aliases.yaml",
               "Bet365OddsMatcher"),
]

logger = logging.getLogger(__name__)


def print_run_summary(results: dict[str, Any], name: str | None = None) -> None:
    """Print formatted summary of experiment results."""
    metrics = results.get("metrics", {})
    train_metrics = results.get("train_metrics", {})
    diagnostics = results.get("diagnostics")

    # Header
    print("\n" + "=" * 70)
    title = name or "RESULTS"
    print(f"{title:^70}")
    print("=" * 70)

    # Train vs Test metrics
    test_acc = metrics.get("accuracy", 0)
    test_auc = metrics.get("roc_auc", 0)
    test_ll = metrics.get("log_loss", 0)
    test_brier = metrics.get("brier_score", 0)

    if train_metrics:
        train_acc = train_metrics.get("accuracy", 0)
        train_auc = train_metrics.get("roc_auc", 0)
        train_ll = train_metrics.get("log_loss", 0)
        train_brier = train_metrics.get("brier_score", 0)
        print(f"\n{'':8} {'Accuracy':>10} {'AUC':>10} {'Log Loss':>11} {'Brier':>10}")
        print(f"{'Train':8} {train_acc:>10.1%} {train_auc:>10.3f} {train_ll:>11.4f} {train_brier:>10.4f}")
        print(f"{'Test':8} {test_acc:>10.1%} {test_auc:>10.3f} {test_ll:>11.4f} {test_brier:>10.4f}")
    else:
        print(f"\nTest: {test_acc:.1%} acc | {test_auc:.3f} AUC | {test_ll:.4f} LL | {test_brier:.4f} Brier")


    if not diagnostics:
        print(f"\nMLflow run: {results.get('run_id', 'N/A')}")
        print("=" * 70 + "\n")
        return

    # Circuit-based segments with subsegments
    segments = diagnostics.segments
    by_circuit = segments.get("by_circuit", {})

    if by_circuit:
        print("\nSegments by Circuit:")
        for circuit in sorted(by_circuit.keys()):
            circuit_data = by_circuit[circuit]
            overall = circuit_data.get("overall", {})

            # Circuit header with overall metrics
            acc = overall.get('accuracy', 0)
            auc = overall.get('roc_auc', 0)
            ll = overall.get('log_loss', 0)
            brier = overall.get('brier_score', 0)
            cal = overall.get('calibration_error', 0)
            err = overall.get('error_rate_80plus', 0)
            n = overall.get('n_matches', 0)
            print(f"\n  {circuit.upper()}  {acc:5.1%} acc | {auc:.3f} AUC | {ll:.4f} ll | {brier:.4f} brier | {cal:.2%} cal | {err:.1%} err80 | n={n:,}")

            # Surface subsegments
            if circuit_data.get("surface"):
                print("    surface:")
                for surface, m in sorted(circuit_data["surface"].items()):
                    acc = m.get('accuracy', 0)
                    auc = m.get('roc_auc', 0)
                    ll = m.get('log_loss', 0)
                    brier = m.get('brier_score', 0)
                    cal = m.get('calibration_error', 0)
                    err = m.get('error_rate_80plus', 0)
                    n = m.get('n_matches', 0)
                    print(f"      {surface:8} {acc:5.1%} | {auc:.3f} | {ll:.4f} | {brier:.4f} | {cal:.2%} | {err:.1%} | n={n:,}")

            # Per-round metrics
            if circuit_data.get("round"):
                print("    round:")
                from mvp.model.diagnostics import ROUND_ORDER
                for rnd in ROUND_ORDER:
                    if rnd not in circuit_data["round"]:
                        continue
                    m = circuit_data["round"][rnd]
                    acc = m.get('accuracy', 0)
                    auc = m.get('roc_auc', 0)
                    ll = m.get('log_loss', 0)
                    brier = m.get('brier_score', 0)
                    cal = m.get('calibration_error', 0)
                    err = m.get('error_rate_80plus', 0)
                    n = m.get('n_matches', 0)
                    print(f"      {rnd:10} {acc:5.1%} | {auc:.3f} | {ll:.4f} | {brier:.4f} | {cal:.2%} | {err:.1%} | n={n:,}")

            # Betting group subsegments (circuit-aware performance groups)
            if circuit_data.get("betting_group"):
                print("    betting group:")
                for group, m in circuit_data["betting_group"].items():
                    acc = m.get('accuracy', 0)
                    auc = m.get('roc_auc', 0)
                    ll = m.get('log_loss', 0)
                    brier = m.get('brier_score', 0)
                    cal = m.get('calibration_error', 0)
                    err = m.get('error_rate_80plus', 0)
                    n = m.get('n_matches', 0)
                    print(f"      {group:10} {acc:5.1%} | {auc:.3f} | {ll:.4f} | {brier:.4f} | {cal:.2%} | {err:.1%} | n={n:,}")

    # Calibration buckets table
    cal_data = diagnostics.calibration
    if cal_data and cal_data.get("buckets"):
        buckets = cal_data["buckets"]
        worst_err = max(b["error"] for b in buckets)
        raw_cal = metrics.get("raw_calibration_error")
        if raw_cal is not None:
            print(f"\nCalibration ({cal_data['calibration_error']:.2%} mean error, {raw_cal:.2%} raw):")
        else:
            print(f"\nCalibration ({cal_data['calibration_error']:.2%} mean error):")
        for b in buckets:
            low, high = b["range"]
            marker = " <- worst" if b["error"] == worst_err else ""
            n_bucket_errors = int(round(b['n'] * (1.0 - b['actual']))) if b['actual'] < 1.0 else 0
            print(f"  {low:.0%}-{high:.0%}  pred={b['predicted_mean']:.1%}  "
                  f"actual={b['actual']:.1%}  err={b['error']:.1%}  n={b['n']:,}  "
                  f"errors={n_bucket_errors:,}{marker}")
        under = sum(1 for b in buckets if b["predicted_mean"] < b["actual"])
        over = sum(1 for b in buckets if b["predicted_mean"] > b["actual"])
        tied = len(buckets) - under - over
        parts = []
        if under:
            parts.append(f"{under} underconfident")
        if over:
            parts.append(f"{over} overconfident")
        if tied:
            parts.append(f"{tied} exact")
        label = "UNDERCONFIDENT" if under > over else "OVERCONFIDENT" if over > under else "BALANCED"
        print(f"  Direction: {label} ({', '.join(parts)})")

        calibrator = results.get("calibrator")
        if calibrator is not None and calibrator.is_fitted:
            print(f"  Platt: slope={calibrator.slope:.4f}, intercept={calibrator.intercept:.4f}")

    # High-confidence errors
    errors = diagnostics.errors
    if errors and "summary" in errors:
        e80 = errors["summary"].get("80plus", {})
        if e80.get("total", 0) > 0:
            print(f"High-conf errors: {e80['error_rate']:.1%} of {e80['total']:,} predictions at 80%+ were wrong")

    # Error conditions
    error_conds = diagnostics.error_conditions
    if error_conds and error_conds.get("conditions"):
        total_err = error_conds.get("total_errors", 0)
        print(f"\nError Conditions (total errors: {total_err:,}):")
        print(f"  {'Condition':30} {'Matches':>8}  {'Accuracy':>8}  {'Errors':>7}  {'Error Share':>11}")
        for c in error_conds["conditions"]:
            print(f"  {c['label']:30} {c['n_matches']:>8,}  {c['accuracy']:>7.1%}  {c['n_errors']:>7,}  {c['error_share']:>10.1%}")

    # Temporal
    temporal = diagnostics.temporal
    if temporal and temporal.get("temporal_drift", 0) > 0:
        print(f"Temporal drift: ±{temporal['temporal_drift']:.1%} from average")

    # Ensemble diagnostics
    if diagnostics.ensemble:
        ediag = diagnostics.ensemble
        per_model = ediag.get("per_model_metrics", {})
        if per_model:
            print("\nPer-Model Comparison:")
            print(f"  {'Model':40} {'Acc':>7} {'AUC':>7} {'LL':>8} {'Cal':>7}")
            print(f"  {'-' * 69}")
            for model_name, m in per_model.items():
                label = model_name
                if model_name != "ensemble":
                    # Shorten path to just filename stem
                    label = Path(model_name).stem
                else:
                    label = "ENSEMBLE"
                acc = m.get("accuracy", 0)
                auc = m.get("roc_auc", 0)
                ll = m.get("log_loss", 0)
                cal = m.get("calibration_error", 0)
                print(f"  {label:40} {acc:6.1%} {auc:7.3f} {ll:8.4f} {cal:7.2%}")

        corr = ediag.get("correlation", {})
        matrix = corr.get("matrix", [])
        names = corr.get("names", [])
        if len(matrix) >= 2:
            print("\n  Prediction Correlations:")
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    n_i = Path(names[i]).stem
                    n_j = Path(names[j]).stem
                    print(f"    {n_i} ↔ {n_j}: {matrix[i][j]:.3f}")

        consensus = ediag.get("consensus", {})
        buckets = consensus.get("buckets", [])
        if buckets:
            print("\n  Consensus Strength:")
            for b in buckets:
                print(f"    {b['label']:6} {b['accuracy']:5.1%} acc  n={b['count']:,} ({b['pct']:.1%})")

        dissenter = ediag.get("dissenter", {})
        if dissenter:
            print("\n  Lone Dissenter Accuracy:")
            for model_name, d in dissenter.items():
                label = Path(model_name).stem
                count = d.get("count", 0)
                if count == 0:
                    print(f"    {label:35} never lone dissenter")
                else:
                    d_acc = d.get("dissenter_correct", 0)
                    m_acc = d.get("majority_correct", 0)
                    print(f"    {label:35} {d_acc:5.1%} vs majority {m_acc:5.1%}  (n={count:,})")

        contrib = ediag.get("contribution", {})
        if contrib:
            print("\n  Leave-One-Out (positive = removing hurts):")
            for model_name, c in contrib.items():
                label = Path(model_name).stem
                ll_delta = c.get("log_loss_delta", 0)
                cal_delta = c.get("calibration_delta", 0)
                ll_sign = "+" if ll_delta >= 0 else ""
                cal_sign = "+" if cal_delta >= 0 else ""
                print(f"    Remove {label:35} LL {ll_sign}{ll_delta:.4f}  Cal {cal_sign}{cal_delta:.4f}")

        meta_coefs = ediag.get("meta_coefficients")
        if meta_coefs is not None:
            meta_intercept = ediag.get("meta_intercept", 0.0)
            base_coefs = {}
            feat_coefs = {}
            for name, coef in meta_coefs.items():
                if "/" in name or name.endswith(".yaml"):
                    base_coefs[name] = coef
                else:
                    feat_coefs[name] = coef
            print(f"\n  Stacking Meta-Model Coefficients (intercept={meta_intercept:+.4f}):")
            if base_coefs:
                print("    Base models:")
                for name, coef in base_coefs.items():
                    label = Path(name).stem
                    print(f"      {label:40} {coef:+.4f}")
            if feat_coefs:
                print("    Meta-features:")
                for name, coef in feat_coefs.items():
                    print(f"      {name:40} {coef:+.4f}")
            if not base_coefs and not feat_coefs:
                for name, coef in meta_coefs.items():
                    label = Path(name).stem
                    print(f"    {label:40} {coef:+.4f}")

        correction = ediag.get("correction_analysis", {})
        corr_sections = correction.get("sections", [])
        if corr_sections:
            from mvp.model.config import EnsembleParams
            ens_params = None
            try:
                config = results.get("_config")
                if config and config.model.params:
                    ens_params = EnsembleParams.model_validate(config.model.params)
            except Exception:
                pass
            primary_label = Path(ens_params.base_models[0].config).stem if ens_params else "primary"
            print(f"\n  Correction Analysis (primary={primary_label}):")
            for section in corr_sections:
                print(f"\n    {section['section']}:")
                print(f"      {'':25} {'Matches':>8} {'Primary':>8} {'Ensemble':>9} {'Improv':>8}")
                for r in section["rows"]:
                    imp = r['improvement']
                    sign = "+" if imp >= 0 else ""
                    print(f"      {r['label']:25} {r['n_matches']:>8,} {r['primary_accuracy']:>7.1%} {r['ensemble_accuracy']:>8.1%} {sign}{imp:>7.1%}")

    print(f"\nMLflow run: {results.get('run_id', 'N/A')}")
    print("=" * 70 + "\n")

_CIRCUIT_LABELS = {"tour": "ATP", "chal": "Challenger"}


def print_predictions(
    predictions: Any,
    book_odds: dict[str, dict[str, dict[str, float]]] | None = None,
) -> None:
    """Print human-readable prediction summary."""

    has_odds = bool(book_odds)
    width = 105 if has_odds else 78
    print("\n" + "=" * width)
    print(f"{'PREDICTIONS':^{width}}")
    print("=" * width)
    print(f"\n{len(predictions)} matches\n")

    # Pre-compute min date per tournament for headers
    tournament_dates: dict[str, str] = {}
    for row in predictions.iter_rows(named=True):
        key = row.get("tournament_name") or "Unknown"
        dt = row.get("effective_match_date")
        if dt is not None:
            date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
            if key not in tournament_dates or date_str < tournament_dates[key]:
                tournament_dates[key] = date_str

    # Group by tournament for readability
    sorted_df = predictions.sort(["tournament_name", "effective_match_date", "round"])
    current_tournament = None

    for row in sorted_df.iter_rows(named=True):
        tournament = row.get("tournament_name") or "Unknown"
        if tournament != current_tournament:
            current_tournament = tournament
            circuit = row.get("circuit") or ""
            label = _CIRCUIT_LABELS.get(circuit, circuit.upper())
            surface = row.get("surface") or ""
            date_str = tournament_dates.get(tournament, "")
            print(f"\n  {label} {tournament} ({surface}) {date_str}")
            print(f"  {'-' * 60}")

        p1 = row.get("p1_name") or "TBD"
        p2 = row.get("p2_name") or "TBD"
        p1_prob = row.get("p1_win_prob") or 0.5
        p2_prob = row.get("p2_win_prob") or 0.5
        rnd = row.get("round") or ""

        consensus = row.get("consensus")
        consensus_tag = f"  [{consensus:.0%}]" if consensus is not None else ""
        line = f"  {rnd:5} {p1:25} {p1_prob:5.1%}  vs  {p2_prob:5.1%} {p2}{consensus_tag}"

        if has_odds:
            match_uid = row.get("match_uid") or ""
            p1_id = row.get("p1_id") or ""
            p2_id = row.get("p2_id") or ""
            odds_parts = []
            for bcode, odds_for_book in book_odds.items():
                match_odds = odds_for_book.get(match_uid)
                if match_odds and p1_id:
                    o1 = match_odds.get(p1_id)
                    o2 = match_odds.get(p2_id)
                    if o1 is not None and o2 is not None:
                        odds_parts.append(f"{bcode.upper()}:{o1:.2f}/{o2:.2f}")
            if odds_parts:
                line += "  " + " | ".join(odds_parts)

        print(line)

    print("\n" + "=" * width + "\n")


# Default directories for each command
MODEL_DIR = Path("models")
EXPERIMENT_DIR = Path("experiments")
PROJECTION_DIR = Path("projections")


def resolve_config_path(name: str, default_dir: Path) -> Path:
    """Resolve config path, checking default directory if not found."""
    path = Path(name)
    if path.exists():
        return path

    # Try default directory
    default_path = default_dir / name
    if default_path.exists():
        return default_path

    # Try with .yaml extension
    if not name.endswith(".yaml"):
        yaml_path = default_dir / f"{name}.yaml"
        if yaml_path.exists():
            return yaml_path

    # Return original for error message
    return path


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments with subcommands."""
    parser = argparse.ArgumentParser(
        prog="python -m mvp",
        description="MVP sports prediction pipeline",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # model subcommand - trains from models/ directory
    model_parser = subparsers.add_parser(
        "model", help="Train model (looks in models/ by default)"
    )
    model_parser.add_argument(
        "config", type=str, help="Config name or path (e.g., 'baseline' or 'baseline.yaml')"
    )
    model_parser.add_argument(
        "--refresh", action="store_true", help="Rebuild matches.parquet before running"
    )
    model_parser.add_argument(
        "--memory-limit", type=int, default=None,
        help="Override memory limit %% (0 to disable, default 75)",
    )

    # experiment subcommand - discovery from experiments/ directory
    exp_parser = subparsers.add_parser(
        "experiment", help="Run experiment/discovery (looks in experiments/ by default)"
    )
    exp_parser.add_argument(
        "config", type=str, help="Config name or path (e.g., 'discover' or 'discover.yaml')"
    )
    exp_parser.add_argument(
        "--output", "-o", type=str, required=True,
        help="Output filename for discovered config (saved to models/)"
    )
    exp_parser.add_argument(
        "--refresh", action="store_true", help="Rebuild matches.parquet before running"
    )
    exp_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Print progress"
    )
    exp_parser.add_argument(
        "--n-jobs", type=int, default=None,
        help="Override n_jobs for parallelism (limits CPU usage)",
    )
    exp_parser.add_argument(
        "--memory-limit", type=int, default=None,
        help="Override memory limit %% (0 to disable, default 75)",
    )

    # tune subcommand - Bayesian hyperparameter optimization
    tune_parser = subparsers.add_parser(
        "tune", help="Tune model hyperparameters via Optuna Bayesian optimization"
    )
    tune_parser.add_argument(
        "config", type=str,
        help="Model config to tune (looks in models/)",
    )
    tune_parser.add_argument(
        "--metric", type=str, nargs="+", default=["log_loss"],
        help="Metric(s) to optimize (default: log_loss). Multiple = multi-objective.",
    )
    tune_parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Log per-trial results",
    )
    tune_parser.add_argument(
        "--memory-limit", type=int, default=None,
        help="Override memory limit %% (0 to disable, default 75)",
    )
    tune_parser.add_argument(
        "--param", action="append", metavar="KEY=VALUE",
        help="Fix a param to a specific value (e.g. --param n_estimators=300)",
    )
    tune_parser.add_argument(
        "--limit", type=int, required=True,
        help="Number of trials to run",
    )

    # tune-review subcommand
    tune_review_parser = subparsers.add_parser(
        "tune-review", help="Review tuning results"
    )
    tune_review_parser.add_argument(
        "config", type=str,
        help="Model config to review (looks in models/)",
    )
    tune_review_parser.add_argument(
        "--top", type=int, default=15,
        help="Number of top trials to show (default: 15)",
    )
    tune_review_parser.add_argument(
        "--sort", type=str, nargs="+", default=["log_loss"],
        help="Metric(s) to sort by (default: log_loss)",
    )
    tune_review_parser.add_argument(
        "--dashboard", action="store_true",
        help="Launch optuna-dashboard in browser",
    )

    # train subcommand - train production model
    subparsers.add_parser(
        "train", help="Train (or retrain) the production model from production.yaml"
    )

    # live subcommand
    live_parser = subparsers.add_parser(
        "live", help="Run live pipeline for active tournaments"
    )
    live_parser.add_argument(
        "--tid", type=str, metavar="TID", help="Target a single active tournament"
    )
    live_parser.add_argument(
        "--refresh", action="store_true", help="Force re-extraction of all data"
    )
    live_parser.add_argument(
        "--refresh-players",
        action="store_true",
        help="Run activity extraction/staging (skipped by default)",
    )

    # confidence subcommand
    conf_parser = subparsers.add_parser(
        "confidence", help="Run confidence validation on a model's OOF predictions"
    )
    conf_parser.add_argument(
        "config", type=str, help="Model config name (e.g., 'tu_log_fs_75_20f')"
    )
    conf_parser.add_argument(
        "--no-refresh", action="store_true",
        help="Use cached OOF if available (skip model re-run)"
    )

    # project subcommand - game projection from projections/ directory
    proj_parser = subparsers.add_parser(
        "project", help="Run game projection (looks in projections/ by default)"
    )
    proj_parser.add_argument(
        "config", type=str, help="Config name or path (e.g., 'baseline')"
    )
    proj_parser.add_argument(
        "--refresh", action="store_true", help="Rebuild matches.parquet before running"
    )

    # analysis subcommand
    analysis_parser = subparsers.add_parser(
        "analysis", help="Build analysis dataset with odds, CLV, and simulations"
    )
    analysis_parser.add_argument(
        "--no-ui",
        action="store_true",
        help="Run pipeline only, skip dashboard",
    )

    return parser.parse_args(args)


def _get_target_sections(production_config_path: Path | str = "production.yaml") -> list[str]:
    """Return available target sections from production.yaml.

    Flat format (legacy): returns ["winner"].
    Sectioned format: returns all section keys that have an 'active' entry.
    """
    import yaml

    with open(production_config_path) as f:
        raw = yaml.safe_load(f)
    if "active" in raw:
        return ["winner"]
    return [k for k, v in raw.items() if isinstance(v, dict) and "active" in v]


def cmd_train(args: argparse.Namespace) -> int:
    """Train the production model from production.yaml."""
    from mvp.atptour.aggregators.matches import MatchesAggregator
    from mvp.model.predictor import ProductionPredictor

    logger.info("Rebuilding matches.parquet")
    MatchesAggregator().run()

    for section in _get_target_sections():
        print(f"\n--- Training {section} models ---")
        predictor = ProductionPredictor(target_section=section)
        predictor.train()
        print(f"{section.capitalize()} model trained and saved.")
        n_voters = predictor.train_voters()
        if n_voters > 0:
            print(f"Trained {n_voters} {section} voter model(s).")
    return 0


def cmd_tune(args: argparse.Namespace) -> int:
    """Run Bayesian hyperparameter optimization."""
    from mvp.model.tuning import HyperparamTuner

    config_path = resolve_config_path(args.config, MODEL_DIR)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {args.config} (tried {config_path})"
        )

    param_overrides = {}
    if args.param:
        for p in args.param:
            if "=" not in p:
                raise ValueError(f"Invalid --param format: {p} (expected KEY=VALUE)")
            k, v = p.split("=", 1)
            import json as _json
            v_lower = v.lower()
            if v_lower == "none":
                v = None
            elif v_lower in ("true", "false"):
                v = _json.loads(v_lower)
            else:
                try:
                    v = _json.loads(v)
                except (_json.JSONDecodeError, ValueError):
                    try:
                        v = int(v)
                    except ValueError:
                        try:
                            v = float(v)
                        except ValueError:
                            pass
            param_overrides[k] = v

    tuner = HyperparamTuner(
        config_path=config_path,
        param_overrides=param_overrides or None,
        metrics=args.metric,
    )

    study = tuner.run(n_trials=args.limit, verbose=args.verbose)
    logger.info("Results saved to %s", tuner.db_path)
    logger.info("Total trials: %d", len(study.trials))
    return 0


def cmd_tune_review(args: argparse.Namespace) -> int:
    """Review tuning results."""
    import optuna

    from mvp.common.base_job import get_data_root
    from mvp.model.tune_review import (
        format_leaderboard,
        format_param_importance,
    )

    config_path = resolve_config_path(args.config, MODEL_DIR)
    state_dir = get_data_root() / "tuning"
    db_path = state_dir / f"{config_path.stem}.db"

    if not db_path.exists():
        print(f"No tuning results found: {db_path}")
        return 1

    if args.dashboard:
        import subprocess
        import sys

        print(f"Launching dashboard for {db_path}...")
        subprocess.run(
            [sys.executable, "-m", "optuna_dashboard", f"sqlite:///{db_path}"],
        )
        return 0

    storage = f"sqlite:///{db_path}"
    study = optuna.load_study(
        study_name=config_path.stem,
        storage=storage,
    )

    completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    print(f"Study: {study.study_name}")
    print(f"Total trials: {len(completed)}")
    print()

    for line in format_leaderboard(study, sort_by=args.sort, top_n=args.top):
        print(line)
    print()

    for line in format_param_importance(study):
        print(line)
    print()

    return 0


def cmd_model(args: argparse.Namespace) -> int:
    """Run model training from config."""
    from mvp.model.runner import ExperimentRunner

    config_path = resolve_config_path(args.config, MODEL_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config} (tried {config_path})")

    if args.refresh:
        from mvp.atptour.aggregators.matches import MatchesAggregator
        logger.info("Rebuilding matches.parquet")
        MatchesAggregator().run()

    runner = ExperimentRunner(config_path=config_path)
    results = runner.run()

    print_run_summary(results, name=runner.run_name)

    return 0


def _is_voter_config(config_path: Path) -> bool:
    """Check if a config file is a voter-system config (has active + voters).

    Supports both flat format ({active, voters}) and sectioned format
    ({winner: {active, voters}, ...}).
    """
    import yaml

    with open(config_path) as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        return False
    # Flat format
    if "active" in data and "voters" in data:
        return True
    # Sectioned format: check if any section has active + voters
    for value in data.values():
        if isinstance(value, dict) and "active" in value and "voters" in value:
            return True
    return False


def _resolve_voter_section(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Extract the winner section from a production config.

    Supports flat ({active, voters}) and sectioned ({winner: {active, voters}}).
    """
    if "active" in raw_config:
        return raw_config
    if "winner" in raw_config:
        return raw_config["winner"]
    raise ValueError("No winner section found in config")


def _run_voter_confidence(args: argparse.Namespace, config_path: Path) -> int:
    """Run confidence validation with voter consensus overlay.

    Mirrors production behavior: each voter is trained on its own filtered
    data up to the fold's training cutoff, then predicts on the full
    (unfiltered) primary test set. Only the binary pick is used for consensus.
    """

    import numpy as np
    import yaml

    from mvp.model.confidence.report import format_report
    from mvp.model.confidence.validator import ConfidenceValidator, prepare_oof
    from mvp.model.config import (
        ExperimentConfig,
        apply_filters,
        get_filter_feature_specs,
    )
    from mvp.model.engine import FeatureEngine, get_feature_columns
    from mvp.model.models import get_model
    from mvp.model.runner import ExperimentRunner
    from mvp.model.splitters import make_splitter

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    voter_config = _resolve_voter_section(raw_config)
    config_name = config_path.stem
    oof_dir = get_data_root() / "confidence" / config_name
    oof_path = oof_dir / "oof.parquet"
    voter_names = [v["name"] for v in voter_config["voters"]]

    if oof_path.exists() and args.no_refresh:
        logger.info("Loading cached OOF from %s", oof_path)
        oof_df = pl.read_parquet(oof_path)
        validator = ConfidenceValidator.from_oof(oof_df, voter_names=voter_names)
    else:
        # Run primary model
        primary_config_path = resolve_config_path(voter_config["active"]["config"], MODEL_DIR)
        logger.info("Running primary model: %s", primary_config_path)
        primary_runner = ExperimentRunner(config_path=primary_config_path, log_to_mlflow=False)
        primary_results = primary_runner.run()
        primary_predictions = primary_results["all_predictions"]

        # Build primary OOF
        oof_df = prepare_oof(primary_predictions)

        # Get primary config for splitter and date range
        primary_config = ExperimentConfig.from_file(str(primary_config_path))
        primary_val = primary_config.validation
        primary_splitter = make_splitter(
            val_type=primary_val.type,
            n_splits=primary_val.n_splits,
            min_train_size=primary_val.min_train_size,
            test_size=primary_val.test_size,
            initial_train_size=primary_val.initial_train_size,
            step_size=primary_val.step_size,
            train_size=primary_val.train_size,
            test_start=primary_val.test_start,
        )

        # Get primary's full filtered DataFrame for fold replay
        FeatureEngine(
            matches_path=get_data_root() / "aggregate" / "atptour" / "matches.parquet",
            cache_dir=get_local_data_root() / "features" / "cache",
        )

        # For each voter: replay primary folds, train on voter-filtered
        # training data, predict on full primary test set
        for voter_entry in voter_config["voters"]:
            name = voter_entry["name"]
            voter_path = resolve_config_path(voter_entry["config"], MODEL_DIR)
            logger.info("Running voter: %s (%s)", name, voter_path)
            voter_cfg = ExperimentConfig.from_file(str(voter_path))

            # Compute voter features on the full (unfiltered) match set
            assert voter_cfg.features is not None
            voter_feature_specs = voter_cfg.features.include
            compute_only = voter_cfg.features.compute_only if voter_cfg.features.compute_only else []
            filter_specs = get_filter_feature_specs(voter_cfg.data.filters)
            extra = compute_only + filter_specs
            all_specs = voter_feature_specs + [s for s in extra if s not in voter_feature_specs]

            voter_engine = FeatureEngine(
                matches_path=get_data_root() / "aggregate" / "atptour" / "matches.parquet",
                cache_dir=get_local_data_root() / "features" / "cache",
            )
            voter_df = voter_engine.compute(all_specs, extra_columns=[
                "won", "reason", "sets_played", "best_of",
                "circuit", "surface", "round", "draw_type",
            ])

            # Apply primary's non-filter constraints (date range, target)
            # so voter_df aligns with the primary's row set
            if primary_config.data.filters:
                voter_df = apply_filters(voter_df, primary_config.data.filters)
            voter_df = voter_df.filter(
                (pl.col("effective_match_date") >= primary_config.data.date_range.start)
                & (pl.col("effective_match_date") <= primary_config.data.date_range.end)
            )
            voter_df = voter_df.filter(pl.col("won").is_not_null())
            # Filter walkovers to match primary
            if "reason" in voter_df.columns:
                voter_df = voter_df.filter(pl.col("reason").fill_null("").ne("W/O"))

            voter_feature_cols = get_feature_columns(voter_feature_specs)

            # Build imputation specs for voter features
            from mvp.model.imputation import (
                apply_imputation,
                build_imputation,
                fit_imputation,
            )
            from mvp.model.registry import get_registry
            voter_build = build_imputation(voter_feature_specs, get_registry())
            voter_augmented_cols = voter_feature_cols + voter_build.aux_base_col_names
            voter_n_model = voter_build.n_model_features

            # Replay primary folds on voter data
            voter_fold_probs: list[np.ndarray] = []
            voter_fold_keys: list[pl.DataFrame] = []
            for fold_idx, (train_idx, test_idx) in enumerate(primary_splitter.split(voter_df)):
                train_fold = voter_df[train_idx]
                test_fold = voter_df[test_idx]

                # Apply voter's own filters to training data only
                if voter_cfg.data.filters:
                    train_filtered = apply_filters(train_fold, voter_cfg.data.filters)
                else:
                    train_filtered = train_fold

                X_train = train_filtered.select(
                    pl.col(c).cast(pl.Float64) for c in voter_augmented_cols
                ).to_numpy()
                y_train = train_filtered["won"].to_numpy().astype(int)

                # Predict on full (unfiltered) test set
                X_test = test_fold.select(
                    pl.col(c).cast(pl.Float64) for c in voter_augmented_cols
                ).to_numpy()

                # Impute and scale (matches runner preprocessing)
                circuit_train = train_filtered["circuit"].to_numpy()
                circuit_test = test_fold["circuit"].to_numpy()
                impute_state = fit_imputation(X_train, circuit_train, voter_build.specs)

                # Scaling stats from real data (before imputation), model cols only
                import warnings as _w
                with _w.catch_warnings():
                    _w.simplefilter("ignore", RuntimeWarning)
                    train_mean = np.nanmean(X_train[:, :voter_n_model], axis=0)
                    train_std = np.nanstd(X_train[:, :voter_n_model], axis=0)
                train_mean = np.where(np.isnan(train_mean), 0.0, train_mean)
                train_std = np.where(np.isnan(train_std), 1.0, train_std)
                train_std[train_std == 0] = 1.0

                X_train = apply_imputation(X_train, circuit_train, impute_state)
                X_test = apply_imputation(X_test, circuit_test, impute_state)
                X_train = X_train[:, :voter_n_model]
                X_test = X_test[:, :voter_n_model]
                X_train = (X_train - train_mean) / train_std
                X_test = (X_test - train_mean) / train_std

                model = get_model(voter_cfg.model.type, voter_cfg.model.params or {})
                model.fit(X_train, y_train)
                y_prob = model.predict_proba(X_test)

                voter_fold_probs.append(y_prob)
                voter_fold_keys.append(test_fold.select("match_uid", "player_id"))

                logger.info(
                    "Voter %s fold %d: trained on %d (filtered from %d), predicted %d",
                    name, fold_idx + 1, len(train_filtered), len(train_fold), len(test_fold),
                )

            # Concatenate voter predictions and join to primary OOF
            all_voter_keys = pl.concat(voter_fold_keys)
            all_voter_probs = np.concatenate(voter_fold_probs)
            voter_probs_df = all_voter_keys.with_columns(
                pl.Series(f"_voter_{name}", all_voter_probs)
            )
            oof_df = oof_df.join(voter_probs_df, on=["match_uid", "player_id"], how="left")

            # Scoped voters: null out predictions for out-of-scope matches
            # Use the voter's full DataFrame (which has all columns including
            # computed features) to determine scope, then map back by key.
            if voter_entry.get("scoped") and voter_cfg.data.filters:
                in_scope_keys = apply_filters(voter_df, voter_cfg.data.filters).select(
                    "match_uid", "player_id"
                ).with_columns(pl.lit(True).alias("_in_scope"))
                voter_col = f"_voter_{name}"
                oof_df = oof_df.join(in_scope_keys, on=["match_uid", "player_id"], how="left")
                oof_df = oof_df.with_columns(
                    pl.when(pl.col("_in_scope").is_not_null())
                    .then(pl.col(voter_col))
                    .otherwise(None)
                    .alias(voter_col)
                ).drop("_in_scope")
                in_scope = oof_df[voter_col].is_not_null().sum()
                logger.info("Voter %s scoped: %d/%d matches in scope", name, in_scope, len(oof_df))

        # Compute voter consensus: count agreements with primary pick
        primary_pick = pl.col("y_prob") >= 0.5
        agree_expr = pl.lit(1)  # primary always agrees with itself
        total_expr = pl.lit(1)  # primary always counts
        for name in voter_names:
            col = f"_voter_{name}"
            has_vote = pl.col(col).is_not_null()
            voter_agrees = primary_pick == (pl.col(col) >= 0.5)
            agree_expr = agree_expr + (has_vote & voter_agrees).cast(pl.Int64)
            total_expr = total_expr + has_vote.cast(pl.Int64)

        oof_df = oof_df.with_columns(
            (agree_expr.cast(pl.Utf8) + pl.lit("-") + (total_expr - agree_expr).cast(pl.Utf8))
            .alias("voter_consensus"),
            total_expr.alias("voter_count"),
        )

        oof_dir.mkdir(parents=True, exist_ok=True)
        oof_df.write_parquet(oof_path)
        logger.info("Cached OOF to %s", oof_path)

        validator = ConfidenceValidator.from_oof(oof_df, voter_names=voter_names)

    logger.info("Running confidence validation...")
    result = validator.validate()

    report = format_report(result, model_name=config_name)
    print(report)

    results_path = oof_dir / "validation_results.json"
    _save_validation_json(result, results_path)
    logger.info("Saved detailed results to %s", results_path)

    return 0


def cmd_confidence(args: argparse.Namespace) -> int:
    """Run confidence validation on a model."""

    from mvp.model.confidence.report import format_report
    from mvp.model.confidence.validator import ConfidenceValidator
    from mvp.model.runner import ExperimentRunner

    config_path = resolve_config_path(args.config, MODEL_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {args.config} (tried {config_path})")

    # Voter-system config: has active + voters
    if _is_voter_config(config_path):
        return _run_voter_confidence(args, config_path)

    config_name = config_path.stem
    oof_dir = get_data_root() / "confidence" / config_name
    oof_path = oof_dir / "oof.parquet"

    # Resolve base model names for ensemble identity slices
    base_names = _get_ensemble_base_names(config_path)

    if oof_path.exists() and args.no_refresh:
        logger.info("Loading cached OOF from %s", oof_path)
        oof_df = pl.read_parquet(oof_path)
        validator = ConfidenceValidator.from_oof(oof_df, base_names=base_names)
    else:
        logger.info("Running model to generate OOF predictions...")
        runner = ExperimentRunner(config_path=config_path)
        results = runner.run()
        all_predictions = results["all_predictions"]
        per_model_oof = results.get("per_model_oof") or None
        # per_model_oof is [] for non-ensemble, convert to None
        if not per_model_oof:
            per_model_oof = None

        validator = ConfidenceValidator(
            all_predictions,
            per_model_oof=per_model_oof,
            base_names=base_names,
        )

        oof_dir.mkdir(parents=True, exist_ok=True)
        validator._oof.write_parquet(oof_path)
        logger.info("Cached OOF to %s", oof_path)

    logger.info("Running confidence validation...")
    result = validator.validate()

    report = format_report(result, model_name=config_name)
    print(report)

    results_path = oof_dir / "validation_results.json"
    _save_validation_json(result, results_path)
    logger.info("Saved detailed results to %s", results_path)

    return 0


def _get_ensemble_base_names(config_path) -> list[str] | None:
    """Extract short base model names from ensemble config, or None if not ensemble."""
    from pathlib import Path

    from mvp.model.config import ExperimentConfig

    config = ExperimentConfig.from_file(config_path)
    if config.model.type != "ensemble" or not config.model.params:
        return None

    from mvp.model.config import EnsembleParams

    ens = EnsembleParams.model_validate(config.model.params)
    return [Path(ref.config).stem for ref in ens.base_models]


def _save_validation_json(result, path):
    """Save ValidationResult as JSON for detailed analysis."""
    import json
    from pathlib import Path

    data = {
        "n_total": result.n_total,
        "profiles": {},
    }
    for slice_label, bucket_profiles in result.profiles.items():
        data["profiles"][slice_label] = {}
        for bucket_label, profile in bucket_profiles.items():
            p = {
                "n_matches": profile.n_matches,
                "accuracy": profile.accuracy,
                "err80": profile.err80,
                "signed_cal": profile.signed_cal,
                "log_loss": profile.log_loss,
                "brier_score": profile.brier_score,
                "roc_auc": profile.roc_auc,
            }
            for wlabel, dist in [("cal_3mo", profile.cal_3mo), ("cal_6mo", profile.cal_6mo), ("cal_12mo", profile.cal_12mo)]:
                if dist:
                    p[wlabel] = {
                        "median": dist.median, "p25": dist.p25, "p75": dist.p75,
                        "min": dist.min, "max": dist.max,
                        "n_windows": dist.n_windows, "median_n_per_window": dist.median_n_per_window,
                    }
            data["profiles"][slice_label][bucket_label] = p

    # Voter analysis results
    if result.voter_correlation is not None:
        corr = result.voter_correlation
        data["voter_correlation"] = {
            "voter_names": corr.voter_names,
            "pairs": {
                f"{a}|{b}": {
                    "agreement_pct": s.agreement_pct,
                    "n_overlap": s.n_overlap,
                    "disagree_a_correct_pct": s.disagree_a_correct_pct,
                    "disagree_b_correct_pct": s.disagree_b_correct_pct,
                    "n_disagree": s.n_disagree,
                }
                for (a, b), s in corr.pairs.items()
            },
        }

    if result.coverage_curve is not None and result.coverage_curve.points:
        data["coverage_curve"] = {
            "n_total": result.coverage_curve.n_total,
            "points": [
                {
                    "threshold_pct": pt.threshold_pct,
                    "n_matches": pt.n_matches,
                    "coverage_pct": pt.coverage_pct,
                    "accuracy": pt.profile.accuracy,
                    "err80": pt.profile.err80,
                    "signed_cal": pt.profile.signed_cal,
                    "log_loss": pt.profile.log_loss,
                }
                for pt in result.coverage_curve.points
            ],
        }

    if result.voter_marginal is not None and result.voter_marginal.voters:
        data["voter_marginal"] = {
            "baseline_cov_100": result.voter_marginal.baseline_cov_100,
            "baseline_acc_100": result.voter_marginal.baseline_acc_100,
            "baseline_cov_80": result.voter_marginal.baseline_cov_80,
            "baseline_acc_80": result.voter_marginal.baseline_acc_80,
            "voters": [
                {
                    "name": v.name,
                    "scope_pct": v.scope_pct,
                    "cov_delta_100": v.cov_delta_100,
                    "acc_delta_100": v.acc_delta_100,
                    "cal_delta_100": v.cal_delta_100,
                    "err80_delta_100": v.err80_delta_100,
                    "cov_delta_80": v.cov_delta_80,
                    "acc_delta_80": v.acc_delta_80,
                    "cal_delta_80": v.cal_delta_80,
                    "err80_delta_80": v.err80_delta_80,
                }
                for v in result.voter_marginal.voters
            ],
        }

    Path(path).write_text(json.dumps(data, indent=2))


_PROJECTION_MODEL_TYPES = {"xgb_regressor", "linear", "ridge"}


def _is_projection_discovery(config_path: Path) -> bool:
    """Detect whether a discovery config targets projection (regression)."""
    import yaml

    with open(config_path) as f:
        raw = yaml.safe_load(f)
    model_type = (raw.get("model") or {}).get("type", "")
    return model_type in _PROJECTION_MODEL_TYPES


def cmd_experiment(args: argparse.Namespace) -> int:
    """Run automated feature discovery."""
    config_path = resolve_config_path(args.config, EXPERIMENT_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config} (tried {config_path})")

    if args.refresh:
        from mvp.atptour.aggregators.matches import MatchesAggregator
        logger.info("Rebuilding matches.parquet")
        MatchesAggregator().run()

    if _is_projection_discovery(config_path):
        return _cmd_experiment_projection(args, config_path)
    return _cmd_experiment_classification(args, config_path)


def _cmd_experiment_classification(args: argparse.Namespace, config_path: Path) -> int:
    """Run classification feature discovery."""
    from mvp.model.discovery import FeatureDiscovery

    # Normalize output path: always in models/, add .yaml if needed
    output_name = args.output
    if not output_name.endswith(".yaml"):
        output_name = f"{output_name}.yaml"
    output_path = MODEL_DIR / output_name

    discovery = FeatureDiscovery(
        config_path=config_path,
        verbose=args.verbose,
    )

    result = discovery.run()

    if result.selected_features:
        discovery._last_result = result
        discovery.save_config(output_path)
        print(f"\nSaved config to: {output_path}")
        print(f"Run with: poetry run py -m mvp model {output_path.stem}")
    else:
        print("\nNo features selected - no config saved")

    return 0


def _cmd_experiment_projection(args: argparse.Namespace, config_path: Path) -> int:
    """Run projection feature discovery."""
    from mvp.projection.discovery import ProjectionDiscovery

    # Normalize output path: always in projections/, add .yaml if needed
    output_name = args.output
    if not output_name.endswith(".yaml"):
        output_name = f"{output_name}.yaml"
    output_path = PROJECTION_DIR / output_name

    discovery = ProjectionDiscovery(
        config_path=config_path,
        verbose=args.verbose,
    )

    result = discovery.run()

    if result.selected_features:
        discovery.save_config(output_path, result)
        print(f"\nSaved config to: {output_path}")
        print(f"Run with: poetry run py -m mvp project {output_path.stem}")
    else:
        print("\nNo features selected - no config saved")

    return 0


def print_projection_summary(results: dict[str, Any], name: str | None = None) -> None:
    """Print formatted summary of projection results."""
    metrics = results.get("metrics", {})
    train_metrics = results.get("train_metrics", {})
    diagnostics = results.get("diagnostics")

    print("\n" + "=" * 70)
    title = name or "PROJECTION RESULTS"
    print(f"{title:^70}")
    print("=" * 70)

    # Train vs Test metrics
    test_mae = metrics.get("mae", 0)
    test_rmse = metrics.get("rmse", 0)
    test_r2 = metrics.get("r_squared", 0)
    test_med = metrics.get("median_ae", 0)

    if train_metrics:
        train_mae = train_metrics.get("mae", 0)
        train_rmse = train_metrics.get("rmse", 0)
        train_r2 = train_metrics.get("r_squared", 0)
        print(f"\n{'':8} {'MAE':>10} {'RMSE':>10} {'R²':>10} {'MedAE':>10}")
        print(f"{'Train':8} {train_mae:>10.3f} {train_rmse:>10.3f} {train_r2:>10.3f} {'':>10}")
        print(f"{'Test':8} {test_mae:>10.3f} {test_rmse:>10.3f} {test_r2:>10.3f} {test_med:>10.3f}")
    else:
        print(f"\nTest: {test_mae:.3f} MAE | {test_rmse:.3f} RMSE | {test_r2:.3f} R² | {test_med:.3f} MedAE")

    if not diagnostics:
        print(f"\nMLflow run: {results.get('run_id', 'N/A')}")
        print("=" * 70 + "\n")
        return

    # Residual summary
    residuals = diagnostics.residuals
    if residuals:
        print(f"\nResiduals: mean={residuals.get('mean_residual', 0):.3f}, "
              f"std={residuals.get('std_residual', 0):.3f}, "
              f"skew={residuals.get('skewness', 0):.3f}")
        bins = residuals.get("by_predicted_bin", [])
        if bins:
            print("  By predicted value:")
            for b in bins:
                low, high = b["range"]
                print(f"    {low:5.1f}-{high:5.1f}  mean_resid={b['mean_residual']:+.3f}  "
                      f"mae={b['mae']:.3f}  n={b['n']:,}")

    # Segment breakdowns
    segments = diagnostics.segments
    for seg_type in ["circuit", "surface", "round", "best_of"]:
        seg_data = segments.get(seg_type, {})
        if not seg_data:
            continue
        print(f"\n  {seg_type}:")
        print(f"    {'':12} {'MAE':>8} {'RMSE':>8} {'R²':>8} {'n':>8}")
        for seg_val, m in sorted(seg_data.items()):
            print(f"    {seg_val:12} {m.get('mae', 0):>8.3f} {m.get('rmse', 0):>8.3f} "
                  f"{m.get('r_squared', 0):>8.3f} {m.get('n', 0):>8,}")

    # Match-level analysis
    match = diagnostics.match_level
    if match:
        print(f"\nMatch-Level Analysis ({match.get('n_matches', 0):,} matches):")
        print(f"  Total games: MAE={match.get('total_games_mae', 0):.3f} "
              f"(actual avg={match.get('total_games_mean_actual', 0):.1f}, "
              f"pred avg={match.get('total_games_mean_pred', 0):.1f})")
        print(f"  Spread: MAE={match.get('spread_mae', 0):.3f}")
        print(f"  Directional accuracy: {match.get('directional_accuracy', 0):.1%}")

    print(f"\nMLflow run: {results.get('run_id', 'N/A')}")
    print("=" * 70 + "\n")


def cmd_project(args: argparse.Namespace) -> int:
    """Run game projection from config."""
    from mvp.projection.runner import ProjectionRunner

    config_path = resolve_config_path(args.config, PROJECTION_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config} (tried {config_path})")

    if args.refresh:
        from mvp.atptour.aggregators.matches import MatchesAggregator
        logger.info("Rebuilding matches.parquet")
        MatchesAggregator().run()

    runner = ProjectionRunner(config_path=config_path)
    results = runner.run()

    print_projection_summary(results, name=runner.run_name)
    return 0



def _fetch_book_quiet(book: BookConfig, run_at=None) -> int:
    """Run a book's odds fetch in background thread."""
    import importlib

    mod = importlib.import_module(f"mvp.{book.domain}.odds")
    return mod.fetch_and_save(run_at=run_at)


def cmd_live(args: argparse.Namespace) -> int:
    """Run live pipeline: extract, aggregate, predict."""
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime

    from mvp.atptour.aggregators.matches import MatchesAggregator
    from mvp.atptour.discovery import TournamentDiscovery
    from mvp.atptour.pipeline import (
        _process_tournaments,
        run_player_data,
        run_rankings,
    )
    from mvp.model.predictor import ProductionPredictor

    current_year = datetime.now().year
    pipeline_run_at = datetime.now()

    from mvp.pipeline_report import PipelineReport
    report = PipelineReport()

    # Start odds fetch in background (fully independent of pipeline).
    odds_pool = ThreadPoolExecutor(max_workers=len(BOOK_REGISTRY))
    book_futures = {
        b.code: odds_pool.submit(_fetch_book_quiet, b, pipeline_run_at)
        for b in BOOK_REGISTRY
    }

    errors: list[str] = []
    predictions = None
    all_odds_maps: dict[str, dict[str, dict[str, float]]] = {}
    existing_map = None

    # --- Stage 1: Rankings, discovery, tournament processing, aggregation ---
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            rankings_future = pool.submit(
                run_rankings, start_year=current_year - 1
            )
            discovery = TournamentDiscovery()
            discovery_future = pool.submit(discovery.get_active_tournaments)
            rankings_future.result()
            pairs = discovery_future.result()

        if args.tid is not None:
            pairs = [(t, y) for t, y in pairs if t == args.tid]
            if not pairs:
                raise ValueError(f"Tournament {args.tid} is not currently active")

        tournaments = [(t, year, False, None) for t, year in pairs]
        logger.info("Processing %d active tournaments", len(tournaments))

        failed = _process_tournaments(
            tournaments, data_root=None, refresh=args.refresh
        )

        run_tids = {(tid, yr) for tid, yr, _, _ in tournaments}
        player_result = run_player_data(
            run_tids=run_tids, refresh_players=args.refresh_players
        )

        logger.info("Running cross-tournament aggregation")
        MatchesAggregator().run()

        if player_result.has_failures:
            logger.warning(
                "%d failed player operation(s) — continuing",
                len(player_result.all_failures),
            )
        report.record_tournaments(
            processed=len(tournaments),
            failed=failed,
        )
        if failed:
            for tid, year, error in failed:
                logger.error(
                    "  FAILED: tournament %s (%d): %s", tid, year, error
                )
                errors.append(f"tournament {tid} ({year}): {error}")
    except Exception as e:
        logger.error("Stage 1 (extract/aggregate) failed: %s", e)
        errors.append(f"extract/aggregate: {e}")
        report.set_errors(errors)
        report.save(get_data_root() / "pipeline" / "runs.jsonl")
        odds_pool.shutdown(wait=False)
        raise RuntimeError(
            f"Pipeline cannot continue — extract/aggregate failed: {e}"
        )

    # --- Stage 2: Winner predictions ---
    target_sections = _get_target_sections()
    try:
        predictor = ProductionPredictor(target_section="winner")
        predictions = predictor.predict(tournament_keys=pairs)

        if len(predictions) > 0:
            predictions = predictor.predict_voters(pairs, predictions)

        if predictions is not None and len(predictions) > 0:
            predictor.save_predictions(predictions)
            report.record_predictions(total=len(predictions))
        else:
            print("\nNo pending matches to predict.")
            report.record_predictions(total=0)
    except Exception as e:
        logger.error("Winner predictions failed: %s", e)
        errors.append(f"winner predictions: {e}")

    # --- Stage 3: Additional target sections ---
    if predictions is not None and len(predictions) > 0:
        for section in target_sections:
            if section == "winner":
                continue
            try:
                ds_predictions_path = (
                    get_data_root() / "predictions" / f"{section}_predictions.parquet"
                )
                ds_predictor = ProductionPredictor(
                    target_section=section,
                    predictions_path=ds_predictions_path,
                )
                ds_predictions = ds_predictor.predict(tournament_keys=pairs)
                if len(ds_predictions) == 0:
                    print(f"\nNo pending {section} matches to predict.")
                    continue
                ds_predictions = ds_predictor.predict_voters(pairs, ds_predictions)
                ds_predictor.save_predictions(ds_predictions)
                print(f"Generated {len(ds_predictions)} {section} predictions")
            except Exception as e:
                logger.error("%s prediction failed: %s", section, e)
                errors.append(f"{section} predictions: {e}")

    # --- Stage 4: Odds fetching ---
    for book in BOOK_REGISTRY:
        try:
            n = book_futures[book.code].result(timeout=30)
            print(f"Fetched {n} {book.label} moneyline odds entries")
            report.record_book_fetched(book.code, n)
        except Exception as e:
            logger.error("%s odds fetch failed: %s", book.label, e)
            errors.append(f"{book.label} odds fetch: {e}")
            report.record_book_fetched(book.code, 0)

    # --- Stage 5: Event mapping ---
    try:
        from mvp.analysis.event_map import (
            load_event_map_with_overrides,
            save_event_mappings,
        )
        from mvp.common.event_mapper import (
            build_match_catalog,
            build_player_lookup,
            map_book_events,
        )

        _cli_dir = Path(__file__).resolve().parent
        _book_mapping_config = [
            (b.code, b.event_id_col, b.stage_rel, _cli_dir / b.aliases_rel)
            for b in BOOK_REGISTRY
        ]

        existing_map = load_event_map_with_overrides()
        data_root = get_data_root()

        unmapped_odds: list[tuple[str, str, Path, pl.DataFrame]] = []
        for book, eid_col, odds_rel, aliases_path in _book_mapping_config:
            odds_path = data_root / odds_rel
            if not odds_path.exists():
                continue
            staged = pl.read_parquet(odds_path)
            staged_latest = staged.sort("fetched_at").group_by([eid_col, "player_name"]).last()
            existing_eids = set(
                existing_map.filter(pl.col("book") == book)["event_id"].to_list()
            )
            unmapped = staged_latest.filter(~pl.col(eid_col).is_in(existing_eids))
            if len(unmapped) > 0:
                unmapped_odds.append((book, eid_col, aliases_path, unmapped))

        if unmapped_odds:
            min_year = min(
                df["fetched_at"].min().year for _, _, _, df in unmapped_odds
            )

            matches_path = data_root / "aggregate" / "atptour" / "matches.parquet"
            if matches_path.exists():
                catalog_df = pl.read_parquet(
                    matches_path,
                    columns=["match_uid", "player_id", "opp_id", "tournament_id",
                             "year", "tournament_name", "draw_type", "draw_p1_id"],
                ).filter(pl.col("year") >= min_year)
                match_catalog = build_match_catalog(catalog_df)
            else:
                match_catalog = {}

            base_lookup = build_player_lookup()

            for book, eid_col, aliases_path, unmapped_df in unmapped_odds:
                try:
                    if aliases_path.exists():
                        import yaml

                        from mvp.common.odds_matching import normalize_name

                        with open(aliases_path) as f:
                            raw = yaml.safe_load(f) or {}
                        book_lookup = {**base_lookup}
                        for name, pid in raw.items():
                            book_lookup[normalize_name(name)] = pid.upper().strip()
                    else:
                        book_lookup = base_lookup

                    map_result = map_book_events(
                        unmapped_df, eid_col, book, book_lookup, match_catalog,
                    )
                    report.record_unresolved_names(book, map_result.unresolved_names)
                    if map_result.event_matches:
                        save_event_mappings(map_result.event_matches, book=book)
                        print(f"Event mapper: {len(map_result.event_matches)} new {book.upper()} mappings")
                except Exception as e:
                    logger.error("Event mapping failed for %s: %s", book.upper(), e)
                    errors.append(f"event mapping {book.upper()}: {e}")
    except Exception as e:
        logger.error("Event mapping setup failed: %s", e)
        errors.append(f"event mapping setup: {e}")

    # --- Stage 6: Odds matching ---
    if predictions is not None and len(predictions) > 0:
        import importlib

        for book in BOOK_REGISTRY:
            try:
                mod = importlib.import_module(f"mvp.{book.domain}.matcher")
                matcher = getattr(mod, book.matcher_class)()
                result = matcher.match(predictions).odds or None
                if result:
                    all_odds_maps[book.code] = result
                    print(f"Matched {book.label} odds for {len(result)}/{len(predictions)} predictions")
            except Exception as e:
                logger.error("%s odds lookup failed: %s", book.label, e)
                errors.append(f"{book.label} odds lookup: {e}")

    odds_pool.shutdown(wait=False)

    # --- Stage 7: Log unmapped predictions ---
    if predictions is not None and len(predictions) > 0 and existing_map is not None:
        try:
            mapped_uids = set(existing_map["match_uid"].to_list())
            never_mapped = predictions.filter(
                ~pl.col("match_uid").is_in(mapped_uids)
            )
            no_odds_items = [
                {
                    "match_uid": row.get("match_uid", "?"),
                    "tournament": row.get("tournament_name", "?"),
                    "p1": row.get("p1_name") or row.get("player_id", "?"),
                    "p2": row.get("p2_name") or row.get("opp_id", "?"),
                }
                for row in never_mapped.iter_rows(named=True)
            ]
            report.record_predictions_without_odds(no_odds_items)
            if len(never_mapped) > 0:
                logger.info(
                    "Predictions with no odds from any book: %d/%d",
                    len(never_mapped), len(predictions),
                )
                for row in never_mapped.sort("effective_match_date").iter_rows(named=True):
                    p1 = row.get("p1_name") or row.get("player_id", "?")
                    p2 = row.get("p2_name") or row.get("opp_id", "?")
                    t = row.get("tournament_name") or "?"
                    uid = row.get("match_uid", "?")
                    logger.info("  No odds: %s — %s vs %s (%s)", t, p1, p2, uid)

            print_predictions(predictions, book_odds=all_odds_maps or None)
        except Exception as e:
            logger.error("Prediction display failed: %s", e)
            errors.append(f"prediction display: {e}")

    # --- Stage 8: Sheets sync ---
    if predictions is not None and len(predictions) > 0:
        try:
            from mvp.gsheets.base import merge_predictions, prepare_predictions
            from mvp.gsheets.sheets import SheetsSync

            matches_path = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
            sheets = SheetsSync()
            existing = sheets.read_existing()

            matches_df = pl.read_parquet(matches_path) if matches_path.exists() else pl.DataFrame()

            prepared = prepare_predictions(predictions)
            book_odds_for_sheets = {
                book.display_name: all_odds_maps[book.code]
                for book in BOOK_REGISTRY
                if book.code in all_odds_maps
            }
            merged = merge_predictions(existing, prepared, matches_df, odds_maps=book_odds_for_sheets or None)
            sheets.write(merged)

            sheets_parquet = get_data_root() / "sheets" / "bets.parquet"
            sheets_parquet.parent.mkdir(parents=True, exist_ok=True)
            merged.write_parquet(sheets_parquet)

            n_new = len(merged) - len(existing)
            print(f"Synced to Google Sheets ({n_new} new matches)")
            report.record_sheets_sync(success=True, count=n_new)
        except Exception as e:
            logger.error("Sheets sync failed: %s", e)
            print(f"Warning: Sheets sync failed ({e}). Predictions saved locally.")
            errors.append(f"sheets sync: {e}")
            report.record_sheets_sync(success=False, count=0, error=str(e))

    # --- Stage 9: Analysis refresh ---
    try:
        from mvp.analysis.refresh import refresh_analysis_data
        refresh_analysis_data(get_data_root(), BOOK_REGISTRY)
    except Exception as e:
        logger.error("Analysis refresh failed: %s", e)
        print(f"Warning: Analysis data refresh failed ({e})")
        errors.append(f"analysis refresh: {e}")

    report.set_errors(errors)
    report.save(get_data_root() / "pipeline" / "runs.jsonl")

    # --- Raise all collected errors at the very end ---
    if errors:
        summary = "; ".join(errors)
        raise RuntimeError(
            f"Pipeline finished with {len(errors)} error(s): {summary}"
        )

    return 0


def cmd_analysis(parsed: argparse.Namespace) -> int:
    """Run standalone analysis pipeline: odds → dataset → simulations."""

    from mvp.analysis.refresh import refresh_analysis_data

    data_root = get_data_root()
    if not refresh_analysis_data(data_root, BOOK_REGISTRY):
        return 1

    if getattr(parsed, "no_ui", False):
        print("Pipeline complete. Skipping dashboard.")
    else:
        import subprocess
        import sys

        print("Launching dashboard...")
        app_path = Path(__file__).resolve().parent / "analysis" / "dashboard" / "app.py"
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(app_path),
             "--server.headless=true",
             "--browser.gatherUsageStats=false",
             "--", str(data_root)],
        )

    return 0


def main(args: list[str] | None = None) -> int:
    """CLI entry point."""
    parsed = parse_args(args)
    logging.basicConfig(
        level=getattr(logging, parsed.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if getattr(parsed, "n_jobs", None) is not None:
        from mvp.model.models import set_n_jobs_override
        set_n_jobs_override(parsed.n_jobs)

    if getattr(parsed, "memory_limit", None) is not None:
        import mvp.model.engine as _engine
        _engine._MEMORY_LIMIT_PCT = parsed.memory_limit

    if parsed.command == "train":
        return cmd_train(parsed)
    elif parsed.command == "model":
        return cmd_model(parsed)
    elif parsed.command == "tune":
        return cmd_tune(parsed)
    elif parsed.command == "tune-review":
        return cmd_tune_review(parsed)
    elif parsed.command == "experiment":
        return cmd_experiment(parsed)
    elif parsed.command == "live":
        return cmd_live(parsed)
    elif parsed.command == "confidence":
        return cmd_confidence(parsed)
    elif parsed.command == "project":
        return cmd_project(parsed)
    elif parsed.command == "analysis":
        return cmd_analysis(parsed)

    return 1


if __name__ == "__main__":
    sys.exit(main())
