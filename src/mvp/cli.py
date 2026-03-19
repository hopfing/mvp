"""Unified CLI entry point."""


import argparse
import logging
import sys
import warnings
from pathlib import Path
from typing import Any

warnings.filterwarnings("ignore", message="All-NaN slice encountered")
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn.utils.parallel")

from mvp.common.base_job import get_data_root
from mvp.draftkings.odds import fetch_and_save

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
        print(f"\n{'':8} {'Accuracy':>10} {'AUC':>10} {'Log Loss':>10} {'Brier':>10}")
        print(f"{'Train':8} {train_acc:>10.1%} {train_auc:>10.3f} {train_ll:>10.3f} {train_brier:>10.4f}")
        print(f"{'Test':8} {test_acc:>10.1%} {test_auc:>10.3f} {test_ll:>10.3f} {test_brier:>10.4f}")
    else:
        print(f"\nTest: {test_acc:.1%} acc | {test_auc:.3f} AUC | {test_ll:.3f} LL | {test_brier:.4f} Brier")


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
            print(f"\n  {circuit.upper()}  {acc:5.1%} acc | {auc:.3f} AUC | {ll:.3f} ll | {brier:.4f} brier | {cal:.1%} cal | {err:.1%} err80 | n={n:,}")

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
                    print(f"      {surface:8} {acc:5.1%} | {auc:.3f} | {ll:.3f} | {brier:.4f} | {cal:.1%} | {err:.1%} | n={n:,}")

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
                    print(f"      {rnd:10} {acc:5.1%} | {auc:.3f} | {ll:.3f} | {brier:.4f} | {cal:.1%} | {err:.1%} | n={n:,}")

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
                    print(f"      {group:10} {acc:5.1%} | {auc:.3f} | {ll:.3f} | {brier:.4f} | {cal:.1%} | {err:.1%} | n={n:,}")

    # Calibration buckets table
    cal_data = diagnostics.calibration
    if cal_data and cal_data.get("buckets"):
        buckets = cal_data["buckets"]
        worst_err = max(b["error"] for b in buckets)
        raw_cal = metrics.get("raw_calibration_error")
        if raw_cal is not None:
            print(f"\nCalibration ({cal_data['calibration_error']:.1%} mean error, {raw_cal:.1%} raw):")
        else:
            print(f"\nCalibration ({cal_data['calibration_error']:.1%} mean error):")
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
            print(f"  {'Model':40} {'Acc':>7} {'AUC':>7} {'LL':>7} {'Cal':>7}")
            print(f"  {'-' * 68}")
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
                print(f"  {label:40} {acc:6.1%} {auc:7.3f} {ll:7.3f} {cal:6.1%}")

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
    odds_map: dict[str, dict[str, float]] | None = None,
    br_odds_map: dict[str, dict[str, float]] | None = None,
) -> None:
    """Print human-readable prediction summary."""
    import polars as pl

    has_odds = odds_map or br_odds_map
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
            if odds_map:
                dk_match = odds_map.get(match_uid)
                if dk_match and p1_id:
                    o1 = dk_match.get(p1_id)
                    o2 = dk_match.get(p2_id)
                    if o1 is not None and o2 is not None:
                        odds_parts.append(f"DK:{o1:.2f}/{o2:.2f}")
            if br_odds_map:
                br_match = br_odds_map.get(match_uid)
                if br_match and p1_id:
                    o1 = br_match.get(p1_id)
                    o2 = br_match.get(p2_id)
                    if o1 is not None and o2 is not None:
                        odds_parts.append(f"BR:{o1:.2f}/{o2:.2f}")
            if odds_parts:
                line += "  " + " | ".join(odds_parts)

        print(line)

    print("\n" + "=" * width + "\n")


# Default directories for each command
MODEL_DIR = Path("models")
EXPERIMENT_DIR = Path("experiments")


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
        "--refresh", action="store_true",
        help="Force re-run model even if cached OOF exists"
    )

    return parser.parse_args(args)


def cmd_train(args: argparse.Namespace) -> int:
    """Train the production model from production.yaml."""
    from mvp.atptour.aggregators.matches import MatchesAggregator
    from mvp.model.predictor import ProductionPredictor

    logger.info("Rebuilding matches.parquet")
    MatchesAggregator().run()

    predictor = ProductionPredictor()
    predictor.train()
    print("Production model trained and saved.")
    n_voters = predictor.train_voters()
    if n_voters > 0:
        print(f"Trained {n_voters} voter model(s).")
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
    """Check if a config file is a voter-system config (has active + voters)."""
    import yaml

    with open(config_path) as f:
        data = yaml.safe_load(f)
    return isinstance(data, dict) and "active" in data and "voters" in data


def _run_voter_confidence(args: argparse.Namespace, config_path: Path) -> int:
    """Run confidence validation with voter consensus overlay.

    Mirrors production behavior: each voter is trained on its own filtered
    data up to the fold's training cutoff, then predicts on the full
    (unfiltered) primary test set. Only the binary pick is used for consensus.
    """
    from pathlib import Path

    import numpy as np
    import polars as pl
    import yaml

    from mvp.model.confidence.report import format_report
    from mvp.model.confidence.validator import ConfidenceValidator, prepare_oof
    from mvp.model.config import ExperimentConfig, apply_filters, get_filter_feature_specs
    from mvp.model.engine import FeatureEngine, get_feature_columns
    from mvp.model.models import get_model
    from mvp.model.runner import ExperimentRunner
    from mvp.model.splitters import make_splitter

    with open(config_path) as f:
        voter_config = yaml.safe_load(f)

    config_name = config_path.stem
    oof_dir = get_data_root() / "confidence" / config_name
    oof_path = oof_dir / "oof.parquet"
    voter_names = [v["name"] for v in voter_config["voters"]]

    if oof_path.exists() and not args.refresh:
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
        )

        # Get primary's full filtered DataFrame for fold replay
        primary_engine = FeatureEngine(
            matches_path=get_data_root() / "aggregate" / "atptour" / "matches.parquet",
            cache_dir=get_data_root() / "features" / "cache",
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
                cache_dir=get_data_root() / "features" / "cache",
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
            from mvp.model.imputation import build_imputation, fit_imputation, apply_imputation
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
    from pathlib import Path

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

    if oof_path.exists() and not args.refresh:
        import polars as pl
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


def cmd_experiment(args: argparse.Namespace) -> int:
    """Run automated feature discovery."""
    from mvp.model.discovery import FeatureDiscovery

    config_path = resolve_config_path(args.config, EXPERIMENT_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config} (tried {config_path})")

    # Normalize output path: always in models/, add .yaml if needed
    output_name = args.output
    if not output_name.endswith(".yaml"):
        output_name = f"{output_name}.yaml"
    output_path = MODEL_DIR / output_name

    if args.refresh:
        from mvp.atptour.aggregators.matches import MatchesAggregator
        logger.info("Rebuilding matches.parquet")
        MatchesAggregator().run()

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


def _fetch_dk_quiet() -> int:
    """Run DK odds fetch with logging suppressed (runs in background thread)."""
    import logging as _logging

    dk_logger = _logging.getLogger("mvp.draftkings")
    ext_logger = _logging.getLogger("mvp.common.base_extractor")
    job_logger = _logging.getLogger("mvp.common.base_job")
    prev_levels = dk_logger.level, ext_logger.level, job_logger.level
    dk_logger.setLevel(_logging.WARNING)
    ext_logger.setLevel(_logging.WARNING)
    job_logger.setLevel(_logging.WARNING)
    try:
        return fetch_and_save()
    finally:
        dk_logger.setLevel(prev_levels[0])
        ext_logger.setLevel(prev_levels[1])
        job_logger.setLevel(prev_levels[2])


def _fetch_br_quiet() -> int:
    """Run BR odds fetch with logging suppressed (runs in background thread)."""
    import logging as _logging

    from mvp.betrivers.odds import fetch_and_save as br_fetch_and_save

    br_logger = _logging.getLogger("mvp.betrivers")
    ext_logger = _logging.getLogger("mvp.common.base_extractor")
    job_logger = _logging.getLogger("mvp.common.base_job")
    prev_levels = br_logger.level, ext_logger.level, job_logger.level
    br_logger.setLevel(_logging.WARNING)
    ext_logger.setLevel(_logging.WARNING)
    job_logger.setLevel(_logging.WARNING)
    try:
        return br_fetch_and_save()
    finally:
        br_logger.setLevel(prev_levels[0])
        ext_logger.setLevel(prev_levels[1])
        job_logger.setLevel(prev_levels[2])


def _fetch_mgm_quiet() -> int:
    """Run MGM odds fetch with logging suppressed (runs in background thread)."""
    import logging as _logging

    from mvp.betmgm.odds import fetch_and_save as mgm_fetch_and_save

    mgm_logger = _logging.getLogger("mvp.betmgm")
    ext_logger = _logging.getLogger("mvp.common.base_extractor")
    job_logger = _logging.getLogger("mvp.common.base_job")
    prev_levels = mgm_logger.level, ext_logger.level, job_logger.level
    mgm_logger.setLevel(_logging.WARNING)
    ext_logger.setLevel(_logging.WARNING)
    job_logger.setLevel(_logging.WARNING)
    try:
        return mgm_fetch_and_save()
    finally:
        mgm_logger.setLevel(prev_levels[0])
        ext_logger.setLevel(prev_levels[1])
        job_logger.setLevel(prev_levels[2])


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

    # Start DK odds fetch in background (fully independent of pipeline).
    # Suppress its logs — result is reported after collection.
    odds_pool = ThreadPoolExecutor(max_workers=3)
    dk_future = odds_pool.submit(_fetch_dk_quiet)
    br_future = odds_pool.submit(_fetch_br_quiet)
    mgm_future = odds_pool.submit(_fetch_mgm_quiet)

    try:  # ensure odds_pool cleanup on early failure
        # Rankings + discovery in parallel
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

        # Report extraction/aggregation failures
        if player_result.has_failures:
            logger.warning(
                "%d failed player operation(s) — continuing",
                len(player_result.all_failures),
            )
        if failed:
            for tid, year, error in failed:
                logger.error(
                    "  FAILED: tournament %s (%d): %s", tid, year, error
                )
            raise RuntimeError(
                f"Pipeline finished with {len(failed)} failed tournament(s)"
            )

        # Predict with production model
        predictor = ProductionPredictor()
        predictions = predictor.predict(tournament_keys=pairs)

        if len(predictions) == 0:
            print("\nNo pending matches to predict.")
            return 0

        predictions = predictor.predict_voters(pairs, predictions)

        if len(predictions) == 0:
            print("\nNo pending matches to predict.")
            return 0

        predictor.save_predictions(predictions)

        # Collect odds from both books and match to predictions
        odds_map: dict[str, dict[str, float]] | None = None
        br_odds_map: dict[str, dict[str, float]] | None = None
        mgm_odds_map: dict[str, dict[str, float]] | None = None
        match_result = None
        br_match_result = None
        mgm_match_result = None
        try:
            n = dk_future.result(timeout=30)
            print(f"Fetched {n} DK moneyline odds entries")
            from mvp.draftkings.matcher import DraftKingsOddsMatcher
            matcher = DraftKingsOddsMatcher()
            match_result = matcher.match(predictions)
            odds_map = match_result.odds or None
            if odds_map:
                print(f"Matched DK odds for {len(odds_map)}/{len(predictions)} predictions")
            if match_result.unmatched_names:
                print(f"Unmatched DK names ({len(match_result.unmatched_names)}):")
                for name in sorted(match_result.unmatched_names):
                    print(f"  {name}")
                print(f"Add aliases to: {matcher.ALIASES_PATH}")
        except Exception as e:
            logger.error("DK odds matching failed: %s", e)
            print(f"Warning: DK odds fetch/match failed ({e})")

        try:
            n = br_future.result(timeout=30)
            print(f"Fetched {n} BR moneyline odds entries")
            from mvp.betrivers.matcher import BetRiversOddsMatcher
            br_matcher = BetRiversOddsMatcher()
            br_match_result = br_matcher.match(predictions)
            br_odds_map = br_match_result.odds or None
            if br_odds_map:
                print(f"Matched BR odds for {len(br_odds_map)}/{len(predictions)} predictions")
            if br_match_result.unmatched_names:
                print(f"Unmatched BR names ({len(br_match_result.unmatched_names)}):")
                for name in sorted(br_match_result.unmatched_names):
                    print(f"  {name}")
                print(f"Add aliases to: {br_matcher.ALIASES_PATH}")
        except Exception as e:
            logger.error("BR odds matching failed: %s", e)
            print(f"Warning: BR odds fetch/match failed ({e})")

        try:
            n = mgm_future.result(timeout=30)
            print(f"Fetched {n} MGM moneyline odds entries")
            from mvp.betmgm.matcher import BetMGMOddsMatcher
            mgm_matcher = BetMGMOddsMatcher()
            mgm_match_result = mgm_matcher.match(predictions)
            mgm_odds_map = mgm_match_result.odds or None
            if mgm_odds_map:
                print(f"Matched MGM odds for {len(mgm_odds_map)}/{len(predictions)} predictions")
            if mgm_match_result.unmatched_names:
                print(f"Unmatched MGM names ({len(mgm_match_result.unmatched_names)}):")
                for name in sorted(mgm_match_result.unmatched_names):
                    print(f"  {name}")
                print(f"Add aliases to: {mgm_matcher.ALIASES_PATH}")
        except Exception as e:
            logger.error("MGM odds matching failed: %s", e)
            print(f"Warning: MGM odds fetch/match failed ({e})")

        odds_pool.shutdown(wait=False)

        print_predictions(predictions, odds_map=odds_map, br_odds_map=br_odds_map)

    except Exception:
        odds_pool.shutdown(wait=False)
        raise

    # Sync to Google Sheets (best-effort, must be last)
    try:
        import polars as pl

        from mvp.gsheets.base import merge_predictions, prepare_predictions
        from mvp.gsheets.sheets import SheetsSync

        matches_path = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        sheets = SheetsSync()
        existing = sheets.read_existing()

        matches_df = pl.read_parquet(matches_path) if matches_path.exists() else pl.DataFrame()

        prepared = prepare_predictions(predictions)
        book_odds = {}
        if odds_map:
            book_odds["DraftKings"] = odds_map
        if br_odds_map:
            book_odds["BetRivers"] = br_odds_map
        if mgm_odds_map:
            book_odds["BetMGM"] = mgm_odds_map
        merged = merge_predictions(existing, prepared, matches_df, odds_maps=book_odds or None)
        sheets.write(merged)

        sheets_parquet = get_data_root() / "sheets" / "bets.parquet"
        sheets_parquet.parent.mkdir(parents=True, exist_ok=True)
        merged.write_parquet(sheets_parquet)

        n_new = len(merged) - len(existing)
        print(f"Synced to Google Sheets ({n_new} new matches)")
    except Exception as e:
        logger.error("Sheets sync failed: %s", e)
        print(f"Warning: Sheets sync failed ({e}). Predictions saved locally.")

    # Build analysis dataset (best-effort)
    try:
        import polars as pl

        from mvp.analysis.dataset import build_analysis_dataset
        from mvp.analysis.event_map import load_event_map_with_overrides, save_event_mappings
        from mvp.analysis.odds import compute_odds_by_book
        from mvp.analysis.report import format_summary

        # Save event mappings from matchers
        if match_result and match_result.event_matches:
            save_event_mappings(match_result.event_matches, book="dk")
        if br_match_result and br_match_result.event_matches:
            save_event_mappings(br_match_result.event_matches, book="br")
        if mgm_match_result and mgm_match_result.event_matches:
            save_event_mappings(mgm_match_result.event_matches, book="mgm")

        # Load event map and compute per-book odds
        event_map = load_event_map_with_overrides()
        all_odds = []

        dk_odds_path = get_data_root() / "stage" / "draftkings" / "moneyline.parquet"
        if dk_odds_path.exists():
            dk_staged = pl.read_parquet(dk_odds_path)
            dk_book_odds = compute_odds_by_book(dk_staged, event_map, "dk", "dk_event_id")
            if len(dk_book_odds) > 0:
                all_odds.append(dk_book_odds)

        br_odds_path = get_data_root() / "stage" / "betrivers" / "moneyline.parquet"
        if br_odds_path.exists():
            br_staged = pl.read_parquet(br_odds_path)
            br_book_odds = compute_odds_by_book(br_staged, event_map, "br", "br_event_id")
            if len(br_book_odds) > 0:
                all_odds.append(br_book_odds)

        mgm_odds_path = get_data_root() / "stage" / "betmgm" / "moneyline.parquet"
        if mgm_odds_path.exists():
            mgm_staged = pl.read_parquet(mgm_odds_path)
            mgm_book_odds = compute_odds_by_book(mgm_staged, event_map, "mgm", "mgm_event_id")
            if len(mgm_book_odds) > 0:
                all_odds.append(mgm_book_odds)

        odds_by_book = pl.concat(all_odds, how="diagonal_relaxed") if all_odds else None

        # Load sheet data
        sheets_path = get_data_root() / "sheets" / "bets.parquet"
        sheet_data = pl.read_parquet(sheets_path) if sheets_path.exists() else None

        # Load ALL predictions (not just current batch) for full analysis
        all_preds_path = get_data_root() / "predictions" / "predictions.parquet"
        all_predictions = pl.read_parquet(all_preds_path) if all_preds_path.exists() else predictions

        # Build results from matches
        matches_path = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
        results_df = None
        if matches_path.exists():
            matches_for_results = pl.read_parquet(matches_path)
            if "won" in matches_for_results.columns:
                won = matches_for_results.filter(pl.col("won") == True).select(
                    "match_uid", pl.col("player_id").alias("winner_id")
                )
                if len(won) > 0:
                    all_pred_uids = set(all_predictions["match_uid"].to_list())
                    won_relevant = won.filter(pl.col("match_uid").is_in(list(all_pred_uids)))
                    if len(won_relevant) > 0:
                        all_pred_p1 = all_predictions.select("match_uid", "p1_id").unique(subset=["match_uid"])
                        results_df = won_relevant.join(all_pred_p1, on="match_uid").with_columns(
                            pl.when(pl.col("winner_id") == pl.col("p1_id"))
                            .then(pl.lit("P1"))
                            .otherwise(pl.lit("P2"))
                            .alias("result")
                        ).select("match_uid", "result")

        ds = build_analysis_dataset(
            predictions=all_predictions,
            results=results_df,
            sheet_data=sheet_data,
            odds_by_book=odds_by_book,
        )

        # Save analysis dataset
        analysis_path = get_data_root() / "analysis" / "analysis.parquet"
        analysis_path.parent.mkdir(parents=True, exist_ok=True)
        ds.write_parquet(analysis_path)

        # Print summary
        summary = format_summary(ds)
        print(f"\n{summary}")

    except Exception as e:
        logger.error("Analysis pipeline failed: %s", e)
        print(f"Warning: Analysis pipeline failed ({e})")

    return 0


def main(args: list[str] | None = None) -> int:
    """CLI entry point."""
    parsed = parse_args(args)
    logging.basicConfig(
        level=getattr(logging, parsed.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if parsed.command == "train":
        return cmd_train(parsed)
    elif parsed.command == "model":
        return cmd_model(parsed)
    elif parsed.command == "experiment":
        return cmd_experiment(parsed)
    elif parsed.command == "live":
        return cmd_live(parsed)
    elif parsed.command == "confidence":
        return cmd_confidence(parsed)

    return 1


if __name__ == "__main__":
    sys.exit(main())
