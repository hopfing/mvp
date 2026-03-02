"""Unified CLI entry point."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

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

    if train_metrics:
        train_acc = train_metrics.get("accuracy", 0)
        train_auc = train_metrics.get("roc_auc", 0)
        train_ll = train_metrics.get("log_loss", 0)
        print(f"\n{'':8} {'Accuracy':>10} {'AUC':>10} {'Log Loss':>10}")
        print(f"{'Train':8} {train_acc:>10.1%} {train_auc:>10.3f} {train_ll:>10.3f}")
        print(f"{'Test':8} {test_acc:>10.1%} {test_auc:>10.3f} {test_ll:>10.3f}")
    else:
        print(f"\nTest: {test_acc:.1%} acc | {test_auc:.3f} AUC | {test_ll:.3f} log_loss")


    if not diagnostics:
        print(f"\nMLflow run: {results.get('run_id', 'N/A')}")
        print("=" * 70 + "\n")
        return

    # Segments with multiple metrics
    segments = diagnostics.segments
    if segments:
        print("\nSegments:")
        for seg_type in ["circuit", "surface", "round_group"]:
            if seg_type not in segments:
                continue
            print(f"  {seg_type}:")
            for name, seg_metrics in segments[seg_type].items():
                acc = seg_metrics.get('accuracy', 0)
                auc = seg_metrics.get('roc_auc', 0)
                ll = seg_metrics.get('log_loss', 0)
                n = seg_metrics.get('n_matches', 0)
                print(f"    {name:12} {acc:5.1%} acc | {auc:.3f} AUC | {ll:.3f} log_loss | n={n:,}")

    # Calibration buckets table
    cal_data = diagnostics.calibration
    if cal_data and cal_data.get("buckets"):
        buckets = cal_data["buckets"]
        worst_err = max(b["error"] for b in buckets)
        print(f"\nCalibration ({cal_data['calibration_error']:.1%} mean error):")
        for b in buckets:
            low, high = b["range"]
            marker = " <- worst" if b["error"] == worst_err else ""
            print(f"  {low:.0%}-{high:.0%}  pred={b['predicted_mean']:.1%}  "
                  f"actual={b['actual']:.1%}  err={b['error']:.1%}  n={b['n']:,}{marker}")

    # High-confidence errors
    errors = diagnostics.errors
    if errors and "summary" in errors:
        e80 = errors["summary"].get("80plus", {})
        if e80.get("total", 0) > 0:
            print(f"High-conf errors: {e80['error_rate']:.1%} of {e80['total']:,} predictions at 80%+ were wrong")

    # Temporal
    temporal = diagnostics.temporal
    if temporal and temporal.get("temporal_drift", 0) > 0:
        print(f"Temporal drift: ±{temporal['temporal_drift']:.1%} from average")

    print(f"\nMLflow run: {results.get('run_id', 'N/A')}")
    print("=" * 70 + "\n")

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
        "--verbose", "-v", action="store_true", help="Print progress"
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

    return parser.parse_args(args)


def cmd_model(args: argparse.Namespace) -> int:
    """Run model training from config."""
    from mvp.atptour.aggregators.matches import MatchesAggregator
    from mvp.model.runner import ExperimentRunner

    config_path = resolve_config_path(args.config, MODEL_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config} (tried {config_path})")

    logger.info("Rebuilding matches.parquet")
    MatchesAggregator().run()

    runner = ExperimentRunner(config_path=config_path)
    results = runner.run()

    print_run_summary(results, name=runner.config.name)

    return 0


def cmd_experiment(args: argparse.Namespace) -> int:
    """Run automated feature discovery."""
    from mvp.atptour.aggregators.matches import MatchesAggregator
    from mvp.model.discovery import FeatureDiscovery

    config_path = resolve_config_path(args.config, EXPERIMENT_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config} (tried {config_path})")

    # Normalize output path: always in models/, add .yaml if needed
    output_name = args.output
    if not output_name.endswith(".yaml"):
        output_name = f"{output_name}.yaml"
    output_path = MODEL_DIR / output_name

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
        print(f"Run with: python -m mvp model {output_path.stem}")
    else:
        print("\nNo features selected - no config saved")

    return 0


def cmd_live(args: argparse.Namespace) -> int:
    """Run live pipeline: extract, aggregate, predict."""
    from datetime import datetime

    from mvp.atptour.aggregators.matches import MatchesAggregator
    from mvp.atptour.discovery import TournamentDiscovery
    from mvp.atptour.pipeline import (
        _process_tournaments,
        run_player_data,
        run_rankings,
    )

    current_year = datetime.now().year
    run_rankings(start_year=current_year - 1)

    # Resolve active tournaments
    discovery = TournamentDiscovery()
    pairs = discovery.get_active_tournaments()
    if args.tid is not None:
        pairs = [(t, y) for t, y in pairs if t == args.tid]
        if not pairs:
            raise ValueError(f"Tournament {args.tid} is not currently active")

    tournaments = [(t, year, False, None) for t, year in pairs]
    logger.info("Processing %d active tournaments", len(tournaments))

    failed = _process_tournaments(tournaments, data_root=None, refresh=args.refresh)

    run_tids = {(tid, yr) for tid, yr, _, _ in tournaments}
    player_result = run_player_data(run_tids=run_tids)

    logger.info("Running cross-tournament aggregation")
    MatchesAggregator().run()

    # TODO: feature engineering (Phase 5)
    # TODO: predict with active model (Phase 5)

    # Report failures
    if failed or player_result.has_failures:
        error_parts = []
        if failed:
            error_parts.append(f"{len(failed)} failed tournament(s)")
            for tid, year, error in failed:
                logger.error("  FAILED: tournament %s (%d): %s", tid, year, error)
        if player_result.has_failures:
            error_parts.append(
                f"{len(player_result.all_failures)} failed player operation(s)"
            )
        raise RuntimeError(f"Pipeline finished with {', '.join(error_parts)}")

    return 0


def main(args: list[str] | None = None) -> int:
    """CLI entry point."""
    parsed = parse_args(args)
    logging.basicConfig(
        level=getattr(logging, parsed.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if parsed.command == "model":
        return cmd_model(parsed)
    elif parsed.command == "experiment":
        return cmd_experiment(parsed)
    elif parsed.command == "live":
        return cmd_live(parsed)

    return 1


if __name__ == "__main__":
    sys.exit(main())
