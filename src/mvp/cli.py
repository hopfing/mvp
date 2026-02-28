"""Unified CLI entry point."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

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
        "--save", type=str, default=None, help="Save recommended config to path"
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

    print(f"Experiment completed: {runner.config.name}")
    print(f"Run ID: {results['run_id']}")
    print(f"Folds: {results['n_folds']}")
    print("Metrics:")
    for name, value in results["metrics"].items():
        print(f"  {name}: {value:.4f}")

    return 0


def cmd_experiment(args: argparse.Namespace) -> int:
    """Run automated feature discovery."""
    from mvp.atptour.aggregators.matches import MatchesAggregator
    from mvp.model.discovery import FeatureDiscovery

    config_path = resolve_config_path(args.config, EXPERIMENT_DIR)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config} (tried {config_path})")

    logger.info("Rebuilding matches.parquet")
    MatchesAggregator().run()

    discovery = FeatureDiscovery(
        config_path=config_path,
        verbose=args.verbose,
    )

    result = discovery.run()

    if args.save and result.selected_features:
        discovery._last_result = result
        discovery.save_config(args.save)
        print(f"Saved recommended config to: {args.save}")

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
