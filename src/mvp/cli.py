"""Unified CLI entry point with model and live subcommands."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


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

    # model subcommand
    model_parser = subparsers.add_parser(
        "model", help="Train model from experiment config"
    )
    model_parser.add_argument(
        "config", type=str, help="Path to experiment config YAML"
    )
    model_parser.add_argument(
        "--matches", type=str, default=None, help="Path to matches.parquet"
    )
    model_parser.add_argument(
        "--mlflow-dir", type=str, default=None, help="MLflow tracking directory"
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

    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    logger.info("Rebuilding matches.parquet")
    MatchesAggregator().run()

    matches_path = Path(args.matches) if args.matches else None
    mlflow_dir = Path(args.mlflow_dir) if args.mlflow_dir else None

    runner = ExperimentRunner(
        config_path=config_path,
        matches_path=matches_path,
        mlflow_dir=mlflow_dir,
    )
    results = runner.run()

    print(f"Experiment completed: {runner.config.name}")
    print(f"Run ID: {results['run_id']}")
    print(f"Folds: {results['n_folds']}")
    print("Metrics:")
    for name, value in results["metrics"].items():
        print(f"  {name}: {value:.4f}")

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
    elif parsed.command == "live":
        return cmd_live(parsed)

    return 1


if __name__ == "__main__":
    sys.exit(main())
