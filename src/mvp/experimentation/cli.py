"""CLI for experiment runner."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from mvp.experimentation.runner import ExperimentRunner


def main(args: list[str] | None = None) -> int:
    """Main CLI entry point.

    Args:
        args: Command line arguments (defaults to sys.argv[1:]).

    Returns:
        Exit code (0 for success).
    """
    parser = argparse.ArgumentParser(
        description="Run experiments",
        prog="python -m mvp.experimentation.cli",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run command
    run_parser = subparsers.add_parser("run", help="Run an experiment")
    run_parser.add_argument("config", type=Path, help="Path to config YAML")
    run_parser.add_argument(
        "--matches",
        type=Path,
        default=None,
        help="Path to matches.parquet",
    )
    run_parser.add_argument(
        "--mlflow-dir",
        type=Path,
        default=None,
        help="MLflow tracking directory",
    )

    parsed = parser.parse_args(args)

    if parsed.command == "run":
        runner = ExperimentRunner(
            config_path=parsed.config,
            matches_path=parsed.matches,
            mlflow_dir=parsed.mlflow_dir,
        )
        results = runner.run()

        print(f"Experiment completed: {runner.config.name}")
        print(f"Run ID: {results['run_id']}")
        print(f"Folds: {results['n_folds']}")
        print("Metrics:")
        for name, value in results["metrics"].items():
            print(f"  {name}: {value:.4f}")

        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
