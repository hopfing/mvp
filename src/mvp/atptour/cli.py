"""CLI entry point with subcommands: live, backfill, model."""

from __future__ import annotations

import argparse
import logging

logger = logging.getLogger(__name__)


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments with subcommands."""
    parser = argparse.ArgumentParser(
        prog="python -m mvp.atptour",
        description="ATP Tour data pipeline",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # live
    live_parser = subparsers.add_parser(
        "live", help="Run live pipeline for active tournaments"
    )
    live_parser.add_argument(
        "--tid", type=str, metavar="TID", help="Target a single active tournament"
    )
    live_parser.add_argument(
        "--refresh", action="store_true", help="Force re-extraction of all data"
    )

    # backfill
    backfill_parser = subparsers.add_parser(
        "backfill", help="Process historical tournaments"
    )
    backfill_parser.add_argument("--year", type=int, required=True, metavar="YEAR")
    backfill_parser.add_argument("--tid", nargs="+", type=str, metavar="TID")
    backfill_parser.add_argument("--circuit", choices=["tour", "chal"])

    # model
    model_parser = subparsers.add_parser(
        "model", help="Train model and generate predictions"
    )
    model_parser.add_argument(
        "--config", type=str, metavar="CONFIG", help="Experiment config file"
    )

    parsed = parser.parse_args(args)

    if parsed.command == "backfill":
        if parsed.circuit and parsed.tid:
            backfill_parser.error("--circuit cannot be used with --tid")

    return parsed


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.command == "live":
        cmd_live(args)
    elif args.command == "backfill":
        cmd_backfill(args)
    elif args.command == "model":
        cmd_model(args)


def cmd_live(args: argparse.Namespace) -> None:
    """Placeholder -- wired in Task 3."""
    raise NotImplementedError


def cmd_backfill(args: argparse.Namespace) -> None:
    """Placeholder -- wired in Task 3."""
    raise NotImplementedError


def cmd_model(args: argparse.Namespace) -> None:
    """Placeholder -- wired in Task 3."""
    raise NotImplementedError
