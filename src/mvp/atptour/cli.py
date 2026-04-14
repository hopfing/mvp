"""CLI entry point for atptour backfill operations."""


import argparse
import logging
from datetime import datetime

from mvp.atptour.aggregators.match_beats_points import MatchBeatsPointsAggregator
from mvp.atptour.discovery import TournamentDiscovery
from mvp.atptour.pipeline import (
    PlayerDataResult,
    _process_tournaments,
    run_player_data,
    run_rankings,
)
from mvp.common.enums import Circuit

logger = logging.getLogger(__name__)


def _current_year() -> int:
    return datetime.now().year


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for backfill subcommand."""
    parser = argparse.ArgumentParser(
        prog="python -m mvp.atptour",
        description="ATP Tour data pipeline - backfill operations",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # backfill
    backfill_parser = subparsers.add_parser(
        "backfill", help="Process historical tournaments"
    )
    backfill_parser.add_argument("--year", type=int, required=True, metavar="YEAR")
    backfill_parser.add_argument("--tid", nargs="+", type=str, metavar="TID")
    backfill_parser.add_argument("--circuit", choices=["tour", "chal"])

    # aggregate-points
    subparsers.add_parser(
        "aggregate-points",
        help="Aggregate match_beats point-level data with reconstructed score state",
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

    if args.command == "backfill":
        cmd_backfill(args)
    elif args.command == "aggregate-points":
        cmd_aggregate_points()


def _resolve_backfill_tournaments(
    *,
    year: int,
    tid: list[str] | None = None,
    circuit: str | None = None,
) -> list[tuple[str, int, bool, Circuit | None]]:
    """Resolve tournaments for historical backfill."""
    discovery = TournamentDiscovery()
    current_year = _current_year()

    if tid:
        if year < current_year:
            return [(t, year, True, None) for t in tid]
        active_tids = {t for t, _ in discovery.get_active_tournaments()}
        return [(t, year, t not in active_tids, None) for t in tid]

    circuit_enum = Circuit(circuit) if circuit else None
    triples = discovery.get_archive_tournaments(year, circuit=circuit_enum)
    if year < current_year:
        return [(t, y, True, c) for t, y, c in triples]
    active_tids = {t for t, _ in discovery.get_active_tournaments()}
    return [(t, y, t not in active_tids, c) for t, y, c in triples]


def cmd_aggregate_points() -> None:
    """Aggregate match_beats to point-level with reconstructed score state."""
    result = MatchBeatsPointsAggregator().run()
    if result is None:
        logger.info("Points aggregation produced no output")
        return
    logger.info("Points aggregation complete: %d rows", len(result))


def cmd_backfill(args) -> None:
    """Historical backfill: extract, stage, per-tournament aggregate."""
    run_rankings(start_year=args.year - 1)

    tournaments = _resolve_backfill_tournaments(
        year=args.year,
        tid=args.tid,
        circuit=args.circuit,
    )
    logger.info("Backfilling %d tournaments for %d", len(tournaments), args.year)

    failed = _process_tournaments(tournaments, data_root=None, refresh=False)

    run_tids = {(tid, yr) for tid, yr, _, _ in tournaments}
    player_result = run_player_data(run_tids=run_tids, live=False)

    _report_failures(tournaments, failed, player_result)


def _report_failures(
    tournaments: list,
    failed: list[tuple[str, int, str]],
    player_result: PlayerDataResult,
) -> None:
    """Log summary and raise on failures."""
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("Tournaments processed: %d", len(tournaments) - len(failed))
    if failed:
        logger.error("Tournaments failed: %d", len(failed))
        for tid, year, error in failed:
            logger.error("  FAILED: tournament %s (%d): %s", tid, year, error)
    else:
        logger.info("All %d tournaments succeeded", len(tournaments))

    for label, failures in [
        ("bio fetch", player_result.failed_bio_fetch),
        ("bio stage", player_result.failed_bio_stage),
        ("activity fetch", player_result.failed_activity_fetch),
        ("activity stage", player_result.failed_activity_stage),
    ]:
        if failures:
            logger.error("Player %s failed: %d", label, len(failures))
        else:
            logger.info("Player %s: all succeeded", label)
    logger.info("=" * 60)

    error_parts = []
    if failed:
        error_parts.append(f"{len(failed)} failed tournament(s)")
    if player_result.has_failures:
        error_parts.append(
            f"{len(player_result.all_failures)} failed player operation(s)"
        )
    if error_parts:
        raise RuntimeError(f"Pipeline finished with {', '.join(error_parts)}")
