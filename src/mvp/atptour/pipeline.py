"""ATP Tour data pipeline — extraction, transformation, and orchestration."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from mvp.atptour.discovery import TournamentDiscovery
from mvp.atptour.extractors.match_stats import MatchStatsExtractor
from mvp.atptour.extractors.overview import OverviewExtractor
from mvp.atptour.extractors.player_activity import PlayerActivityExtractor
from mvp.atptour.extractors.player_bio import PlayerBioExtractor
from mvp.atptour.extractors.rankings import RankingsExtractor
from mvp.atptour.extractors.results import ResultsExtractor
from mvp.atptour.extractors.schedule import ScheduleExtractor
from mvp.atptour.pipeline_utils import get_active_players
from mvp.atptour.transformers.match_stats import MatchStatsTransformer
from mvp.atptour.transformers.overview import OverviewTransformer
from mvp.atptour.transformers.player_activity import (
    PlayerActivityStager,
    PlayerActivityTransformer,
)
from mvp.atptour.transformers.player_bio import (
    PlayerBioStager,
    PlayerBioTransformer,
)
from mvp.atptour.transformers.rankings import RankingsTransformer
from mvp.atptour.transformers.results import ResultsTransformer
from mvp.atptour.transformers.schedule import ScheduleTransformer
from mvp.common.enums import Circuit

logger = logging.getLogger(__name__)


def _current_year() -> int:
    return datetime.now().year


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments. Accepts optional args list for testing."""
    parser = argparse.ArgumentParser(description="Run ATP Tour data pipeline")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument("--tid", nargs="+", type=str, metavar="TID")
    parser.add_argument("--year", type=int, metavar="YEAR")
    parser.add_argument("--circuit", choices=["tour", "chal"])
    parser.add_argument("--refresh", action="store_true")
    parsed = parser.parse_args(args)
    if parsed.tid and not parsed.year:
        parser.error("--tid requires --year")
    if parsed.circuit and not parsed.year:
        parser.error("--circuit requires --year")
    if parsed.circuit and parsed.tid:
        parser.error("--circuit cannot be used with --tid")
    return parsed


def _resolve_tournaments(
    args,
    discovery: TournamentDiscovery,
) -> list[tuple[str, int, bool, Circuit | None]]:
    """Determine (tid, year, is_archive, circuit) tuples from CLI args."""
    current_year = _current_year()

    if args.tid:
        year = args.year
        if year < current_year:
            return [(tid, year, True, None) for tid in args.tid]
        active_tids = {tid for tid, _ in discovery.get_active_tournaments()}
        return [(tid, year, tid not in active_tids, None) for tid in args.tid]

    if args.year:
        year = args.year
        circuit = Circuit(args.circuit) if args.circuit else None
        triples = discovery.get_archive_tournaments(year, circuit=circuit)
        if year < current_year:
            return [(tid, y, True, c) for tid, y, c in triples]
        active_tids = {tid for tid, _ in discovery.get_active_tournaments()}
        return [(tid, y, tid not in active_tids, c) for tid, y, c in triples]

    pairs = discovery.get_active_tournaments()
    return [(tid, year, False, None) for tid, year in pairs]


def _process_tournaments(
    tournaments: list[tuple[str, int, bool, Circuit | None]],
    *,
    data_root: Path | None,
    refresh: bool,
) -> list[tuple[str, int, str]]:
    """Per-tournament extraction + transformation loop."""
    failed: list[tuple[str, int, str]] = []
    total = len(tournaments)

    for idx, (tid, year, is_archive, circuit) in enumerate(tournaments, 1):
        try:
            tournament = OverviewExtractor(data_root=data_root).run(
                tournament_id=tid,
                year=year,
                is_archive=is_archive,
                refresh=refresh,
                circuit=circuit,
            )
            logger.info("[%d/%d] Processing %s", idx, total, tournament.logging_id)

            OverviewTransformer(tournament, data_root=data_root).run()
            ScheduleExtractor(data_root=data_root).run(tournament)
            ScheduleTransformer(tournament, data_root=data_root).run()

            if refresh:
                results_refresh = True
                stats_refresh = True
            elif is_archive:
                results_refresh = False
                stats_refresh = False
            else:
                results_refresh = True
                stats_refresh = False

            ResultsExtractor(data_root=data_root).run(
                tournament, refresh=results_refresh
            )
            ResultsTransformer(tournament, data_root=data_root).run()
            MatchStatsExtractor(data_root=data_root).run(
                tournament, refresh=stats_refresh
            )
            MatchStatsTransformer(tournament, data_root=data_root).run()

            logger.info("[%d/%d] Completed %s", idx, total, tournament.logging_id)
        except Exception as e:
            logger.exception("Failed processing tournament %s (%d)", tid, year)
            failed.append((tid, year, str(e)))

    return failed


def run_pipeline(
    *,
    data_root: Path | None = None,
    year: int | None = None,
    tournament_ids: list[str] | None = None,
    circuit: str | None = None,
    refresh: bool = False,
) -> None:
    """Core pipeline function — callable from code.

    Raises RuntimeError if any operations fail.
    """
    start_year = (year - 1) if year else (_current_year() - 1)
    RankingsExtractor(start_year=start_year, data_root=data_root).run()
    RankingsTransformer(data_root=data_root).run(start_year=start_year)
    RankingsTransformer(data_root=data_root).consolidate()

    discovery = TournamentDiscovery(data_root=data_root)

    # Build a namespace compatible with _resolve_tournaments
    ns = argparse.Namespace(
        tid=tournament_ids,
        year=year,
        circuit=circuit,
    )
    tournaments = _resolve_tournaments(ns, discovery)

    logger.info("Processing %d tournaments", len(tournaments))
    failed = _process_tournaments(tournaments, data_root=data_root, refresh=refresh)

    # Player data — scoped to current run
    if data_root is None:
        default_root = Path(__file__).resolve().parents[3] / "data"
    else:
        default_root = data_root
    tournaments_stage_dir = default_root / "stage" / "atptour" / "tournaments"
    all_player_tournaments = get_active_players(tournaments_stage_dir)
    run_tids = {(tid, yr) for tid, yr, _, _ in tournaments}
    player_tournaments: dict[str, set[tuple[str, int]]] = {}
    for pid, tid_years in all_player_tournaments.items():
        scoped = tid_years & run_tids
        if scoped:
            player_tournaments[pid] = scoped
    player_ids = sorted(player_tournaments.keys())

    failed_bio_fetch: list[tuple[str, str]] = []
    failed_bio_stage: list[tuple[str, str]] = []
    if player_ids:
        failed_bio_fetch = PlayerBioExtractor(data_root=data_root).run(player_ids)
        failed_bio_stage = PlayerBioStager(data_root=data_root).run()
        PlayerBioTransformer(data_root=data_root).run()

    failed_activity_fetch: list[tuple[str, str]] = []
    failed_activity_stage: list[tuple[str, str]] = []
    if player_tournaments:
        failed_activity_fetch = PlayerActivityExtractor(data_root=data_root).run(
            player_tournaments
        )
        failed_activity_stage = PlayerActivityStager(data_root=data_root).run()
        PlayerActivityTransformer(data_root=data_root).run()

    _log_summary(
        tournaments,
        failed,
        failed_bio_fetch,
        failed_bio_stage,
        failed_activity_fetch,
        failed_activity_stage,
    )

    player_failures = (
        failed_bio_fetch
        + failed_bio_stage
        + failed_activity_fetch
        + failed_activity_stage
    )
    error_parts = []
    if failed:
        error_parts.append(f"{len(failed)} failed tournament(s)")
    if player_failures:
        error_parts.append(f"{len(player_failures)} failed player operation(s)")
    if error_parts:
        raise RuntimeError(
            f"Pipeline finished with {', '.join(error_parts)}"
        )


def _log_summary(
    tournaments,
    failed,
    failed_bio_fetch,
    failed_bio_stage,
    failed_activity_fetch,
    failed_activity_stage,
):
    """Log pipeline summary."""
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
        ("bio fetch", failed_bio_fetch),
        ("bio stage", failed_bio_stage),
        ("activity fetch", failed_activity_fetch),
        ("activity stage", failed_activity_stage),
    ]:
        if failures:
            logger.error("Player %s failed: %d", label, len(failures))
        else:
            logger.info("Player %s: all succeeded", label)
    logger.info("=" * 60)


def main():
    """CLI entry point."""
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    run_pipeline(
        year=args.year,
        tournament_ids=args.tid,
        circuit=args.circuit,
        refresh=args.refresh,
    )
