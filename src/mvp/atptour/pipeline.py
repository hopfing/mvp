"""ATP Tour data pipeline — extraction, transformation, and orchestration."""

import argparse
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from mvp.atptour.aggregators.matches import MatchesAggregator
from mvp.atptour.aggregators.tournament_matches import TournamentMatchesAggregator
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


@dataclass
class PlayerDataResult:
    """Collect failure lists from player data processing."""

    failed_bio_fetch: list[tuple[str, str]] = field(default_factory=list)
    failed_bio_stage: list[tuple[str, str]] = field(default_factory=list)
    failed_activity_fetch: list[tuple[str, str]] = field(default_factory=list)
    failed_activity_stage: list[tuple[str, str]] = field(default_factory=list)

    @property
    def has_failures(self) -> bool:
        return bool(
            self.failed_bio_fetch
            or self.failed_bio_stage
            or self.failed_activity_fetch
            or self.failed_activity_stage
        )

    @property
    def all_failures(self) -> list:
        return (
            self.failed_bio_fetch
            + self.failed_bio_stage
            + self.failed_activity_fetch
            + self.failed_activity_stage
        )


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
    parser.add_argument(
        "--aggregate-only",
        action="store_true",
        help="Skip extraction/staging, only run Layer 2 aggregation",
    )
    parsed = parser.parse_args(args)
    if parsed.tid and not parsed.year:
        parser.error("--tid requires --year")
    if parsed.circuit and not parsed.year:
        parser.error("--circuit requires --year")
    if parsed.circuit and parsed.tid:
        parser.error("--circuit cannot be used with --tid")
    if parsed.aggregate_only and (parsed.tid or parsed.year or parsed.circuit):
        parser.error(
            "--aggregate-only cannot be used with --tid, --year, or --circuit"
        )
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

            TournamentMatchesAggregator(
                circuit=tournament.circuit,
                tid=tournament.tournament_id,
                year=tournament.year,
                data_root=data_root,
            ).run()

            logger.info("[%d/%d] Completed %s", idx, total, tournament.logging_id)
        except Exception as e:
            logger.exception("Failed processing tournament %s (%d)", tid, year)
            failed.append((tid, year, str(e)))

    return failed


def run_rankings(*, start_year: int, data_root: Path | None = None) -> None:
    """Extract, transform, and consolidate rankings."""
    RankingsExtractor(start_year=start_year, data_root=data_root).run()
    tx = RankingsTransformer(data_root=data_root)
    tx.run(start_year=start_year)
    tx.consolidate()


def run_player_data(
    *,
    run_tids: set[tuple[str, int]],
    data_root: Path | None = None,
) -> PlayerDataResult:
    """Extract and transform player bio + activity data, scoped to run tournaments."""
    if data_root is None:
        default_root = Path(__file__).resolve().parents[3] / "data"
    else:
        default_root = data_root
    tournaments_stage_dir = default_root / "stage" / "atptour" / "tournaments"

    all_player_tournaments = get_active_players(tournaments_stage_dir)
    player_tournaments: dict[str, set[tuple[str, int]]] = {}
    for pid, tid_years in all_player_tournaments.items():
        scoped = tid_years & run_tids
        if scoped:
            player_tournaments[pid] = scoped
    player_ids = sorted(player_tournaments.keys())

    result = PlayerDataResult()

    if player_ids:
        result.failed_bio_fetch = PlayerBioExtractor(data_root=data_root).run(
            player_ids
        )
        result.failed_bio_stage = PlayerBioStager(data_root=data_root).run()
        PlayerBioTransformer(data_root=data_root).run()

    if player_tournaments:
        result.failed_activity_fetch = PlayerActivityExtractor(
            data_root=data_root
        ).run(player_tournaments)
        result.failed_activity_stage = PlayerActivityStager(data_root=data_root).run()
        PlayerActivityTransformer(data_root=data_root).run()

    return result


def run_pipeline(
    *,
    data_root: Path | None = None,
    year: int | None = None,
    tournament_ids: list[str] | None = None,
    circuit: str | None = None,
    refresh: bool = False,
    aggregate_only: bool = False,
) -> None:
    """Core pipeline function — callable from code.

    When aggregate_only is True, skips extraction/staging and only runs
    Layer 2 aggregation (MatchesAggregator).

    Raises RuntimeError if any operations fail.
    """
    if aggregate_only:
        logger.info("Running Layer 2 aggregation only")
        MatchesAggregator(data_root=data_root).run()
        return

    start_year = (year - 1) if year else (_current_year() - 1)
    run_rankings(start_year=start_year, data_root=data_root)

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

    run_tids = {(tid, yr) for tid, yr, _, _ in tournaments}
    player_result = run_player_data(run_tids=run_tids, data_root=data_root)

    # Layer 2: cross-tournament aggregation
    logger.info("Running Layer 2 aggregation")
    MatchesAggregator(data_root=data_root).run()

    _log_summary(tournaments, failed, player_result)

    error_parts = []
    if failed:
        error_parts.append(f"{len(failed)} failed tournament(s)")
    if player_result.has_failures:
        error_parts.append(
            f"{len(player_result.all_failures)} failed player operation(s)"
        )
    if error_parts:
        raise RuntimeError(
            f"Pipeline finished with {', '.join(error_parts)}"
        )


def _log_summary(
    tournaments,
    failed,
    player_result: PlayerDataResult,
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
        aggregate_only=args.aggregate_only,
    )
