"""ATP Tour data pipeline — extraction, transformation, and orchestration."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from mvp.atptour.aggregators.tournament_matches import TournamentMatchesAggregator
from mvp.atptour.extractors.match_centre import DataType, MatchCentreExtractor
from mvp.atptour.extractors.match_stats import MatchStatsExtractor
from mvp.atptour.extractors.overview import OverviewExtractor
from mvp.atptour.extractors.player_activity import PlayerActivityExtractor
from mvp.atptour.extractors.player_bio import PlayerBioExtractor
from mvp.atptour.extractors.rankings import RankingsExtractor
from mvp.atptour.extractors.results import ResultsExtractor
from mvp.atptour.extractors.schedule import ScheduleExtractor
from mvp.atptour.pipeline_utils import get_active_players, get_players_with_results
from mvp.atptour.transformers.match_beats import MatchBeatsTransformer
from mvp.atptour.transformers.rally_analysis import RallyAnalysisTransformer
from mvp.atptour.transformers.stroke_analysis import StrokeAnalysisTransformer
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

MAX_TOURNAMENT_WORKERS = 3


class _WorkerThreadFilter(logging.Filter):
    """Blocks log records from registered worker threads on main handlers."""

    def __init__(self) -> None:
        super().__init__()
        self._blocked: set[int] = set()
        self._lock = threading.Lock()

    def register(self, thread_id: int) -> None:
        with self._lock:
            self._blocked.add(thread_id)

    def unregister(self, thread_id: int) -> None:
        with self._lock:
            self._blocked.discard(thread_id)

    def filter(self, record: logging.LogRecord) -> bool:
        return record.thread not in self._blocked


class _BufferHandler(logging.Handler):
    """Captures log records from a specific thread for later replay."""

    def __init__(self, thread_id: int) -> None:
        super().__init__()
        self._thread_id = thread_id
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.thread == self._thread_id:
            self.records.append(record)


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


def _process_single_tournament(
    tid: str,
    year: int,
    is_archive: bool,
    circuit: Circuit | None,
    *,
    data_root: Path | None,
    refresh: bool,
    thread_filter: _WorkerThreadFilter,
    replay_lock: threading.Lock,
) -> tuple[str, int, str] | None:
    """Process one tournament with suppressed logging.

    Detail logs are captured in a buffer and suppressed from the console.
    Only WARNING+ records are replayed after completion. A concise progress
    line is printed to stdout for each tournament.

    Returns ``(tid, year, error_str)`` on failure, ``None`` on success.
    """
    thread_id = threading.current_thread().ident
    thread_filter.register(thread_id)
    buffer = _BufferHandler(thread_id)
    logging.getLogger().addHandler(buffer)

    logging_id = f"{tid} ({year})"
    error_result = None

    try:
        tournament = OverviewExtractor(data_root=data_root).run(
            tournament_id=tid,
            year=year,
            is_archive=is_archive,
            refresh=refresh,
            circuit=circuit,
        )
        logging_id = tournament.logging_id
        logger.info("Processing %s", logging_id)

        OverviewTransformer(tournament, data_root=data_root).run()
        ScheduleExtractor(data_root=data_root).run(tournament)
        ScheduleTransformer(tournament, data_root=data_root).run()

        results_refresh = True  # Always refresh results
        stats_refresh = refresh  # Only refresh stats when explicitly requested

        ResultsExtractor(data_root=data_root).run(
            tournament, refresh=results_refresh
        )
        ResultsTransformer(tournament, data_root=data_root).run()
        new_stats = MatchStatsExtractor(data_root=data_root).run(
            tournament, refresh=stats_refresh
        )
        if new_stats > 0:
            MatchStatsTransformer(tournament, data_root=data_root).run()
        else:
            logger.info(
                "%s: no new match stats, skipping transform",
                logging_id,
            )

        new_centre = MatchCentreExtractor(
            data_root=data_root,
            data_types=[
                DataType.MATCH_BEATS,
                DataType.STROKE_ANALYSIS,
                DataType.RALLY_ANALYSIS,
            ],
        ).run(tournament, refresh=stats_refresh)
        if new_centre > 0:
            MatchBeatsTransformer(tournament, data_root=data_root).run()
            StrokeAnalysisTransformer(tournament, data_root=data_root).run()
            RallyAnalysisTransformer(tournament, data_root=data_root).run()
        else:
            logger.info(
                "%s: no new match centre data, skipping transforms",
                logging_id,
            )

        TournamentMatchesAggregator(
            circuit=tournament.circuit,
            tid=tournament.tournament_id,
            year=tournament.year,
            data_root=data_root,
        ).run()

        return None
    except Exception as e:
        logger.exception("Failed processing tournament %s (%d)", tid, year)
        error_result = (tid, year, str(e))
        return error_result
    finally:
        logging.getLogger().removeHandler(buffer)
        thread_filter.unregister(thread_id)
        with replay_lock:
            # Progress line
            if error_result is None:
                print(f"  Completed {logging_id}")
            else:
                print(f"  FAILED {logging_id}")
            # Only surface warnings/errors
            for record in buffer.records:
                if record.levelno >= logging.WARNING:
                    for handler in logging.getLogger().handlers:
                        handler.emit(record)


def _process_tournaments(
    tournaments: list[tuple[str, int, bool, Circuit | None]],
    *,
    data_root: Path | None,
    refresh: bool,
) -> list[tuple[str, int, str]]:
    """Per-tournament extraction + transformation loop (parallel)."""
    if not tournaments:
        return []

    total = len(tournaments)
    logger.info(
        "Processing %d tournaments in parallel (%d workers)",
        total,
        MAX_TOURNAMENT_WORKERS,
    )

    thread_filter = _WorkerThreadFilter()
    replay_lock = threading.Lock()

    for handler in logging.getLogger().handlers:
        handler.addFilter(thread_filter)

    failed: list[tuple[str, int, str]] = []
    try:
        with ThreadPoolExecutor(max_workers=MAX_TOURNAMENT_WORKERS) as pool:
            futures = {
                pool.submit(
                    _process_single_tournament,
                    tid,
                    year,
                    is_archive,
                    circuit,
                    data_root=data_root,
                    refresh=refresh,
                    thread_filter=thread_filter,
                    replay_lock=replay_lock,
                ): (tid, year)
                for tid, year, is_archive, circuit in tournaments
            }
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    failed.append(result)
    finally:
        for handler in logging.getLogger().handlers:
            handler.removeFilter(thread_filter)

    n_failed = len(failed)
    logger.info(
        "Completed %d/%d tournaments (%d failed)",
        total - n_failed,
        total,
        n_failed,
    )

    return failed


def run_rankings(*, start_year: int, data_root: Path | None = None) -> None:
    """Extract, transform, and consolidate rankings."""
    new_pages = RankingsExtractor(start_year=start_year, data_root=data_root).run()
    if new_pages == 0:
        logger.info("Rankings: no new pages, skipping transform/consolidate")
        return
    tx = RankingsTransformer(data_root=data_root)
    tx.run(start_year=start_year)
    tx.consolidate()


def run_player_data(
    *,
    run_tids: set[tuple[str, int]],
    data_root: Path | None = None,
    live: bool = True,
    refresh_players: bool = False,
) -> PlayerDataResult:
    """Extract and transform player bio + activity data, scoped to run tournaments.

    Args:
        refresh_players: When True, run activity extraction/staging even if
            it would normally be skipped. Also forces bio stager/transformer
            even when no new bios were fetched.
    """
    if data_root is None:
        default_root = Path(__file__).resolve().parents[3] / "data"
    else:
        default_root = data_root
    tournaments_stage_dir = default_root / "stage" / "atptour" / "tournaments"

    source = "schedule" if live else "results"
    all_player_tournaments = get_active_players(tournaments_stage_dir, source=source)
    player_tournaments: dict[str, set[tuple[str, int]]] = {}
    for pid, tid_years in all_player_tournaments.items():
        scoped = tid_years & run_tids
        if scoped:
            player_tournaments[pid] = scoped
    player_ids = sorted(player_tournaments.keys())

    result = PlayerDataResult()

    if player_ids:
        result.failed_bio_fetch, new_bios = PlayerBioExtractor(
            data_root=data_root
        ).run(player_ids)
        if new_bios > 0 or refresh_players:
            result.failed_bio_stage = PlayerBioStager(data_root=data_root).run()
            PlayerBioTransformer(data_root=data_root).run()
        else:
            logger.info("Player bios: no new fetches, skipping stager/transformer")

    if player_tournaments and refresh_players:
        players_with_results = get_players_with_results(
            tournaments_stage_dir, run_tids
        )
        activity_tournaments = {
            pid: tids
            for pid, tids in player_tournaments.items()
            if pid not in players_with_results
        }
        logger.info(
            "Activity scope: %d scheduled, %d with results, %d need activity",
            len(player_tournaments),
            len(players_with_results & set(player_tournaments)),
            len(activity_tournaments),
        )
        result.failed_activity_fetch = PlayerActivityExtractor(
            data_root=data_root
        ).run(activity_tournaments)
        result.failed_activity_stage = PlayerActivityStager(
            data_root=data_root
        ).run(player_ids=set(activity_tournaments))
        PlayerActivityTransformer(data_root=data_root).run()
    elif player_tournaments:
        logger.info(
            "Activity: skipped (use --refresh-players to run)"
        )

    return result
