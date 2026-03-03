"""ATP Tour data pipeline — extraction, transformation, and orchestration."""

import logging
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
from mvp.atptour.pipeline_utils import get_active_players
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

            results_refresh = True  # Always refresh results
            stats_refresh = refresh  # Only refresh stats when explicitly requested

            ResultsExtractor(data_root=data_root).run(
                tournament, refresh=results_refresh
            )
            ResultsTransformer(tournament, data_root=data_root).run()
            MatchStatsExtractor(data_root=data_root).run(
                tournament, refresh=stats_refresh
            )
            MatchStatsTransformer(tournament, data_root=data_root).run()

            # MatchBeats extraction + transformation (2022+ only, handled internally)
            MatchCentreExtractor(
                data_root=data_root,
                data_types=[
                    DataType.MATCH_BEATS,
                    DataType.STROKE_ANALYSIS,
                    DataType.RALLY_ANALYSIS,
                ],
            ).run(tournament, refresh=stats_refresh)
            MatchBeatsTransformer(tournament, data_root=data_root).run()
            StrokeAnalysisTransformer(tournament, data_root=data_root).run()
            RallyAnalysisTransformer(tournament, data_root=data_root).run()

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
