"""MatchCentreExtractor - fetch data from Infosys Match Centre API.

Fetches multiple data types (match_beats, stroke_analysis, rally_analysis)
based on availability from the status endpoint.
"""

import logging
from dataclasses import dataclass
from enum import StrEnum

import polars as pl
from curl_cffi import requests

from mvp.atptour.extractors.match_beats_decrypt import decrypt_response
from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

API_BASE = "https://itp-atp-sls.infosys-platforms.com/prod/api"


class DataType(StrEnum):
    """Available Match Centre data types."""

    MATCH_BEATS = "match_beats"
    STROKE_ANALYSIS = "stroke_analysis"
    RALLY_ANALYSIS = "rally_analysis"


@dataclass
class DataTypeConfig:
    """Configuration for a data type."""

    status_flag: str  # Key in matchCenter status object
    endpoint: str  # API endpoint path
    folder: str  # Output folder name
    completeness_check: str | None = None  # Field to check for complete data


DATA_TYPE_CONFIGS: dict[DataType, DataTypeConfig] = {
    DataType.MATCH_BEATS: DataTypeConfig(
        status_flag="matchBeats",
        endpoint=f"{API_BASE}/match-beats/data",
        folder="match_beats",
        completeness_check="isMatchComplete",
    ),
    DataType.STROKE_ANALYSIS: DataTypeConfig(
        status_flag="strokeSummary",
        endpoint=f"{API_BASE}/stroke-analysis/rally/v2",
        folder="stroke_analysis",
        completeness_check="matchCompleted",
    ),
    DataType.RALLY_ANALYSIS: DataTypeConfig(
        status_flag="rallyAnalysis",
        endpoint=f"{API_BASE}/rally-analysis",
        folder="rally_analysis",
        completeness_check="matchCompleted",
    ),
}

STATUS_ENDPOINT = f"{API_BASE}/match-beats/status"


class MatchCentreExtractor(BaseExtractor):
    """Fetch data from Infosys Match Centre API.

    Fetches status once per match, then fetches all requested data types
    that are available according to the status response.
    """

    def __init__(
        self,
        data_root=None,
        data_types: list[DataType] | None = None,
    ):
        super().__init__(domain="atptour", data_root=data_root)
        self.data_types = data_types or [DataType.MATCH_BEATS]

    def run(self, tournament: Tournament, refresh: bool = False) -> int:
        """Fetch Match Centre data for all matches in a tournament.

        Returns:
            Total number of new files saved across all data types.
        """
        if tournament.year < 2022:
            logger.debug(
                "Skipping Match Centre for %s (pre-2022)", tournament.logging_id
            )
            return 0

        match_ids = self._get_match_ids(tournament)
        if not match_ids:
            logger.info("No match IDs for %s", tournament.logging_id)
            return 0

        # Determine which matches need fetching for each data type
        to_fetch_by_type = {}
        for dt in self.data_types:
            config = DATA_TYPE_CONFIGS[dt]
            data_dir = self.build_path("raw", tournament.path, config.folder)
            if refresh:
                existing = set()
            else:
                existing = {p.stem.upper() for p in self.list_files(data_dir, "*.json")}
            to_fetch = [mid.upper() for mid in match_ids if mid.upper() not in existing]
            to_fetch_by_type[dt] = set(to_fetch)

        # Get union of all matches that need fetching
        all_to_fetch = set()
        for matches in to_fetch_by_type.values():
            all_to_fetch.update(matches)

        if not all_to_fetch:
            logger.info("%s: all data already fetched", tournament.logging_id)
            return 0

        logger.info(
            "%s: %d match IDs, checking %d for new data",
            tournament.logging_id,
            len(match_ids),
            len(all_to_fetch),
        )

        stats = {dt: {"saved": 0, "skipped": 0, "failed": 0} for dt in self.data_types}
        status_failures = 0

        for match_id in sorted(all_to_fetch):
            mid = match_id.upper()

            # Fetch status once per match
            status = self._get_match_status(
                tournament.year, tournament.tournament_id, mid
            )
            if status is None:
                # A failed status call is an error (egress/WAF block, upstream
                # outage, decrypt failure) — NOT a benign "data not available"
                # skip. Track it separately so a total block can't masquerade as
                # a quiet skipped count.
                status_failures += 1
                continue

            match_center = status.get("matchCenter", {})

            # Fetch each requested data type if available
            for dt in self.data_types:
                if mid not in to_fetch_by_type[dt]:
                    continue

                config = DATA_TYPE_CONFIGS[dt]

                if not match_center.get(config.status_flag, False):
                    logger.debug(
                        "%s not available for %s match %s",
                        dt.value,
                        tournament.logging_id,
                        mid,
                    )
                    stats[dt]["skipped"] += 1
                    continue

                data = self._fetch_data(
                    config.endpoint,
                    tournament.year,
                    tournament.tournament_id,
                    mid,
                )
                if data is None:
                    stats[dt]["failed"] += 1
                    continue

                # Check completeness if configured
                if config.completeness_check:
                    if not data.get(config.completeness_check, False):
                        logger.debug(
                            "Skipping incomplete %s for %s match %s",
                            dt.value,
                            tournament.logging_id,
                            mid,
                        )
                        stats[dt]["skipped"] += 1
                        continue

                target = self.build_path(
                    "raw", tournament.path, f"{config.folder}/{mid}.json"
                )
                self.save_json(data, target)
                stats[dt]["saved"] += 1

        # Log results. Elevate to WARNING when anything actually failed (status
        # call or data fetch) so an egress/WAF block or upstream outage surfaces
        # — WARNING records survive the pipeline's WARNING-only worker-log replay
        # instead of being swallowed as INFO.
        total_saved = 0
        total_failed = sum(stats[dt]["failed"] for dt in self.data_types)
        level = logging.WARNING if (status_failures or total_failed) else logging.INFO
        if status_failures:
            logger.log(
                level,
                "%s: match-centre status fetch FAILED for %d/%d match(es) "
                "— possible egress/WAF block or upstream outage",
                tournament.logging_id,
                status_failures,
                len(all_to_fetch),
            )
        for dt in self.data_types:
            s = stats[dt]
            total_saved += s["saved"]
            logger.log(
                level,
                "%s: %s - saved %d, skipped %d, failed %d",
                tournament.logging_id,
                dt.value,
                s["saved"],
                s["skipped"],
                s["failed"],
            )
        return total_saved

    def _get_match_ids(self, tournament: Tournament) -> list[str]:
        """Read match IDs from staged results parquet."""
        path = self.build_path("stage", tournament.path, "results.parquet")
        if not path.exists():
            logger.warning("No results parquet for %s", tournament.logging_id)
            return []

        codes = (
            pl.read_parquet(path, columns=["match_id"])
            .filter(pl.col("match_id").is_not_null())
            .unique()
            .sort("match_id")
            .to_series()
            .to_list()
        )
        return codes

    def _get_match_status(
        self, year: int, event_id: str, match_id: str
    ) -> dict | None:
        """Fetch match status to check data availability."""
        url = f"{STATUS_ENDPOINT}/year/{year}/eventId/{event_id}/matchId/{match_id}"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response_json = response.json()

            last_modified = response_json.get("lastModified")
            encrypted = response_json.get("response")
            if not last_modified or not encrypted:
                return None

            return decrypt_response(encrypted, last_modified)
        except (requests.RequestsError, ValueError, KeyError) as e:
            logger.debug("Status fetch failed for match %s: %s", match_id, e)
            return None

    def _fetch_data(
        self, endpoint: str, year: int, event_id: str, match_id: str
    ) -> dict | None:
        """Fetch and decrypt data from an endpoint."""
        url = f"{endpoint}/year/{year}/eventId/{event_id}/matchId/{match_id}"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response_json = response.json()

            last_modified = response_json.get("lastModified")
            encrypted = response_json.get("response")
            if not last_modified or not encrypted:
                return None

            return decrypt_response(encrypted, last_modified)
        except (requests.RequestsError, ValueError, KeyError) as e:
            logger.warning("Fetch failed for %s: %s", endpoint, e)
            return None
