"""MatchBeatsExtractor - fetch point-by-point data from Infosys API."""

import logging

import polars as pl
import requests

from mvp.atptour.extractors.match_beats_decrypt import decrypt_response
from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

API_BASE = "https://itp-atp-sls.infosys-platforms.com/prod/api"
MATCHBEATS_STATUS = f"{API_BASE}/match-beats/status"
MATCHBEATS_DATA = f"{API_BASE}/match-beats/data"


class MatchBeatsExtractor(BaseExtractor):
    """Fetch MatchBeats point-by-point data from Infosys API."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self, tournament: Tournament, refresh: bool = False) -> None:
        """Fetch MatchBeats data for all matches in a tournament."""
        if tournament.year < 2022:
            logger.debug(
                "Skipping MatchBeats for %s (pre-2022)", tournament.logging_id
            )
            return

        match_ids = self._get_match_ids(tournament)
        if not match_ids:
            logger.info("No match IDs for %s", tournament.logging_id)
            return

        beats_dir = self.build_path("raw", tournament.path, "match_beats")
        if refresh:
            existing = set()
        else:
            existing = {p.stem for p in self.list_files(beats_dir, "*.json")}
        to_fetch = [mid for mid in match_ids if mid.upper() not in existing]

        logger.info(
            "%s: %d match IDs, %d already fetched, %d to fetch",
            tournament.logging_id,
            len(match_ids),
            len(existing),
            len(to_fetch),
        )

        saved = 0
        skipped = 0
        failed = 0

        for match_id in to_fetch:
            mid = match_id.upper()

            # Check status first to see if data is available
            status = self._get_match_status(
                tournament.year, tournament.tournament_id, mid
            )
            if status is None:
                skipped += 1
                continue

            if not status.get("matchCenter", {}).get("matchBeats", False):
                logger.debug(
                    "MatchBeats not available for %s match %s",
                    tournament.logging_id,
                    mid,
                )
                skipped += 1
                continue

            # Fetch the actual data
            data = self._fetch_match_data(
                tournament.year, tournament.tournament_id, mid
            )
            if data is None:
                failed += 1
                continue

            if not data.get("isMatchComplete", False):
                logger.debug(
                    "Skipping stub data for %s match %s",
                    tournament.logging_id,
                    mid,
                )
                skipped += 1
                continue

            target = self.build_path(
                "raw", tournament.path, f"match_beats/{mid}.json"
            )
            self.save_json(data, target)
            saved += 1

        logger.info(
            "%s: saved %d MatchBeats, skipped %d stubs, %d failed",
            tournament.logging_id,
            saved,
            skipped,
            failed,
        )

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
        url = f"{MATCHBEATS_STATUS}/year/{year}/eventId/{event_id}/matchId/{match_id}"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response_json = response.json()

            last_modified = response_json.get("lastModified")
            encrypted = response_json.get("response")
            if not last_modified or not encrypted:
                return None

            return decrypt_response(encrypted, last_modified)
        except (requests.RequestException, ValueError, KeyError):
            return None

    def _fetch_match_data(
        self, year: int, event_id: str, match_id: str
    ) -> dict | None:
        """Fetch and decrypt match beats data."""
        url = f"{MATCHBEATS_DATA}/year/{year}/eventId/{event_id}/matchId/{match_id}"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response_json = response.json()

            last_modified = response_json.get("lastModified")
            encrypted = response_json.get("response")
            if not last_modified or not encrypted:
                return None

            return decrypt_response(encrypted, last_modified)
        except (requests.RequestException, ValueError, KeyError) as e:
            logger.warning("MatchBeats fetch failed: %s", e)
            return None
