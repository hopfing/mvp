"""MatchBeatsExtractor - fetch point-by-point data from Infosys API."""

import logging

import polars as pl
import requests

from mvp.atptour.extractors.match_beats_decrypt import decrypt_response
from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

MATCHBEATS_BASE = "https://itp-atp-sls.infosys-platforms.com/prod/api/match-beats/data"


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
            url = self._build_url(
                year=tournament.year,
                event_id=tournament.tournament_id,
                match_id=match_id,
            )

            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.warning(
                    "MatchBeats request failed for %s match %s: %s",
                    tournament.logging_id,
                    match_id,
                    e,
                )
                failed += 1
                continue

            try:
                response_json = response.json()
                last_modified = response_json.get("lastModified")
                encrypted = response_json.get("response")

                if not last_modified or not encrypted:
                    logger.debug(
                        "Missing lastModified or response for %s match %s",
                        tournament.logging_id,
                        match_id,
                    )
                    skipped += 1
                    continue

                data = decrypt_response(encrypted, last_modified)

            except (ValueError, KeyError) as e:
                logger.warning(
                    "MatchBeats decrypt failed for %s match %s: %s",
                    tournament.logging_id,
                    match_id,
                    e,
                )
                failed += 1
                continue

            if not data.get("isMatchComplete", False):
                logger.debug(
                    "Skipping stub data for %s match %s",
                    tournament.logging_id,
                    match_id,
                )
                skipped += 1
                continue

            target = self.build_path(
                "raw", tournament.path, f"match_beats/{match_id.upper()}.json"
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

    def _build_url(self, year: int, event_id: str, match_id: str) -> str:
        """Build MatchBeats API URL with uppercase match ID."""
        mid = match_id.upper()
        return f"{MATCHBEATS_BASE}/year/{year}/eventId/{event_id}/matchId/{mid}"
