"""MatchStatsExtractor — fetch per-match JSON from Hawkeye API."""

import logging

import polars as pl
import requests

from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

HAWKEYE_BASE = "https://www.atptour.com/-/Hawkeye/MatchStats/Complete"


class MatchStatsExtractor(BaseExtractor):
    """Fetch match stats JSON from the ATP Hawkeye API."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)
        self.session.headers.update(
            {
                "Referer": "https://www.atptour.com/",
                "Origin": "https://www.atptour.com",
            }
        )

    def run(self, tournament: Tournament, refresh: bool = False) -> None:
        """Fetch match stats for all match IDs found in staged results."""
        match_ids = self._get_match_ids(tournament)
        if not match_ids:
            logger.info("No match IDs for %s", tournament.logging_id)
            return

        stats_dir = self.build_path("raw", tournament.path, "match_stats")
        if refresh:
            existing = set()
        else:
            existing = {p.stem.upper() for p in self.list_files(stats_dir, "*.json")}
        to_fetch = [mid for mid in match_ids if mid.upper() not in existing]

        logger.info(
            "%s: %d match IDs, %d already fetched, %d to fetch",
            tournament.logging_id,
            len(match_ids),
            len(existing),
            len(to_fetch),
        )

        saved = 0
        failed = 0
        for match_id in to_fetch:
            url = f"{HAWKEYE_BASE}/{tournament.year}/{tournament.tournament_id}/{match_id}"
            target = self.build_path(
                "raw", tournament.path, f"match_stats/{match_id}.json"
            )

            try:
                data = self.fetch_json(url)
            except requests.RequestException as e:
                logger.warning(
                    "Hawkeye request failed for %s match %s: %s",
                    tournament.logging_id,
                    match_id,
                    e,
                )
                failed += 1
                continue
            except ValueError as e:
                logger.warning(
                    "Hawkeye returned non-JSON for %s match %s: %s",
                    tournament.logging_id,
                    match_id,
                    e,
                )
                failed += 1
                continue

            if data is None:
                logger.debug(
                    "Null response for %s match %s",
                    tournament.logging_id,
                    match_id,
                )
                continue

            self.save_json(data, target)
            saved += 1

        logger.info(
            "%s: fetched %d match stats, %d failed",
            tournament.logging_id,
            saved,
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
