"""ResultsExtractor -- fetch singles and doubles results HTML."""

import logging
from pathlib import Path

from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class ResultsExtractor(BaseExtractor):
    """Fetch results HTML (singles + doubles) for a tournament."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self, tournament: Tournament, refresh: bool = True) -> None:
        logger.info("Fetching results for %s", tournament.logging_id)
        self._fetch_singles(tournament, refresh)
        self._fetch_doubles(tournament, refresh)
        logger.info("Saved results for %s", tournament.logging_id)

    def _fetch_singles(self, tournament: Tournament, refresh: bool) -> Path | None:
        target = self.build_path("raw", tournament.path, "results_singles.html")
        if not refresh and target.exists():
            logger.info(
                "Skipping singles results for %s (exists)", tournament.logging_id
            )
            return None
        url = self._results_url(tournament)
        html = self.fetch_html(url)
        return self.save_html(html, target)

    def _fetch_doubles(self, tournament: Tournament, refresh: bool) -> Path | None:
        target = self.build_path("raw", tournament.path, "results_doubles.html")
        if not refresh and target.exists():
            logger.info(
                "Skipping doubles results for %s (exists)", tournament.logging_id
            )
            return None
        url = f"{self._results_url(tournament)}?matchType=doubles"
        html = self.fetch_html(url)
        return self.save_html(html, target)

    def _results_url(self, tournament: Tournament) -> str:
        prefix = tournament.scores_url_prefix
        base = (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{tournament.url_slug}/{tournament.tournament_id}"
        )
        if tournament.is_archive:
            return f"{base}/{tournament.year}/results"
        return f"{base}/results"
