"""ScheduleExtractor -- fetch daily schedule HTML for active tournaments."""

import logging
from pathlib import Path

from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class ScheduleExtractor(BaseExtractor):
    """Fetch daily schedule HTML for an active tournament."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self, tournament: Tournament) -> Path | None:
        if tournament.is_archive:
            logger.info("Skipping schedule for %s (archive)", tournament.logging_id)
            return None

        prefix = tournament.scores_url_prefix
        url = (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{tournament.url_slug}/{tournament.tournament_id}/daily-schedule"
        )

        logger.info("Fetching schedule for %s", tournament.logging_id)
        html = self.fetch_html(url)

        target = self.build_path(
            "raw",
            f"{tournament.path}/schedule",
            "schedule.html",
            version="datetime",
        )
        path = self.save_html(html, target)
        logger.info("Saved schedule for %s", tournament.logging_id)
        return path
