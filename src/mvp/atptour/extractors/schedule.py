"""ScheduleExtractor -- fetch daily schedule HTML for active tournaments."""

import logging
import re
from pathlib import Path

from mvp.atptour.tournament import Tournament
from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

_DAY_OPTION_RE = re.compile(r'<option\s+value="(\d+)"')


class ScheduleExtractor(BaseExtractor):
    """Fetch daily schedule HTML for an active tournament."""

    def __init__(self, data_root=None):
        super().__init__(domain="atptour", data_root=data_root)

    def _schedule_url(self, tournament: Tournament, day: int | None = None) -> str:
        prefix = tournament.scores_url_prefix
        url = (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{tournament.url_slug}/{tournament.tournament_id}/daily-schedule"
        )
        if day is not None:
            url += f"?day={day}"
        return url

    def run(self, tournament: Tournament) -> Path | None:
        if tournament.is_archive:
            logger.info("Skipping schedule for %s (archive)", tournament.logging_id)
            return None

        logger.info("Fetching schedule for %s", tournament.logging_id)
        html = self.fetch_html(self._schedule_url(tournament))

        # Parse the day selector to discover all available days
        all_days = set(_DAY_OPTION_RE.findall(html))
        selected_match = re.search(
            r'<option\s+value="(\d+)"\s+selected', html
        )
        selected_day = int(selected_match.group(1)) if selected_match else None

        path = self._save_schedule(tournament, html, day=selected_day)

        # Fetch any additional days not returned by the default page
        other_days = {int(d) for d in all_days} - (
            {selected_day} if selected_day is not None else set()
        )
        for day in sorted(other_days):
            logger.info(
                "Fetching schedule day %d for %s", day, tournament.logging_id
            )
            day_html = self.fetch_html(self._schedule_url(tournament, day=day))
            self._save_schedule(tournament, day_html, day=day)

        return path

    def _save_schedule(
        self, tournament: Tournament, html: str, day: int | None = None
    ) -> Path:
        filename = f"schedule_d{day}.html" if day is not None else "schedule.html"
        target = self.build_path(
            "raw",
            f"{tournament.path}/schedule",
            filename,
            version="datetime",
        )
        path = self.save_html(html, target)
        logger.info("Saved schedule for %s", tournament.logging_id)
        return path
