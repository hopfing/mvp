"""Tournament discovery — archive listing and active tournament detection."""

import logging
import re

from bs4 import BeautifulSoup

from mvp.common.base_extractor import BaseExtractor
from mvp.common.enums import Circuit

logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://www.atptour.com/en/scores/results-archive"

# Maps storage directory name -> URL query parameter value
ARCHIVE_FILTERS = {
    "atpgs": "atpgs",
    "ch": "ch",
}

_ARCHIVE_FILTER_CIRCUIT: dict[str, Circuit] = {
    "atpgs": Circuit.tour,
    "ch": Circuit.chal,
}

_TID_PATTERN = re.compile(r"/en/tournaments/[^/]+/(\d+)/overview")

_SCORES_URL = "https://app.atptour.com/api/v2/gateway/livematches/website"


class TournamentDiscovery(BaseExtractor):
    """Discover tournaments from archive pages and live scores API."""

    def __init__(self, data_root=None):
        super().__init__(
            domain="atptour",
            data_root=data_root,
            cloudflare_fallback=True,
            cloudflare_browser_fetch=True,
        )

    def get_archive_tournaments(
        self,
        year: int,
        circuit: Circuit | None = None,
    ) -> list[tuple[str, int, Circuit]]:
        """Parse archive HTML to list tournaments for a given year.

        Returns list of (tournament_id, year, circuit) tuples.
        """
        filters = _ARCHIVE_FILTER_CIRCUIT
        if circuit is not None:
            filters = {
                dir_name: c
                for dir_name, c in _ARCHIVE_FILTER_CIRCUIT.items()
                if c == circuit
            }
            if not filters:
                raise ValueError(f"No archive filter for circuit {circuit!r}.")

        results: list[tuple[str, int, Circuit]] = []

        for dir_name in filters:
            path = self._ensure_archive_html(dir_name, year)
            html = self.read_html(path)
            soup = BeautifulSoup(html, "lxml")

            for link in soup.select("a.tournament__profile[href]"):
                href = link["href"]
                if not href:
                    continue
                match = _TID_PATTERN.search(href)
                if not match:
                    logger.warning(
                        "Unexpected tournament profile URL format: %s", href
                    )
                    continue
                tid = match.group(1)
                results.append((tid, year, filters[dir_name]))

        seen = set()
        deduped = []
        for entry in results:
            if entry not in seen:
                seen.add(entry)
                deduped.append(entry)

        logger.info("Found %d archive tournaments for %d", len(deduped), year)
        return deduped

    def _ensure_archive_html(self, dir_name: str, year: int):
        """Return path to archive HTML, fetching if missing."""
        path = self.build_path("raw", f"results_archive/{dir_name}", f"{year}.html")
        if not path.exists():
            url_param = ARCHIVE_FILTERS[dir_name]
            url = f"{ARCHIVE_URL}?tournamentType={url_param}&year={year}"
            logger.info("Fetching missing archive HTML: %s/%d", dir_name, year)
            html = self.fetch_html(url)
            self.save_html(html, path)
        return path

    def get_active_tournaments(self) -> list[tuple[str, int]]:
        """Query live scores API for currently active tournaments.

        Returns list of (tournament_id, year) tuples.
        """
        circuits = ["tour", "challenger"]

        # NOTE: This endpoint's Cloudflare gating is volatile. As of 2026-05-18
        # it returned 500 without Origin/Referer; by 2026-05-26 sending those
        # headers triggered a Cloudflare JS challenge (403) while a bare
        # chrome-impersonated request passed. Revisit if 403/500 recurs here.
        results = []
        for circuit in circuits:
            url = f"{_SCORES_URL}?scoringTournamentLevel={circuit}"
            logger.info("Fetching %s tournaments", circuit.title())
            data = self.fetch_json(url)
            tournaments = data["Data"]["LiveMatchesTournamentsOrdered"]
            for t in tournaments:
                event_id = t["EventId"]
                event_year = t["EventYear"]
                if not isinstance(event_id, int) or not isinstance(event_year, int):
                    raise TypeError(
                        f"Expected int EventId and EventYear, got "
                        f"{type(event_id).__name__} and {type(event_year).__name__} "
                        f"for tournament {t!r}"
                    )
                results.append((str(event_id), event_year))

        logger.info("Found %d active tournaments", len(results))
        return results
