"""RankingsExtractor — fetch weekly ATP singles rankings HTML pages."""

import logging
from datetime import date

from bs4 import BeautifulSoup

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

RANKINGS_URL = "https://www.atptour.com/en/rankings/singles"


class RankingsExtractor(BaseExtractor):
    """Discover available ranking dates and fetch missing HTML pages."""

    def __init__(self, start_year: int = 2025, data_root=None):
        super().__init__(
            domain="atptour",
            data_root=data_root,
            cloudflare_fallback=True,
            cloudflare_browser_fetch=True,
        )
        self.start_year = start_year

    def run(self) -> int:
        """Fetch all rankings pages not yet saved locally.

        Returns:
            Number of new pages fetched.
        """
        discovery_url = f"{RANKINGS_URL}?rankRange=0-100"
        html = self.fetch_html(discovery_url)

        available = self._get_available_dates(html)
        target_dates = [d for d in available if d.year >= self.start_year]
        existing = self._get_existing_dates()
        to_fetch = [d for d in target_dates if d not in existing]

        logger.info(
            "Rankings: %d available, %d in range, %d existing, %d to fetch",
            len(available),
            len(target_dates),
            len(existing),
            len(to_fetch),
        )

        for ranking_date in to_fetch:
            date_str = ranking_date.isoformat()
            url = f"{RANKINGS_URL}?rankRange=0-5000&dateWeek={date_str}"
            page_html = self.fetch_html(url)
            filename = f"rankings_singles_{ranking_date.strftime('%Y%m%d')}.html"
            target = self.build_path("raw", "rankings", filename)
            self.save_html(page_html, target)

        logger.info("Rankings: fetched %d new pages", len(to_fetch))
        return len(to_fetch)

    def _get_available_dates(self, html: str) -> list[date]:
        """Parse the DateWeek dropdown from rankings discovery page."""
        soup = BeautifulSoup(html, "lxml")
        dropdown = soup.select_one(
            'div.atp_filters-dropdown[data-key="DateWeek"] select'
        )
        if dropdown is None:
            raise ValueError("Could not find DateWeek dropdown in rankings page.")
        dates = []
        for option in dropdown.find_all("option"):
            value = option["value"]
            if value == "Current Week":
                continue
            dates.append(date.fromisoformat(value))
        return sorted(dates)

    def _get_existing_dates(self) -> set[date]:
        """Find ranking dates already saved as raw HTML files."""
        rankings_dir = self.build_path("raw", "rankings")
        files = self.list_files(rankings_dir, "rankings_singles_*.html")
        dates = set()
        for f in files:
            date_str = f.stem.replace("rankings_singles_", "")
            dates.add(date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])))
        return dates
