"""Base class for extractors providing HTTP session management and fetch logic."""

import logging
import random
import time

import requests

from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)


class BaseExtractor(BaseJob):
    """Base extractor with HTTP session, retry, and backoff."""

    def __init__(self, domain: str, data_root=None, timeout: int = 30):
        super().__init__(domain=domain, data_root=data_root)
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            }
        )
        return session

    def _fetch(
        self,
        url: str,
        retries: int = 3,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        min_delay = 0.75
        max_delay = 1.25
        for attempt in range(retries + 1):
            try:
                time.sleep(random.uniform(min_delay, max_delay))
                logger.info("Fetching URL: %s", url)
                response = self.session.get(
                    url, timeout=self.timeout, headers=headers
                )
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning("Fetch failed: %s", e)
                min_delay *= 1.25
                max_delay *= 1.25
                if attempt == retries:
                    raise

    def fetch_json(self, url: str) -> dict | list | None:
        headers = {"Accept": "application/json"}
        response = self._fetch(url, headers=headers)
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            raise ValueError(
                f"Expected JSON response, got content-type '{content_type}' "
                f"from {url}. Response preview: {response.text[:200]}"
            )
        return response.json()

    def fetch_html(self, url: str) -> str:
        response = self._fetch(url)
        return response.text
