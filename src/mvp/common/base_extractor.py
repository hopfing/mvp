"""Base class for extractors providing HTTP session management and fetch logic."""

import json
import logging
import random
import time

from curl_cffi import requests

from mvp.common.base_job import BaseJob
from mvp.common.cf_solver import (
    CloudflareChallengeError,
    get_solver,
    is_cf_challenge,
)

logger = logging.getLogger(__name__)


class BaseExtractor(BaseJob):
    """Base extractor with HTTP session, retry, and backoff."""

    def __init__(
        self,
        domain: str,
        data_root=None,
        timeout: int = 30,
        run_at=None,
        impersonate: str | None = "chrome131",
        cloudflare_fallback: bool = False,
        cloudflare_browser_fetch: bool = False,
    ):
        super().__init__(domain=domain, data_root=data_root, run_at=run_at)
        self.timeout = timeout
        self.impersonate = impersonate
        # Tier A/B: on a Cloudflare challenge, solve once via browser, harvest
        # cookies, and retry over curl_cffi. Cheap (one shared solve per host)
        # and parallel-safe. Off by default so books/other domains untouched.
        self.cloudflare_fallback = cloudflare_fallback
        # Tier C: if the harvested cookies don't clear the challenge, fetch the
        # URL through the browser itself. Expensive (one navigation per URL) —
        # enable only for low/bounded-volume extractors, never per-match ones.
        self.cloudflare_browser_fetch = cloudflare_browser_fetch
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        if self.impersonate:
            session = requests.Session(impersonate=self.impersonate)
        else:
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
                "Accept-Encoding": "gzip, deflate",
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
                # Detect a Cloudflare challenge before raise_for_status / retry,
                # so we don't burn the retry budget on the challenge page.
                if self.cloudflare_fallback and is_cf_challenge(
                    response.status_code, response.text
                ):
                    return self._clear_cf_challenge(url, headers)
                response.raise_for_status()
                return response
            except requests.RequestsError as e:
                logger.warning("Fetch failed: %s", e)
                min_delay *= 1.25
                max_delay *= 1.25
                if attempt == retries:
                    raise

    def _clear_cf_challenge(
        self, url: str, headers: dict[str, str] | None
    ) -> requests.Response:
        """Tier A/B: solve the challenge via browser, retry over curl_cffi.

        Harvests cleared cookies + UA into this session and retries once. If
        the cookies clear the challenge, returns the curl_cffi Response. If not
        (atptour may decision per-request rather than via a clearance cookie),
        raises CloudflareChallengeError so fetch_json/fetch_html can fall back
        to a browser fetch.
        """
        solver = get_solver()
        logger.warning("Cloudflare challenge on %s — clearing via browser", url)
        cookies, user_agent = solver.solve(url)
        if user_agent:
            self.session.headers["User-Agent"] = user_agent
        for cookie in cookies:
            self.session.cookies.set(
                cookie["name"], cookie["value"], domain=cookie.get("domain", "")
            )
        response = self.session.get(url, timeout=self.timeout, headers=headers)
        if response.status_code == 200 and not is_cf_challenge(
            response.status_code, response.text
        ):
            logger.info("Cloudflare cookie harvest cleared %s", url)
            return response
        raise CloudflareChallengeError(url)

    def fetch_json(
        self, url: str, headers: dict[str, str] | None = None
    ) -> dict | list | None:
        merged = {"Accept": "application/json"}
        if headers:
            merged.update(headers)
        try:
            response = self._fetch(url, headers=merged)
        except CloudflareChallengeError:
            if not self.cloudflare_browser_fetch:
                raise
            return json.loads(get_solver().fetch_text(url))
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            raise ValueError(
                f"Expected JSON response, got content-type '{content_type}' "
                f"from {url}. Response preview: {response.text[:200]}"
            )
        return response.json()

    def fetch_html(self, url: str) -> str:
        try:
            response = self._fetch(url)
        except CloudflareChallengeError:
            if not self.cloudflare_browser_fetch:
                raise
            return get_solver().fetch_text(url)
        return response.text
