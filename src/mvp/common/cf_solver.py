"""Shared Cloudflare challenge solver using undetected_chromedriver.

atptour.com intermittently serves a Cloudflare "Just a moment" JS challenge
(HTTP 403) to the live pipeline's datacenter (VPN) egress IP that the
curl_cffi-based extractors cannot solve. This module clears the challenge with
a real browser and exposes two recovery paths to `BaseExtractor`:

  - Tier A/B (`solve`): launch UC once per host, harvest the cleared cookies +
    User-Agent into a lock-protected per-host store, and let the caller retry
    over fast curl_cffi. Deduped via a freshness TTL so concurrent worker
    threads share a single solve.
  - Tier C (`fetch_text`): if the harvested cookies do not clear the challenge,
    fetch the URL through the browser itself via a same-origin in-page fetch().

Only one browser operation runs at a time (RLock), bounding Chrome memory on
the 16GB Beelink. The driver is launched lazily on first challenge and torn
down by the caller (`cmd_live` finally block) via `close()`.

Every `uc.Chrome()` launch passes `user_multi_procs=True`: without it the
patcher unconditionally unlinks and re-downloads the SHARED chromedriver binary
on launch, so a concurrent `uc.Chrome()` elsewhere (the bet365 scraper runs in
the odds pool during the same cycle) deletes the binary mid-launch and both
crash with Errno 2. The flag routes the patcher through its internal
multiprocessing lock and skips the unlink/redownload when already patched.
"""

import logging
import os
import subprocess
import threading
import time
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Substrings that identify a Cloudflare interstitial/challenge body.
_CHALLENGE_MARKERS = (
    "just a moment",
    "cf-browser-verification",
    "challenge-platform",
    "_cf_chl",
    "cloudflare",
)

# How long harvested cookies are considered fresh before a re-solve.
_COOKIE_TTL_SECONDS = 600

# Seconds to wait after navigation for the JS challenge to auto-resolve.
_SOLVE_WAIT_SECONDS = 12


def body_has_challenge(body: str) -> bool:
    """True if a response body looks like a Cloudflare challenge page."""
    if not body:
        return False
    head = body[:2000].lower()
    return any(marker in head for marker in _CHALLENGE_MARKERS)


def is_cf_challenge(status_code: int, body: str) -> bool:
    """True if a response is a Cloudflare challenge (403 + challenge body)."""
    return status_code == 403 and body_has_challenge(body)


class CloudflareChallengeError(Exception):
    """A Cloudflare challenge could not be cleared via cookie harvest.

    Standalone (not a RequestsError/ValueError) so existing per-fetch
    except clauses do not silently absorb it.
    """


def _host(url: str) -> str:
    return urlparse(url).netloc


class CloudflareSolver:
    """Serialized UC-backed challenge solver with a per-host cookie store."""

    _driver_timeout = 30

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # host -> {"cookies": list[dict], "ua": str, "set_at": float}
        self._store: dict[str, dict] = {}
        self._driver = None
        self._virtual_display = None

    def solve(self, url: str) -> tuple[list[dict], str]:
        """Clear the challenge for url's host; return (cookies, user_agent).

        Deduped: if another thread already solved this host within the TTL,
        return the stored cookies without relaunching the browser.
        """
        host = _host(url)
        with self._lock:
            cached = self._fresh_entry(host)
            if cached is not None:
                return cached["cookies"], cached["ua"]

            driver = self._ensure_driver()
            logger.info("CF solver: clearing challenge via %s", url)
            self._navigate(driver, url)
            user_agent = driver.execute_script("return navigator.userAgent")
            cookies = driver.get_cookies()
            self._store[host] = {
                "cookies": cookies,
                "ua": user_agent,
                "set_at": time.monotonic(),
            }
            return cookies, user_agent

    def fetch_text(self, url: str) -> str:
        """Fetch url's body through the browser (same-origin in-page fetch).

        Navigates to the full target URL (not the host root: an API gateway
        root may redirect to another host, breaking same-origin for the fetch),
        then issues an in-page fetch so the body is the raw response. Raises
        CloudflareChallengeError if the browser is still challenged or the
        navigation/fetch times out.
        """
        host = _host(url)
        with self._lock:
            driver = self._ensure_driver()
            logger.info("CF solver: browser-fetching %s", url)
            self._navigate(driver, url)
            text = self._in_page_fetch(driver, url)
            if text is None or text.startswith("__CF_ERR__"):
                raise CloudflareChallengeError(
                    f"browser fetch failed for {url}: {text}"
                )
            if body_has_challenge(text):
                raise CloudflareChallengeError(
                    f"browser still challenged for {url}"
                )
            # Refresh the cookie store — the navigation may have cleared the
            # challenge for this host.
            self._store[host] = {
                "cookies": driver.get_cookies(),
                "ua": driver.execute_script("return navigator.userAgent"),
                "set_at": time.monotonic(),
            }
            return text

    def close(self) -> None:
        """Tear down the browser and virtual display. Idempotent."""
        with self._lock:
            if self._driver is not None:
                try:
                    self._driver.quit()
                except Exception:
                    pass
                self._driver = None
            if self._virtual_display is not None:
                try:
                    self._virtual_display.stop()
                except Exception:
                    pass
                self._virtual_display = None
            self._store.clear()

    # internal -------------------------------------------------------------

    def _fresh_entry(self, host: str) -> dict | None:
        entry = self._store.get(host)
        if entry and (time.monotonic() - entry["set_at"]) < _COOKIE_TTL_SECONDS:
            return entry
        return None

    def _navigate(self, driver, url: str) -> None:
        """Navigate with a page-load timeout, then wait for the JS challenge.

        Selenium errors (incl. page-load timeout) become CloudflareChallengeError
        so the caller fails soft / aborts cleanly instead of leaking the raw
        exception type while the solver holds the lock.
        """
        # Lazy import keeps selenium out of the import path of the many
        # non-browser extractors (same rationale as the lazy uc import below).
        from selenium.common.exceptions import TimeoutException, WebDriverException

        try:
            driver.set_page_load_timeout(self._driver_timeout)
            driver.get(url)
        except (TimeoutException, WebDriverException) as e:
            raise CloudflareChallengeError(f"navigation failed for {url}: {e}")
        time.sleep(_SOLVE_WAIT_SECONDS)

    def _in_page_fetch(self, driver, url: str) -> str | None:
        from selenium.common.exceptions import TimeoutException, WebDriverException

        script = (
            "const cb = arguments[arguments.length - 1];"
            "fetch(arguments[0], {credentials: 'include'})"
            "  .then(r => r.text())"
            "  .then(t => cb(t))"
            "  .catch(e => cb('__CF_ERR__' + e));"
        )
        try:
            driver.set_script_timeout(self._driver_timeout)
            return driver.execute_async_script(script, url)
        except (TimeoutException, WebDriverException) as e:
            raise CloudflareChallengeError(
                f"browser fetch timed out for {url}: {e}"
            )

    def _ensure_driver(self):
        if self._driver is not None:
            return self._driver

        import undetected_chromedriver as uc

        # Virtual display for headless hosts / SSH sessions (mirrors bet365).
        if os.name != "nt" and not os.environ.get("DISPLAY"):
            try:
                from pyvirtualdisplay import Display

                self._virtual_display = Display(visible=False, size=(1920, 1080))
                self._virtual_display.start()
                logger.info("CF solver: started virtual display")
            except ImportError:
                os.environ["DISPLAY"] = ":99"
                subprocess.Popen(
                    ["Xvfb", ":99", "-screen", "0", "1920x1080x24"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                time.sleep(1)

        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")

        try:
            version = (
                subprocess.check_output(["google-chrome", "--version"], text=True)
                .strip()
                .split()[-1]
            )
            chrome_major = int(version.split(".")[0])
        except Exception:
            chrome_major = None

        logger.info("CF solver: launching Chrome (version=%s)", chrome_major)
        # user_multi_procs=True: serialize patcher access so this launch does
        # not race the bet365 scraper's concurrent uc.Chrome() on the shared
        # chromedriver binary.
        self._driver = uc.Chrome(
            options=options, version_main=chrome_major, user_multi_procs=True
        )
        return self._driver


_solver: CloudflareSolver | None = None
_solver_lock = threading.Lock()


def get_solver() -> CloudflareSolver:
    """Return the process-wide CloudflareSolver singleton (lazy)."""
    global _solver
    with _solver_lock:
        if _solver is None:
            _solver = CloudflareSolver()
        return _solver
