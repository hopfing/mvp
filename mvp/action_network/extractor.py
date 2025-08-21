from datetime import datetime
import logging
import time
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright

from mvp.action_network.job import ActionNetworkJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


class ActionNetworkExtractor(ActionNetworkJob):
    """
        Class used to send requests to ActionNetwork API and store output JSON.
        """
    BASE_URL = "https://api.actionnetwork.com/web/"

    def __init__(self, league, game_date):
        super().__init__(game_date=game_date, league=league)
        self.api_token = self._get_auth_token()

    @property
    def headers(self):
        return {
            'Origin': 'https://www.actionnetwork.com',
            'Referer': 'https://www.actionnetwork.com/mlb/odds',
            'Authorization': self.api_token
        }

    def _get_auth_token(self):
        """
        Attempt to load API authorization from secrets file, refreshing if
        missing or expired.

        :return: authorization token used in request headers
        """
        auth_token = self.secrets.get("auth_token")
        auth_expires = self.secrets.get("auth_expires", 0)

        if not auth_token or auth_expires <= time.time():
            reason = "No" if not auth_token else "Expired"
            logger.warning("%s authorization token found in secrets", reason)
            auth_token, auth_expires = self._refresh_auth_token()
            self.update_secrets({
                "auth_token": auth_token,
                "auth_expires": auth_expires
            })

        logger.info("Found valid AN_SESSION_TOKEN_V1 cookie.")
        logger.info("Authorization valid until %s", time.ctime(auth_expires))

        return auth_token

    def _refresh_auth_token(self):
        """
        Navigate to Action Network website and login to retrieve auth token.

        :return: authorization token used in request headers
        """
        logger.info("Refreshing authorization token")
        email = self.secrets.get("email")
        password = self.secrets.get("password")

        if not email or not password:
            raise RuntimeError("Missing email or password for Action Network.")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                channel="chrome",
                headless=False,
                args=["--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context()
            page = context.new_page()

            page.goto("https://www.actionnetwork.com/")
            page.click("button:has-text('Log In')")

            page.wait_for_selector("a:has-text('Sign in with Google')",
                                   timeout=15000)

            with page.expect_popup() as pi:
                page.click("a:has-text('Sign in with Google')")
            google = pi.value

            google.wait_for_selector("input[type='email']", timeout=15000)
            google.fill("input[type='email']", email)
            google.click("button:has-text('Next')")

            google.wait_for_selector("input[type='password']", timeout=15000)
            google.fill("input[type='password']", password)
            google.click("button:has-text('Next')")

            google.wait_for_event("close", timeout=60000)

            cookies = context.cookies()

            browser.close()

        auth_cookie = next(
            (
                c for c in cookies
                if c["name"] == "AN_SESSION_TOKEN_V1"
            ),
            None
        )

        token = auth_cookie["value"]
        expires = auth_cookie["expires"]

        return token, expires

    @property
    def league_endpoints(self):
        """
        Generate URLs for the league endpoints.
        :return: Dictionary with URLs for each endpoint.
        """
        endpoints = [
            *self.GLOBAL_ENDPOINTS, *self.league_config.get("endpoints", [])
        ]
        periods = [
            *self.GLOBAL_PERIODS, *self.league_config.get("periods", [])
        ]

        return [
            {
                "name": endpoint.value,
                "path": self.ENDPOINTS[endpoint],
                "url": (
                    f"{self.BASE_URL}{self.ENDPOINTS[endpoint]}{self.league}"
                    f"?bookIds={','.join(self.SPORTSBOOKS)}"
                    f"&date={self.game_date_compact}"
                    f"&periods={','.join(periods)}"
                ),
            }
            for endpoint in endpoints
        ]

    def run(self):

        manifest = {
            "league": self.league,
            "game_date": self.game_date,
            "run_datetime": self.run_datetime,
            "items": []
        }
        files_saved = []

        if len(self.league_endpoints) == 0:
            logger.warning(
                "No API endpoints configured for %s",
                self.league.upper()
            )
            return files_saved

        for endpoint in self.league_endpoints:
            logger.info(
                "Fetching data from %s %s endpoint.",
                self.league.upper(), endpoint["name"]
            )
            data = self._fetch_content(
                url=endpoint["url"],
                headers=self.headers
            )
            file_path = self.build_file_path(
                dir_path=self.raw_dir,
                file_name=endpoint["name"],
                file_type="json",
            )
            self.save_json(data, file_path)
            logger.info(
                "%s %s data saved to %s",
                self.league.upper(), endpoint["name"], file_path
            )
            endpoint_meta = {
                "endpoint": endpoint["name"],
                "path": file_path.as_posix(),
                "url": endpoint["url"],
                "retrieved_at": datetime.now(ZoneInfo("America/Chicago"))
            }
            manifest["items"].append(endpoint_meta)
            files_saved.append(file_path)

        manifest_file = self.build_file_path(
            dir_path=self.raw_dir,
            file_name="manifest",
            file_type="json"
        )
        self.save_json(manifest, manifest_file)
        logger.info(
            "Manifest file saved to %s",
            manifest_file
        )

        return files_saved
