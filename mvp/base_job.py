from abc import ABC, abstractmethod
from datetime import datetime
import json
import logging
from pathlib import Path
import random
import time
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import yaml

from config import PROJECT_ROOT, SECRETS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class BaseJob(ABC):
    """
    Base class for pipeline tasks that fetch or transform data from various
    external sources.
    """

    def __init__(
            self,
            league: str,
            game_date: Optional[str] = None
    ):
        self.league = league.lower()
        if game_date is None:
            self.game_date = datetime.now(ZoneInfo("America/Chicago")).date()
        else:
            try:
                self.game_date = datetime.strptime(
                    game_date, "%Y-%m-%d"
                ).date()
            except ValueError:
                raise ValueError(
                    f"{game_date} does not match YYYY-MM-DD format.")

    @property
    def game_date_compact(self):
        return self.game_date.strftime("%Y%m%d")

    @property
    @abstractmethod
    def source(self) -> str:
        """Return data source name for ETL pipelines"""
        pass

    @property
    def _all_secrets(self):
        with open(SECRETS, 'r') as f:
            secrets = yaml.safe_load(f)

        return secrets

    @property
    def secrets(self):
        return self._all_secrets[self.source]

    def update_secrets(self, new_secrets: dict):

        secrets = self._all_secrets

        for key, value in new_secrets.items():
            secrets[self.source][key] = value

        tmp = SECRETS.with_suffix(SECRETS.suffix + ".tmp")
        with open(tmp, "w") as f:
            yaml.safe_dump(secrets, f, default_flow_style=False)
        tmp.replace(SECRETS)

    @property
    def _daily_dir(self) -> Path:
        """
        Directory path for storing data files.
        Format: {league}/{year}/{month}/{day}
        """
        return (Path(self.league) / str(self.game_date.year)
                / f"{self.game_date.month:02d}" / f"{self.game_date.day:02d}")

    @property
    def raw_dir(self):
        """
        Directory path for storing raw data files.
        Format: data/raw/{league}/{year}/{month}/{day}
        """
        return (Path(PROJECT_ROOT) / "data" / "raw" / self.source /
                self._daily_dir)

    def build_file_path(
            self,
            dir_path: Path,
            file_name: str,
            file_type: str
    ) -> Path:
        """Centralized filename builder"""
        return dir_path / f"{file_name}_{self.game_date_compact}.{file_type}"

    def _fetch_content(
            self,
            url,
            headers: Optional[dict] = None,
            retries: int = 2,
            backoff: float = 0.75
    ):

        base_headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/138.0.0.0 Safari/537.36',
        }

        if headers is None:
            headers = base_headers
        else:
            headers = {**base_headers, **headers}

        timeout = (5, 20)

        for attempt in range(retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectTimeout) as err:
                if attempt == retries:
                    logger.error(
                        "Timeout on %s after %d attempts: %s",
                        url, retries + 1, err
                    )
                    raise
                sleep = backoff * (2 ** attempt) + random.random() * 0.2
                logger.warning(
                    "Timeout (%s) on %s (attempt %d/%d). Retrying in %.2fs…",
                    err.__class__.__name__, url, attempt + 1, retries + 1,
                    sleep
                )
                time.sleep(sleep)
            except requests.exceptions.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status in RETRYABLE_STATUS and attempt < retries:
                    sleep = backoff * (2 ** attempt) + random.random() * 0.2
                    logger.warning(
                        "HTTP %s for %s (attempt %d/%d). Retrying in %.2fs…",
                        status, url, attempt + 1, retries + 1, sleep
                    )
                    time.sleep(sleep)
                    continue
                raise
            except requests.exceptions.JSONDecodeError:
                logger.error("Non-JSON response from %s", url)
                raise

    def save_json(
            self,
            data,
            path: Path
    ):
        """Save JSON data to given file path, creating directory if needed."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
