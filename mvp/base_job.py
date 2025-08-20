from abc import ABC, abstractmethod
from datetime import datetime
import json
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import yaml

from config import PROJECT_ROOT, SECRETS


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

    def _fetch_content(self, url, headers: Optional[dict] = None):

        base_headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/138.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip, deflate',
        }

        if headers is None:
            headers = base_headers
        else:
            headers = {**base_headers, **headers}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()

    def save_json(
            self,
            data,
            path: Path
    ):
        """Save JSON data to given file path, creating directory if needed."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
