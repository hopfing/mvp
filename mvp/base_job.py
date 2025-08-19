from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import yaml

from config import SECRETS


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
