from abc import ABC
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo


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
