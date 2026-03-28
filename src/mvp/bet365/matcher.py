"""Look up Bet365 odds for predictions via event map."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher

ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"


class Bet365OddsMatcher(BaseOddsMatcher):
    """Looks up Bet365 odds for predictions using the event map."""

    event_id_column = "b365_event_id"
    book_label = "B365"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="bet365", data_root=data_root)
