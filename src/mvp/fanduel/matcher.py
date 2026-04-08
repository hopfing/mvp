"""Look up FanDuel odds for predictions via event map."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher

ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"


class FanDuelOddsMatcher(BaseOddsMatcher):
    """Looks up FD odds for predictions using the event map."""

    event_id_column = "fd_event_id"
    book_label = "FD"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="fanduel", data_root=data_root)
