"""Look up BetRivers odds for predictions via event map."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher

ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"


class BetRiversOddsMatcher(BaseOddsMatcher):
    """Looks up BR odds for predictions using the event map."""

    event_id_column = "br_event_id"
    book_label = "BR"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="betrivers", data_root=data_root)
