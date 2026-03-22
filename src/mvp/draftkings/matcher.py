"""Look up DraftKings odds for predictions via event map."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher

ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"


class DraftKingsOddsMatcher(BaseOddsMatcher):
    """Looks up DK odds for predictions using the event map."""

    event_id_column = "dk_event_id"
    book_label = "DK"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="draftkings", data_root=data_root)
