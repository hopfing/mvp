"""Look up BetMGM odds for predictions via event map."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher

ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"


class BetMGMOddsMatcher(BaseOddsMatcher):
    """Looks up MGM odds for predictions using the event map."""

    event_id_column = "mgm_event_id"
    book_label = "MGM"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="betmgm", data_root=data_root)
