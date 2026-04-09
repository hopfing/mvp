"""Look up Caesars odds for predictions via event map."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher

ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"


class CaesarsOddsMatcher(BaseOddsMatcher):
    """Looks up CZR odds for predictions using the event map."""

    event_id_column = "czr_event_id"
    book_label = "CZR"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="caesars", data_root=data_root)
