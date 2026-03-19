"""Match BetRivers odds to predictions by player-pair matching."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher


class BetRiversOddsMatcher(BaseOddsMatcher):
    """Matches BetRivers odds to predictions using player name resolution."""

    event_id_column = "br_event_id"
    book_label = "BR"
    ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="betrivers", data_root=data_root)
