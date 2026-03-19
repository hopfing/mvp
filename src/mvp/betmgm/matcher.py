"""Match BetMGM odds to predictions by player-pair matching."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher


class BetMGMOddsMatcher(BaseOddsMatcher):
    """Matches BetMGM odds to predictions using player name resolution."""

    event_id_column = "mgm_event_id"
    book_label = "MGM"
    ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="betmgm", data_root=data_root)
