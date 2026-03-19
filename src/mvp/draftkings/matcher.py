"""Match DraftKings odds to predictions by player-pair matching."""

from pathlib import Path

from mvp.common.odds_matching import BaseOddsMatcher, normalize_name

_TOURNAMENT_PREFIXES = [
    "challenger quals. - ",
    "challenger quals - ",
    "challenger - ",
    "atp - ",
    "wta - ",
]


def normalize_tournament(tournament: str) -> str:
    """Normalize a tournament name, stripping DK circuit prefixes."""
    lower = tournament.strip().lower()
    for prefix in _TOURNAMENT_PREFIXES:
        if lower.startswith(prefix):
            tournament = tournament[len(prefix):]
            break
    return normalize_name(tournament)


class DraftKingsOddsMatcher(BaseOddsMatcher):
    """Matches DK odds to predictions using player name resolution."""

    event_id_column = "dk_event_id"
    book_label = "DK"
    ALIASES_PATH = Path(__file__).resolve().parent / "player_aliases.yaml"

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="draftkings", data_root=data_root)
