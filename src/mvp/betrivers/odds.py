"""BetRivers odds scraper for tennis markets via Kambi API."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import polars as pl

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

KAMBI_API_BASE = (
    "https://eu-offering-api.kambicdn.com/offering/v2018/rsiusil/listView"
)

TENNIS_ENDPOINT = f"{KAMBI_API_BASE}/tennis.json"

_INCLUDE_TERM_KEYS = {
    "atp",
    "challenger",
    "challenger_qual_",
}

MONEYLINE_CRITERION_ID = 1001159551


@dataclass
class BetRiversOddsEntry:
    book: str
    br_event_id: str
    market: str
    br_selection_id: str
    player_name: str
    side: str
    odds: float
    points: float | None
    tournament: str
    br_tournament_id: str
    circuit: str
    opponent_name: str
    fetched_at: datetime


def _is_atp_challenger(term_key: str) -> bool:
    """Check if a Kambi path termKey is ATP or Challenger men's singles."""
    return term_key in _INCLUDE_TERM_KEYS
