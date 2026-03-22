"""BetRivers odds scraper for tennis markets via Kambi API."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

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
    event_status: str
    fetched_at: datetime


def _is_atp_challenger(term_key: str) -> bool:
    """Check if a Kambi path termKey is ATP or Challenger men's singles."""
    return term_key in _INCLUDE_TERM_KEYS


def _parse_response(
    data: dict,
    fetched_at: datetime,
) -> list[BetRiversOddsEntry]:
    """Parse Kambi API response into BetRiversOddsEntry objects.

    Filters to ATP/Challenger men's singles moneyline only.
    """
    entries = []

    for event_wrapper in data.get("events", []):
        event = event_wrapper.get("event", {})
        path = event.get("path", [])

        if len(path) < 2:
            continue
        circuit_key = path[1].get("termKey", "")
        if not _is_atp_challenger(circuit_key):
            continue

        event_id = str(event.get("id", ""))
        home_name = event.get("homeName", "")
        away_name = event.get("awayName", "")
        tournament = event.get("group", "")
        tournament_id = str(event.get("groupId", ""))
        event_status = (event.get("state") or "").upper().strip()

        for offer in event_wrapper.get("betOffers", []):
            criterion = offer.get("criterion", {})
            if criterion.get("id") != MONEYLINE_CRITERION_ID:
                continue

            outcomes = offer.get("outcomes", [])
            if len(outcomes) < 2:
                continue

            for outcome in outcomes:
                label = outcome.get("participant", "") or outcome.get("label", "")
                raw_odds = outcome.get("odds")
                if raw_odds is None:
                    continue

                opponent = away_name if label == home_name else home_name

                entries.append(BetRiversOddsEntry(
                    book="br",
                    br_event_id=event_id,
                    market="moneyline",
                    br_selection_id=str(outcome.get("id", "")),
                    player_name=label,
                    side=outcome.get("type", ""),
                    odds=raw_odds / 1000,
                    points=None,
                    tournament=tournament,
                    br_tournament_id=tournament_id,
                    circuit=circuit_key,
                    opponent_name=opponent,
                    event_status=event_status,
                    fetched_at=fetched_at,
                ))

    return entries


class BetRiversOddsScraper(BaseExtractor):
    """Scraper for BetRivers tennis odds via Kambi API."""

    def __init__(self, data_root=None):
        super().__init__(domain="betrivers", data_root=data_root)

    def fetch_all_odds(self) -> tuple[list[BetRiversOddsEntry], dict]:
        """Fetch all tennis odds from Kambi API."""
        url = f"{TENNIS_ENDPOINT}?lang=en_US&market=US-IL"
        resp = self._fetch(url)
        data = resp.json()
        now = datetime.now(UTC)
        entries = _parse_response(data, now)
        logger.info("Fetched %d BR odds entries", len(entries))
        return entries, data

    def fetch_and_save(self) -> int:
        """Fetch odds, save raw JSON + stage parquet."""
        run_at = datetime.now(UTC)
        entries, raw = self.fetch_all_odds()

        if not entries:
            logger.info("No BR odds entries found")
            return 0

        raw_path = self.build_path("raw", "moneyline", "odds.json", version="datetime")
        self.save_json([raw], raw_path)

        stage_path = self.build_path("stage", "moneyline.parquet")
        new_df = pl.DataFrame([
            {
                "book": e.book,
                "br_event_id": e.br_event_id,
                "market": e.market,
                "br_selection_id": e.br_selection_id,
                "player_name": e.player_name,
                "side": e.side,
                "odds": e.odds,
                "points": e.points,
                "tournament": e.tournament,
                "br_tournament_id": e.br_tournament_id,
                "circuit": e.circuit,
                "opponent_name": e.opponent_name,
                "event_status": e.event_status,
                "fetched_at": e.fetched_at,
                "run_at": run_at,
            }
            for e in entries
        ])

        if stage_path.exists():
            existing = pl.read_parquet(stage_path)
            new_df = pl.concat([existing, new_df], how="diagonal_relaxed")

        self.save_parquet(new_df, stage_path)
        return len(entries)


# Module-level convenience for CLI
def fetch_and_save() -> int:
    """Full flow: fetch odds, save raw + stage parquet."""
    scraper = BetRiversOddsScraper()
    return scraper.fetch_and_save()
