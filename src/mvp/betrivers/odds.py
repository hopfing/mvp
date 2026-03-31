"""BetRivers odds scraper for tennis markets via Kambi API."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

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

    def __init__(self, data_root=None, run_at=None):
        super().__init__(domain="betrivers", data_root=data_root,
                         run_at=run_at)

    def fetch_all_odds(self) -> tuple[list[BetRiversOddsEntry], dict]:
        """Fetch all tennis odds from Kambi API."""
        url = f"{TENNIS_ENDPOINT}?lang=en_US&market=US-IL"
        resp = self._fetch(url)
        data = resp.json()
        now = datetime.now(UTC)
        entries = _parse_response(data, now)
        logger.info("Fetched %d BR odds entries", len(entries))
        return entries, data

    def fetch_and_save_raw(self) -> int:
        """Fetch odds from BR and save raw JSON.

        Returns number of entries fetched.
        """
        entries, raw = self.fetch_all_odds()

        if not entries:
            logger.info("No BR odds entries found")
            return 0

        raw_path = self.build_path("raw", "moneyline", "odds.json", version="datetime")
        self.save_json([raw], raw_path)
        return len(entries)

    def stage(self) -> list[Path]:
        """Parse raw JSON files that don't have staged counterparts.

        Returns list of staged parquet paths written.
        """
        raw_dir = self.build_path("raw", "moneyline")
        stage_dir = self.build_path("stage", "moneyline")
        raw_files = self.list_files(raw_dir, "odds_*.json")
        if not raw_files:
            return []

        existing = {p.stem for p in self.list_files(stage_dir, "*.parquet")}

        staged: list[Path] = []
        for raw_path in raw_files:
            if raw_path.stem in existing:
                continue

            try:
                data_list = self.read_json(raw_path)
            except Exception:
                logger.warning("Skipping corrupt raw file: %s", raw_path.name)
                continue

            # Derive timestamps from raw filename
            parts = raw_path.stem.replace("odds_", "")
            file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")

            all_entries: list[BetRiversOddsEntry] = []
            for item in data_list:
                all_entries.extend(_parse_response(item, file_ts))

            if not all_entries:
                continue

            df = pl.DataFrame([
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
                    "run_at": file_ts,
                }
                for e in all_entries
            ])

            out_path = stage_dir / f"{raw_path.stem}.parquet"
            result = self.save_parquet(df, out_path)
            if result:
                staged.append(result)

        if staged:
            logger.info("BR staged %d new snapshots", len(staged))
        return staged

    def consolidate(self) -> Path | None:
        """Merge all per-snapshot parquets into moneyline.parquet."""
        stage_dir = self.build_path("stage", "moneyline")
        snapshots = self.list_files(stage_dir, "*.parquet")
        if not snapshots:
            logger.info("No BR snapshots to consolidate")
            return None

        dfs = []
        for f in snapshots:
            _df = pl.read_parquet(f)
            tz_cols = [
                c for c, dt in _df.schema.items()
                if isinstance(dt, pl.Datetime) and dt.time_zone is not None
            ]
            if tz_cols:
                _df = _df.with_columns(
                    pl.col(c).dt.replace_time_zone(None) for c in tz_cols
                )
            dfs.append(_df)
        df = pl.concat(dfs, how="diagonal_relaxed")

        out_path = self.build_path("stage", "moneyline.parquet")
        return self.save_parquet(df, out_path)

    def run(self) -> int:
        """Full flow: fetch raw, stage, consolidate."""
        n = self.fetch_and_save_raw()
        self.stage()
        self.consolidate()
        return n


# Module-level convenience for CLI
def fetch_and_save(run_at=None) -> int:
    """Full flow: fetch, stage, consolidate."""
    scraper = BetRiversOddsScraper(run_at=run_at)
    return scraper.run()
