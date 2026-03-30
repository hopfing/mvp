"""BetMGM odds scraper for tennis markets via bwin CDS API."""

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

FIXTURES_URL = "https://www.il.betmgm.com/cds-api/bettingoffer/fixtures"

ACCESS_ID = "ZTg4YWEwMTgtZTlhYy00MWRkLWIzYWYtZjMzODI5ZDE0Mjc5"

_BASE_PARAMS = {
    "x-bwin-accessid": ACCESS_ID,
    "lang": "en-us",
    "country": "US",
    "userCountry": "US",
    "subdivision": "US-Illinois",
    "fixtureTypes": "Standard",
    "state": "Latest",
    "offerMapping": "Filtered",
    "offerCategories": "Gridable",
    "fixtureCategories": "Gridable,NonGridable,Other",
    "sportIds": "5",
    "isPriceBoost": "false",
    "statisticsModes": "Rank",
    "sortBy": "Tags",
}

_API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.il.betmgm.com/en/sports/tennis-5",
    "x-bwin-browser-url": "https://www.il.betmgm.com/en/sports/tennis-5",
    "x-from-product": "host-app",
    "x-device-type": "desktop",
}

def _classify_circuit(comp_name: str) -> str | None:
    """Classify competition name into circuit, or None to skip."""
    name = comp_name.lower()
    if "doubles" in name:
        return None
    if "atp challenger" in name:
        return "challenger"
    if name.startswith("atp"):
        return "atp"
    return None

_STAGE_MAP = {
    "PreMatch": "NOT_STARTED",
    "Live": "STARTED",
}

PAGE_SIZE = 50

_COUNTRY_CODE_RE = re.compile(r"\s*\([A-Z]{2,3}\)\s*$")


def _strip_country_code(name: str) -> str:
    """Remove trailing country code like ' (ESP)' from player names."""
    return _COUNTRY_CODE_RE.sub("", name).strip()


@dataclass
class BetMGMOddsEntry:
    book: str
    mgm_event_id: str
    market: str
    player_name: str
    odds: float
    tournament: str
    mgm_tournament_id: str
    circuit: str
    opponent_name: str
    event_status: str
    fetched_at: datetime


def _parse_fixtures(
    fixtures: list[dict],
    fetched_at: datetime,
) -> list[BetMGMOddsEntry]:
    """Parse CDS API fixtures into BetMGMOddsEntry objects.

    Filters to ATP/Challenger men's singles moneyline only.
    """
    entries = []

    for fixture in fixtures:
        competition = fixture.get("competition", {})
        comp_name = competition.get("name", {}).get("value", "")
        circuit = _classify_circuit(comp_name)
        if circuit is None:
            continue

        participants = fixture.get("participants", [])
        if len(participants) < 2:
            continue

        p1_full = participants[0].get("name", {}).get("value", "")
        p2_full = participants[1].get("name", {}).get("value", "")

        if "/" in p1_full or "/" in p2_full:
            continue

        p1_name = _strip_country_code(p1_full)
        p2_name = _strip_country_code(p2_full)

        match_winner = None
        for game in fixture.get("games", []):
            if game.get("name", {}).get("value", "") == "Match winner":
                match_winner = game
                break

        if match_winner is None:
            continue

        results = match_winner.get("results", [])
        if len(results) < 2:
            continue

        event_id = str(fixture.get("id", ""))
        tournament = competition.get("name", {}).get("value", "") or fixture.get("tournament", {}).get("name", {}).get("value", "")
        tournament_id = str(fixture.get("tournament", {}).get("id", ""))
        stage = fixture.get("stage", "")
        event_status = _STAGE_MAP.get(stage, "NOT_STARTED")

        r1_odds = results[0].get("odds")
        r2_odds = results[1].get("odds")

        if r1_odds is None or r2_odds is None:
            continue

        entries.append(BetMGMOddsEntry(
            book="mgm",
            mgm_event_id=event_id,
            market="moneyline",
            player_name=p1_name,
            odds=r1_odds,
            tournament=tournament,
            mgm_tournament_id=tournament_id,
            circuit=circuit,
            opponent_name=p2_name,
            event_status=event_status,
            fetched_at=fetched_at,
        ))
        entries.append(BetMGMOddsEntry(
            book="mgm",
            mgm_event_id=event_id,
            market="moneyline",
            player_name=p2_name,
            odds=r2_odds,
            tournament=tournament,
            mgm_tournament_id=tournament_id,
            circuit=circuit,
            opponent_name=p1_name,
            event_status=event_status,
            fetched_at=fetched_at,
        ))

    return entries


class BetMGMOddsScraper(BaseExtractor):
    """Scraper for BetMGM tennis odds via bwin CDS API."""

    def __init__(self, data_root=None, run_at=None):
        super().__init__(domain="betmgm", data_root=data_root,
                         run_at=run_at)

    def _create_session(self):
        """Override to use cloudscraper for Cloudflare bypass."""
        import cloudscraper

        session = cloudscraper.create_scraper()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        })
        return session

    def fetch_all_odds(self) -> tuple[list[BetMGMOddsEntry], list[dict]]:
        """Fetch all tennis fixtures, paginating as needed."""
        all_fixtures = []
        raw_responses = []
        skip = 0

        while True:
            params = {**_BASE_PARAMS, "skip": str(skip), "take": str(PAGE_SIZE)}
            url = FIXTURES_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
            resp = self._fetch(url, headers=_API_HEADERS)
            data = resp.json()
            raw_responses.append(data)

            fixtures = data.get("fixtures", [])
            total = data.get("totalCount", 0)
            logger.info("MGM page skip=%d: %d fixtures (totalCount=%d, status=%d)", skip, len(fixtures), total, resp.status_code)

            if not fixtures and total > 0:
                logger.warning("MGM returned 0 fixtures but totalCount=%d — possible Cloudflare block", total)
            if not fixtures and total == 0:
                logger.warning("MGM returned 0 fixtures and totalCount=0 — API may be blocking or no tennis events")

            all_fixtures.extend(fixtures)

            skip += PAGE_SIZE
            if skip >= total:
                break

        now = datetime.now(UTC)
        entries = _parse_fixtures(all_fixtures, now)
        logger.info(
            "MGM fetch complete: %d fixtures -> %d entries (filtered to ATP/Challenger singles moneyline)",
            len(all_fixtures), len(entries),
        )
        return entries, raw_responses

    def fetch_and_save_raw(self) -> int:
        """Fetch odds from MGM and save raw JSON.

        Returns number of entries fetched.
        """
        entries, raw = self.fetch_all_odds()

        if not entries:
            logger.info("No MGM odds entries found")
            return 0

        raw_path = self.build_path("raw", "moneyline", "odds.json", version="datetime")
        self.save_json(raw, raw_path)
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

            all_fixtures: list[dict] = []
            if isinstance(data_list, list):
                for item in data_list:
                    all_fixtures.extend(item.get("fixtures", []))
            else:
                all_fixtures.extend(data_list.get("fixtures", []))

            # Derive timestamps from raw filename
            parts = raw_path.stem.replace("odds_", "")
            file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")

            entries = _parse_fixtures(all_fixtures, file_ts)

            if not entries:
                continue

            df = pl.DataFrame([
                {
                    "book": e.book,
                    "mgm_event_id": e.mgm_event_id,
                    "market": e.market,
                    "player_name": e.player_name,
                    "odds": e.odds,
                    "tournament": e.tournament,
                    "mgm_tournament_id": e.mgm_tournament_id,
                    "circuit": e.circuit,
                    "opponent_name": e.opponent_name,
                    "event_status": e.event_status,
                    "fetched_at": e.fetched_at,
                    "run_at": file_ts,
                }
                for e in entries
            ])

            out_path = stage_dir / f"{raw_path.stem}.parquet"
            result = self.save_parquet(df, out_path)
            if result:
                staged.append(result)

        if staged:
            logger.info("MGM staged %d new snapshots", len(staged))
        return staged

    def consolidate(self) -> Path | None:
        """Merge all per-snapshot parquets into moneyline.parquet."""
        stage_dir = self.build_path("stage", "moneyline")
        snapshots = self.list_files(stage_dir, "*.parquet")
        if not snapshots:
            logger.info("No MGM snapshots to consolidate")
            return None

        dfs = [pl.read_parquet(f) for f in snapshots]
        df = pl.concat(dfs, how="diagonal_relaxed")

        out_path = self.build_path("stage", "moneyline.parquet")
        return self.save_parquet(df, out_path)

    def run(self) -> int:
        """Full flow: fetch raw, stage, consolidate."""
        n = self.fetch_and_save_raw()
        self.stage()
        self.consolidate()
        return n


def fetch_and_save(run_at=None) -> int:
    """Full flow: fetch, stage, consolidate."""
    scraper = BetMGMOddsScraper(run_at=run_at)
    return scraper.run()
