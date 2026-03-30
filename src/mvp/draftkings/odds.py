"""DraftKings odds scraper for tennis markets."""


import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

TENNIS_URL = "https://sportsbook.draftkings.com/sports/tennis"
LEAGUE_API_BASE = (
    "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/leagues"
)

SUBCATEGORIES = {
    "moneyline": 6364,
    "game_spread": 16089,
    "total_games": 16090,
    "total_sets": 5369,
}

_INCLUDE_PATTERNS = [
    re.compile(r"^atp-"),
    re.compile(r"^challenger-"),
    re.compile(r"^australian-open-men"),
    re.compile(r"^french-open-men"),
    re.compile(r"^wimbledon-men"),
    re.compile(r"^us-open-men"),
]

_EXCLUDE_PATTERNS = [
    re.compile(r"-doubles"),
    re.compile(r"-women"),
]

_API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://sportsbook.draftkings.com/",
    "Origin": "https://sportsbook.draftkings.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}


@dataclass
class OddsEntry:
    book: str
    dk_event_id: str
    market: str
    dk_selection_id: str
    player_name: str
    country_code: str
    side: str
    odds: float
    points: float | None
    tournament: str
    dk_tournament_id: str
    opponent_name: str
    event_status: str
    fetched_at: datetime


def _is_atp_challenger(slug: str) -> bool:
    """Check if a league slug is ATP or Challenger men's singles."""
    slug = slug.lower().strip()
    for pat in _EXCLUDE_PATTERNS:
        if pat.search(slug):
            return False
    for pat in _INCLUDE_PATTERNS:
        if pat.search(slug):
            return True
    return False


def _parse_odds_response(
    data: dict,
    market: str,
    fetched_at: datetime,
) -> list[OddsEntry]:
    """Parse odds API response into OddsEntry objects."""
    entries = []
    subcategory_id = SUBCATEGORIES.get(market)

    event_map: dict[str, dict] = {}
    for ev in data.get("events", []):
        event_map[str(ev.get("id", ""))] = ev

    selections_by_market: dict[str, list[dict]] = {}
    for sel in data.get("selections", []):
        selections_by_market.setdefault(sel.get("marketId", ""), []).append(sel)

    league_map: dict[str, dict] = {}
    for lg in data.get("leagues", []):
        league_map[str(lg.get("id", ""))] = lg

    for mkt in data.get("markets", []):
        if subcategory_id is not None and mkt.get("subcategoryId") != subcategory_id:
            continue

        event_id = str(mkt.get("eventId", ""))
        ev = event_map.get(event_id, {})
        league_id = str(ev.get("leagueId", "") or mkt.get("leagueId", ""))
        tournament = league_map.get(league_id, {}).get("name", "")
        event_status = (ev.get("status") or "").upper().strip()

        sels = selections_by_market.get(mkt.get("id", ""), [])
        if not sels:
            continue

        player_names = [sel.get("label", "") for sel in sels]

        for sel in sels:
            label = sel.get("label", "")
            true_odds = sel.get("trueOdds")
            display_odds = sel.get("displayOdds", {}).get("decimal")
            odds_val = true_odds if true_odds is not None else display_odds
            if odds_val is None:
                continue
            odds_val = float(odds_val)

            points = sel.get("points")
            if points is not None:
                points = float(points)

            participants = sel.get("participants", [])
            country_code = participants[0].get("countryCode", "") if participants else ""

            opponent = next((n for n in player_names if n != label), "")

            entries.append(OddsEntry(
                book="dk",
                dk_event_id=event_id,
                market=market,
                dk_selection_id=str(sel.get("id", "")),
                player_name=label,
                country_code=country_code.upper().strip() if country_code else "",
                side=sel.get("outcomeType", "").lower(),
                odds=odds_val,
                points=points,
                tournament=tournament,
                dk_tournament_id=league_id,
                opponent_name=opponent,
                event_status=event_status,
                fetched_at=fetched_at,
            ))

    return entries


class DraftKingsOddsScraper(BaseExtractor):
    """Scraper for DraftKings tennis odds."""

    def __init__(self, data_root=None, run_at=None):
        super().__init__(domain="draftkings", data_root=data_root,
                         run_at=run_at)

    def _warm_session(self) -> None:
        """Visit the tennis page to establish cookies needed for the API."""
        self._fetch(TENNIS_URL)

    def fetch_tennis_leagues(self) -> list[dict]:
        """Fetch active tennis leagues from DK, filtered to ATP/Challenger."""
        html = self.fetch_html(TENNIS_URL)

        match = re.search(
            r"window\.__INITIAL_STATE__\s*=\s*(\{.+?\});?\s*</script>",
            html, re.DOTALL,
        )
        if not match:
            raise ValueError("Could not find __INITIAL_STATE__ in DK tennis page")

        state = json.loads(match.group(1))

        leagues = []
        for sport in state.get("sports", {}).get("data", []):
            if sport.get("nameIdentifier", "").lower() != "tennis":
                continue
            for eg in sport.get("eventGroupInfos", []):
                slug = eg.get("nameIdentifier", "")
                if _is_atp_challenger(slug):
                    leagues.append({
                        "dk_tournament_id": str(eg.get("eventGroupId", "")),
                        "name": eg.get("eventGroupName", ""),
                        "slug": slug,
                    })
        return leagues

    def fetch_league_odds(
        self,
        dk_tournament_id: str,
        market: str = "moneyline",
    ) -> tuple[list[OddsEntry], dict]:
        """Fetch odds for one league. Returns (entries, raw_response)."""
        url = f"{LEAGUE_API_BASE}/{dk_tournament_id}"
        resp = self._fetch(url, headers=_API_HEADERS)
        data = resp.json()

        now = datetime.now(UTC)
        entries = _parse_odds_response(data, market, now)
        return entries, data

    def fetch_all_odds(
        self,
        market: str = "moneyline",
    ) -> tuple[list[OddsEntry], list[dict]]:
        """Discover leagues + fetch odds for each."""
        if market not in SUBCATEGORIES:
            raise ValueError(f"Unknown market: {market}. Choose from {list(SUBCATEGORIES)}")

        leagues = self.fetch_tennis_leagues()
        logger.info("Found %d ATP/Challenger leagues on DK", len(leagues))

        all_entries: list[OddsEntry] = []
        raw_responses: list[dict] = []

        for league in leagues:
            try:
                entries, raw = self.fetch_league_odds(
                    dk_tournament_id=league["dk_tournament_id"],
                    market=market,
                )
                all_entries.extend(entries)
                raw_responses.append({"league": league, "response": raw})
                logger.info("  %s: %d entries", league["name"], len(entries))
            except Exception as e:
                logger.warning("Failed to fetch odds for %s: %s", league["name"], e)

        return all_entries, raw_responses

    def fetch_and_save_raw(self, market: str = "moneyline") -> int:
        """Fetch odds from DK and save raw JSON.

        Returns number of entries fetched.
        """
        entries, raw_responses = self.fetch_all_odds(market=market)

        if not entries:
            logger.info("No DK odds entries found")
            return 0

        raw_path = self.build_path("raw", market, "odds.json", version="datetime")
        self.save_json(raw_responses, raw_path)
        return len(entries)

    def stage(self, market: str = "moneyline") -> list[Path]:
        """Parse raw JSON files that don't have staged counterparts.

        Returns list of staged parquet paths written.
        """
        raw_dir = self.build_path("raw", market)
        stage_dir = self.build_path("stage", market)
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
            file_ts = datetime.now()
            try:
                parts = raw_path.stem.replace("odds_", "")
                file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")
            except ValueError:
                pass

            all_entries: list[OddsEntry] = []
            for item in data_list:
                resp = item.get("response", item)
                all_entries.extend(_parse_odds_response(resp, market, file_ts))

            if not all_entries:
                continue

            df = pl.DataFrame([
                {
                    "book": e.book,
                    "dk_event_id": e.dk_event_id,
                    "market": e.market,
                    "dk_selection_id": e.dk_selection_id,
                    "player_name": e.player_name,
                    "country_code": e.country_code,
                    "side": e.side,
                    "odds": e.odds,
                    "points": e.points,
                    "tournament": e.tournament,
                    "dk_tournament_id": e.dk_tournament_id,
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
            logger.info("DK staged %d new snapshots", len(staged))
        return staged

    def consolidate(self, market: str = "moneyline") -> Path | None:
        """Merge all per-snapshot parquets into moneyline.parquet."""
        stage_dir = self.build_path("stage", market)
        snapshots = self.list_files(stage_dir, "*.parquet")
        if not snapshots:
            logger.info("No DK snapshots to consolidate")
            return None

        dfs = [pl.read_parquet(f) for f in snapshots]
        df = pl.concat(dfs, how="diagonal_relaxed")

        out_path = self.build_path("stage", f"{market}.parquet")
        return self.save_parquet(df, out_path)

    def run(self, market: str = "moneyline") -> int:
        """Full flow: fetch raw, stage, consolidate."""
        n = self.fetch_and_save_raw(market=market)
        self.stage(market=market)
        self.consolidate(market=market)
        return n


# Module-level convenience for CLI
def fetch_and_save(market: str = "moneyline", run_at=None) -> int:
    """Full flow: fetch, stage, consolidate."""
    scraper = DraftKingsOddsScraper(run_at=run_at)
    return scraper.run(market=market)
