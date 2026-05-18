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

def _classify_circuit(tournament_name: str, comp_name: str) -> str | None:
    """Classify a fixture into a circuit, or None to skip.

    Uses the structural fixture.tournament.name bucket as the primary signal
    (e.g. "ATP", "Challenger", "Grand Slam Tournaments") and the competition
    name as a secondary filter for doubles and (within Grand Slam) gender.
    """
    comp_lower = comp_name.lower()
    if "doubles" in comp_lower:
        return None
    if tournament_name == "ATP":
        return "atp"
    if tournament_name == "Challenger":
        return "challenger"
    if tournament_name == "Grand Slam Tournaments":
        if " - women" in comp_lower:
            return None
        if " - men" in comp_lower:
            return "grand_slam"
        return None
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


_GAMES_MARKETS = {
    "Total games: Match": "total_games",
    "Player to win the most games in the match": "game_spread",
}


@dataclass
class BetMGMOddsEntry:
    book: str
    mgm_event_id: str
    market: str
    player_name: str
    side: str
    odds: float
    points: float | None
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
        tournament_meta = fixture.get("tournament", {})
        tournament_name = tournament_meta.get("name", {}).get("value", "")
        circuit = _classify_circuit(tournament_name, comp_name)
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
        tournament = comp_name or tournament_name
        tournament_id = str(tournament_meta.get("id", ""))
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
            side="home",
            odds=r1_odds,
            points=None,
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
            side="away",
            odds=r2_odds,
            points=None,
            tournament=tournament,
            mgm_tournament_id=tournament_id,
            circuit=circuit,
            opponent_name=p1_name,
            event_status=event_status,
            fetched_at=fetched_at,
        ))

    return entries


def _parse_all_markets(
    fixtures: list[dict],
    fetched_at: datetime,
) -> list[BetMGMOddsEntry]:
    """Parse all market types from CDS API fixtures."""
    entries = _parse_fixtures(fixtures, fetched_at)

    for fixture in fixtures:
        competition = fixture.get("competition", {})
        comp_name = competition.get("name", {}).get("value", "")
        tournament_meta = fixture.get("tournament", {})
        tournament_name = tournament_meta.get("name", {}).get("value", "")
        circuit = _classify_circuit(tournament_name, comp_name)
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

        event_id = str(fixture.get("id", ""))
        tournament = comp_name or tournament_name
        tournament_id = str(tournament_meta.get("id", ""))
        stage = fixture.get("stage", "")
        event_status = _STAGE_MAP.get(stage, "NOT_STARTED")

        for game in fixture.get("games", []):
            name_obj = game.get("name", {})
            game_name = name_obj.get("value", "") if isinstance(name_obj, dict) else str(name_obj)
            market = _GAMES_MARKETS.get(game_name)
            if not market:
                continue

            results = game.get("results", [])
            if len(results) < 2:
                continue

            if market == "total_games":
                line = game.get("attr")
                if line is None:
                    continue
                try:
                    line = float(str(line).replace(",", "."))
                except (ValueError, TypeError):
                    continue
                for r in results:
                    r_name = r.get("name", {})
                    label = r_name.get("value", "") if isinstance(r_name, dict) else str(r_name)
                    r_odds = r.get("odds")
                    if r_odds is None:
                        continue
                    side = "over" if "Over" in label else "under"
                    entries.append(BetMGMOddsEntry(
                        book="mgm",
                        mgm_event_id=event_id,
                        market="total_games",
                        player_name=label.split()[0],  # "Over" or "Under"
                        side=side,
                        odds=float(r_odds),
                        points=line,
                        tournament=tournament,
                        mgm_tournament_id=tournament_id,
                        circuit=circuit,
                        opponent_name="",
                        event_status=event_status,
                        fetched_at=fetched_at,
                    ))

            elif market == "game_spread":
                for r in results:
                    r_name = r.get("name", {})
                    label = r_name.get("value", "") if isinstance(r_name, dict) else str(r_name)
                    r_odds = r.get("odds")
                    attr = r.get("attr")
                    if r_odds is None or attr is None:
                        continue
                    try:
                        spread = float(str(attr).replace(",", "."))
                    except (ValueError, TypeError):
                        continue
                    player = _strip_country_code(label.rsplit(" ", 1)[0]) if " " in label else label
                    opponent = p2_name if player == p1_name else p1_name
                    entries.append(BetMGMOddsEntry(
                        book="mgm",
                        mgm_event_id=event_id,
                        market="game_spread",
                        player_name=player,
                        side="home" if player == p1_name else "away",
                        odds=float(r_odds),
                        points=spread,
                        tournament=tournament,
                        mgm_tournament_id=tournament_id,
                        circuit=circuit,
                        opponent_name=opponent,
                        event_status=event_status,
                        fetched_at=fetched_at,
                    ))

    return entries


def _entries_to_df(entries: list[BetMGMOddsEntry], run_at: datetime) -> pl.DataFrame:
    return pl.DataFrame([
        {
            "book": e.book,
            "mgm_event_id": e.mgm_event_id,
            "market": e.market,
            "player_name": e.player_name,
            "side": e.side,
            "odds": e.odds,
            "points": e.points,
            "tournament": e.tournament,
            "mgm_tournament_id": e.mgm_tournament_id,
            "circuit": e.circuit,
            "opponent_name": e.opponent_name,
            "event_status": e.event_status,
            "fetched_at": e.fetched_at,
            "run_at": run_at,
        }
        for e in entries
    ])


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

            df = _entries_to_df(entries, file_ts)

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

    def _stage_all_markets(self) -> list[Path]:
        """Stage all market types from raw JSON into per-market parquets."""
        raw_dir = self.build_path("raw", "moneyline")
        raw_files = self.list_files(raw_dir, "odds_*.json")
        if not raw_files:
            return []

        markets = list(_GAMES_MARKETS.values())
        existing_by_market = {}
        for market in markets:
            stage_dir = self.build_path("stage", market)
            existing_by_market[market] = {
                p.stem for p in self.list_files(stage_dir, "*.parquet")
            } | {
                p.stem for p in self.list_files(stage_dir, "*.empty")
            }

        staged: list[Path] = []
        for raw_path in raw_files:
            if all(raw_path.stem in existing_by_market[m] for m in markets):
                continue

            try:
                data_list = self.read_json(raw_path)
            except Exception:
                continue

            all_fixtures: list[dict] = []
            if isinstance(data_list, list):
                for item in data_list:
                    all_fixtures.extend(item.get("fixtures", []))
            else:
                all_fixtures.extend(data_list.get("fixtures", []))

            parts = raw_path.stem.replace("odds_", "")
            file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")

            entries = _parse_all_markets(all_fixtures, file_ts)

            for market in markets:
                if raw_path.stem in existing_by_market[market]:
                    continue
                stage_dir = self.build_path("stage", market)
                market_entries = [e for e in entries if e.market == market]
                if not market_entries:
                    stage_dir.mkdir(parents=True, exist_ok=True)
                    (stage_dir / f"{raw_path.stem}.empty").touch()
                    continue
                df = _entries_to_df(market_entries, file_ts)
                out_path = stage_dir / f"{raw_path.stem}.parquet"
                result = self.save_parquet(df, out_path)
                if result:
                    staged.append(result)

        if staged:
            logger.info("MGM staged %d per-market snapshots", len(staged))
        return staged

    def _consolidate_market(self, market: str) -> Path | None:
        """Merge per-snapshot parquets into {market}.parquet."""
        stage_dir = self.build_path("stage", market)
        snapshots = self.list_files(stage_dir, "*.parquet")
        if not snapshots:
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

        out_path = self.build_path("stage", f"{market}.parquet")
        return self.save_parquet(df, out_path)

    def run(self) -> int:
        """Full flow: fetch raw, stage moneyline + all markets."""
        n = self.fetch_and_save_raw()
        self.stage()
        self.consolidate()
        self._stage_all_markets()
        for market in _GAMES_MARKETS.values():
            self._consolidate_market(market)
        return n


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from collections import Counter
    scraper = BetMGMOddsScraper()
    _, raw_responses = scraper.fetch_all_odds()
    all_fixtures = []
    for page in raw_responses:
        all_fixtures.extend(page.get("fixtures", []))
    now = datetime.now(UTC)
    all_entries = _parse_all_markets(all_fixtures, now)
    print(f"\nTotal: {len(all_entries)} entries")
    market_counts = Counter(e.market for e in all_entries)
    for market, count in market_counts.most_common():
        sample = next(e for e in all_entries if e.market == market)
        print(f"  {market:20s}: {count:4d} entries"
              f"  e.g. {sample.player_name} pts={sample.points} @ {sample.odds}")
