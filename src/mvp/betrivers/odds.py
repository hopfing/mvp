"""BetRivers odds scraper for tennis markets."""

import logging
import random
import time
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

BR_SITE_API = (
    "https://il.betrivers.com/api/service/sportsbook/offering"
    "/listview/filtered/events"
)
BR_CAGE_CODE = 847

_INCLUDE_TERM_KEYS = {
    "atp",
    "challenger",
    "challenger_qual_",
    "grand_slam",
}

# path[2] termKey substrings that disqualify an event within an included
# top-level category (currently only relevant inside grand_slam, which mixes
# men's, women's, and doubles draws under one circuit key).
_PATH_EXCLUDE_SUBSTRINGS = ("women", "doubles")

MONEYLINE_CRITERION_ID = 1001159551

# betOfferType -> market name mapping for the site API
_OFFER_TYPE_MAP = {
    "TWO_WAY": "moneyline",
    "TWO_WAY_HANDICAP": "game_spread",
    "OVER_UNDER": "total_games",
}


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
    """Check if a Kambi path[1] termKey is an included men's-singles category."""
    return term_key in _INCLUDE_TERM_KEYS


def _is_included_path(path: list[dict]) -> bool:
    """Decide whether a Kambi event path should be included.

    path[1].termKey identifies the top-level tour category; for the Grand Slam
    branch we additionally reject sub-paths that surface women's or doubles
    draws (the atp/challenger branches don't currently mix those in).
    """
    if len(path) < 2:
        return False
    if not _is_atp_challenger(path[1].get("termKey", "")):
        return False
    if len(path) >= 3:
        sub_key = path[2].get("termKey", "").lower()
        if any(s in sub_key for s in _PATH_EXCLUDE_SUBSTRINGS):
            return False
    return True


def _parse_kambi_response(
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

        if not _is_included_path(path):
            continue
        circuit_key = path[1].get("termKey", "")

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


def _extract_group_ids(data: dict) -> dict[int, dict]:
    """Extract unique groupId -> {name, circuit} from Kambi response."""
    groups: dict[int, dict] = {}
    for event_wrapper in data.get("events", []):
        event = event_wrapper.get("event", {})
        path = event.get("path", [])
        if not _is_included_path(path):
            continue
        circuit_key = path[1].get("termKey", "")
        gid = event.get("groupId")
        if gid and gid not in groups:
            groups[gid] = {
                "name": event.get("group", ""),
                "circuit": circuit_key,
            }
    return groups


def _parse_site_response(
    data: dict,
    tournament: str,
    circuit: str,
    fetched_at: datetime,
) -> list[BetRiversOddsEntry]:
    """Parse BR site API response into OddsEntry objects for all market types."""
    entries = []
    for item in data.get("items", []):
        event_id = str(item.get("id", ""))
        participants = item.get("participants", [])
        home_name = ""
        away_name = ""
        for p in participants:
            if p.get("home"):
                home_name = p.get("name", "")
            else:
                away_name = p.get("name", "")

        state = (item.get("state") or "").upper().strip()

        tournament_id = ""
        for info in item.get("eventInfo", []):
            # Last eventInfo entry is the tournament
            tournament_id = str(info.get("id", ""))

        for offer in item.get("betOffers", []):
            offer_type = offer.get("betOfferType", "")
            market = _OFFER_TYPE_MAP.get(offer_type)
            if not market:
                continue

            for outcome in offer.get("outcomes", []):
                label = outcome.get("label", "")
                odds_val = outcome.get("odds")
                if odds_val is None:
                    continue
                odds_val = float(odds_val)

                line = outcome.get("line")
                if line is not None:
                    line = float(line)

                opponent = away_name if label == home_name else home_name

                entries.append(BetRiversOddsEntry(
                    book="br",
                    br_event_id=event_id,
                    market=market,
                    br_selection_id=str(outcome.get("id", "")),
                    player_name=label,
                    side=outcome.get("type", "").lower(),
                    odds=odds_val,
                    points=line,
                    tournament=tournament,
                    br_tournament_id=tournament_id,
                    circuit=circuit,
                    opponent_name=opponent,
                    event_status=state,
                    fetched_at=fetched_at,
                ))
    return entries


def _entries_to_df(entries: list[BetRiversOddsEntry], run_at: datetime) -> pl.DataFrame:
    return pl.DataFrame([
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


class BetRiversOddsScraper(BaseExtractor):
    """Scraper for BetRivers tennis odds."""

    def __init__(self, data_root=None, run_at=None):
        super().__init__(domain="betrivers", data_root=data_root,
                         run_at=run_at)

    # ── Kambi (moneyline only, existing pipeline) ────────────────────────

    def fetch_all_odds(self) -> tuple[list[BetRiversOddsEntry], dict]:
        """Fetch all tennis odds from Kambi API."""
        url = f"{TENNIS_ENDPOINT}?lang=en_US&market=US-IL"
        resp = self._fetch(url)
        data = resp.json()
        now = datetime.now(UTC)
        entries = _parse_kambi_response(data, now)
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

            parts = raw_path.stem.replace("odds_", "")
            file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")

            all_entries: list[BetRiversOddsEntry] = []
            for item in data_list:
                all_entries.extend(_parse_kambi_response(item, file_ts))

            if not all_entries:
                continue

            df = _entries_to_df(all_entries, file_ts)
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

    def _run_moneyline(self) -> int:
        """Moneyline via Kambi (existing pipeline)."""
        n = self.fetch_and_save_raw()
        self.stage()
        self.consolidate()
        return n

    # ── Site API (all markets) ───────────────────────────────────────────

    def _warm_br_session(self) -> None:
        """Visit the BR site to establish Cloudflare cookies."""
        self._fetch("https://il.betrivers.com/?page=sportsbook")

    def _discover_groups(self) -> dict[int, dict]:
        """Fetch Kambi tennis listing and extract group IDs."""
        url = f"{TENNIS_ENDPOINT}?lang=en_US&market=US-IL"
        resp = self._fetch(url)
        data = resp.json()
        return _extract_group_ids(data)

    def _fetch_group_markets(
        self, group_id: int, page: int = 1, page_size: int = 20,
    ) -> dict:
        """POST to BR site API for one group's events with all markets."""
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://il.betrivers.com",
            "Referer": "https://il.betrivers.com/",
        }
        payload = {
            "cageCode": BR_CAGE_CODE,
            "eventFeedTypes": ["LIVE", "PREMATCH"],
            "groupIds": [group_id],
            "mainLineOnly": True,
            "pageNr": page,
            "pageSize": page_size,
            "offset": 0,
        }
        time.sleep(random.uniform(0.75, 1.25))
        logger.info("POST BR site API group=%d page=%d", group_id, page)
        resp = self.session.post(
            f"{BR_SITE_API}?cageCode={BR_CAGE_CODE}",
            json=payload,
            headers=headers,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_all_markets(self) -> tuple[list[BetRiversOddsEntry], list[dict]]:
        """Discover groups via Kambi, then fetch all markets via site API."""
        self._warm_br_session()
        groups = self._discover_groups()
        logger.info("Found %d BR tournament groups", len(groups))

        all_entries: list[BetRiversOddsEntry] = []
        raw_responses: list[dict] = []
        now = datetime.now(UTC)

        for gid, info in groups.items():
            try:
                data = self._fetch_group_markets(gid)
                entries = _parse_site_response(
                    data, info["name"], info["circuit"], now,
                )
                all_entries.extend(entries)
                raw_responses.append({
                    "group_id": gid,
                    "group_name": info["name"],
                    "circuit": info["circuit"],
                    "response": data,
                })
                logger.info("  %s: %d entries", info["name"], len(entries))

                # Handle pagination
                paging = data.get("paging", {})
                total_pages = paging.get("totalPages", 1)
                for page in range(2, total_pages + 1):
                    page_data = self._fetch_group_markets(gid, page=page)
                    page_entries = _parse_site_response(
                        page_data, info["name"], info["circuit"], now,
                    )
                    all_entries.extend(page_entries)
                    raw_responses.append({
                        "group_id": gid,
                        "group_name": info["name"],
                        "circuit": info["circuit"],
                        "page": page,
                        "response": page_data,
                    })
            except Exception as e:
                logger.warning("Failed to fetch BR group %s (%d): %s",
                               info["name"], gid, e)

        return all_entries, raw_responses

    def fetch_and_save_all_raw(self) -> int:
        """Fetch all markets from BR site API and save raw JSON."""
        entries, raw_responses = self.fetch_all_markets()
        if not entries:
            logger.info("No BR all-market entries found")
            return 0

        # Save one raw file (site API returns all markets per call)
        raw_path = self.build_path("raw", "site_markets", "odds.json",
                                   version="datetime")
        self.save_json(raw_responses, raw_path)
        return len(entries)

    def _stage_site_markets(self) -> list[Path]:
        """Stage raw site-API JSON into per-market parquets."""
        raw_dir = self.build_path("raw", "site_markets")
        raw_files = self.list_files(raw_dir, "odds_*.json")
        if not raw_files:
            return []

        markets = list(_OFFER_TYPE_MAP.values())
        existing_by_market = {}
        for market in markets:
            stage_dir = self.build_path("stage", market)
            existing_by_market[market] = {
                p.stem for p in self.list_files(stage_dir, "*.parquet")
            }

        staged: list[Path] = []
        for raw_path in raw_files:
            # Skip if already staged for all markets
            if all(raw_path.stem in existing_by_market[m] for m in markets):
                continue

            try:
                data_list = self.read_json(raw_path)
            except Exception:
                logger.warning("Skipping corrupt raw file: %s", raw_path.name)
                continue

            parts = raw_path.stem.replace("odds_", "")
            file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")

            all_entries: list[BetRiversOddsEntry] = []
            for item in data_list:
                resp = item.get("response", item)
                group_name = item.get("group_name", "")
                circuit = item.get("circuit", "")
                all_entries.extend(
                    _parse_site_response(resp, group_name, circuit, file_ts)
                )

            if not all_entries:
                continue

            # Split by market and stage separately
            for market in markets:
                if raw_path.stem in existing_by_market[market]:
                    continue
                market_entries = [e for e in all_entries if e.market == market]
                if not market_entries:
                    continue
                df = _entries_to_df(market_entries, file_ts)
                stage_dir = self.build_path("stage", market)
                out_path = stage_dir / f"{raw_path.stem}.parquet"
                result = self.save_parquet(df, out_path)
                if result:
                    staged.append(result)

        if staged:
            logger.info("BR staged %d per-market snapshots", len(staged))
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

    def _run_site_markets(self) -> int:
        """All markets via site API, staged per-market."""
        n = self.fetch_and_save_all_raw()
        self._stage_site_markets()
        for market in _OFFER_TYPE_MAP.values():
            self._consolidate_market(market)
        return n

    def run(self) -> int:
        """Full flow: moneyline (Kambi) + all markets (site API)."""
        n_ml = self._run_moneyline()
        n_site = self._run_site_markets()
        return n_ml + n_site


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from collections import Counter
    scraper = BetRiversOddsScraper()
    entries, _ = scraper.fetch_all_markets()
    print(f"\nTotal: {len(entries)} entries")
    market_counts = Counter(e.market for e in entries)
    for market, count in market_counts.most_common():
        sample = next(e for e in entries if e.market == market)
        print(f"  {market:20s}: {count:4d} entries"
              f"  e.g. {sample.player_name} pts={sample.points} @ {sample.odds}")
    tournament_counts = Counter(e.tournament for e in entries)
    print(f"\nTournaments: {len(tournament_counts)}")
    for tourn, count in tournament_counts.most_common():
        print(f"  {tourn:30s}: {count:4d}")
