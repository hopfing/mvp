"""FanDuel odds scraper for tennis markets.

Data source notes (validated 2026-04-08):

  Endpoints
    - HTML shell:        https://sportsbook.fanduel.com/
    - SPORT page:        https://api.sportsbook.fanduel.com/sbapi/content-managed-page
                            ?page=SPORT
                            &eventTypeId=2
                            &_ak={ACCESS_KEY}
                            &timezone=America/Chicago
    - Competition page:  https://api.sportsbook.fanduel.com/sbapi/competition-page
                            ?_ak={ACCESS_KEY}
                            &eventTypeId=2
                            &competitionId={competition_id}

  Required headers
    - x-sportsbook-region: IL    (CloudFront 400s without it; error message
                                   helpfully reads "Missing x-sportsbook-region header")
    - Browser basics: User-Agent, Accept, Referer (set by BaseExtractor session
      defaults plus _API_HEADERS overrides below).

  Access key (_ak)
    - The _ak query param is a build-time constant baked into FanDuel's main JS
      bundle. The bundle filename is content-hashed and rotates on each deploy
      (e.g. main.87e84a562c7b85b8a306.js), so the value can change.
    - Discovery: fetch the HTML shell, locate the main.*.js script src, fetch
      that bundle, regex out the const string anchored on the SB_STUBS_REMOTE
      sentinel that immediately precedes it. See _discover_access_key().
    - We re-discover on every run rather than caching to avoid staleness bugs.

  Response shape (both endpoints)
    - { "layout": {...}, "attachments": { "eventTypes", "competitions",
                                           "events", "markets" } }
    - Events live in attachments.events, keyed by eventId. Each event has
      {eventId, name, competitionId, countryCode (tournament location),
       openDate (ISO UTC)}.
    - Markets live in attachments.markets, keyed by marketId. Each market has
      {marketId, eventId, marketName, marketType, marketStatus, inPlay,
       runners[]}.
    - Each runner has {selectionId, runnerName, result.type ("HOME"/"AWAY"),
      runnerStatus, winRunnerOdds.trueOdds.decimalOdds.decimalOdds}.

  Filtering rules
    - Singles only: skip events whose name contains " / " (doubles).
    - ATP / Challenger / Men's slams only via competition name regex
      (_INCLUDE_PATTERNS / _EXCLUDE_PATTERNS).
    - Skip outright/futures events: require " v " in event name. The first
      "event" in each competition-page response is the tournament winner
      futures and has the competition name verbatim.
    - Moneyline only: filter markets to marketName == "Moneyline".

  Event status derivation
    - FanDuel has no event-level status field. Derive from market.marketStatus
      and market.inPlay:
        OPEN + !inPlay  -> NOT_STARTED
        inPlay          -> IN_PLAY
        otherwise       -> FINISHED
"""


import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

SPORTSBOOK_HOME = "https://sportsbook.fanduel.com/"
FD_API_BASE = "https://api.sportsbook.fanduel.com/sbapi"
SPORT_PAGE_URL = f"{FD_API_BASE}/content-managed-page"
COMPETITION_PAGE_URL = f"{FD_API_BASE}/competition-page"

EVENT_TYPE_ID_TENNIS = 2
SPORTSBOOK_REGION = "IL"
TIMEZONE = "America/Chicago"

_API_HEADERS = {
    "Accept": "application/json",
    "Referer": "https://sportsbook.fanduel.com/",
    "Origin": "https://sportsbook.fanduel.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "x-sportsbook-region": SPORTSBOOK_REGION,
}

# Competition-name patterns. Tested against the SPORT page on 2026-04-08:
# include matches "ATP Monte Carlo 2026", "Monza Challenger 2026",
# "Men's Wimbledon 2026"; exclude rejects "ITF ...", "WTA ...", "Women's ...",
# "Mens UTR Pro Series Australia", "Billie Jean King Cup".
_INCLUDE_PATTERNS = [
    re.compile(r"^ATP\s", re.IGNORECASE),
    re.compile(r"\sChallenger\b", re.IGNORECASE),
    re.compile(r"^Men'?s\s+(Wimbledon|Australian Open|French Open|US Open)",
               re.IGNORECASE),
]
_EXCLUDE_PATTERNS = [
    re.compile(r"^ITF\b", re.IGNORECASE),
    re.compile(r"^WTA\b", re.IGNORECASE),
    re.compile(r"^Women'?s\b", re.IGNORECASE),
    re.compile(r"UTR Pro Series", re.IGNORECASE),
    re.compile(r"Billie Jean King Cup", re.IGNORECASE),
]

# main.*.js bundle filename pattern in the HTML shell
_MAIN_BUNDLE_RE = re.compile(r'src="(/static/js/main\.[a-f0-9]+\.js)"')

# Access key extraction patterns. Both target main.js. The primary anchors on
# the SB_STUBS_REMOTE constant that immediately precedes the const declaration;
# the fallback brackets the same const between SB_STUBS_REMOTE and the
# [a.FANDUEL] product key object that follows it. If FanDuel renames either
# sentinel string, this will fail loudly and we investigate.
_ACCESS_KEY_PATTERNS = [
    re.compile(
        r'SB_STUBS_REMOTE="SB_STUBS_REMOTE";const\s+[a-zA-Z_$]\w*'
        r'="([A-Za-z0-9]{12,24})"'
    ),
    re.compile(
        r'SB_STUBS_REMOTE.{0,80}const\s+[a-zA-Z_$]\w*="([A-Za-z0-9]{12,24})"'
        r'.{0,400}\[[a-zA-Z_$]\.FANDUEL\]'
    ),
]


@dataclass
class OddsEntry:
    book: str
    fd_event_id: str
    market: str
    fd_selection_id: str
    player_name: str
    country_code: str
    side: str
    odds: float
    points: float | None
    tournament: str
    fd_tournament_id: str
    opponent_name: str
    event_status: str
    fetched_at: datetime


def _is_atp_challenger(competition_name: str) -> bool:
    """Filter competition names to ATP / Challenger / men's slams."""
    name = (competition_name or "").strip()
    if not name:
        return False
    for pat in _EXCLUDE_PATTERNS:
        if pat.search(name):
            return False
    for pat in _INCLUDE_PATTERNS:
        if pat.search(name):
            return True
    return False


def _is_doubles(event_name: str) -> bool:
    """FanDuel formats doubles event names with ' / ' between partner names."""
    return " / " in (event_name or "")


def _is_outright(event_name: str, competition_name: str) -> bool:
    """The tournament-winner futures event has no ' v ' separator and its
    name typically matches the competition name verbatim."""
    name = (event_name or "").strip()
    if " v " not in name:
        return True
    if name == (competition_name or "").strip():
        return True
    return False


def _derive_event_status(market_status: str, in_play: bool) -> str:
    """Map FanDuel market state to the pipeline's event_status vocabulary."""
    if in_play:
        return "IN_PLAY"
    if (market_status or "").upper() == "OPEN":
        return "NOT_STARTED"
    return "FINISHED"


def _extract_access_key(bundle_js: str) -> str:
    """Regex out the _ak constant from FanDuel's main.js bundle."""
    for pat in _ACCESS_KEY_PATTERNS:
        m = pat.search(bundle_js)
        if m:
            return m.group(1)
    raise ValueError(
        "Could not extract FanDuel access key from main.js bundle. "
        "FanDuel may have refactored the source layout — investigate "
        "_ACCESS_KEY_PATTERNS in mvp/fanduel/odds.py."
    )


def _extract_competitions(sport_page_data: dict) -> list[dict]:
    """Pull tennis competitions from the SPORT page response, filtered to
    ATP / Challenger / men's slams."""
    competitions = sport_page_data.get("attachments", {}).get("competitions", {})
    out: list[dict] = []
    for comp_id, comp in competitions.items():
        if not isinstance(comp, dict):
            continue
        name = comp.get("name", "")
        if not _is_atp_challenger(name):
            continue
        out.append({
            "fd_competition_id": str(comp.get("competitionId") or comp_id),
            "name": name,
        })
    return out


def _parse_competition_response(
    data: dict,
    competition_name: str,
    fetched_at: datetime,
) -> list[OddsEntry]:
    """Parse one competition-page response into OddsEntry rows.

    Walks attachments.markets, joins each Moneyline market back to its event
    (for the event name + start time), and emits one OddsEntry per runner.
    """
    att = data.get("attachments", {}) or {}
    events = att.get("events", {}) or {}
    markets = att.get("markets", {}) or {}

    entries: list[OddsEntry] = []
    for market in markets.values():
        if not isinstance(market, dict):
            continue
        if market.get("marketName") != "Moneyline":
            continue

        event_id = str(market.get("eventId") or "")
        event = events.get(event_id) or events.get(int(event_id) if event_id.isdigit() else event_id) or {}
        event_name = (event.get("name") or "").strip()
        if _is_outright(event_name, competition_name):
            continue
        if _is_doubles(event_name):
            continue

        runners = market.get("runners") or []
        if len(runners) != 2:
            continue

        runner_names = [(r.get("runnerName") or "").strip() for r in runners]
        event_status = _derive_event_status(
            market.get("marketStatus", ""), bool(market.get("inPlay")),
        )
        competition_id = str(
            market.get("competitionId") or event.get("competitionId") or ""
        )

        for runner in runners:
            name = (runner.get("runnerName") or "").strip()
            if not name:
                continue

            win_odds = runner.get("winRunnerOdds") or {}
            true_odds = win_odds.get("trueOdds") or {}
            decimal_block = true_odds.get("decimalOdds") or {}
            odds_val = decimal_block.get("decimalOdds")
            if odds_val is None:
                continue
            try:
                odds_val = float(odds_val)
            except (TypeError, ValueError):
                continue

            side = (runner.get("result") or {}).get("type", "")
            opponent = next((n for n in runner_names if n != name), "")

            entries.append(OddsEntry(
                book="fd",
                fd_event_id=event_id,
                market="moneyline",
                fd_selection_id=str(runner.get("selectionId") or ""),
                player_name=name,
                country_code="",  # FanDuel doesn't expose runner country in payload
                side=side.lower(),
                odds=odds_val,
                points=None,
                tournament=competition_name,
                fd_tournament_id=competition_id,
                opponent_name=opponent,
                event_status=event_status,
                fetched_at=fetched_at,
            ))

    return entries


class FanDuelOddsScraper(BaseExtractor):
    """Scraper for FanDuel tennis odds."""

    def __init__(self, data_root=None, run_at=None):
        super().__init__(
            domain="fanduel",
            data_root=data_root,
            run_at=run_at,
            impersonate=None,
        )

    def _discover_access_key(self) -> str:
        """Fetch HTML shell, locate main.*.js, fetch it, extract the _ak const."""
        html = self.fetch_html(SPORTSBOOK_HOME)
        m = _MAIN_BUNDLE_RE.search(html)
        if not m:
            raise ValueError(
                f"Could not find main.*.js script tag in FanDuel HTML at "
                f"{SPORTSBOOK_HOME}"
            )
        bundle_url = "https://sportsbook.fanduel.com" + m.group(1)
        bundle_js = self.fetch_html(bundle_url)
        key = _extract_access_key(bundle_js)
        logger.info("Discovered FanDuel _ak from %s", bundle_url)
        return key

    def fetch_competitions(self, access_key: str) -> list[dict]:
        """Fetch SPORT page and return ATP/Challenger competitions."""
        params = {
            "page": "SPORT",
            "eventTypeId": EVENT_TYPE_ID_TENNIS,
            "_ak": access_key,
            "timezone": TIMEZONE,
        }
        resp = self.session.get(
            SPORT_PAGE_URL,
            params=params,
            headers=_API_HEADERS,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return _extract_competitions(resp.json())

    def fetch_competition_odds(
        self, access_key: str, fd_competition_id: str,
    ) -> dict:
        """Fetch competition-page for a single competition; returns raw JSON."""
        params = {
            "_ak": access_key,
            "eventTypeId": EVENT_TYPE_ID_TENNIS,
            "competitionId": fd_competition_id,
        }
        resp = self.session.get(
            COMPETITION_PAGE_URL,
            params=params,
            headers=_API_HEADERS,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_all_odds(self) -> tuple[list[OddsEntry], list[dict]]:
        """Discover competitions, fetch each, return (entries, raw responses)."""
        access_key = self._discover_access_key()
        competitions = self.fetch_competitions(access_key)
        logger.info("Found %d ATP/Challenger competitions on FD", len(competitions))

        all_entries: list[OddsEntry] = []
        raw_responses: list[dict] = []
        now = datetime.now(UTC)

        for comp in competitions:
            comp_id = comp["fd_competition_id"]
            comp_name = comp["name"]
            try:
                raw = self.fetch_competition_odds(access_key, comp_id)
                entries = _parse_competition_response(raw, comp_name, now)
                all_entries.extend(entries)
                raw_responses.append({"competition": comp, "response": raw})
                logger.info("  %s: %d entries", comp_name, len(entries))
            except Exception as e:
                logger.warning(
                    "Failed to fetch FD odds for %s (%s): %s",
                    comp_name, comp_id, e,
                )

        return all_entries, raw_responses

    def fetch_and_save_raw(self) -> int:
        """Fetch odds and write the combined raw JSON file."""
        entries, raw_responses = self.fetch_all_odds()
        if not entries:
            logger.info("No FD odds entries found")
            return 0
        raw_path = self.build_path(
            "raw", "moneyline", "odds.json", version="datetime",
        )
        self.save_json(raw_responses, raw_path)
        return len(entries)

    def stage(self) -> list[Path]:
        """Parse raw JSON snapshots that don't have staged counterparts."""
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
            try:
                file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")
            except ValueError:
                logger.warning("Cannot parse timestamp from %s", raw_path.name)
                continue

            all_entries: list[OddsEntry] = []
            for item in data_list:
                resp = item.get("response", item)
                comp = item.get("competition") or {}
                comp_name = comp.get("name") or ""
                all_entries.extend(
                    _parse_competition_response(resp, comp_name, file_ts)
                )
            if not all_entries:
                continue

            df = pl.DataFrame([
                {
                    "book": e.book,
                    "fd_event_id": e.fd_event_id,
                    "market": e.market,
                    "fd_selection_id": e.fd_selection_id,
                    "player_name": e.player_name,
                    "country_code": e.country_code,
                    "side": e.side,
                    "odds": e.odds,
                    "points": e.points,
                    "tournament": e.tournament,
                    "fd_tournament_id": e.fd_tournament_id,
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
            logger.info("FD staged %d new snapshots", len(staged))
        return staged

    def consolidate(self) -> Path | None:
        """Merge per-snapshot stage parquets into stage/fanduel/moneyline.parquet."""
        stage_dir = self.build_path("stage", "moneyline")
        snapshots = self.list_files(stage_dir, "*.parquet")
        if not snapshots:
            logger.info("No FD snapshots to consolidate")
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from collections import Counter
    scraper = FanDuelOddsScraper()
    entries, _ = scraper.fetch_all_odds()
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
