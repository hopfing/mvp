"""Caesars odds scraper for tennis markets.

Data source notes (validated from sample responses 2026-04-08):

  Endpoints
    - Top-level index:   https://api.americanwagering.com/regions/us/locations/il
                            /brands/czr/sb/v4/sports/tennis/tabs
    - Per-competition:   https://api.americanwagering.com/regions/us/locations/il
                            /brands/czr/sb/v4/sports/tennis/competitions/
                            {competitionId}/tabs

  Usage pattern
    - Top-level call is used ONLY to discover the list of eligible
      competitions. We then hit the per-competition endpoint for each, which
      is authoritative for its competition. This mirrors the other books and
      avoids trusting top-level completeness at scales (e.g. Grand Slams)
      that the sample did not cover.

  Required headers
    - Sample did not reveal exact header requirements. We start with
      BaseExtractor defaults plus Accept/Referer/Origin/Sec-Fetch-*
      listed in _API_HEADERS below. If Caesars returns 4xx, the error
      usually names the missing header (compare FanDuel's
      x-sportsbook-region case).

  Response shape (both endpoints)
    - { "sportId", "eventCount", "competitions": [
        { "id", "name", "collectionName", "events": [
          { "id", "name", "startTime", "type", "started", "active",
            "tradedInPlay", "keyMarketGroups": [
              { "markets": [ { "templateId", "placeholder",
                "tradedInPlay", "selections": [ { "id", "name", "type",
                "active", "price": { "d": <decimal> } } ] } ] } ],
            "metadata": { "homeTeamName", "awayTeamName" } } ] } ] }

  Filtering rules
    - Singles ATP + Challenger only, via collectionName membership in
      {"ATP", "Challenger"}. Cleaner than FanDuel's name regex because
      Caesars splits doubles and women into separate collectionName values.
    - Moneyline only, via market templateId in
      {"_7cMatch_20Betting_7c", "_7cMatch_20Betting_20Live_7c"}.
    - Skip events with type != "MATCH" (defensive against futures leakage).
    - Skip markets where placeholder == true or selections is empty.
    - Skip selections where price.d is missing or not castable to float.

  Names are pipe-wrapped everywhere (|Carlos Alcaraz|, |Match Betting|,
  |Over|). _strip_pipes() removes the wrapping.

  Event status derivation
    - Caesars exposes event.started + event.active + per-market
      tradedInPlay. Map:
        any market.templateId == "_7cMatch_20Betting_20Live_7c"
          OR market.tradedInPlay == true            -> IN_PLAY
        elif event.started == false and event.active  -> NOT_STARTED
        else                                          -> FINISHED
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

CZR_API_BASE = (
    "https://api.americanwagering.com/regions/us/locations/il"
    "/brands/czr/sb/v4/sports/tennis"
)
TABS_URL = f"{CZR_API_BASE}/tabs"
COMPETITION_URL_TEMPLATE = f"{CZR_API_BASE}/competitions/{{competition_id}}/tabs"

_API_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Referer": "https://sportsbook.caesars.com/",
    "Origin": "https://sportsbook.caesars.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "x-app-version": "7.44.0",
    "x-platform": "cordova-desktop",
    "x-unique-device-id": "b8dd4d8c-a99a-4244-a0cc-c260b12b208d",
}

_INCLUDED_COLLECTIONS = frozenset({"ATP", "Challenger"})

_MONEYLINE_TEMPLATE_IDS = frozenset({
    "_7cMatch_20Betting_7c",        # pre-match moneyline
    "_7cMatch_20Betting_20Live_7c", # in-play moneyline
})

_IN_PLAY_TEMPLATE_ID = "_7cMatch_20Betting_20Live_7c"


@dataclass
class OddsEntry:
    book: str
    czr_event_id: str
    market: str
    czr_selection_id: str
    player_name: str
    country_code: str
    side: str
    odds: float
    points: float | None
    tournament: str
    czr_tournament_id: str
    opponent_name: str
    event_status: str
    fetched_at: datetime


def _strip_pipes(name: str | None) -> str:
    """Strip the leading and trailing `|` characters that Caesars wraps
    around every name field. Internal pipes (pathological) are preserved."""
    if not name:
        return ""
    s = name
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|"):
        s = s[:-1]
    return s


def _is_atp_or_challenger(collection_name: str | None) -> bool:
    """Return True if this Caesars collectionName is ATP or Challenger
    men's singles. Doubles collections (`ATP Doubles`, `Challenger Doubles`)
    and women's (`WTA`, `WTA Doubles`, `UTR`) are excluded.

    Grand Slam handling: slams were not present in the sample used to
    design this scraper. If the first post-ship slam does not appear
    under `ATP`, this filter will need extending — the zero-eligible
    warning in fetch_competitions() is the canary for that case.
    """
    return collection_name in _INCLUDED_COLLECTIONS


def _extract_competitions(data: dict) -> list[dict]:
    """Walk a top-level tennis/tabs response and return eligible
    competitions as dicts with stable keys. Used for competition
    discovery only — the per-competition endpoint is authoritative
    for actual odds.
    """
    competitions = (data or {}).get("competitions") or []
    out: list[dict] = []
    for comp in competitions:
        if not isinstance(comp, dict):
            continue
        collection_name = comp.get("collectionName")
        if not _is_atp_or_challenger(collection_name):
            continue
        comp_id = str(comp.get("id") or "")
        if not comp_id:
            continue
        out.append({
            "czr_competition_id": comp_id,
            "name": comp.get("name") or "",
            "collection_name": collection_name,
        })
    return out


def _derive_event_status(event: dict, markets: list[dict]) -> str:
    """Map Caesars event + market flags to the pipeline's event_status
    vocabulary (NOT_STARTED / IN_PLAY / FINISHED).

    Precedence:
      1. Any moneyline market with the in-play template or tradedInPlay
         set → IN_PLAY.
      2. Event not started and still active → NOT_STARTED.
      3. Everything else → FINISHED.
    """
    for market in markets:
        if not isinstance(market, dict):
            continue
        if market.get("templateId") == _IN_PLAY_TEMPLATE_ID:
            return "IN_PLAY"
        if market.get("tradedInPlay") is True:
            return "IN_PLAY"

    started = bool(event.get("started"))
    active = bool(event.get("active"))
    if not started and active:
        return "NOT_STARTED"
    return "FINISHED"


def _parse_competition_response(
    data: dict,
    fetched_at: datetime,
) -> list[OddsEntry]:
    """Walk a per-competition tennis/competitions/{id}/tabs response
    and return OddsEntry rows for every eligible moneyline selection.

    Walks competitions[].events[].keyMarketGroups[].markets[], applies
    the moneyline template filter, skips placeholders, and emits one
    OddsEntry per runner (two per event). Any selection with a
    malformed price drops the whole pair — cleaner than emitting a
    half-populated row whose opponent_name would be empty.
    """
    entries: list[OddsEntry] = []
    competitions = (data or {}).get("competitions") or []

    for comp in competitions:
        if not isinstance(comp, dict):
            continue
        comp_id = str(comp.get("id") or "")
        comp_name = comp.get("name") or ""

        for event in comp.get("events") or []:
            if not isinstance(event, dict):
                continue
            if event.get("type") != "MATCH":
                continue

            event_id = str(event.get("id") or "")
            if not event_id:
                continue

            # Flatten markets across all keyMarketGroups for this event.
            all_markets: list[dict] = []
            for group in event.get("keyMarketGroups") or []:
                if not isinstance(group, dict):
                    continue
                for market in group.get("markets") or []:
                    if isinstance(market, dict):
                        all_markets.append(market)

            moneyline_markets = [
                m for m in all_markets
                if m.get("templateId") in _MONEYLINE_TEMPLATE_IDS
                and not m.get("placeholder", False)
                and isinstance(m.get("selections"), list)
                and len(m.get("selections") or []) >= 2
            ]
            if not moneyline_markets:
                continue

            event_status = _derive_event_status(event, moneyline_markets)

            # Prefer pre-match moneyline if both pre-match and in-play
            # are present on the same event (sample never showed this,
            # but defensive). Take the first qualifying market.
            market = moneyline_markets[0]
            selections = market.get("selections") or []
            if len(selections) != 2:
                # More than 2 selections on a moneyline shouldn't
                # happen; bail rather than guess.
                continue

            # Decode both selection prices up front; if either fails,
            # drop the whole pair.
            parsed_selections: list[dict] = []
            bad = False
            for sel in selections:
                if not isinstance(sel, dict):
                    bad = True
                    break
                price_d = (sel.get("price") or {}).get("d")
                try:
                    odds_val = float(price_d)
                except (TypeError, ValueError):
                    bad = True
                    break
                parsed_selections.append({
                    "id": str(sel.get("id") or ""),
                    "name": _strip_pipes(sel.get("name")),
                    "type": (sel.get("type") or "").lower(),
                    "odds": odds_val,
                })
            if bad:
                logger.debug(
                    "CZR: dropping event %s due to malformed price", event_id,
                )
                continue

            runner_names = [s["name"] for s in parsed_selections]
            for sel in parsed_selections:
                opponent = next(
                    (n for n in runner_names if n != sel["name"]), "",
                )
                entries.append(OddsEntry(
                    book="czr",
                    czr_event_id=event_id,
                    market="moneyline",
                    czr_selection_id=sel["id"],
                    player_name=sel["name"],
                    country_code="",
                    side=sel["type"],
                    odds=sel["odds"],
                    points=None,
                    tournament=comp_name,
                    czr_tournament_id=comp_id,
                    opponent_name=opponent,
                    event_status=event_status,
                    fetched_at=fetched_at,
                ))

    return entries


SPORTSBOOK_URL = "https://sportsbook.caesars.com/us/il/bet/tennis"


class CaesarsOddsScraper(BaseExtractor):
    """Scraper for Caesars tennis odds."""

    def __init__(self, data_root=None, run_at=None):
        super().__init__(domain="caesars", data_root=data_root, run_at=run_at)
        self._waf_resolved = False

    def _acquire_waf_session(self) -> None:
        """Use undetected-chromedriver to solve the AWS WAF challenge
        and transfer the WAF token + cookies to our requests session.

        Caesars uses AWS WAF Bot Control which requires a JavaScript
        challenge. We follow the same approach as Bet365
        (src/mvp/bet365/odds.py) — launch Chrome via
        undetected-chromedriver, let it load the sportsbook page
        (solving the WAF challenge), then extract the WAF token from
        the performance logs and cookies from the browser session.
        """
        import undetected_chromedriver as uc

        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        driver = None
        waf_token = None
        try:
            # Detect Chrome major version (B365 pattern, extended for Windows)
            import subprocess as _sp
            chrome_major = None
            try:
                if os.name == "nt":
                    _out = _sp.check_output(
                        'reg query "HKEY_CURRENT_USER\\Software\\Google\\Chrome\\BLBeacon" /v version',
                        shell=True, text=True,
                    )
                    chrome_major = int(_out.strip().split()[-1].split(".")[0])
                else:
                    _ver = _sp.check_output(
                        ["google-chrome", "--version"], text=True,
                    ).strip().split()[-1]
                    chrome_major = int(_ver.split(".")[0])
            except Exception:
                pass

            logger.info("CZR: launching Chrome (version=%s) to solve WAF challenge", chrome_major)
            driver = uc.Chrome(options=options, version_main=chrome_major)
            driver.get(SPORTSBOOK_URL)
            time.sleep(12)

            # Extract WAF token from performance logs — look for
            # requests to api.americanwagering.com that carry the token.
            for entry in driver.get_log("performance"):
                try:
                    msg = json.loads(entry["message"])["message"]
                    if msg["method"] != "Network.requestWillBeSent":
                        continue
                    url = msg["params"]["request"]["url"]
                    if "api.americanwagering.com" not in url:
                        continue
                    headers = msg["params"]["request"]["headers"]
                    token = headers.get("x-aws-waf-token")
                    if token:
                        waf_token = token
                        break
                except (KeyError, json.JSONDecodeError):
                    continue

            # Transfer cookies to requests session
            for cookie in driver.get_cookies():
                self.session.cookies.set(
                    cookie["name"],
                    cookie["value"],
                    domain=cookie.get("domain", ""),
                    path=cookie.get("path", "/"),
                )

        except Exception as e:
            logger.error("CZR: Chrome WAF acquisition failed: %s", e)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        if waf_token:
            self.session.headers["x-aws-waf-token"] = waf_token
            logger.info("CZR: acquired WAF token from Chrome")
        else:
            logger.warning("CZR: no WAF token captured — API calls may fail")

        self._waf_resolved = True

    def fetch_competitions(self) -> list[dict]:
        """Hit the top-level tabs endpoint and return filtered
        competition list. Discovery-only — does not use the inline
        event data."""
        if not self._waf_resolved:
            self._acquire_waf_session()

        resp = self.session.get(
            TABS_URL,
            headers=_API_HEADERS,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        competitions = _extract_competitions(data)
        if not competitions:
            seen = [
                c.get("collectionName") for c in (data or {}).get("competitions") or []
                if isinstance(c, dict)
            ]
            logger.warning(
                "CZR: zero eligible competitions after filter; "
                "collectionNames actually returned: %s",
                seen,
            )
        logger.info("CZR: discovered %d eligible competitions", len(competitions))
        return competitions

    def fetch_competition_odds(self, competition_id: str) -> dict:
        """Hit the per-competition tabs endpoint; returns raw JSON dict.
        This is the authoritative source for odds — the top-level
        tabs endpoint is only used for discovery."""
        url = COMPETITION_URL_TEMPLATE.format(competition_id=competition_id)
        resp = self.session.get(
            url,
            headers=_API_HEADERS,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_all_odds(self) -> tuple[list[OddsEntry], list[dict]]:
        """Full flow: discover competitions, fetch each, parse each.
        Returns (all parsed entries, raw per-competition responses).

        One failing competition is logged and skipped — it does not
        kill the whole run (matches FanDuel behavior).
        """
        competitions = self.fetch_competitions()
        all_entries: list[OddsEntry] = []
        raw_responses: list[dict] = []
        now = datetime.now(UTC)

        for comp in competitions:
            comp_id = comp["czr_competition_id"]
            comp_name = comp["name"]
            try:
                raw = self.fetch_competition_odds(comp_id)
                entries = _parse_competition_response(raw, now)
                # Canary: we had a non-empty raw events list but parsed
                # zero entries — log a warning, probably a parser bug.
                raw_events = raw.get("competitions") or []
                first = raw_events[0] if raw_events else {}
                had_raw_events = bool(first.get("events"))
                if had_raw_events and not entries:
                    logger.warning(
                        "CZR: %s raw response had events but parser emitted "
                        "zero rows — parser filter may be too aggressive",
                        comp_name,
                    )
                all_entries.extend(entries)
                raw_responses.append({"competition": comp, "response": raw})
                logger.info("  %s: %d entries", comp_name, len(entries))
            except Exception as e:
                logger.warning(
                    "Failed to fetch CZR odds for %s (%s): %s",
                    comp_name, comp_id, e,
                )

        return all_entries, raw_responses

    def fetch_and_save_raw(self) -> int:
        """Fetch odds and write the combined raw JSON file.

        The raw payload is a list of {competition, response} dicts,
        one per successfully fetched competition. This preserves full
        market data (moneyline AND spread/totals etc.) for future
        non-moneyline work without re-scraping.
        """
        entries, raw_responses = self.fetch_all_odds()
        if not entries:
            logger.info("No CZR odds entries found")
            return 0
        raw_path = self.build_path(
            "raw", "moneyline", "odds.json", version="datetime",
        )
        self.save_json(raw_responses, raw_path)
        return len(entries)

    def stage(self) -> list[Path]:
        """Parse any raw JSON snapshots that don't have staged
        counterparts. Matches FanDuel's stage() shape."""
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
                logger.warning("Skipping corrupt CZR raw file: %s", raw_path.name)
                continue

            parts = raw_path.stem.replace("odds_", "")
            try:
                file_ts = datetime.strptime(parts, "%Y%m%d_%H%M%S")
            except ValueError:
                logger.warning("Cannot parse timestamp from %s", raw_path.name)
                continue

            all_entries: list[OddsEntry] = []
            for item in data_list:
                resp = item.get("response", item) if isinstance(item, dict) else item
                all_entries.extend(
                    _parse_competition_response(resp, file_ts)
                )
            if not all_entries:
                continue

            df = pl.DataFrame([
                {
                    "book": e.book,
                    "czr_event_id": e.czr_event_id,
                    "market": e.market,
                    "czr_selection_id": e.czr_selection_id,
                    "player_name": e.player_name,
                    "country_code": e.country_code,
                    "side": e.side,
                    "odds": e.odds,
                    "points": e.points,
                    "tournament": e.tournament,
                    "czr_tournament_id": e.czr_tournament_id,
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
            logger.info("CZR staged %d new snapshots", len(staged))
        return staged

    def consolidate(self) -> Path | None:
        """Merge per-snapshot stage parquets into
        stage/caesars/moneyline.parquet."""
        stage_dir = self.build_path("stage", "moneyline")
        snapshots = self.list_files(stage_dir, "*.parquet")
        if not snapshots:
            logger.info("No CZR snapshots to consolidate")
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



def _summarize_raw_file(raw_path: Path) -> None:
    """Read the most recently written raw JSON and print a human-readable
    summary to stdout. Used by the __main__ verification block to make
    the scraper output cross-checkable against the live Caesars site."""
    import sys

    with raw_path.open() as f:
        raw_responses = json.load(f)

    print(f"\nRaw file: {raw_path}", file=sys.stdout)
    print(f"Competitions fetched: {len(raw_responses)}", file=sys.stdout)
    print("", file=sys.stdout)

    total_raw_events = 0
    total_parsed_entries = 0
    skip_reasons: dict[str, int] = {
        "non_match_type": 0,
        "no_moneyline_market": 0,
        "placeholder_market": 0,
        "fewer_than_two_selections": 0,
        "malformed_price": 0,
    }
    sample_names: list[str] = []

    print(f"{'Competition':<45} {'Collection':<20} {'Raw':>6} {'Parsed':>7}",
          file=sys.stdout)
    print("-" * 80, file=sys.stdout)

    for item in raw_responses:
        comp = item.get("competition") or {}
        resp = item.get("response") or {}
        comp_name = comp.get("name", "")
        collection = comp.get("collection_name", "")

        raw_events = 0
        for c in resp.get("competitions") or []:
            raw_events += len(c.get("events") or [])
        total_raw_events += raw_events

        parsed = _parse_competition_response(resp, datetime.now(UTC))
        total_parsed_entries += len(parsed)
        for e in parsed[:2]:
            if len(sample_names) < 6 and e.player_name not in sample_names:
                sample_names.append(e.player_name)

        print(f"{comp_name:<45} {collection:<20} {raw_events:>6} {len(parsed):>7}",
              file=sys.stdout)

    print("-" * 80, file=sys.stdout)
    print(f"{'TOTAL':<45} {'':<20} {total_raw_events:>6} {total_parsed_entries:>7}",
          file=sys.stdout)
    print("", file=sys.stdout)
    print(f"Sample pipe-stripped names: {sample_names}", file=sys.stdout)
    print("", file=sys.stdout)
    print("Cross-check: open caesars.com tennis page and confirm the",
          file=sys.stdout)
    print("per-competition 'Raw' counts match what the site shows live.",
          file=sys.stdout)


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    scraper = CaesarsOddsScraper()
    try:
        n = scraper.fetch_and_save_raw()
    except Exception as e:
        print(f"FAILED: {e}", file=sys.stderr)
        sys.exit(1)

    if n == 0:
        print("fetch_and_save_raw returned 0 entries — nothing to summarize",
              file=sys.stderr)
        sys.exit(1)

    # Find the raw file we just wrote (newest odds_*.json in raw dir).
    raw_dir = scraper.build_path("raw", "moneyline")
    raw_files = sorted(
        scraper.list_files(raw_dir, "odds_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not raw_files:
        print("No raw files found after fetch — something is wrong",
              file=sys.stderr)
        sys.exit(1)

    _summarize_raw_file(raw_files[0])
    sys.exit(0)
