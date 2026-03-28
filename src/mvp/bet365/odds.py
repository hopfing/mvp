"""Bet365 odds scraper for tennis markets via pipe-delimited API.

Uses undetected-chromedriver with system Chrome to load the bet365 SPA,
then captures the pipe-delimited API responses from the performance log.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction

import polars as pl
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)

SITE_URL = "https://www.il.bet365.com/"

# SPA URLs — navigate here to trigger the matchmarketscontentapi calls
_CIRCUIT_URLS = {
    "atp": "https://www.il.bet365.com/#/AC/B13/C1/D1002/G83/J10/Q1/F%5E24/",
    "challenger": "https://www.il.bet365.com/#/AC/B13/C1/D1002/G83/J12/Q1/F%5E24/",
}


def _frac_to_decimal(frac_str: str) -> float:
    """Convert fractional odds (e.g. '8/11') to decimal.

    Fractional odds represent profit-to-stake, so decimal = fraction + 1.
    Examples: '1/1' -> 2.0, '4/6' -> 1.667, '8/11' -> 1.727
    """
    return float(Fraction(frac_str)) + 1.0


def _parse_record(record: str) -> tuple[str, dict[str, str]]:
    """Parse a single pipe-delimited record into (type, fields).

    Records look like: 'PA;ID=123;NA=Foo;OD=1/2'
    Returns ('PA', {'ID': '123', 'NA': 'Foo', 'OD': '1/2'})
    """
    parts = record.split(";")
    rec_type = parts[0] if parts else ""
    fields: dict[str, str] = {}
    for part in parts[1:]:
        eq = part.find("=")
        if eq > 0:
            fields[part[:eq]] = part[eq + 1:]
    return rec_type, fields


@dataclass
class Bet365OddsEntry:
    book: str
    b365_event_id: str
    market: str
    player_name: str
    odds: float
    tournament: str
    circuit: str
    opponent_name: str
    event_status: str
    fetched_at: datetime


def _parse_pipe_response(
    raw: str,
    circuit: str,
    fetched_at: datetime,
) -> list[Bet365OddsEntry]:
    """Parse Bet365's pipe-delimited response into odds entries.

    The format uses | as record separator and ; as field separator.
    Key record types:
      MG (SY=fk) — tournament header: NA=name, L3=round
      PA (SY=ed) — match details: NA=p1, N2=p2, FI=event_id, BC=start_time
      PA (SY=gb) — odds: OD=fractional, FI=event_id, PZ=position_index

    Odds come in pairs of gb blocks: first block = p1 odds, second = p2 odds,
    matched by PZ index.
    """
    records = raw.split("|")
    entries: list[Bet365OddsEntry] = []

    current_tournament = ""
    matches_by_pz: dict[str, dict] = {}
    gb_blocks: list[dict[str, str]] = []
    in_gb_block = False
    current_gb: dict[str, str] = {}

    for record in records:
        record = record.strip()
        if not record:
            continue

        rec_type, fields = _parse_record(record)

        if rec_type == "MG" and fields.get("SY") == "fk":
            current_tournament = fields.get("NA", "")

        elif rec_type == "PA" and "N2" in fields:
            p1 = fields.get("NA", "")
            p2 = fields.get("N2", "")
            if "/" in p1 or "/" in p2:
                continue

            pz = fields.get("PZ", "")
            fi = fields.get("FI", "")
            if pz and fi:
                matches_by_pz[pz] = {
                    "p1": p1,
                    "p2": p2,
                    "fi": fi,
                    "tournament": current_tournament,
                }

        elif rec_type == "MA" and fields.get("SY") == "gb":
            if in_gb_block and current_gb:
                gb_blocks.append(current_gb)
            in_gb_block = True
            current_gb = {}

        elif rec_type == "PA" and in_gb_block and "OD" in fields:
            pz = fields.get("PZ", "")
            od = fields.get("OD", "")
            if pz and od:
                current_gb[pz] = od

    if in_gb_block and current_gb:
        gb_blocks.append(current_gb)

    for i in range(0, len(gb_blocks) - 1, 2):
        p1_odds_block = gb_blocks[i]
        p2_odds_block = gb_blocks[i + 1]

        for pz, match_info in matches_by_pz.items():
            p1_frac = p1_odds_block.get(pz)
            p2_frac = p2_odds_block.get(pz)

            if not p1_frac or not p2_frac:
                continue

            try:
                p1_dec = _frac_to_decimal(p1_frac)
                p2_dec = _frac_to_decimal(p2_frac)
            except (ValueError, ZeroDivisionError):
                logger.warning(
                    "Bad odds %s/%s for %s vs %s",
                    p1_frac, p2_frac, match_info["p1"], match_info["p2"],
                )
                continue

            event_id = match_info["fi"]
            entries.append(Bet365OddsEntry(
                book="b365",
                b365_event_id=event_id,
                market="moneyline",
                player_name=match_info["p1"],
                odds=p1_dec,
                tournament=match_info["tournament"],
                circuit=circuit,
                opponent_name=match_info["p2"],
                event_status="NOT_STARTED",
                fetched_at=fetched_at,
            ))
            entries.append(Bet365OddsEntry(
                book="b365",
                b365_event_id=event_id,
                market="moneyline",
                player_name=match_info["p2"],
                odds=p2_dec,
                tournament=match_info["tournament"],
                circuit=circuit,
                opponent_name=match_info["p1"],
                event_status="NOT_STARTED",
                fetched_at=fetched_at,
            ))

    return entries


def _extract_api_responses(driver, circuit_key: str) -> str | None:
    """Extract matchmarketscontentapi response for a circuit from perf logs."""
    logs = driver.get_log("performance")
    circuit_markers = {"atp": "J10", "challenger": "J12"}
    marker = circuit_markers.get(circuit_key, "")

    api_urls = []
    for entry in logs:
        msg = json.loads(entry["message"])["message"]
        if msg["method"] != "Network.responseReceived":
            continue
        url = msg["params"]["response"]["url"]
        if "contentapi" in url:
            api_urls.append(url[:100])
        if "matchmarketscontentapi" not in url:
            continue
        if marker and marker not in url:
            continue
        rid = msg["params"]["requestId"]
        status = msg["params"]["response"]["status"]
        print(f"[B365] {circuit_key}: found matchmarketscontentapi (status={status}, rid={rid})")
        try:
            body = driver.execute_cdp_cmd(
                "Network.getResponseBody", {"requestId": rid},
            )
            data = body.get("body", "")
            print(f"[B365] {circuit_key}: body={len(data)} chars")
            if data:
                return data
        except Exception as e:
            print(f"[B365] {circuit_key}: getResponseBody failed: {e}")

    print(f"[B365] {circuit_key}: perf log had {len(logs)} entries, contentapi URLs: {api_urls}")
    return None


class Bet365OddsScraper(BaseJob):
    """Scraper for Bet365 tennis odds via undetected-chromedriver."""

    def __init__(self, data_root=None):
        super().__init__(domain="bet365", data_root=data_root)

    def fetch_all_odds(self) -> tuple[list[Bet365OddsEntry], list[str]]:
        """Launch Chrome, navigate to tennis pages, capture API responses."""
        all_entries: list[Bet365OddsEntry] = []
        raw_responses: list[str] = []
        now = datetime.now(UTC)

        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        driver = None
        try:
            # Detect installed Chrome major version
            import subprocess as _sp
            try:
                _ver = _sp.check_output(
                    ["google-chrome", "--version"], text=True,
                ).strip().split()[-1]
                chrome_major = int(_ver.split(".")[0])
            except Exception:
                chrome_major = None
            print(f"[B365] Chrome version: {chrome_major}")
            driver = uc.Chrome(options=options, version_main=chrome_major)
            print(f"[B365] Chrome launched")

            # Load homepage and dismiss cookie consent
            driver.get(SITE_URL)
            import time
            time.sleep(5)
            print(f"[B365] Homepage: {driver.current_url}")

            # Skip cookie consent — clicking it resets the SPA on some systems.
            # The SPA loads content behind the overlay regardless.

            # Navigate to each circuit and capture responses
            for circuit, url in _CIRCUIT_URLS.items():
                try:
                    driver.get(url)
                    time.sleep(12)
                    print(f"[B365] {circuit}: {driver.current_url}")

                    raw = _extract_api_responses(driver, circuit)
                    if raw:
                        raw_responses.append(raw)
                        print(f"[B365] {circuit}: raw preview: {raw[:300]}")
                        entries = _parse_pipe_response(raw, circuit, now)
                        all_entries.extend(entries)
                        print(f"[B365] {circuit}: parsed {len(entries)} entries")
                        logger.info("B365 %s: %d entries", circuit, len(entries))
                    else:
                        logger.warning("B365 %s: no API response captured", circuit)
                except Exception as e:
                    logger.error("B365 %s failed: %s", circuit, e)

        except Exception as e:
            logger.error("B365 Chrome launch failed: %s", e)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        logger.info("B365 fetch complete: %d total entries", len(all_entries))
        return all_entries, raw_responses

    def fetch_and_save(self) -> int:
        """Fetch odds, save raw text + stage parquet."""
        run_at = datetime.now(UTC)
        entries, raw_responses = self.fetch_all_odds()

        # Always save raw responses for debugging, even if parser finds 0 entries
        circuits = list(_CIRCUIT_URLS.keys())
        for i, raw in enumerate(raw_responses):
            circuit = circuits[i] if i < len(circuits) else "unknown"
            raw_path = self.build_path(
                "raw", "moneyline", f"odds_{circuit}.txt", version="datetime",
            )
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(raw, encoding="utf-8")
            logger.info("Saved raw B365 %s response to %s", circuit, raw_path)

        if not entries:
            logger.info("No B365 odds entries found")
            return 0

        stage_path = self.build_path("stage", "moneyline.parquet")
        new_df = pl.DataFrame([
            {
                "book": e.book,
                "b365_event_id": e.b365_event_id,
                "market": e.market,
                "player_name": e.player_name,
                "odds": e.odds,
                "tournament": e.tournament,
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


def fetch_and_save() -> int:
    """Full flow: fetch odds, save raw + stage parquet."""
    scraper = Bet365OddsScraper()
    return scraper.fetch_and_save()
