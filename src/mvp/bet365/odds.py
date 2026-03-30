"""Bet365 odds scraper for tennis markets via pipe-delimited API.

Uses undetected-chromedriver with system Chrome to load the bet365 SPA,
then captures the pipe-delimited API responses from the performance log.
"""

import json
import logging
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from pathlib import Path

import polars as pl
import undetected_chromedriver as uc

from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)

SITE_URL = "https://www.il.bet365.com/"

# SPA URL template for navigation.
_URL_TEMPLATE = "https://www.il.bet365.com/#/AC/B13/C1/D1002/G83/J{j}/Q1/F%5E24/"

# Direct API URL template for fetch() calls from within the SPA.
_API_URL_TEMPLATE = (
    "https://www.il.bet365.com/matchmarketscontentapi/upcomingmatches"
    "?lid=32&zid=0"
    "&pd=%23AC%23B13%23C1%23D1002%23G83%23J{j}%23Q1%23F%5E24%23"
    "&cid=198&cgid=3&ctid=198&csid=28"
)

# Fallback J codes if dynamic discovery fails.
_FALLBACK_J_CODES = {"atp": "10", "challenger": "12"}

# Tours we care about — keys are matched case-insensitively against nav labels.
_TARGET_TOURS = {"ATP Tour": "atp", "Challenger Tour": "challenger"}


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


def _discover_j_codes(raw: str) -> dict[str, str]:
    """Parse the navigation sidebar to discover J codes for target tours.

    The nav section contains MA records after MG;SY=cm with entries like:
      MA;NA=ATP Tour;PD=#AC#B13#C1#D1002#G83#J10#Q1#F^24#;...
      MA;NA=Challenger Tour;PD=#AC#B13#C1#D1002#G83#J12#Q1#F^24#;...

    Returns dict mapping circuit key ("atp", "challenger") to J-code string.
    """
    result: dict[str, str] = {}
    in_nav = False

    for record in raw.split("|"):
        record = record.strip()
        if not record:
            continue

        if "MG" in record and "SY=cm" in record:
            in_nav = True
            continue

        if in_nav and record.startswith("MA;"):
            _, fields = _parse_record(record)
            name = fields.get("NA", "")
            pd = fields.get("PD", "")
            circuit_key = _TARGET_TOURS.get(name)
            if circuit_key and pd:
                j_match = re.search(r"J(\d+)", pd)
                if j_match:
                    result[circuit_key] = j_match.group(1)
        elif in_nav and not record.startswith("MA;"):
            in_nav = False

    return result


def _classify_circuit(tournament_name: str) -> str | None:
    """Classify tournament as atp/challenger or None (skip) from its name."""
    name = tournament_name.lower()
    if name.startswith("atp"):
        return "atp"
    if name.startswith("challenger"):
        return "challenger"
    # WTA, ITF, UTR, etc. — not our target
    return None


def _parse_pipe_response(
    raw: str,
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

    Only ATP and Challenger tournaments are kept; WTA and others are skipped.
    """
    records = raw.split("|")
    entries: list[Bet365OddsEntry] = []

    current_tournament = ""
    current_circuit: str | None = None
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
            current_circuit = _classify_circuit(current_tournament)

        elif rec_type == "PA" and "N2" in fields:
            if current_circuit is None:
                continue

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
                    "circuit": current_circuit,
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
                circuit=match_info["circuit"],
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
                circuit=match_info["circuit"],
                opponent_name=match_info["p1"],
                event_status="NOT_STARTED",
                fetched_at=fetched_at,
            ))

    return entries


def _extract_api_responses(driver, j_filter: str | None = None) -> str | None:
    """Extract a matchmarketscontentapi response from perf logs.

    Args:
        j_filter: If set, only match responses whose URL contains this J code
                  (e.g. "J12"). If None, returns the last response found.
    """
    logs = driver.get_log("performance")
    result: str | None = None

    for entry in logs:
        msg = json.loads(entry["message"])["message"]
        if msg["method"] != "Network.responseReceived":
            continue
        url = msg["params"]["response"]["url"]
        if "matchmarketscontentapi" not in url:
            continue
        if j_filter and f"J{j_filter}" not in url and f"%23J{j_filter}%23" not in url:
            continue
        try:
            body = driver.execute_cdp_cmd(
                "Network.getResponseBody",
                {"requestId": msg["params"]["requestId"]},
            )
            data = body.get("body", "")
            if data:
                result = data
        except Exception:
            pass

    return result


def _click_tour_tab(driver, tour_name: str) -> str | None:
    """Click a tour tab in the SPA and capture the resulting API response.

    Finds the tab element by its text content, clicks it (which fires
    the matchmarketscontentapi call), then captures the response matching
    the expected J code from perf logs.
    """
    # Drain any existing perf logs before clicking.
    try:
        driver.get_log("performance")
    except Exception:
        pass

    # Find and click the tab element by data-content attribute.
    # Use Selenium's native click (not JS .click()) so the SPA's event
    # handlers fire the same way as a real user click.
    from selenium.webdriver.common.by import By

    try:
        el = driver.find_element(
            By.CSS_SELECTOR, f'[data-content="{tour_name}"]',
        )
        el.click()
    except Exception as e:
        logger.warning("B365: could not find/click tab '%s': %s", tour_name, e)
        return None

    logger.info("B365: clicked '%s' tab, waiting for API response", tour_name)
    time.sleep(8)

    return _extract_api_responses(driver)


class Bet365OddsScraper(BaseJob):
    """Scraper for Bet365 tennis odds via undetected-chromedriver."""

    def __init__(self, data_root=None):
        super().__init__(domain="bet365", data_root=data_root)

    def fetch_all_odds(self) -> tuple[list[Bet365OddsEntry], list[tuple[str, str]]]:
        """Launch Chrome, navigate to tennis pages, capture API responses."""
        all_entries: list[Bet365OddsEntry] = []
        raw_responses: list[tuple[str, str]] = []  # (tab_name, raw_text)
        now = datetime.now(UTC)

        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        driver = None
        virtual_display = None
        try:
            # Start a virtual display if no DISPLAY is set (e.g. SSH session)
            if os.name != "nt" and not os.environ.get("DISPLAY"):
                try:
                    from pyvirtualdisplay import Display
                    virtual_display = Display(visible=False, size=(1920, 1080))
                    virtual_display.start()
                    logger.info("B365: started virtual display")
                except ImportError:
                    # Fall back to xvfb-run manually
                    os.environ["DISPLAY"] = ":99"
                    subprocess.Popen(
                        ["Xvfb", ":99", "-screen", "0", "1920x1080x24"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    )
                    time.sleep(1)

            # Detect installed Chrome major version
            _sp = subprocess
            try:
                _ver = _sp.check_output(
                    ["google-chrome", "--version"], text=True,
                ).strip().split()[-1]
                chrome_major = int(_ver.split(".")[0])
            except Exception:
                chrome_major = None
            driver = uc.Chrome(options=options, version_main=chrome_major)

            # Load SPA fully to establish session. Navigate to J10 via
            # the normal SPA route so all JS/session state initializes.
            driver.get(SITE_URL)
            time.sleep(5)
            driver.get(_URL_TEMPLATE.format(j="10"))
            time.sleep(12)

            # Capture ATP data from perf logs (the SPA-triggered API call).
            raw = _extract_api_responses(driver)

            seen_event_ids: set[str] = set()

            if raw:
                raw_responses.append(("atp", raw))
                entries = _parse_pipe_response(raw, now)
                new = [e for e in entries
                       if e.b365_event_id not in seen_event_ids]
                seen_event_ids.update(e.b365_event_id for e in new)
                all_entries.extend(new)
                logger.info(
                    "B365 atp (SPA): %d entries (%d new)",
                    len(entries), len(new),
                )
            else:
                logger.warning("B365: no ATP API response captured")

            # Click Challenger tab and capture its API response.
            try:
                raw = _click_tour_tab(driver, "Challenger Tour")
                if raw:
                    raw_responses.append(("challenger", raw))
                    entries = _parse_pipe_response(raw, now)
                    new = [e for e in entries
                           if e.b365_event_id not in seen_event_ids]
                    seen_event_ids.update(e.b365_event_id for e in new)
                    all_entries.extend(new)
                    logger.info(
                        "B365 challenger (click): %d entries (%d new)",
                        len(entries), len(new),
                    )
                else:
                    logger.warning(
                        "B365: no API response after clicking Challenger Tour",
                    )
            except Exception as e:
                logger.error("B365 challenger click failed: %s", e)

        except Exception as e:
            logger.error("B365 Chrome launch failed: %s", e)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
            if virtual_display:
                try:
                    virtual_display.stop()
                except Exception:
                    pass

        logger.info("B365 fetch complete: %d total entries", len(all_entries))
        return all_entries, raw_responses

    def fetch_and_save_raw(self) -> int:
        """Fetch odds from B365 and save raw text files.

        Returns number of entries fetched.
        """
        entries, raw_responses = self.fetch_all_odds()

        for tab, raw in raw_responses:
            raw_path = self.build_path(
                "raw", "moneyline", f"odds_{tab}.txt", version="datetime",
            )
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(raw, encoding="utf-8")
            logger.info("Saved raw B365 %s response to %s", tab, raw_path)

        return len(entries)

    def stage(self) -> list[Path]:
        """Parse raw files that don't have staged counterparts.

        Each raw .txt file produces a per-snapshot parquet in
        stage/bet365/moneyline/<stem>.parquet.
        Returns list of staged parquet paths written.
        """
        raw_dir = self.build_path("raw", "moneyline")
        stage_dir = self.build_path("stage", "moneyline")
        raw_files = self.list_files(raw_dir, "odds_*.txt")
        if not raw_files:
            return []

        existing = {p.stem for p in self.list_files(stage_dir, "*.parquet")}

        staged: list[Path] = []
        for raw_path in raw_files:
            if raw_path.stem in existing:
                continue

            try:
                raw_text = raw_path.read_text(encoding="utf-8")
            except Exception:
                logger.warning("Skipping unreadable raw file: %s", raw_path.name)
                continue

            fetched_at = datetime.now(UTC)  # approximate; close enough
            entries = _parse_pipe_response(raw_text, fetched_at)

            if not entries:
                continue

            # B365 raw files have tab in name: odds_j10_20260330_130003.txt
            # Extract the datetime portion
            run_at = datetime.now(UTC)
            try:
                stem = raw_path.stem
                # Strip "odds_" prefix and optional tab prefix (e.g. "j10_")
                ts_part = "_".join(stem.split("_")[-2:])
                run_at = datetime.strptime(ts_part, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
            except (ValueError, IndexError):
                pass

            df = pl.DataFrame([
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

            out_path = stage_dir / f"{raw_path.stem}.parquet"
            result = self.save_parquet(df, out_path)
            if result:
                staged.append(result)

        if staged:
            logger.info("B365 staged %d new snapshots", len(staged))
        return staged

    def consolidate(self) -> Path | None:
        """Merge all per-snapshot parquets into moneyline.parquet."""
        stage_dir = self.build_path("stage", "moneyline")
        snapshots = self.list_files(stage_dir, "*.parquet")
        if not snapshots:
            logger.info("No B365 snapshots to consolidate")
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


def fetch_and_save() -> int:
    """Full flow: fetch, stage, consolidate."""
    scraper = Bet365OddsScraper()
    return scraper.run()
