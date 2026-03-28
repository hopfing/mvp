"""Bet365 odds scraper for tennis markets via pipe-delimited API."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from urllib.parse import quote

import polars as pl

from mvp.common.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

UPCOMING_URL = "https://www.il.bet365.com/matchmarketscontentapi/upcomingmatches"
TENNIS_URL = "https://www.il.bet365.com/#/AC#B13#C1#D1002#G83#"

_API_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://www.il.bet365.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

_BASE_PARAMS = {
    "lid": "32",
    "zid": "0",
    "cid": "198",
    "cgid": "3",
    "ctid": "198",
    "csid": "28",
}

# pd parameter templates — J10 = ATP Tour, J12 = Challenger, F^24 = Next 24 Hours
_PD_ATP = "#AC#B13#C1#D1002#G83#J10#Q1#F^24#"
_PD_CHALLENGER = "#AC#B13#C1#D1002#G83#J12#Q1#F^24#"

_CIRCUITS = [
    ("atp", _PD_ATP),
    ("challenger", _PD_CHALLENGER),
]


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

    # State tracking
    current_tournament = ""
    # Match details keyed by PZ index
    matches_by_pz: dict[str, dict] = {}
    # Odds collection: list of dicts keyed by PZ
    gb_blocks: list[dict[str, str]] = []
    in_gb_block = False
    current_gb: dict[str, str] = {}

    for record in records:
        record = record.strip()
        if not record:
            continue

        rec_type, fields = _parse_record(record)

        if rec_type == "MG" and fields.get("SY") == "fk":
            # Tournament header
            current_tournament = fields.get("NA", "")

        elif rec_type == "PA" and fields.get("SY") == "ed":
            # Match detail record — skip doubles
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
            # Start of a new gb (gameboard) section — flush previous
            if in_gb_block and current_gb:
                gb_blocks.append(current_gb)
            in_gb_block = True
            current_gb = {}

        elif rec_type == "PA" and in_gb_block and "OD" in fields:
            # Odds record within a gb block
            pz = fields.get("PZ", "")
            od = fields.get("OD", "")
            if pz and od:
                current_gb[pz] = od

    # Flush last gb block
    if in_gb_block and current_gb:
        gb_blocks.append(current_gb)

    # Pair gb blocks: first = p1 odds, second = p2 odds
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


class Bet365OddsScraper(BaseExtractor):
    """Scraper for Bet365 tennis odds via pipe-delimited API."""

    def __init__(self, data_root=None):
        super().__init__(domain="bet365", data_root=data_root)

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

    def _warm_session(self) -> None:
        """Visit the tennis page to establish cookies needed for the API."""
        self._fetch(TENNIS_URL)

    def _fetch_circuit(self, pd_param: str) -> str:
        """Fetch upcoming matches for a given pd parameter. Returns raw text."""
        params = {**_BASE_PARAMS, "pd": pd_param}
        url = UPCOMING_URL + "?" + "&".join(
            f"{k}={quote(v, safe='')}" for k, v in params.items()
        )
        resp = self._fetch(url, headers=_API_HEADERS)
        return resp.text

    def fetch_all_odds(self) -> tuple[list[Bet365OddsEntry], list[str]]:
        """Fetch ATP + Challenger odds. Returns entries and raw responses."""
        all_entries: list[Bet365OddsEntry] = []
        raw_responses: list[str] = []
        now = datetime.now(UTC)

        self._warm_session()

        for circuit, pd_param in _CIRCUITS:
            try:
                raw = self._fetch_circuit(pd_param)
                raw_responses.append(raw)
                entries = _parse_pipe_response(raw, circuit, now)
                all_entries.extend(entries)
                logger.info(
                    "B365 %s: %d entries", circuit, len(entries),
                )
            except Exception as e:
                logger.error("B365 %s fetch failed: %s", circuit, e)

        logger.info(
            "B365 fetch complete: %d total entries", len(all_entries),
        )
        return all_entries, raw_responses

    def fetch_and_save(self) -> int:
        """Fetch odds, save raw text + stage parquet."""
        run_at = datetime.now(UTC)
        entries, raw_responses = self.fetch_all_odds()

        if not entries:
            logger.info("No B365 odds entries found")
            return 0

        # Save raw responses as text files
        for i, raw in enumerate(raw_responses):
            circuit = _CIRCUITS[i][0] if i < len(_CIRCUITS) else "unknown"
            raw_path = self.build_path(
                "raw", "moneyline", f"odds_{circuit}.txt", version="datetime",
            )
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(raw, encoding="utf-8")
            logger.info("Saved raw B365 %s response to %s", circuit, raw_path)

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
