"""Transform raw schedule HTML into staged parquet via ScheduleRecord schema."""

import datetime as dt
import logging
import re
from datetime import date, datetime
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup, Tag

from mvp.atptour.schemas.schedule import ScheduleRecord
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob
from mvp.common.enums import Circuit
from mvp.common.utils import polars_schema_overrides

logger = logging.getLogger(__name__)


def _parse_snapshot_timestamp(stem: str) -> datetime:
    """Parse 'schedule_YYYYMMDD_HHMMSS' into datetime."""
    parts = stem.split("_", 1)
    return datetime.strptime(parts[1], "%Y%m%d_%H%M%S")


def _normalize_score(score_span: Tag) -> str | None:
    """Convert HTML score to normalized string.

    '76<sup>6</sup> 61' becomes '76(6) 61'.
    All-ndash or empty text returns None.
    """
    if score_span is None:
        return None

    # Replace <sup> tags with parenthesized content
    for sup in score_span.find_all("sup"):
        sup.replace_with(f"({sup.get_text()})")

    text = score_span.get_text()
    # Normalize whitespace: collapse runs of whitespace to single space, strip edges
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None

    # Check if all characters are ndash
    if all(c == "\u2013" for c in text):
        return None

    return text


def _extract_player_info(div: Tag) -> tuple[str, str, str, str | None]:
    """Extract player ID, name, country, seed/entry from a player/opponent div.

    Returns (player_id, display_name, country_code, seed_entry).
    For doubles: multiple names joined with ' / ', first player's ID, first country.
    """
    # Check for doubles structure (multiple names in a "names" div)
    names_div = div.select_one("div.names")
    if names_div:
        # Doubles
        name_links = names_div.select("div.name a")
        names = []
        first_id = None
        for link in name_links:
            name_text = link.get_text(separator=" ", strip=True)
            names.append(name_text)
            if first_id is None:
                href = link.get("href", "")
                parts = href.strip("/").split("/")
                first_id = parts[-2] if len(parts) >= 2 else ""
        display_name = " / ".join(names)

        # Country: first flag in countries div
        countries_div = div.select_one("div.countries")
        if countries_div:
            flag_use = countries_div.select_one("svg use")
            flag_href = flag_use["href"] if flag_use else ""
        else:
            flag_use = div.select_one("svg use")
            flag_href = flag_use["href"] if flag_use else ""
        country = flag_href.split("#flag-")[-1] if "#flag-" in flag_href else ""

        # Seed/entry from rank span (in "players" div)
        rank_span = div.select_one("div.players div.rank span")
        seed_entry = rank_span.get_text(strip=True) if rank_span else ""
        seed_entry = seed_entry if seed_entry else None

        return first_id, display_name, country, seed_entry

    # Singles
    name_div = div.select_one("div.name")
    link = name_div.select_one("a") if name_div else None
    if link:
        name_text = link.get_text(separator=" ", strip=True)
        href = link.get("href", "")
        parts = href.strip("/").split("/")
        player_id = parts[-2] if len(parts) >= 2 else ""
    else:
        name_text = ""
        player_id = ""

    # Country from flag svg
    flag_use = div.select_one("svg use")
    flag_href = flag_use["href"] if flag_use else ""
    country = flag_href.split("#flag-")[-1] if "#flag-" in flag_href else ""

    # Seed/entry
    rank_span = div.select_one("div.rank span")
    seed_entry = rank_span.get_text(strip=True) if rank_span else ""
    seed_entry = seed_entry if seed_entry else None

    return player_id, name_text, country, seed_entry


def _parse_schedule_html(
    html: str,
    tournament_id: str,
    year: int,
    circuit: "Circuit",
    snapshot_timestamp: datetime,
    source_file: str,
    parsed_at: datetime,
) -> list[ScheduleRecord]:
    """Parse schedule HTML into list of ScheduleRecord."""
    soup = BeautifulSoup(html, "lxml")
    records = []

    for match_div in soup.select("div.schedule[data-matchdate]"):
        # Match date
        match_date_str = match_div["data-matchdate"]
        match_date = date.fromisoformat(match_date_str)

        # Scheduled datetime
        datetime_str = match_div.get("data-datetime", "").strip()
        if datetime_str:
            scheduled_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        else:
            scheduled_datetime = None

        time_suffix = match_div.get("data-suffix", "").strip()
        display_time = match_div.get("data-displaytime", "").strip()

        # Court name from <strong> in schedule-location-timestamp
        header = match_div.select_one("div.schedule-header")
        court_name = None
        if header:
            strong = header.select_one("div.schedule-location-timestamp strong")
            if strong:
                court_name = strong.get_text(strip=True) or None

        # Round
        round_div = match_div.select_one("div.schedule-header div.schedule-type")
        round_str = round_div.get_text(strip=True) if round_div else ""

        # Players
        content = match_div.select_one("div.schedule-content")
        if content is None:
            continue

        player_div = content.select_one("div.player")
        opponent_div = content.select_one("div.opponent")
        if player_div is None or opponent_div is None:
            continue

        p1_id, p1_name, p1_country, p1_seed_entry = _extract_player_info(player_div)
        p2_id, p2_name, p2_country, p2_seed_entry = _extract_player_info(opponent_div)

        # Status
        status_div = content.select_one("div.status")
        status = status_div.get_text(strip=True) if status_div else None
        if status == "":
            status = None

        # Score
        score_span = content.select_one("span.schedule-cta-score")
        score = _normalize_score(score_span)

        records.append(
            ScheduleRecord(
                tournament_id=tournament_id,
                year=year,
                circuit=circuit,
                match_date=match_date,
                scheduled_datetime=scheduled_datetime,
                time_suffix=time_suffix,
                display_time=display_time,
                court_name=court_name,
                round=round_str,
                p1_id=p1_id,
                p1_name=p1_name,
                p1_country=p1_country,
                p1_seed_entry=p1_seed_entry,
                p2_id=p2_id,
                p2_name=p2_name,
                p2_country=p2_country,
                p2_seed_entry=p2_seed_entry,
                status=status,
                score=score,
                snapshot_timestamp=snapshot_timestamp,
                source_file=source_file,
                parsed_at=parsed_at,
            )
        )

    return records


class ScheduleTransformer(BaseJob):
    """Transform raw schedule HTML files into a staged parquet file."""

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament

    def run(self) -> Path | None:
        """Process schedule HTML files. Returns parquet path or None."""
        schedule_dir = self.build_path(
            "raw",
            f"tournaments/{self.tournament.circuit.value}/{self.tournament.tournament_id}/{self.tournament.year}/schedule",
        )
        html_files = self.list_files(schedule_dir, "schedule_*.html")
        if not html_files:
            logger.info(
                "No schedule files for %s", self.tournament.logging_id
            )
            return None

        all_records: list[ScheduleRecord] = []
        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)

        for html_path in html_files:
            snapshot_ts = _parse_snapshot_timestamp(html_path.stem)
            source_file = str(self._display_path(html_path))
            html_content = self.read_html(html_path)
            records = _parse_schedule_html(
                html_content,
                tournament_id=self.tournament.tournament_id,
                year=self.tournament.year,
                circuit=self.tournament.circuit,
                snapshot_timestamp=snapshot_ts,
                source_file=source_file,
                parsed_at=parsed_at,
            )
            all_records.extend(records)

        if not all_records:
            return None

        df = pl.DataFrame(
            [r.model_dump() for r in all_records],
            schema_overrides=polars_schema_overrides(ScheduleRecord),
        )
        target = self.build_path(
            "stage",
            f"tournaments/{self.tournament.circuit.value}/{self.tournament.tournament_id}/{self.tournament.year}",
            "schedule.parquet",
        )
        return self.save_parquet(df, target)
