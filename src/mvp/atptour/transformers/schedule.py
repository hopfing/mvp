"""Transform raw schedule HTML into staged parquet via ScheduleRecord schema."""

import datetime as dt
import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup, Tag

from mvp.atptour.mappings import parse_seed_entry
from mvp.atptour.schemas.schedule import ScheduleRecord
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob
from mvp.common.enums import Circuit, DrawType
from mvp.common.utils import polars_schema

logger = logging.getLogger(__name__)

_MATCH_DURATION_ESTIMATE = timedelta(minutes=90)


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


def _extract_player_info(div: Tag) -> tuple[str, str, str, str | None, bool]:
    """Extract player ID, name, country, seed/entry, and doubles flag.

    Returns (player_id, display_name, country_code, seed_entry, is_doubles).
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

        return first_id, display_name, country, seed_entry, True

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

    return player_id, name_text, country, seed_entry, False


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

    for group in soup.select("div.content-group"):
        court_name = None
        court_match_num = 0
        anchor_time: datetime | None = None
        anchor_position: int = 0

        for match_div in group.select("div.schedule[data-matchdate]"):
            court_match_num += 1

            match_date_str = match_div["data-matchdate"]
            match_date = date.fromisoformat(match_date_str)

            datetime_str = match_div.get("data-datetime", "").strip()
            if datetime_str:
                scheduled_datetime = datetime.strptime(
                    datetime_str, "%Y-%m-%d %H:%M:%S"
                )
            else:
                scheduled_datetime = None

            time_suffix = match_div.get("data-suffix", "").strip()
            display_time = match_div.get("data-displaytime", "").strip()

            # Court name: extract from <strong> if present, otherwise propagate
            header = match_div.select_one("div.schedule-header")
            if header:
                strong = header.select_one(
                    "div.schedule-location-timestamp strong"
                )
                if strong:
                    extracted = strong.get_text(strip=True) or None
                    if extracted:
                        court_name = extracted

            # Time anchor and estimation
            if datetime_str:
                is_time_estimated = False
                if time_suffix in ("Starts At", "Not Before"):
                    anchor_time = scheduled_datetime
                    anchor_position = court_match_num
            elif time_suffix == "Followed By" and anchor_time is not None:
                offset = (court_match_num - anchor_position) * _MATCH_DURATION_ESTIMATE
                scheduled_datetime = anchor_time + offset
                is_time_estimated = True
            else:
                is_time_estimated = True

            # Round
            round_div = match_div.select_one(
                "div.schedule-header div.schedule-type"
            )
            round_str = round_div.get_text(strip=True) if round_div else ""

            # Players
            content = match_div.select_one("div.schedule-content")
            if content is None:
                continue

            player_div = content.select_one("div.player")
            opponent_div = content.select_one("div.opponent")
            if player_div is None or opponent_div is None:
                continue

            p1_id, p1_name, p1_country, p1_seed_entry, p1_is_dbl = (
                _extract_player_info(player_div)
            )
            p2_id, p2_name, p2_country, p2_seed_entry, p2_is_dbl = (
                _extract_player_info(opponent_div)
            )

            # Skip matches without ATP profile links (e.g., WTA players)
            if not p1_id or not p2_id:
                continue

            is_doubles = p1_is_dbl or p2_is_dbl
            draw_type = DrawType.doubles if is_doubles else DrawType.singles

            status_div = content.select_one("div.status")
            status = status_div.get_text(strip=True) if status_div else None
            if status == "":
                status = None

            score_span = content.select_one("span.schedule-cta-score")
            score = _normalize_score(score_span)

            records.append(
                ScheduleRecord(
                    tournament_id=tournament_id,
                    year=year,
                    circuit=circuit,
                    draw_type=draw_type,
                    match_date=match_date,
                    scheduled_datetime=scheduled_datetime,
                    time_suffix=time_suffix,
                    display_time=display_time,
                    court_name=court_name,
                    court_match_num=court_match_num,
                    is_time_estimated=is_time_estimated,
                    round=round_str,
                    p1_id=p1_id,
                    p1_name=p1_name,
                    p1_country=p1_country,
                    p1_seed=parse_seed_entry(p1_seed_entry)[0],
                    p1_entry=parse_seed_entry(p1_seed_entry)[1],
                    p2_id=p2_id,
                    p2_name=p2_name,
                    p2_country=p2_country,
                    p2_seed=parse_seed_entry(p2_seed_entry)[0],
                    p2_entry=parse_seed_entry(p2_seed_entry)[1],
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

    def stage(self) -> list[Path]:
        """Parse new HTML snapshots into per-snapshot parquets.

        Skips snapshots that already have a current staged parquet.
        Returns list of parquet paths written.
        """
        schedule_dir = self.build_path(
            "raw",
            f"tournaments/{self.tournament.circuit.value}/{self.tournament.tournament_id}/{self.tournament.year}/schedule",
        )
        html_files = self.list_files(schedule_dir, "schedule_*.html")
        if not html_files:
            logger.info("No schedule files for %s", self.tournament.logging_id)
            return []

        snapshot_stage_dir = self.build_path(
            "stage",
            f"tournaments/{self.tournament.circuit.value}/{self.tournament.tournament_id}/{self.tournament.year}/schedule",
        )
        existing = {p.stem: p for p in self.list_files(snapshot_stage_dir, "*.parquet")}

        to_process = []
        for html_path in html_files:
            staged_path = existing.get(html_path.stem)
            if staged_path is None:
                to_process.append(html_path)
            elif html_path.stat().st_mtime > staged_path.stat().st_mtime:
                to_process.append(html_path)
            elif not self.is_schema_current(staged_path, ScheduleRecord.SCHEMA_HASH):
                to_process.append(html_path)

        if not to_process:
            logger.info(
                "Schedule stage: all %d snapshots already staged for %s",
                len(html_files),
                self.tournament.logging_id,
            )
            return []

        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        paths: list[Path] = []

        for html_path in to_process:
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
            if not records:
                continue

            df = pl.DataFrame(
                [r.model_dump() for r in records],
                schema_overrides=polars_schema(ScheduleRecord),
            )

            out_path = self.build_path(
                "stage",
                f"tournaments/{self.tournament.circuit.value}/{self.tournament.tournament_id}/{self.tournament.year}/schedule",
                f"{html_path.stem}.parquet",
            )
            result = self.save_parquet(df, out_path, schema_hash=ScheduleRecord.SCHEMA_HASH)
            if result is not None:
                paths.append(result)

        logger.info(
            "Schedule stage: %d to process (%d skipped) for %s",
            len(to_process),
            len(html_files) - len(to_process),
            self.tournament.logging_id,
        )
        return paths

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
            schema_overrides=polars_schema(ScheduleRecord),
        )

        df = self._dedup(df)
        self._assert_unique(df, ["match_uid"])

        target = self.build_path(
            "stage",
            f"tournaments/{self.tournament.circuit.value}/{self.tournament.tournament_id}/{self.tournament.year}",
            "schedule.parquet",
        )
        return self.save_parquet(df, target)

    def _dedup(self, df: pl.DataFrame) -> pl.DataFrame:
        """Deduplicate by match_uid, keeping latest snapshot.

        Also detects replaced draw entries: if a player appears in multiple
        matches in the same round (excluding RR) with different opponents,
        only the match from the latest snapshot survives.
        """
        before = len(df)
        has_uid = df.filter(pl.col("match_uid").is_not_null())
        no_uid = df.filter(pl.col("match_uid").is_null())

        # First, keep latest data per match_uid
        has_uid = (
            has_uid.sort("snapshot_timestamp")
            .group_by("match_uid")
            .last()
            .select(df.columns)
        )

        # Drop replaced draw entries: same player+round, different opponent
        replaced_uids: set[str] = set()
        non_rr = has_uid.filter(pl.col("round") != "RR")
        for side, opp_side in [("p1_id", "p2_id"), ("p2_id", "p1_id")]:
            # Group by (player, round) and find duplicates
            grouped = non_rr.group_by([side, "round"]).agg(
                pl.col("match_uid"),
                pl.col(opp_side),
                pl.col("snapshot_timestamp"),
            )
            dupes = grouped.filter(pl.col("match_uid").list.len() > 1)
            for row in dupes.iter_rows(named=True):
                uids = row["match_uid"]
                timestamps = row["snapshot_timestamp"]
                # Keep the match from the latest snapshot, mark others as replaced
                latest_idx = timestamps.index(max(timestamps))
                for i, uid in enumerate(uids):
                    if i != latest_idx:
                        replaced_uids.add(uid)

        if replaced_uids:
            logger.info(
                "Dropped %d replaced draw entries for %s",
                len(replaced_uids),
                self.tournament.logging_id,
            )
            has_uid = has_uid.filter(~pl.col("match_uid").is_in(list(replaced_uids)))

        df = pl.concat([has_uid, no_uid])
        df = df.drop("snapshot_timestamp")
        dupes_removed = before - len(df)
        if dupes_removed > 0:
            logger.info(
                "Deduped %d duplicate match_uids for %s",
                dupes_removed,
                self.tournament.logging_id,
            )
        return df

    @staticmethod
    def _assert_unique(df: pl.DataFrame, key_cols: list[str]) -> None:
        """Assert primary key uniqueness, excluding null-uid rows."""
        check = df.filter(pl.col(key_cols[0]).is_not_null())
        dupes = check.group_by(key_cols).len().filter(pl.col("len") > 1)
        if len(dupes) > 0:
            samples = dupes.head(5)[key_cols].to_dicts()
            raise ValueError(
                f"Duplicate primary keys in schedule: {samples}"
            )
