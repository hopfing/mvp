"""Transformer for stats_plus data - JSON to Parquet."""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.stats_plus import (
    FRAC_STATS,
    NUM_STATS,
    SCHEMA_HASH,
    StatsPlusRecord,
)
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob
from mvp.common.utils import polars_schema

logger = logging.getLogger(__name__)

# Leading "N/M" of a frac value like "42/73 (58%)". The percentage is redundant.
_FRAC_RE = re.compile(r"^\s*(\d+)\s*/\s*(\d+)")
# Leading integer of a num value like "285".
_INT_RE = re.compile(r"^\s*(-?\d+)")


def _parse_frac(value: object) -> tuple[int | None, int | None]:
    """Parse ``"N/M (P%)"`` -> ``(N, M)``. ``"0/0 (0%)"`` -> ``(0, 0)``.

    A missing/blank/unparseable value -> ``(None, None)``.
    """
    if not isinstance(value, str):
        return None, None
    m = _FRAC_RE.match(value)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _parse_int(value: object) -> int | None:
    """Parse a num value to int; blank/unparseable -> ``None``.

    The feed emits ``-1`` as a "not tracked" sentinel (seen in e.g. Unforced
    Errors), so any negative is treated as missing -> ``None``.
    """
    if isinstance(value, int):
        return value if value >= 0 else None
    if not isinstance(value, str):
        return None
    m = _INT_RE.match(value)
    if not m:
        return None
    parsed = int(m.group(1))
    return parsed if parsed >= 0 else None


class StatsPlusTransformer(BaseJob):
    """Transform stats_plus JSON files to match-level Parquet."""

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament

    def run(self) -> None:
        """Transform all stats_plus JSON files for the tournament."""
        raw_dir = self.build_path("raw", self.tournament.path, "stats_plus")
        if not raw_dir.exists():
            logger.debug("No stats_plus directory for %s", self.tournament.logging_id)
            return

        json_files = list(raw_dir.glob("*.json"))
        if not json_files:
            logger.debug("No stats_plus JSON files for %s", self.tournament.logging_id)
            return

        records = []
        parsed_at = datetime.now()

        for json_file in json_files:
            try:
                record = self._transform_file(json_file, parsed_at)
                if record:
                    records.append(record)
            except Exception as e:
                logger.warning("Failed to transform %s: %s", json_file, e)
                continue

        if not records:
            logger.info("%s: no valid stats_plus records", self.tournament.logging_id)
            return

        rows = [r.model_dump() for r in records]
        df = pl.DataFrame(rows, schema_overrides=polars_schema(StatsPlusRecord))

        output_path = self.build_path(
            "stage", self.tournament.path, "stats_plus.parquet"
        )
        self.save_parquet(df, output_path, schema_hash=SCHEMA_HASH)

        logger.info(
            "%s: staged %d matches from %d files",
            self.tournament.logging_id,
            len(records),
            len(json_files),
        )

    def _transform_file(
        self, json_file: Path, parsed_at: datetime
    ) -> StatsPlusRecord | None:
        """Transform a single JSON file to a match-level record from set0."""
        with open(json_file) as f:
            data = json.load(f)

        if not data.get("matchCompleted", False):
            return None

        set0 = data.get("setStats", {}).get("set0", [])
        if not set0:
            return None

        match_id = json_file.stem
        is_doubles = data.get("isDoubles", False)

        players = data.get("players", [])
        p1_id = players[0]["player1Id"] if players else ""
        p2_id = players[1]["player1Id"] if len(players) > 1 else ""
        if not p1_id or not p2_id:
            logger.warning(
                "stats_plus %s has blank player id (p1=%r, p2=%r) — row won't join",
                json_file.name, p1_id, p2_id,
            )

        fields: dict = {
            "tournament_id": self.tournament.tournament_id,
            "year": self.tournament.year,
            "match_id": match_id,
            "is_doubles": is_doubles,
            "p1_id": p1_id,  # map_player_id validator normalizes
            "p2_id": p2_id,
            "sets_completed": data.get("setsCompleted"),
            "source_file": str(json_file),
            "parsed_at": parsed_at,
        }

        # Key off the stat `name` (not list position): row counts vary 16/19/22
        # and absent stats must stay null, so position-based mapping is unsafe.
        by_name = {row.get("name"): row for row in set0}

        for name, (num_stem, den_stem) in FRAC_STATS.items():
            row = by_name.get(name)
            if row is None:
                continue
            p1_num, p1_den = _parse_frac(row.get("player1"))
            p2_num, p2_den = _parse_frac(row.get("player2"))
            fields[f"p1_{num_stem}"] = p1_num
            fields[f"p1_{den_stem}"] = p1_den
            fields[f"p2_{num_stem}"] = p2_num
            fields[f"p2_{den_stem}"] = p2_den

        for name, stem in NUM_STATS.items():
            row = by_name.get(name)
            if row is None:
                continue
            fields[f"p1_{stem}"] = _parse_int(row.get("player1"))
            fields[f"p2_{stem}"] = _parse_int(row.get("player2"))

        return StatsPlusRecord(**fields)
