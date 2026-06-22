"""Transformer for stats_plus data — JSON to normalized long Parquet.

Emits one row per ``(match, set_num, stat)`` (see
``schemas/stats_plus.StatsPlusRowRecord``), the same N-rows-per-file staging
shape as the match_beats transformer.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.stats_plus import (
    SCHEMA_HASH,
    STAT_REGISTRY,
    StatsPlusRowRecord,
)
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob
from mvp.common.utils import polars_schema

logger = logging.getLogger(__name__)

# Leading "N/M" of a frac value like "42/73 (58%)". The percentage is redundant.
_FRAC_RE = re.compile(r"^\s*(\d+)\s*/\s*(\d+)")
# Leading integer of a num value like "285".
_INT_RE = re.compile(r"^\s*(-?\d+)")
# Leading number of an influence value like "6%" or "6.0 %".
_INFLUENCE_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)")


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
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if not isinstance(value, str):
        return None
    m = _INT_RE.match(value)
    if not m:
        return None
    parsed = int(m.group(1))
    return parsed if parsed >= 0 else None


def _parse_influence(value: object) -> float | None:
    """Parse an influence value like ``"6%"`` -> ``0.06`` (fraction in [0, 1]).

    Only the observed ``"P%"`` *string* format is parsed. Per the spec the
    format is observed-but-not-guaranteed, so anything else — null, blank,
    unparseable, or a bare number whose unit we can't assume — stages as
    ``None`` rather than guessing. An all-null ``influence`` column is then the
    signal that the feed format changed.
    """
    if not isinstance(value, str):
        return None
    m = _INFLUENCE_RE.match(value)
    if not m:
        return None
    return float(m.group(1)) / 100.0


class StatsPlusTransformer(BaseJob):
    """Transform stats_plus JSON to normalized long Parquet (one row per
    match x set x stat)."""

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

        all_records: list[StatsPlusRowRecord] = []
        parsed_at = datetime.now()

        for json_file in json_files:
            try:
                all_records.extend(self._transform_file(json_file, parsed_at))
            except Exception as e:
                logger.warning("Failed to transform %s: %s", json_file, e)
                continue

        if not all_records:
            logger.info("%s: no valid stats_plus records", self.tournament.logging_id)
            return

        rows = [r.model_dump() for r in all_records]
        # schema_overrides pins column dtypes from the model — required so an
        # all-null column (e.g. p1_den for a tournament of only `num` stats, or
        # influence) lands as Int64/Float64 rather than polars' Null dtype,
        # which would break downstream concat/join.
        df = pl.DataFrame(rows, schema_overrides=polars_schema(StatsPlusRowRecord))

        output_path = self.build_path(
            "stage", self.tournament.path, "stats_plus.parquet"
        )
        self.save_parquet(df, output_path, schema_hash=SCHEMA_HASH)

        logger.info(
            "%s: staged %d rows from %d files",
            self.tournament.logging_id,
            len(all_records),
            len(json_files),
        )

    def _transform_file(
        self, json_file: Path, parsed_at: datetime
    ) -> list[StatsPlusRowRecord]:
        """Transform a single JSON file to a list of (set x stat) records.

        Returns an empty list for an incomplete match or absent set stats.
        """
        with open(json_file) as f:
            data = json.load(f)

        if not data.get("matchCompleted", False):
            return []

        set_stats = data.get("setStats") or {}
        if not set_stats:
            return []

        match_id = json_file.stem
        is_doubles = data.get("isDoubles", False)

        players = data.get("players", [])
        p1_id = players[0]["player1Id"] if players else ""
        p2_id = players[1]["player1Id"] if len(players) > 1 else ""
        if not p1_id or not p2_id:
            logger.warning(
                "stats_plus %s has blank player id (p1=%r, p2=%r) — rows won't join",
                json_file.name, p1_id, p2_id,
            )

        base = {
            "tournament_id": self.tournament.tournament_id,
            "year": self.tournament.year,
            "match_id": match_id,  # uppercased by the schema validator
            "is_doubles": is_doubles,
            "p1_id": p1_id,  # map_player_id validator normalizes
            "p2_id": p2_id,
            "sets_completed": data.get("setsCompleted"),
            "source_file": str(json_file),
            "parsed_at": parsed_at,
        }

        records: list[StatsPlusRowRecord] = []
        # Iterate whatever set keys are actually present (set0 = match total,
        # set1..N = sets). Do NOT assume a contiguous 0..sets_completed range —
        # retirements yield short / non-contiguous set lists.
        for set_key, stat_rows in set_stats.items():
            if not set_key.startswith("set"):
                continue
            try:
                set_num = int(set_key[len("set"):])
            except ValueError:
                continue
            if not stat_rows:
                continue
            for row in stat_rows:
                name = row.get("name")
                reg = STAT_REGISTRY.get(name)
                if reg is None:
                    continue  # unknown / extra / nameless stat — no key, skip
                stat_key, kind = reg
                if kind == "frac":
                    p1_num, p1_den = _parse_frac(row.get("player1"))
                    p2_num, p2_den = _parse_frac(row.get("player2"))
                else:
                    p1_num, p1_den = _parse_int(row.get("player1")), None
                    p2_num, p2_den = _parse_int(row.get("player2")), None
                records.append(
                    StatsPlusRowRecord(
                        **base,
                        set_num=set_num,
                        stat_key=stat_key,
                        stat_name=name,
                        p1_num=p1_num,
                        p1_den=p1_den,
                        p2_num=p2_num,
                        p2_den=p2_den,
                        influence=_parse_influence(row.get("influence")),
                    )
                )
        return records
