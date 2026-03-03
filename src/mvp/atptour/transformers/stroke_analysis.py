"""Transformer for stroke_analysis data - JSON to Parquet."""

import json
import logging
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.stroke_analysis import (
    SCHEMA_HASH,
    SHOT_TYPE_MAP,
    StrokeAnalysisRecord,
)
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)


class StrokeAnalysisTransformer(BaseJob):
    """Transform stroke_analysis JSON files to match-level Parquet."""

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament

    def run(self) -> None:
        """Transform all stroke_analysis JSON files for the tournament."""
        raw_dir = self.build_path("raw", self.tournament.path, "stroke_analysis")
        if not raw_dir.exists():
            logger.debug(
                "No stroke_analysis directory for %s", self.tournament.logging_id
            )
            return

        json_files = list(raw_dir.glob("*.json"))
        if not json_files:
            logger.debug(
                "No stroke_analysis JSON files for %s", self.tournament.logging_id
            )
            return

        all_records = []
        parsed_at = datetime.now()

        for json_file in json_files:
            try:
                record = self._transform_file(json_file, parsed_at)
                if record:
                    all_records.append(record)
            except Exception as e:
                logger.warning(
                    "Failed to transform %s: %s",
                    json_file,
                    e,
                )
                continue

        if not all_records:
            logger.info(
                "%s: no valid stroke_analysis records", self.tournament.logging_id
            )
            return

        df = pl.DataFrame(
            [r.model_dump() for r in all_records],
            infer_schema_length=None,
        )
        df = df.with_columns(pl.lit(SCHEMA_HASH).alias("schema_hash"))

        output_path = self.build_path(
            "stage", self.tournament.path, "stroke_analysis.parquet"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)

        logger.info(
            "%s: staged %d matches from %d files",
            self.tournament.logging_id,
            len(all_records),
            len(json_files),
        )

    def _transform_file(
        self, json_file: Path, parsed_at: datetime
    ) -> StrokeAnalysisRecord | None:
        """Transform a single JSON file to a match-level record."""
        with open(json_file) as f:
            data = json.load(f)

        if not data.get("matchCompleted", False):
            return None

        match_id = json_file.stem
        is_doubles = data.get("isDoubles", False)

        players = data.get("players", [])
        p1_id = players[0]["player1Id"] if players else ""
        p2_id = players[1]["player1Id"] if len(players) > 1 else ""

        rally_shots = data.get("rallyShots", {})
        fields: dict = {
            "tournament_id": self.tournament.tournament_id,
            "year": self.tournament.year,
            "match_id": match_id,
            "is_doubles": is_doubles,
            "p1_id": p1_id.upper(),
            "p2_id": p2_id.upper(),
            "source_file": str(json_file),
            "parsed_at": parsed_at,
        }

        self._extract_total_points_count(rally_shots, fields)
        self._extract_shot_type_totals(rally_shots, fields)

        return StrokeAnalysisRecord(**fields)

    def _extract_total_points_count(
        self, rally_shots: dict, fields: dict
    ) -> None:
        """Extract FH/BH match totals from totalPointsCount[0]."""
        tpc = rally_shots.get("totalPointsCount", [])
        if not tpc:
            return

        item = tpc[0]
        fh = item.get("forehand", {})
        bh = item.get("backhand", {})

        fields["p1_fh_winners"] = fh.get("player1Wins", 0)
        fields["p1_fh_forced_errors"] = fh.get("player1Frcs", 0)
        fields["p1_fh_unforced_errors"] = fh.get("player1Unfs", 0)
        fields["p1_bh_winners"] = bh.get("player1Wins", 0)
        fields["p1_bh_forced_errors"] = bh.get("player1Frcs", 0)
        fields["p1_bh_unforced_errors"] = bh.get("player1Unfs", 0)

        fields["p2_fh_winners"] = fh.get("player2Wins", 0)
        fields["p2_fh_forced_errors"] = fh.get("player2Frcs", 0)
        fields["p2_fh_unforced_errors"] = fh.get("player2Unfs", 0)
        fields["p2_bh_winners"] = bh.get("player2Wins", 0)
        fields["p2_bh_forced_errors"] = bh.get("player2Frcs", 0)
        fields["p2_bh_unforced_errors"] = bh.get("player2Unfs", 0)

    def _extract_shot_type_totals(
        self, rally_shots: dict, fields: dict
    ) -> None:
        """Extract per-shot-type totals from allPoints[0], summed across sets."""
        all_points = rally_shots.get("allPoints", [])
        if not all_points:
            return

        ap = all_points[0]
        fh_entries = ap.get("forehand", [])
        bh_entries = ap.get("backhand", [])

        if not fh_entries:
            return

        # Sum across all groups (sets) for each shot type
        n_entries = len(fh_entries)
        n_shot_types = 7
        n_groups = n_entries // n_shot_types

        for shot_idx in range(n_shot_types):
            fh_first = fh_entries[shot_idx]
            shot_name = fh_first.get("name", "")
            col_prefix = SHOT_TYPE_MAP.get(shot_name)
            if not col_prefix:
                continue

            p1_w = 0
            p1_f = 0
            p1_u = 0
            p1_o = 0
            p2_w = 0
            p2_f = 0
            p2_u = 0
            p2_o = 0

            for group in range(n_groups):
                idx = group * n_shot_types + shot_idx
                fh = fh_entries[idx]
                bh = bh_entries[idx]

                p1_w += fh.get("player1Wins", 0) + bh.get("player1Wins", 0)
                p1_f += fh.get("player1Frcs", 0) + bh.get("player1Frcs", 0)
                p1_u += fh.get("player1Unfs", 0) + bh.get("player1Unfs", 0)
                p1_o += fh.get("player1Others", 0) + bh.get("player1Others", 0)
                p2_w += fh.get("player2Wins", 0) + bh.get("player2Wins", 0)
                p2_f += fh.get("player2Frcs", 0) + bh.get("player2Frcs", 0)
                p2_u += fh.get("player2Unfs", 0) + bh.get("player2Unfs", 0)
                p2_o += fh.get("player2Others", 0) + bh.get("player2Others", 0)

            fields[f"p1_{col_prefix}_winners"] = p1_w
            fields[f"p1_{col_prefix}_forced_errors"] = p1_f
            fields[f"p1_{col_prefix}_unforced_errors"] = p1_u
            fields[f"p1_{col_prefix}_others"] = p1_o
            fields[f"p2_{col_prefix}_winners"] = p2_w
            fields[f"p2_{col_prefix}_forced_errors"] = p2_f
            fields[f"p2_{col_prefix}_unforced_errors"] = p2_u
            fields[f"p2_{col_prefix}_others"] = p2_o
