"""Transformer for rally_analysis data - JSON to Parquet."""

import json
import logging
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.rally_analysis import (
    CATEGORY_TO_LENGTH,
    SCHEMA_HASH,
    RallyAnalysisRecord,
)
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)


class RallyAnalysisTransformer(BaseJob):
    """Transform rally_analysis JSON files to match-level Parquet."""

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament

    def run(self) -> None:
        """Transform all rally_analysis JSON files for the tournament."""
        raw_dir = self.build_path("raw", self.tournament.path, "rally_analysis")
        if not raw_dir.exists():
            logger.debug(
                "No rally_analysis directory for %s", self.tournament.logging_id
            )
            return

        json_files = list(raw_dir.glob("*.json"))
        if not json_files:
            logger.debug(
                "No rally_analysis JSON files for %s", self.tournament.logging_id
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
                "%s: no valid rally_analysis records", self.tournament.logging_id
            )
            return

        df = pl.DataFrame(
            [r.model_dump() for r in all_records],
            infer_schema_length=None,
        )
        df = df.with_columns(pl.lit(SCHEMA_HASH).alias("schema_hash"))

        output_path = self.build_path(
            "stage", self.tournament.path, "rally_analysis.parquet"
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
    ) -> RallyAnalysisRecord | None:
        """Transform a single JSON file to a match-level record."""
        with open(json_file) as f:
            data = json.load(f)

        if not data.get("matchCompleted", False):
            return None

        match_id = json_file.stem
        is_doubles = data.get("isDoubles", False)

        players = data.get("playerDetails", [])
        p1_id = players[0]["player1Id"] if players else ""
        p2_id = players[1]["player1Id"] if len(players) > 1 else ""

        fields: dict = {
            "tournament_id": self.tournament.tournament_id,
            "year": self.tournament.year,
            "match_id": match_id,
            "is_doubles": is_doubles,
            "p1_id": p1_id.upper(),
            "p2_id": p2_id.upper(),
            "points_missing": data.get("pointsMissing", False),
            "source_file": str(json_file),
            "parsed_at": parsed_at,
        }

        self._aggregate_rally_lengths(data.get("rallyData", []), fields)

        return RallyAnalysisRecord(**fields)

    def _aggregate_rally_lengths(
        self, rally_data: list, fields: dict
    ) -> None:
        """Sum point counts into short/medium/long rally length buckets."""
        for i, category in enumerate(rally_data):
            length = CATEGORY_TO_LENGTH.get(i)
            if length:
                fields[f"p1_{length}_won"] = (
                    fields.get(f"p1_{length}_won", 0) + len(category.get("t1win", []))
                )
                fields[f"p1_{length}_err"] = (
                    fields.get(f"p1_{length}_err", 0) + len(category.get("t1err", []))
                )
                fields[f"p2_{length}_won"] = (
                    fields.get(f"p2_{length}_won", 0) + len(category.get("t2win", []))
                )
                fields[f"p2_{length}_err"] = (
                    fields.get(f"p2_{length}_err", 0) + len(category.get("t2err", []))
                )
            elif category.get("name") == "UNCLASSIFIED":
                fields["p1_unclassified_won"] = len(category.get("t1win", []))
                fields["p1_unclassified_err"] = len(category.get("t1err", []))
                fields["p2_unclassified_won"] = len(category.get("t2win", []))
                fields["p2_unclassified_err"] = len(category.get("t2err", []))
