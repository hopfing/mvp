"""Transformer for MatchBeats data - JSON to Parquet."""

import json
import logging
from datetime import datetime
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.match_beats import (
    SCHEMA_HASH,
    MatchBeatsPointRecord,
    PointResult,
)
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)


class MatchBeatsTransformer(BaseJob):
    """Transform MatchBeats JSON files to point-level Parquet."""

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament

    def run(self) -> None:
        """Transform all MatchBeats JSON files for the tournament."""
        raw_dir = self.build_path("raw", self.tournament.path, "match_beats")
        if not raw_dir.exists():
            logger.debug(
                "No match_beats directory for %s", self.tournament.logging_id
            )
            return

        json_files = list(raw_dir.glob("*.json"))
        if not json_files:
            logger.debug(
                "No match_beats JSON files for %s", self.tournament.logging_id
            )
            return

        all_records = []
        parsed_at = datetime.now()

        for json_file in json_files:
            try:
                records = self._transform_file(json_file, parsed_at)
                all_records.extend(records)
            except Exception as e:
                logger.warning(
                    "Failed to transform %s: %s",
                    json_file,
                    e,
                )
                continue

        if not all_records:
            logger.info(
                "%s: no valid match_beats records", self.tournament.logging_id
            )
            return

        # Convert to DataFrame with explicit schema inference
        df = pl.DataFrame(
            [r.model_dump() for r in all_records],
            infer_schema_length=None,  # Scan all rows for schema
        )

        # Add schema hash
        df = df.with_columns(pl.lit(SCHEMA_HASH).alias("schema_hash"))

        # Write parquet
        output_path = self.build_path(
            "stage", self.tournament.path, "match_beats.parquet"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)

        logger.info(
            "%s: staged %d points from %d matches",
            self.tournament.logging_id,
            len(all_records),
            len(json_files),
        )

    def _transform_file(
        self, json_file: Path, parsed_at: datetime
    ) -> list[MatchBeatsPointRecord]:
        """Transform a single JSON file to point records."""
        with open(json_file) as f:
            data = json.load(f)

        match_id = data.get("matchId", json_file.stem)
        is_doubles = data.get("isDoubles", False)

        # Extract player IDs
        player_data = data.get("playerData", {})
        p1_id = player_data.get("tm1Ply1Id", "")
        p2_id = player_data.get("tm2Ply1Id", "")

        records = []

        for set_data in data.get("setData", []):
            set_num = set_data.get("set", 0)
            set_winner = set_data.get("setWinner")
            if isinstance(set_winner, int):
                set_winner = str(set_winner)

            for game_data in set_data.get("gameData", []):
                game_num = game_data.get("game", 0)
                game_duration = game_data.get("duration")
                easy_hold = game_data.get("easyHold")
                difficult_hold = game_data.get("difficultHold")
                multiple_deuces = game_data.get("multipleDeuces")
                game_winner = game_data.get("gameWinner")
                if isinstance(game_winner, int):
                    game_winner = str(game_winner)
                is_tiebreak = game_data.get("isTieBreak", False)

                for point_data in game_data.get("pointData", []):
                    try:
                        record = self._create_point_record(
                            point_data=point_data,
                            tournament_id=self.tournament.tournament_id,
                            year=self.tournament.year,
                            match_id=match_id,
                            is_doubles=is_doubles,
                            p1_id=p1_id,
                            p2_id=p2_id,
                            set_num=set_num,
                            set_winner=set_winner,
                            game_num=game_num,
                            game_duration=game_duration,
                            easy_hold=easy_hold,
                            difficult_hold=difficult_hold,
                            multiple_deuces=multiple_deuces,
                            game_winner=game_winner,
                            is_tiebreak=is_tiebreak,
                            source_file=str(json_file),
                            parsed_at=parsed_at,
                        )
                        records.append(record)
                    except Exception as e:
                        logger.debug(
                            "Skipping point in %s: %s",
                            json_file.name,
                            e,
                        )
                        continue

        return records

    def _create_point_record(
        self,
        point_data: dict,
        tournament_id: str,
        year: int,
        match_id: str,
        is_doubles: bool,
        p1_id: str,
        p2_id: str,
        set_num: int,
        set_winner: str | None,
        game_num: int,
        game_duration: int | None,
        easy_hold: bool | None,
        difficult_hold: bool | None,
        multiple_deuces: bool | None,
        game_winner: str | None,
        is_tiebreak: bool,
        source_file: str,
        parsed_at: datetime,
    ) -> MatchBeatsPointRecord:
        """Create a MatchBeatsPointRecord from raw point data."""
        # Normalize result — "N" means no data, map to None
        result = point_data.get("result", "N")
        if result == "N" or result not in [e.value for e in PointResult]:
            result = None

        # Normalize scorer/server (can be int or str)
        scorer = point_data.get("scorer", "1")
        if isinstance(scorer, int):
            scorer = str(scorer)
        server = point_data.get("server", "1")
        if isinstance(server, int):
            server = str(server)

        return MatchBeatsPointRecord(
            tournament_id=tournament_id,
            year=year,
            match_id=match_id,
            is_doubles=is_doubles,
            p1_id=p1_id,
            p2_id=p2_id,
            set_num=set_num,
            set_winner=set_winner,
            game_num=game_num,
            game_duration=game_duration,
            easy_hold=easy_hold,
            difficult_hold=difficult_hold,
            multiple_deuces=multiple_deuces,
            game_winner=game_winner,
            is_tiebreak=is_tiebreak,
            point_num=point_data.get("point", 0),
            point_id=point_data.get("pointId", ""),
            result=result,
            scorer=scorer,
            server=server,
            serve=point_data.get("serve", 1),
            serve_speed=point_data.get("serveSpeed", 0.0),
            fault_serve_speed=point_data.get("faultSrvSpd", 0.0),
            p1_rally_shots=point_data.get("tm1Rally", 0),
            p2_rally_shots=point_data.get("tm2Rally", 0),
            rally_length_missing=point_data.get("rallyLengthMissing", False),
            is_break_point=point_data.get("isBrkPt", False),
            break_points_in_game=point_data.get("brkPts", 0),
            break_points_lost=point_data.get("brkPtsLost", 0),
            is_crucial_point=point_data.get("isCrucialPt", False),
            p1_game_score=str(point_data.get("tm1GameScore", "0")),
            p2_game_score=str(point_data.get("tm2GameScore", "0")),
            match_duration_at_point=point_data.get("currentMatchDuration", 0),
            source_file=source_file,
            parsed_at=parsed_at,
        )
