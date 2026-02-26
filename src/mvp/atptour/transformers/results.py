"""Transform raw results HTML into staged parquet via ResultRecord schema."""

import datetime as dt
import logging
from pathlib import Path

import polars as pl

from mvp.atptour.mappings import (
    map_player_id,
    parse_duration,
    parse_seed_entry,
)
from mvp.atptour.parsers.results import ResultsParser
from mvp.atptour.schemas.results import ResultRecord
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob
from mvp.common.enums import DrawType
from mvp.common.utils import polars_schema

logger = logging.getLogger(__name__)

UNITED_CUP_TID = "9900"


class ResultsTransformer(BaseJob):
    """Transform raw results HTML into staged parquet.

    Processes both singles and doubles draws for a tournament, producing
    a single unified results.parquet file.
    """

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament
        self._parser = ResultsParser()

    def run(self) -> list[Path]:
        """Process singles and doubles draws. Returns list of parquet paths (0 or 1)."""
        records: list[ResultRecord] = []

        singles = self._process_draw(DrawType.singles, "results_singles.html")
        records.extend(singles)

        if self.tournament.tournament_id == UNITED_CUP_TID:
            logger.info(
                "Skipping doubles for United Cup (%s)", self.tournament.logging_id
            )
        else:
            doubles = self._process_draw(DrawType.doubles, "results_doubles.html")
            records.extend(doubles)

        if not records:
            logger.info("No results records for %s", self.tournament.logging_id)
            return []

        rows = [r.model_dump() for r in records]
        overrides = polars_schema(ResultRecord)
        df = pl.DataFrame(rows, schema_overrides=overrides)

        df = self._dedup(df)
        self._assert_unique(df, ["match_uid"])

        out_path = self.build_path("stage", self.tournament.path, "results.parquet")
        result = self.save_parquet(df, out_path)
        if result is None:
            return []
        return [result]

    def _process_draw(
        self, draw_type: DrawType, filename: str
    ) -> list[ResultRecord]:
        """Parse one draw's HTML and validate into ResultRecord instances."""
        raw_path = self.build_path("raw", self.tournament.path, filename)

        if not raw_path.exists():
            logger.info(
                "No %s file for %s: %s",
                draw_type.value,
                self.tournament.logging_id,
                self._display_path(raw_path),
            )
            return []

        html = self.read_html(raw_path)
        is_doubles = draw_type == DrawType.doubles

        if is_doubles:
            raw_matches = self._parser.parse_doubles(html)
        else:
            raw_matches = self._parser.parse_singles(html)

        if not raw_matches:
            logger.info(
                "No matches parsed from %s for %s",
                filename,
                self.tournament.logging_id,
            )
            return []

        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        source_file = str(self._display_path(raw_path))
        records = []

        for match in raw_matches:
            record = self._build_record(
                match, draw_type, is_doubles, source_file, parsed_at
            )
            records.append(record)

        logger.info(
            "Parsed %d %s results for %s",
            len(records),
            draw_type.value,
            self.tournament.logging_id,
        )
        return records

    def _build_record(
        self,
        match: dict,
        draw_type: DrawType,
        is_doubles: bool,
        source_file: str,
        parsed_at: dt.datetime,
    ) -> ResultRecord:
        """Map a raw parser dict to a validated ResultRecord."""
        p1_id = map_player_id(match["player_id"])
        p2_id = map_player_id(match["opp_id"])

        winner_id = p1_id if match["player_won"] else p2_id

        p1_seed, p1_entry = parse_seed_entry(match["player_seed_entry"])
        p2_seed, p2_entry = parse_seed_entry(match["opp_seed_entry"])

        duration_text = match["duration_text"]
        duration_seconds = None
        if duration_text:
            duration_seconds = parse_duration(duration_text)

        p1_country = match["player_country"].upper()
        p2_country = match["opp_country"].upper()

        scores = self._flatten_scores(match)

        # Build partner fields for doubles
        partner_kwargs = {}
        if is_doubles:
            partner_kwargs = {
                "p1_partner_id": map_player_id(match["partner_id"]),
                "p1_partner_name": match["partner_name"],
                "p1_partner_country": match["partner_country"].upper(),
                "p2_partner_id": map_player_id(match["opp_partner_id"]),
                "p2_partner_name": match["opp_partner_name"],
                "p2_partner_country": match["opp_partner_country"].upper(),
            }

        return ResultRecord(
            tournament_id=self.tournament.tournament_id,
            year=self.tournament.year,
            circuit=self.tournament.circuit,
            draw_type=draw_type,
            round=match["round_text"],
            match_id=match.get("match_id"),
            winner_id=winner_id,
            p1_id=p1_id,
            p1_name=match["player_name"],
            p1_country=p1_country,
            p1_seed=p1_seed,
            p1_entry=p1_entry,
            p2_id=p2_id,
            p2_name=match["opp_name"],
            p2_country=p2_country,
            p2_seed=p2_seed,
            p2_entry=p2_entry,
            result_type=match["result_type"],
            duration_seconds=duration_seconds,
            source_file=source_file,
            parsed_at=parsed_at,
            **scores,
            **partner_kwargs,
        )

    @staticmethod
    def _flatten_scores(match: dict) -> dict:
        """Flatten variable-length score lists into v3 per-set columns.

        Maps: player_scores[i] -> p1_set{i+1}_games
              opp_scores[i] -> p2_set{i+1}_games
              player_tiebreaks[i] -> p1_set{i+1}_tiebreak
              opp_tiebreaks[i] -> p2_set{i+1}_tiebreak
        5 sets max, remaining None.
        """
        p_scores = match["player_scores"]
        o_scores = match["opp_scores"]
        p_tb = match["player_tiebreaks"]
        o_tb = match["opp_tiebreaks"]

        flat: dict = {}
        for i in range(5):
            set_num = i + 1
            flat[f"p1_set{set_num}_games"] = (
                p_scores[i] if i < len(p_scores) else None
            )
            flat[f"p1_set{set_num}_tiebreak"] = (
                p_tb[i] if i < len(p_tb) else None
            )
            flat[f"p2_set{set_num}_games"] = (
                o_scores[i] if i < len(o_scores) else None
            )
            flat[f"p2_set{set_num}_tiebreak"] = (
                o_tb[i] if i < len(o_tb) else None
            )
        return flat

    def _dedup(self, df: pl.DataFrame) -> pl.DataFrame:
        """Deduplicate rows by match_uid, preserving null-uid rows."""
        before = len(df)
        has_uid = df.filter(pl.col("match_uid").is_not_null())
        no_uid = df.filter(pl.col("match_uid").is_null())
        has_uid = has_uid.unique(subset=["match_uid"], keep="first")
        df = pl.concat([has_uid, no_uid])
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
                f"Duplicate primary keys in results: {samples}"
            )
