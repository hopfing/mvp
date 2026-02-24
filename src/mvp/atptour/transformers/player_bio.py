"""PlayerBio stager and transformer (consolidator)."""

import datetime as dt
import logging
from datetime import date
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.player_bio import PlayerBioRecord
from mvp.common.base_job import BaseJob
from mvp.common.utils import polars_schema_overrides

logger = logging.getLogger(__name__)


def _parse_birth_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return date.fromisoformat(raw[:10])


def _parse_bio_json(
    player_id: str,
    data: dict,
    source_file: str,
    parsed_at: dt.datetime,
) -> PlayerBioRecord:
    return PlayerBioRecord(
        player_id=player_id,
        first_name=data["FirstName"],
        last_name=data["LastName"],
        birth_date=_parse_birth_date(data["BirthDate"]),
        birth_city=data["BirthCity"],
        nationality=data["Nationality"],
        natl_id=data["NatlId"],
        height_cm=data["HeightCm"],
        weight_kg=data["WeightKg"],
        right_handed=data["PlayHand"]["Id"] if data["PlayHand"] else None,
        twohand_backhand=data["BackHand"]["Id"] if data["BackHand"] else None,
        pro_year=data["ProYear"],
        is_active=data["Active"]["Id"] if data["Active"] else None,
        is_dbl_specialist=data["DblSpecialist"],
        source_file=source_file,
        parsed_at=parsed_at,
    )


class PlayerBioStager(BaseJob):
    """Parse raw player bio JSON into per-player staged parquets."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> list[tuple[str, str]]:
        """Stage raw bio JSONs into per-player parquets.

        Only processes files that are new or have been updated since last staging.

        Returns:
            List of (player_id, error_message) tuples for failed records.
        """
        raw_dir = self.build_path("raw", "players")
        raw_files = self.list_files(raw_dir, "*.json")
        if not raw_files:
            return []

        stage_dir = self.build_path("stage", "players")
        existing = {p.stem: p for p in self.list_files(stage_dir, "*.parquet")}

        to_process = []
        for raw_path in raw_files:
            pid = raw_path.stem
            staged_path = existing.get(pid)
            if (
                staged_path is None
                or raw_path.stat().st_mtime > staged_path.stat().st_mtime
            ):
                to_process.append(raw_path)

        to_process.sort(key=lambda p: p.stem)
        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)

        failed: list[tuple[str, str]] = []
        for raw_path in to_process:
            pid = raw_path.stem
            try:
                data = self.read_json(raw_path)
                source_file = str(self._display_path(raw_path))
                record = _parse_bio_json(pid, data, source_file, parsed_at)
                df = pl.DataFrame(
                    [record.model_dump()],
                    schema_overrides=polars_schema_overrides(PlayerBioRecord),
                )
                target = self.build_path("stage", "players", f"{pid}.parquet")
                self.save_parquet(df, target)
            except Exception as e:
                logger.warning("Failed to stage bio for %s: %s", pid, e)
                failed.append((pid, str(e)))

        logger.info(
            "Player bio stager: %d raw files, %d to process, %d failed",
            len(raw_files),
            len(to_process),
            len(failed),
        )
        return failed


class PlayerBioTransformer(BaseJob):
    """Consolidate per-player parquets into a single players.parquet."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> Path | None:
        """Merge all per-player staged parquets into players.parquet.

        Returns the consolidated file path, or None if no files to merge.
        """
        stage_dir = self.build_path("stage", "players")
        parquet_files = self.list_files(stage_dir, "*.parquet")
        if not parquet_files:
            logger.info("No player bio parquets to consolidate")
            return None

        dfs = [pl.read_parquet(p) for p in parquet_files]
        combined = pl.concat(dfs, how="diagonal_relaxed")

        self._assert_unique(combined, ["player_id"])

        target = self.build_path("stage", "players.parquet")
        result = self.save_parquet(combined, target)

        logger.info(
            "Player bio consolidate: merged %d files, %d total rows",
            len(parquet_files),
            len(combined),
        )
        return result

    @staticmethod
    def _assert_unique(df: pl.DataFrame, key_cols: list[str]) -> None:
        """Assert primary key uniqueness."""
        dupes = df.group_by(key_cols).len().filter(pl.col("len") > 1)
        if len(dupes) > 0:
            samples = dupes.head(5)[key_cols].to_dicts()
            raise ValueError(
                f"Duplicate primary keys in player_bio: {samples}"
            )
