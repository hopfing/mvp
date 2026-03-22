"""Transform raw overview JSON into staged parquet via OverviewRecord schema."""

import datetime as dt
import logging
from pathlib import Path

import polars as pl

from mvp.atptour.schemas.overview import OverviewRecord
from mvp.atptour.tournament import Tournament
from mvp.common.base_job import BaseJob
from mvp.common.utils import polars_schema

logger = logging.getLogger(__name__)


class OverviewTransformer(BaseJob):
    """Transform a single raw overview JSON into a staged parquet file."""

    def __init__(self, tournament: Tournament, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)
        self.tournament = tournament

    def run(self) -> list[Path]:
        """Process overview JSON. Returns parquet paths (0 or 1)."""
        raw_path = self.build_path("raw", self.tournament.path, "overview.json")

        if not raw_path.exists():
            logger.info(
                "No overview file for %s", self.tournament.logging_id
            )
            return []

        data = self.read_json(raw_path)
        source_file = str(self._display_path(raw_path))
        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)

        location = data["Location"]
        parts = location.split(",")
        city = parts[0].strip()
        country = parts[-1].strip() if len(parts) >= 2 else None
        if not country:
            country = None

        record = OverviewRecord(
            tournament_id=self.tournament.tournament_id,
            year=self.tournament.year,
            tournament_name=self.tournament.name,
            city=city,
            country=country,
            circuit=self.tournament.circuit,
            sponsor_title=data["SponsorTitle"],
            event_type=data["EventType"],
            event_type_detail=data["EventTypeDetail"],
            singles_draw_size=data["SinglesDrawSize"],
            doubles_draw_size=data["DoublesDrawSize"],
            surface=data["Surface"],
            surface_detail=data["SurfaceSubCat"],
            indoor=data["InOutdoor"],
            prize=data["Prize"],
            total_financial_commitment=data["TotalFinancialCommitment"],
            location=location,
            source_file=source_file,
            parsed_at=parsed_at,
        )

        rows = [record.model_dump()]
        overrides = polars_schema(OverviewRecord)
        df = pl.DataFrame(rows, schema_overrides=overrides)

        self.assert_unique(df, ["tournament_id", "year"], "overview")

        out_path = self.build_path(
            "stage", self.tournament.path, "overview.parquet"
        )
        result = self.save_parquet(df, out_path)
        if result is None:
            return []
        return [result]

