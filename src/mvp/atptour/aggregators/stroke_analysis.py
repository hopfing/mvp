"""Aggregator for stroke_analysis match-level data to player-match level."""

import logging
from pathlib import Path

import polars as pl

from mvp.common.base_job import BaseJob
from mvp.atptour.aggregators.helpers import pivot_to_player_match

logger = logging.getLogger(__name__)


class StrokeAnalysisAggregator(BaseJob):
    """Aggregate stroke_analysis staged data to player-match level.

    Input: stage/{tournament}/stroke_analysis.parquet (match-level, p1/p2)
    Output: aggregate/atptour/stroke_analysis.parquet (player-match level)
    """

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> pl.DataFrame | None:
        """Aggregate all staged stroke_analysis data."""
        stage_root = self.build_path("stage", "tournaments")
        if not stage_root.exists():
            logger.warning("No staged data at %s", stage_root)
            return None

        parquet_files = list(stage_root.glob("**/stroke_analysis.parquet"))
        if not parquet_files:
            logger.info("No stroke_analysis.parquet files found")
            return None

        logger.info("Aggregating %d stroke_analysis files", len(parquet_files))

        all_dfs = []
        for pq_file in parquet_files:
            try:
                df = pl.read_parquet(pq_file)
                df = df.filter(~pl.col("is_doubles"))
                if len(df) > 0:
                    all_dfs.append(df)
            except Exception as e:
                logger.warning("Failed to read %s: %s", pq_file, e)

        if not all_dfs:
            logger.info("No singles matches found")
            return None

        combined = pl.concat(all_dfs)
        result = pivot_to_player_match(combined)

        output = self.build_path("aggregate", "stroke_analysis.parquet")
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)

        logger.info("Aggregated %d player-matches to %s", len(result), output)
        return result

