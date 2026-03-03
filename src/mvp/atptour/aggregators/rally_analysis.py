"""Aggregator for rally_analysis match-level data to player-match level."""

import logging
from pathlib import Path

import polars as pl

from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)


class RallyAnalysisAggregator(BaseJob):
    """Aggregate rally_analysis staged data to player-match level.

    Input: stage/{tournament}/rally_analysis.parquet (match-level, p1/p2)
    Output: aggregate/atptour/rally_analysis.parquet (player-match level)
    """

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> pl.DataFrame | None:
        """Aggregate all staged rally_analysis data."""
        stage_root = self.build_path("stage", "tournaments")
        if not stage_root.exists():
            logger.warning("No staged data at %s", stage_root)
            return None

        parquet_files = list(stage_root.glob("**/rally_analysis.parquet"))
        if not parquet_files:
            logger.info("No rally_analysis.parquet files found")
            return None

        logger.info("Aggregating %d rally_analysis files", len(parquet_files))

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
        result = self._pivot_to_player_match(combined)

        output = self.build_path("aggregate", "rally_analysis.parquet")
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)

        logger.info("Aggregated %d player-matches to %s", len(result), output)
        return result

    def _pivot_to_player_match(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pivot match-level p1_/p2_ data to player-match level player_/opp_."""
        p1_cols = [c for c in df.columns if c.startswith("p1_") and c not in ("p1_id", "p2_id")]
        p2_cols = [c for c in df.columns if c.startswith("p2_") and c not in ("p1_id", "p2_id")]

        # P1 perspective: p1_* -> player_*, p2_* -> opp_*
        p1_renames = {c: "player_" + c[3:] for c in p1_cols}
        p1_renames.update({c: "opp_" + c[3:] for c in p2_cols})
        p1_renames["p1_id"] = "player_id"
        p1_renames["p2_id"] = "opp_id"

        # P2 perspective: p2_* -> player_*, p1_* -> opp_*
        p2_renames = {c: "player_" + c[3:] for c in p2_cols}
        p2_renames.update({c: "opp_" + c[3:] for c in p1_cols})
        p2_renames["p2_id"] = "player_id"
        p2_renames["p1_id"] = "opp_id"

        p1_perspective = df.rename(p1_renames)
        p2_perspective = df.rename(p2_renames).select(p1_perspective.columns)

        return pl.concat([p1_perspective, p2_perspective])
