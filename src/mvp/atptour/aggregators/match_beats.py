"""Aggregator for MatchBeats point-level data to player-match level."""

import logging
from pathlib import Path

import polars as pl

from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)


class MatchBeatsAggregator(BaseJob):
    """Aggregate point-level MatchBeats data to player-match level.

    Input: stage/{tournament}/match_beats.parquet (point-level)
    Output: aggregate/atptour/match_beats.parquet (player-match level)
    """

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> pl.DataFrame | None:
        """Aggregate all staged match_beats data."""
        stage_root = self.build_path("stage", "tournaments")
        if not stage_root.exists():
            logger.warning("No staged data at %s", stage_root)
            return None

        parquet_files = list(stage_root.glob("**/match_beats.parquet"))
        if not parquet_files:
            logger.info("No match_beats.parquet files found")
            return None

        logger.info("Aggregating %d match_beats files", len(parquet_files))

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
        result = self._aggregate_match_level(combined)

        output = self.build_path("aggregate", "match_beats.parquet")
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)

        logger.info("Aggregated %d matches to %s", len(result), output)
        return result

    def _aggregate_match_level(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aggregate points to match level with p1_/p2_ columns."""
        return df.group_by(
            ["tournament_id", "year", "match_id", "p1_id", "p2_id"]
        ).agg(
            pl.len().alias("total_points"),
            (pl.col("scorer") == "1").sum().alias("p1_points_won"),
            (pl.col("scorer") == "2").sum().alias("p2_points_won"),
            # Serve stats - P1
            (pl.col("server") == "1").sum().alias("p1_service_points"),
            ((pl.col("server") == "1") & (pl.col("serve") == 1)).sum().alias("p1_first_serve_points"),
            ((pl.col("server") == "1") & (pl.col("serve") == 1) & (pl.col("scorer") == "1")).sum().alias("p1_first_serve_won"),
            ((pl.col("server") == "1") & (pl.col("serve") == 2)).sum().alias("p1_second_serve_points"),
            ((pl.col("server") == "1") & (pl.col("serve") == 2) & (pl.col("scorer") == "1")).sum().alias("p1_second_serve_won"),
            ((pl.col("server") == "1") & (pl.col("result") == "A")).sum().alias("p1_aces"),
            ((pl.col("server") == "1") & (pl.col("result") == "DF")).sum().alias("p1_dfs"),
            # Serve stats - P2
            (pl.col("server") == "2").sum().alias("p2_service_points"),
            ((pl.col("server") == "2") & (pl.col("serve") == 1)).sum().alias("p2_first_serve_points"),
            ((pl.col("server") == "2") & (pl.col("serve") == 1) & (pl.col("scorer") == "2")).sum().alias("p2_first_serve_won"),
            ((pl.col("server") == "2") & (pl.col("serve") == 2)).sum().alias("p2_second_serve_points"),
            ((pl.col("server") == "2") & (pl.col("serve") == 2) & (pl.col("scorer") == "2")).sum().alias("p2_second_serve_won"),
            ((pl.col("server") == "2") & (pl.col("result") == "A")).sum().alias("p2_aces"),
            ((pl.col("server") == "2") & (pl.col("result") == "DF")).sum().alias("p2_dfs"),
            # Return stats (return points = opponent serving)
            (pl.col("server") == "2").sum().alias("p1_return_points"),
            ((pl.col("server") == "2") & (pl.col("scorer") == "1")).sum().alias("p1_return_points_won"),
            (pl.col("server") == "1").sum().alias("p2_return_points"),
            ((pl.col("server") == "1") & (pl.col("scorer") == "2")).sum().alias("p2_return_points_won"),
            # Break points - P1
            ((pl.col("server") == "1") & pl.col("is_break_point")).sum().alias("p1_bp_faced"),
            ((pl.col("server") == "1") & pl.col("is_break_point") & (pl.col("scorer") == "1")).sum().alias("p1_bp_saved"),
            ((pl.col("server") == "2") & pl.col("is_break_point")).sum().alias("p1_bp_opportunities"),
            ((pl.col("server") == "2") & pl.col("is_break_point") & (pl.col("scorer") == "1")).sum().alias("p1_bp_converted"),
            # Break points - P2
            ((pl.col("server") == "2") & pl.col("is_break_point")).sum().alias("p2_bp_faced"),
            ((pl.col("server") == "2") & pl.col("is_break_point") & (pl.col("scorer") == "2")).sum().alias("p2_bp_saved"),
            ((pl.col("server") == "1") & pl.col("is_break_point")).sum().alias("p2_bp_opportunities"),
            ((pl.col("server") == "1") & pl.col("is_break_point") & (pl.col("scorer") == "2")).sum().alias("p2_bp_converted"),
            # Winners - by the player who hit the winner (scorer won with W)
            ((pl.col("scorer") == "1") & (pl.col("result") == "W")).sum().alias("p1_winners"),
            ((pl.col("scorer") == "2") & (pl.col("result") == "W")).sum().alias("p2_winners"),
            # Errors - by the player who made the error (they lost the point)
            ((pl.col("scorer") == "2") & (pl.col("result") == "UE")).sum().alias("p1_ues"),
            ((pl.col("scorer") == "2") & (pl.col("result") == "FE")).sum().alias("p1_fes"),
            ((pl.col("scorer") == "1") & (pl.col("result") == "UE")).sum().alias("p2_ues"),
            ((pl.col("scorer") == "1") & (pl.col("result") == "FE")).sum().alias("p2_fes"),
        )
