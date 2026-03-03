"""Aggregator for MatchBeats point-level data to match-level statistics."""

import logging
from pathlib import Path

import polars as pl

from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)


class MatchBeatsAggregator(BaseJob):
    """Aggregate point-level MatchBeats data to match-level statistics.

    Input: stage/{tournament}/match_beats.parquet (point-level)
    Output: aggregate/{tournament}/match_beats.parquet (match-level, one row per match)

    Statistics are computed separately for p1 and p2, representing the
    players as they appear in the raw data (team 1 and team 2).
    """

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> None:
        """Aggregate all staged match_beats data."""
        stage_root = self.build_path("stage", "tournaments")
        if not stage_root.exists():
            logger.warning("No staged data at %s", stage_root)
            return

        # Find all match_beats.parquet files
        parquet_files = list(stage_root.glob("**/match_beats.parquet"))
        if not parquet_files:
            logger.info("No match_beats.parquet files found")
            return

        logger.info("Aggregating %d match_beats files", len(parquet_files))

        all_aggregated = []
        for pq_file in parquet_files:
            try:
                df = pl.read_parquet(pq_file)
                agg = self._aggregate_tournament(df, pq_file)
                if agg is not None and len(agg) > 0:
                    all_aggregated.append(agg)
            except Exception as e:
                logger.warning("Failed to aggregate %s: %s", pq_file, e)
                continue

        if not all_aggregated:
            logger.info("No matches aggregated")
            return

        result = pl.concat(all_aggregated)

        output = self.build_path("aggregate", "match_beats.parquet")
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)

        logger.info(
            "Aggregated %d matches from %d tournaments to %s",
            len(result),
            len(all_aggregated),
            output,
        )

    def _aggregate_tournament(
        self, df: pl.DataFrame, source: Path
    ) -> pl.DataFrame | None:
        """Aggregate a single tournament's point data to match level."""
        if len(df) == 0:
            return None

        # Filter to singles only for now (doubles has different dynamics)
        df = df.filter(~pl.col("is_doubles"))
        if len(df) == 0:
            return None

        # Aggregate by match
        agg = (
            df.group_by(["tournament_id", "year", "match_id", "p1_id", "p2_id"])
            .agg(self._build_aggregations())
            .with_columns(pl.lit(str(source)).alias("source_file"))
        )

        return agg

    def _build_aggregations(self) -> list:
        """Build list of aggregation expressions."""
        return [
            # Total points
            pl.len().alias("total_points"),
            # Points by result type
            (pl.col("result") == "A").sum().alias("p1_aces"),
            (pl.col("result") == "DF").sum().alias("total_double_faults"),
            (pl.col("result") == "W").sum().alias("total_winners"),
            (pl.col("result") == "UE").sum().alias("total_unforced_errors"),
            (pl.col("result") == "FE").sum().alias("total_forced_errors"),
            # Points won by each player
            (pl.col("scorer") == "1").sum().alias("p1_points_won"),
            (pl.col("scorer") == "2").sum().alias("p2_points_won"),
            # Serve stats - p1 serving
            (
                (pl.col("server") == "1") & (pl.col("result") == "A")
            ).sum().alias("p1_aces_served"),
            (
                (pl.col("server") == "1") & (pl.col("result") == "DF")
            ).sum().alias("p1_double_faults"),
            (
                (pl.col("server") == "1") & (pl.col("scorer") == "1")
            ).sum().alias("p1_service_points_won"),
            (pl.col("server") == "1").sum().alias("p1_service_points_played"),
            # Serve stats - p2 serving
            (
                (pl.col("server") == "2") & (pl.col("result") == "A")
            ).sum().alias("p2_aces_served"),
            (
                (pl.col("server") == "2") & (pl.col("result") == "DF")
            ).sum().alias("p2_double_faults"),
            (
                (pl.col("server") == "2") & (pl.col("scorer") == "2")
            ).sum().alias("p2_service_points_won"),
            (pl.col("server") == "2").sum().alias("p2_service_points_played"),
            # Return stats (points won when opponent serving)
            (
                (pl.col("server") == "2") & (pl.col("scorer") == "1")
            ).sum().alias("p1_return_points_won"),
            (
                (pl.col("server") == "1") & (pl.col("scorer") == "2")
            ).sum().alias("p2_return_points_won"),
            # Serve speed (1st serve only)
            pl.col("serve_speed")
            .filter(pl.col("serve") == 1)
            .mean()
            .alias("avg_first_serve_speed"),
            pl.col("serve_speed")
            .filter(pl.col("serve") == 1)
            .max()
            .alias("max_first_serve_speed"),
            pl.col("serve_speed")
            .filter(pl.col("serve") == 2)
            .mean()
            .alias("avg_second_serve_speed"),
            # Rally length
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots"))
            .filter(~pl.col("rally_length_missing"))
            .mean()
            .alias("avg_rally_length"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots"))
            .filter(~pl.col("rally_length_missing"))
            .max()
            .alias("max_rally_length"),
            # Rally length buckets (excluding serve points)
            (
                (pl.col("p1_rally_shots") + pl.col("p2_rally_shots") <= 4)
                & (~pl.col("rally_length_missing"))
                & (pl.col("result") != "A")
                & (pl.col("result") != "DF")
            ).sum().alias("short_rally_points"),
            (
                (pl.col("p1_rally_shots") + pl.col("p2_rally_shots") > 4)
                & (pl.col("p1_rally_shots") + pl.col("p2_rally_shots") <= 8)
                & (~pl.col("rally_length_missing"))
            ).sum().alias("medium_rally_points"),
            (
                (pl.col("p1_rally_shots") + pl.col("p2_rally_shots") > 8)
                & (~pl.col("rally_length_missing"))
            ).sum().alias("long_rally_points"),
            # Break points
            pl.col("is_break_point").sum().alias("total_break_points"),
            # Break points faced by p1 (when p1 serving)
            (
                (pl.col("server") == "1") & pl.col("is_break_point")
            ).sum().alias("p1_break_points_faced"),
            # Break points saved by p1 (p1 serving, break point, p1 wins)
            (
                (pl.col("server") == "1")
                & pl.col("is_break_point")
                & (pl.col("scorer") == "1")
            ).sum().alias("p1_break_points_saved"),
            # Break points faced by p2
            (
                (pl.col("server") == "2") & pl.col("is_break_point")
            ).sum().alias("p2_break_points_faced"),
            # Break points saved by p2
            (
                (pl.col("server") == "2")
                & pl.col("is_break_point")
                & (pl.col("scorer") == "2")
            ).sum().alias("p2_break_points_saved"),
            # Game stats
            pl.col("game_duration").filter(pl.col("point_num") == 1).mean().alias(
                "avg_game_duration"
            ),
            pl.col("easy_hold")
            .filter(pl.col("point_num") == 1)
            .sum()
            .alias("total_easy_holds"),
            pl.col("difficult_hold")
            .filter(pl.col("point_num") == 1)
            .sum()
            .alias("total_difficult_holds"),
            # Sets and games played
            pl.col("set_num").max().alias("sets_played"),
            pl.col("game_num").max().alias("max_games_in_set"),
        ]
