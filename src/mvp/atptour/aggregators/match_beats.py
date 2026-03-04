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

        combined = pl.concat(all_dfs, how="diagonal_relaxed")
        result = self._aggregate_match_level(combined)
        result = self._pivot_to_player_match(result)

        output = self.build_path("aggregate", "match_beats.parquet")
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)

        logger.info("Aggregated %d matches to %s", len(result), output)
        return result

    def _aggregate_match_level(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aggregate points to match level with p1_/p2_ columns."""
        main = df.group_by(
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
            # Service games
            ((pl.col("server") == "1") & (pl.col("point_num") == 1)).sum().alias("p1_service_games"),
            ((pl.col("server") == "2") & (pl.col("point_num") == 1)).sum().alias("p2_service_games"),
            # Serve speed - P1
            pl.col("serve_speed").filter((pl.col("server") == "1") & (pl.col("serve") == 1)).mean().alias("p1_avg_1st_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "1") & (pl.col("serve") == 1)).max().alias("p1_max_1st_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "1") & (pl.col("serve") == 1)).std().alias("p1_std_1st_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "1") & (pl.col("serve") == 2)).mean().alias("p1_avg_2nd_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "1") & (pl.col("serve") == 2)).max().alias("p1_max_2nd_serve_speed"),
            pl.col("fault_serve_speed").filter(pl.col("server") == "1").mean().alias("p1_avg_fault_serve_speed"),
            pl.col("fault_serve_speed").filter(pl.col("server") == "1").max().alias("p1_max_fault_serve_speed"),
            # Serve speed - P2
            pl.col("serve_speed").filter((pl.col("server") == "2") & (pl.col("serve") == 1)).mean().alias("p2_avg_1st_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "2") & (pl.col("serve") == 1)).max().alias("p2_max_1st_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "2") & (pl.col("serve") == 1)).std().alias("p2_std_1st_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "2") & (pl.col("serve") == 2)).mean().alias("p2_avg_2nd_serve_speed"),
            pl.col("serve_speed").filter((pl.col("server") == "2") & (pl.col("serve") == 2)).max().alias("p2_max_2nd_serve_speed"),
            pl.col("fault_serve_speed").filter(pl.col("server") == "2").mean().alias("p2_avg_fault_serve_speed"),
            pl.col("fault_serve_speed").filter(pl.col("server") == "2").max().alias("p2_max_fault_serve_speed"),
            # Rally overall
            (~pl.col("rally_length_missing")).sum().alias("rally_points_with_data"),
            ((pl.col("p1_rally_shots") + pl.col("p2_rally_shots") <= 4) & ~pl.col("rally_length_missing")).sum().alias("rally_short_count"),
            ((pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).is_between(5, 8) & ~pl.col("rally_length_missing")).sum().alias("rally_medium_count"),
            ((pl.col("p1_rally_shots") + pl.col("p2_rally_shots") >= 9) & ~pl.col("rally_length_missing")).sum().alias("rally_long_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter(~pl.col("rally_length_missing")).sum().alias("rally_total_shots"),
            # Rally by outcome - P1
            ((pl.col("scorer") == "1") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_won_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("scorer") == "1") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_won_shots"),
            ((pl.col("scorer") == "2") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_lost_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("scorer") == "2") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_lost_shots"),
            # Rally by serve context - P1
            ((pl.col("server") == "1") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_serving_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("server") == "1") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_serving_shots"),
            ((pl.col("server") == "2") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_returning_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("server") == "2") & ~pl.col("rally_length_missing")).sum().alias("p1_rally_returning_shots"),
            # Rally by outcome - P2
            ((pl.col("scorer") == "2") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_won_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("scorer") == "2") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_won_shots"),
            ((pl.col("scorer") == "1") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_lost_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("scorer") == "1") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_lost_shots"),
            # Rally by serve context - P2
            ((pl.col("server") == "2") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_serving_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("server") == "2") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_serving_shots"),
            ((pl.col("server") == "1") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_returning_count"),
            (pl.col("p1_rally_shots") + pl.col("p2_rally_shots")).filter((pl.col("server") == "1") & ~pl.col("rally_length_missing")).sum().alias("p2_rally_returning_shots"),
            # Crucial points
            (pl.col("is_crucial_point") & (pl.col("scorer") == "1")).sum().alias("p1_crucial_points_won"),
            pl.col("is_crucial_point").sum().alias("p1_crucial_points_played"),
            (pl.col("is_crucial_point") & (pl.col("scorer") == "2")).sum().alias("p2_crucial_points_won"),
            pl.col("is_crucial_point").sum().alias("p2_crucial_points_played"),
            # Tiebreak points
            (pl.col("is_tiebreak") & (pl.col("scorer") == "1")).sum().alias("p1_tiebreak_points_won"),
            pl.col("is_tiebreak").sum().alias("p1_tiebreak_points_played"),
            (pl.col("is_tiebreak") & (pl.col("scorer") == "2")).sum().alias("p2_tiebreak_points_won"),
            pl.col("is_tiebreak").sum().alias("p2_tiebreak_points_played"),
            # Game quality - P1 serving (deduplicate by first point of each game)
            pl.col("easy_hold").filter((pl.col("server") == "1") & (pl.col("point_num") == 1)).sum().alias("p1_easy_holds"),
            pl.col("difficult_hold").filter((pl.col("server") == "1") & (pl.col("point_num") == 1)).sum().alias("p1_difficult_holds"),
            pl.col("multiple_deuces").filter((pl.col("server") == "1") & (pl.col("point_num") == 1)).sum().alias("p1_games_multiple_deuces"),
            # Game quality - P2 serving
            pl.col("easy_hold").filter((pl.col("server") == "2") & (pl.col("point_num") == 1)).sum().alias("p2_easy_holds"),
            pl.col("difficult_hold").filter((pl.col("server") == "2") & (pl.col("point_num") == 1)).sum().alias("p2_difficult_holds"),
            pl.col("multiple_deuces").filter((pl.col("server") == "2") & (pl.col("point_num") == 1)).sum().alias("p2_games_multiple_deuces"),
            # Games won (one entry per game via point_num == 1)
            (pl.col("game_winner").filter(pl.col("point_num") == 1) == "1").sum().alias("p1_games_won"),
            (pl.col("game_winner").filter(pl.col("point_num") == 1) == "2").sum().alias("p2_games_won"),
            # Match context
            pl.col("match_duration_at_point").max().alias("match_duration"),
            pl.col("set_num").max().alias("sets_played"),
        )

        # Sets won (needs set-level dedup)
        sets_won = (
            df.select(["tournament_id", "year", "match_id", "set_num", "set_winner"])
            .unique(subset=["tournament_id", "year", "match_id", "set_num"])
            .group_by(["tournament_id", "year", "match_id"])
            .agg(
                (pl.col("set_winner") == "1").sum().alias("p1_sets_won"),
                (pl.col("set_winner") == "2").sum().alias("p2_sets_won"),
            )
        )

        return main.join(sets_won, on=["tournament_id", "year", "match_id"], how="left")

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
