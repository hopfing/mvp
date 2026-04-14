"""Aggregator for MatchBeats point-level data with reconstructed score state.

Produces one row per point across all matches with match_beats coverage (2022+).
Output feeds score-state-dependent serve modeling (see
projection/iid/score_state_model.py and projection/iid/serve_discovery.py).
"""

import logging
from pathlib import Path

import polars as pl

from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)

_NULLABLE_FLOAT_COLS = ["serve_speed", "fault_serve_speed"]

# Tennis game score encoding used by Infosys Match Centre ("AD" for advantage).
_SERVER_GAME_POINT = (
    (pl.col("game_score_server") == "AD")
    | (
        (pl.col("game_score_server") == "40")
        & pl.col("game_score_returner").is_in(["0", "15", "30"])
    )
)
_RETURNER_GAME_POINT = (
    (pl.col("game_score_returner") == "AD")
    | (
        (pl.col("game_score_returner") == "40")
        & pl.col("game_score_server").is_in(["0", "15", "30"])
    )
)


class MatchBeatsPointsAggregator(BaseJob):
    """Aggregate point-level MatchBeats data to a cross-tournament point-level table.

    Input:  stage/tournaments/**/match_beats.parquet
    Output: aggregate/atptour/match_beats_points.parquet
    """

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self) -> pl.DataFrame | None:
        stage_root = self.build_path("stage", "tournaments")
        if not stage_root.exists():
            logger.warning("No staged data at %s", stage_root)
            return None

        parquet_files = list(stage_root.glob("**/match_beats.parquet"))
        if not parquet_files:
            logger.info("No match_beats.parquet files found")
            return None

        logger.info("Reading %d match_beats files", len(parquet_files))
        frames: list[pl.DataFrame] = []
        for pq_file in parquet_files:
            try:
                df = pl.read_parquet(pq_file)
                df = df.cast({c: pl.Float64 for c in _NULLABLE_FLOAT_COLS if c in df.columns})
                df = df.filter(~pl.col("is_doubles"))
                if len(df) > 0:
                    frames.append(df)
            except Exception as e:
                logger.warning("Failed to read %s: %s", pq_file, e)
        if not frames:
            logger.info("No singles point data found")
            return None

        combined = pl.concat(frames).sort(
            ["tournament_id", "year", "match_id", "set_num", "game_num", "point_num"]
        )

        combined = self._derive_server_perspective(combined)
        combined = self._derive_set_and_match_scores(combined)
        combined = self._join_match_metadata(combined)
        combined = self._derive_set_and_match_points(combined)

        result = combined.with_columns(
            (pl.col("scorer") == pl.col("server")).alias("point_won_by_server")
        ).select(
            [
                # Identity
                "match_uid", "tournament_id", "year", "circuit", "surface", "round",
                "effective_match_date", "best_of",
                "set_num", "game_num", "point_num",
                # Actors
                "server_id", "returner_id", "server",
                # Score state (pre-point)
                "game_score_server", "game_score_returner",
                "set_score_server_games", "set_score_returner_games",
                "sets_won_server", "sets_won_returner",
                "is_tiebreak", "is_break_point", "is_set_point", "is_match_point",
                "serve", "serve_speed",
                # Target
                "point_won_by_server",
            ]
        )

        output = self.build_path("aggregate", "match_beats_points.parquet")
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)

        logger.info("Aggregated %d point rows to %s", len(result), output)
        return result

    def _derive_server_perspective(self, df: pl.DataFrame) -> pl.DataFrame:
        """Shift raw (post-point) game scores to pre-point, then map to server perspective.

        Infosys' `tm1GameScore` / `tm2GameScore` are POST-point scores (what the score
        becomes after the point is played). The score-state model wants the PRE-point
        state (what the score is when the point starts), which we derive by shifting
        by 1 within each (match, set, game) group. The first point of each game has
        pre-state "0" for both players.
        """
        game_keys = ["tournament_id", "year", "match_id", "set_num", "game_num"]
        df = df.with_columns(
            [
                pl.col("p1_game_score").shift(1).over(game_keys).fill_null("0").alias("p1_game_score_pre"),
                pl.col("p2_game_score").shift(1).over(game_keys).fill_null("0").alias("p2_game_score_pre"),
            ]
        )
        is_p1_serving = pl.col("server") == "1"
        return df.with_columns(
            [
                pl.when(is_p1_serving).then(pl.col("p1_id")).otherwise(pl.col("p2_id")).alias("server_id"),
                pl.when(is_p1_serving).then(pl.col("p2_id")).otherwise(pl.col("p1_id")).alias("returner_id"),
                pl.when(is_p1_serving).then(pl.col("p1_game_score_pre")).otherwise(pl.col("p2_game_score_pre")).alias("game_score_server"),
                pl.when(is_p1_serving).then(pl.col("p2_game_score_pre")).otherwise(pl.col("p1_game_score_pre")).alias("game_score_returner"),
            ]
        )

    def _derive_set_and_match_scores(self, df: pl.DataFrame) -> pl.DataFrame:
        """Compute set_score_*_games (pre-current-game) and sets_won_* (pre-current-set).

        Strategy: dedupe to one row per game / set, cumsum game_winner / set_winner
        within match/set and match respectively, subtract current contribution to
        produce the "before this game/set" value, then join back to points.
        """
        match_set_game_keys = ["tournament_id", "year", "match_id", "set_num", "game_num"]
        match_set_keys = ["tournament_id", "year", "match_id", "set_num"]
        match_keys = ["tournament_id", "year", "match_id"]

        p1_won_int = pl.col("game_winner").eq("1").fill_null(False).cast(pl.Int32)
        p2_won_int = pl.col("game_winner").eq("2").fill_null(False).cast(pl.Int32)

        games_pre = (
            df.group_by(match_set_game_keys)
            .agg(pl.col("game_winner").first())
            .sort(match_set_game_keys)
            .with_columns(
                [
                    (p1_won_int.cum_sum().over(match_set_keys) - p1_won_int).alias("set_score_p1_before_game"),
                    (p2_won_int.cum_sum().over(match_set_keys) - p2_won_int).alias("set_score_p2_before_game"),
                ]
            )
            .select(match_set_game_keys + ["set_score_p1_before_game", "set_score_p2_before_game"])
        )

        p1_set_won_int = pl.col("set_winner").eq("1").fill_null(False).cast(pl.Int32)
        p2_set_won_int = pl.col("set_winner").eq("2").fill_null(False).cast(pl.Int32)
        sets_pre = (
            df.group_by(match_set_keys)
            .agg(pl.col("set_winner").first())
            .sort(match_set_keys)
            .with_columns(
                [
                    (p1_set_won_int.cum_sum().over(match_keys) - p1_set_won_int).alias("sets_won_p1_before_set"),
                    (p2_set_won_int.cum_sum().over(match_keys) - p2_set_won_int).alias("sets_won_p2_before_set"),
                ]
            )
            .select(match_set_keys + ["sets_won_p1_before_set", "sets_won_p2_before_set"])
        )

        df = df.join(games_pre, on=match_set_game_keys, how="left")
        df = df.join(sets_pre, on=match_set_keys, how="left")

        is_p1_serving = pl.col("server") == "1"
        return df.with_columns(
            [
                pl.when(is_p1_serving).then(pl.col("set_score_p1_before_game")).otherwise(pl.col("set_score_p2_before_game")).alias("set_score_server_games"),
                pl.when(is_p1_serving).then(pl.col("set_score_p2_before_game")).otherwise(pl.col("set_score_p1_before_game")).alias("set_score_returner_games"),
                pl.when(is_p1_serving).then(pl.col("sets_won_p1_before_set")).otherwise(pl.col("sets_won_p2_before_set")).alias("sets_won_server"),
                pl.when(is_p1_serving).then(pl.col("sets_won_p2_before_set")).otherwise(pl.col("sets_won_p1_before_set")).alias("sets_won_returner"),
            ]
        )

    def _join_match_metadata(self, df: pl.DataFrame) -> pl.DataFrame:
        matches_path = self.build_path("aggregate", "matches.parquet")
        if not matches_path.exists():
            raise FileNotFoundError(
                f"matches.parquet not found at {matches_path}; run MatchesAggregator first"
            )

        matches = pl.read_parquet(matches_path)
        match_meta = (
            matches.select(
                [
                    "tournament_id", "year", "match_id",
                    "match_uid", "circuit", "surface", "round",
                    "effective_match_date", "best_of",
                ]
            )
            .unique(subset=["tournament_id", "year", "match_id"])
        )
        return df.join(match_meta, on=["tournament_id", "year", "match_id"], how="inner")

    def _derive_set_and_match_points(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flag is_set_point and is_match_point.

        Non-tiebreak points: winning the point wins the game AND the resulting set
        score is a terminal win (6-with-2 margin or 7). Match point additionally
        requires the resulting sets-won to reach sets_to_win (2 for BO3, 3 for BO5).

        Tiebreak points: set to False. Tiebreak score-state derivation requires
        per-point tiebreak score, which is encoded inconsistently in the raw data;
        deferred to a follow-up.
        """
        server_set_win_if_game_win = (
            ((pl.col("set_score_server_games") + 1 >= 6) & (pl.col("set_score_server_games") + 1 - pl.col("set_score_returner_games") >= 2))
            | (pl.col("set_score_server_games") + 1 == 7)
        )
        returner_set_win_if_game_win = (
            ((pl.col("set_score_returner_games") + 1 >= 6) & (pl.col("set_score_returner_games") + 1 - pl.col("set_score_server_games") >= 2))
            | (pl.col("set_score_returner_games") + 1 == 7)
        )

        sets_to_win = pl.when(pl.col("best_of") == 5).then(3).otherwise(2)
        server_match_win_after = pl.col("sets_won_server") + 1 >= sets_to_win
        returner_match_win_after = pl.col("sets_won_returner") + 1 >= sets_to_win

        is_set_point_std = (
            (_SERVER_GAME_POINT & server_set_win_if_game_win)
            | (_RETURNER_GAME_POINT & returner_set_win_if_game_win)
        )
        is_match_point_std = (
            (_SERVER_GAME_POINT & server_set_win_if_game_win & server_match_win_after)
            | (_RETURNER_GAME_POINT & returner_set_win_if_game_win & returner_match_win_after)
        )

        return df.with_columns(
            [
                pl.when(pl.col("is_tiebreak")).then(False).otherwise(is_set_point_std).alias("is_set_point"),
                pl.when(pl.col("is_tiebreak")).then(False).otherwise(is_match_point_std).alias("is_match_point"),
            ]
        )
