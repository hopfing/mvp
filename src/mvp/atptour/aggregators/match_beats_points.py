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

    def is_stale(self) -> bool:
        """Whether the aggregate needs rebuilding from the staged tournaments.

        Mirrors the pipeline's staging idiom (`pipeline._feed_stage_is_stale`):
        rebuild when the output is missing or any staged input is newer than
        it. Only metadata is touched — the inputs are stat'd, not read.

        This exists because `run` is a FULL rebuild: it reads every staged
        `match_beats.parquet` and concatenates them. On the live 15-minute
        tick most runs have no newly-completed match with match-centre
        coverage, so gating turns those into a no-op and the full concat runs
        only when there is genuinely new point data.
        """
        stage_root = self.build_path("stage", "tournaments")
        if not stage_root.exists():
            return False
        staged = list(stage_root.glob("**/match_beats.parquet"))
        if not staged:
            return False
        output = self.build_path("aggregate", "match_beats_points.parquet")
        if not output.exists():
            return True
        output_mtime = output.stat().st_mtime
        return any(f.stat().st_mtime > output_mtime for f in staged)

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
        # Scanned per file and concatenated lazily, NOT through a single glob
        # scan. The staged files carry two schemas — `serve_speed` is Null-typed
        # in the files whose matches have no speed data and Double in the rest —
        # and a glob scan resolves the dtype from whichever file it happens to
        # match first, then fails at collect on the mismatch. Casting each file
        # to a common dtype before the concat is what makes the union
        # well-typed, so the per-file loop is load-bearing, not incidental.
        frames: list[pl.LazyFrame] = []
        unreadable = 0
        for pq_file in parquet_files:
            try:
                lf = pl.scan_parquet(pq_file)
                cols = lf.collect_schema().names()
                # Materialised and discarded purely to validate the file before
                # it joins the union. `collect_schema` reads the footer only, so
                # it cannot see row-level corruption, and the upstream writer
                # (transformers/match_beats.py:79) is a bare write_parquet — a
                # kill mid-write leaves a truncated file in staging. Without
                # this, one such file would fail the single collect below, and
                # since a failed rebuild never refreshes the output mtime,
                # `is_stale` would have every subsequent tick retry it: the
                # aggregate wedges permanently rather than losing one file's
                # points. Staged files are <=0.2MB, so the extra pass costs
                # nothing against the peak.
                lf.collect()
            except Exception as e:
                unreadable += 1
                logger.error("Skipping unreadable %s: %s", pq_file, e)
                continue
            lf = lf.cast({c: pl.Float64 for c in _NULLABLE_FLOAT_COLS if c in cols})
            frames.append(lf.filter(~pl.col("is_doubles")))
        if unreadable:
            logger.error(
                "Skipped %d unreadable match_beats file(s) — points are missing "
                "from this aggregate", unreadable,
            )
        if not frames:
            logger.info("No singles point data found")
            return None

        combined = pl.concat(frames).sort(
            ["tournament_id", "year", "match_id", "set_num", "game_num", "point_num"]
        )

        combined = self._derive_server_perspective(combined)
        combined = self._derive_rally_shots(combined)
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
                # Rally shape. Present in the staged data since 2022 and dropped
                # here until now, which is why no feature has been able to reach
                # it — the signal could not enter the model while the aggregate
                # discarded it. Null where the feed is unusable; see
                # `_derive_rally_shots`.
                "rally_shots_server", "rally_shots_returner", "rally_shots_total",
                "rally_length_missing",
                # Target
                "point_won_by_server",
            ]
        ).collect(engine="streaming")

        if result.is_empty():
            logger.info("No singles point data found")
            return None

        # Through save_parquet rather than a bare write_parquet: parquet writers
        # stream row groups to the destination progressively, so a kill during
        # the write leaves a truncated file at the target path carrying a fresh
        # mtime — which is exactly what `is_stale` reads to decide whether to
        # rebuild. The pipeline would then never retry, and every downstream
        # reader would be served the corrupt file. save_parquet writes to a tmp
        # path and renames.
        output = self.build_path("aggregate", "match_beats_points.parquet")
        self.save_parquet(result, output)

        logger.info("Aggregated %d point rows to %s", len(result), output)
        return result

    def _derive_server_perspective(self, df: pl.LazyFrame) -> pl.LazyFrame:
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
                pl.when(is_p1_serving).then(pl.col("p1_rally_shots")).otherwise(pl.col("p2_rally_shots")).alias("_rally_shots_server_raw"),
                pl.when(is_p1_serving).then(pl.col("p2_rally_shots")).otherwise(pl.col("p1_rally_shots")).alias("_rally_shots_returner_raw"),
            ]
        )

    # Per-player shot counts outside this range are the feed's sentinel space,
    # not rallies. The bound is deliberately loose: the 99th percentile of an
    # unflagged count is 9 and the median total rally is 2, so 100 cannot
    # exclude a real point, while the observed junk runs to -43,204 and 316,229.
    _RALLY_MIN_SHOTS = 0
    _RALLY_MAX_SHOTS = 100

    def _derive_rally_shots(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Server-oriented rally shot counts, nulled where the feed is unusable.

        `rally_length_missing` cannot carry this on its own. It marks 34.6% of
        points, and NONE of the 516,799 points holding a negative shot count are
        among them — the two sets are disjoint. Passing the flag through and
        trusting it would admit values from -43,204 to 316,229 into whatever
        rolling mean consumes these, which is not a subtle failure mode.

        So validity is derived here, at the boundary where the columns enter the
        aggregate, rather than left to each downstream consumer to rediscover.
        Invalid counts become NULL rather than 0: a rally of length zero is a
        real thing (an ace, a double fault) and must not be confused with one
        the feed could not report. `rally_length_missing` is still carried
        through, so a consumer can distinguish "the feed declared it missing"
        from "we rejected the value".
        """
        lo, hi = self._RALLY_MIN_SHOTS, self._RALLY_MAX_SHOTS
        s = pl.col("_rally_shots_server_raw")
        r = pl.col("_rally_shots_returner_raw")
        usable = (
            ~pl.col("rally_length_missing").fill_null(True)
            & s.is_between(lo, hi) & r.is_between(lo, hi)
        )
        return df.with_columns(
            [
                pl.when(usable).then(s).otherwise(None).cast(pl.Int32).alias("rally_shots_server"),
                pl.when(usable).then(r).otherwise(None).cast(pl.Int32).alias("rally_shots_returner"),
                pl.when(usable).then(s + r).otherwise(None).cast(pl.Int32).alias("rally_shots_total"),
            ]
        )

    def _derive_set_and_match_scores(self, df: pl.LazyFrame) -> pl.LazyFrame:
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

    def _join_match_metadata(self, df: pl.LazyFrame) -> pl.LazyFrame:
        matches_path = self.build_path("aggregate", "matches.parquet")
        if not matches_path.exists():
            raise FileNotFoundError(
                f"matches.parquet not found at {matches_path}; run MatchesAggregator first"
            )

        # Scanned, not read: matches.parquet is ~460MB across ~400 columns and
        # only these nine are wanted, so the projection is pushed into the read
        # rather than materialising the whole frame to select from it.
        match_meta = (
            pl.scan_parquet(matches_path)
            .select(
                [
                    "tournament_id", "year", "match_id",
                    "match_uid", "circuit", "surface", "round",
                    "effective_match_date", "best_of",
                ]
            )
            .unique(subset=["tournament_id", "year", "match_id"])
        )
        return df.join(match_meta, on=["tournament_id", "year", "match_id"], how="inner")

    def _derive_set_and_match_points(self, df: pl.LazyFrame) -> pl.LazyFrame:
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
