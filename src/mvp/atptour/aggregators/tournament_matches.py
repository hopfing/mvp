"""Per-tournament cross-dataset aggregation into player-match rows."""

import logging
from pathlib import Path

import polars as pl

from mvp.atptour.aggregators.helpers import (
    explode_match_stats,
    explode_results,
    explode_schedule,
)
from mvp.atptour.aggregators.match_beats import MatchBeatsAggregator
from mvp.atptour.aggregators.rally_analysis import RallyAnalysisAggregator
from mvp.atptour.aggregators.stroke_analysis import StrokeAnalysisAggregator
from mvp.common.base_job import BaseJob
from mvp.common.enums import Circuit

logger = logging.getLogger(__name__)

# Set score field suffixes for generating schema entries
_SET_SCORE_FIELDS = [
    f"{side}_set{n}_{kind}"
    for side in ("player", "opp")
    for n in range(1, 6)
    for kind in ("games", "tiebreak")
]

# All 26 stat field names (service, return, points)
_STAT_FIELDS = [
    "svc_aces", "svc_double_faults",
    "svc_first_serve_in", "svc_first_serve_att",
    "svc_first_serve_pts_won", "svc_first_serve_pts_played",
    "svc_second_serve_pts_won", "svc_second_serve_pts_played",
    "svc_bp_saved", "svc_bp_faced",
    "svc_games_played", "svc_serve_rating",
    "ret_first_serve_pts_won", "ret_first_serve_pts_played",
    "ret_second_serve_pts_won", "ret_second_serve_pts_played",
    "ret_bp_converted", "ret_bp_opportunities",
    "ret_games_played", "ret_return_rating",
    "pts_service_pts_won", "pts_service_pts_played",
    "pts_return_pts_won", "pts_return_pts_played",
    "pts_total_pts_won", "pts_total_pts_played",
]

# Match Beats fields that overlap with Match Stats — waterfalled
# Priority: match_stats > match_beats
# Maps: match_stats column -> match_beats player field suffix (after pivot)
_MB_WATERFALL_MAP = {
    "svc_aces": "aces",
    "svc_double_faults": "dfs",
    "svc_first_serve_pts_won": "first_serve_won",
    "svc_first_serve_pts_played": "first_serve_points",
    "svc_second_serve_pts_won": "second_serve_won",
    "svc_second_serve_pts_played": "second_serve_points",
    "svc_bp_saved": "bp_saved",
    "svc_bp_faced": "bp_faced",
    "ret_bp_converted": "bp_converted",
    "ret_bp_opportunities": "bp_opportunities",
    "pts_total_pts_won": "points_won",
    "pts_return_pts_won": "return_points_won",
    "pts_return_pts_played": "return_points",
    "pts_service_pts_played": "service_points",
}

# Match Beats player fields unique to match_beats (no match_stats equivalent)
_MB_UNIQUE_PLAYER_FIELDS = [
    "winners",
    "ues",
    "fes",
    "avg_1st_serve_speed",
    "max_1st_serve_speed",
    "avg_2nd_serve_speed",
    "max_2nd_serve_speed",
    "std_1st_serve_speed",
    "service_games",
    "avg_fault_serve_speed",
    "max_fault_serve_speed",
    "rally_won_count",
    "rally_won_shots",
    "rally_lost_count",
    "rally_lost_shots",
    "rally_serving_count",
    "rally_serving_shots",
    "rally_returning_count",
    "rally_returning_shots",
    "crucial_points_won",
    "crucial_points_played",
    "tiebreak_points_won",
    "tiebreak_points_played",
    "easy_holds",
    "difficult_holds",
    "games_multiple_deuces",
    "games_won",
    "sets_won",
]

# All match_beats player fields (waterfalled + unique) — used for load/prepare
_MB_ALL_PLAYER_FIELDS = list(_MB_WATERFALL_MAP.values()) + _MB_UNIQUE_PLAYER_FIELDS

_MB_SHARED_FIELDS = [
    "total_points",
    "rally_points_with_data",
    "rally_short_count",
    "rally_medium_count",
    "rally_long_count",
    "rally_total_shots",
    "mb_match_duration",
    "mb_sets_played",
]

# Stroke Analysis fields (after pivot: player_/opp_ prefix)
_STROKE_ANALYSIS_FIELDS = [
    "fh_winners", "fh_forced_errors", "fh_unforced_errors",
    "bh_winners", "bh_forced_errors", "bh_unforced_errors",
    "ground_stroke_winners", "ground_stroke_forced_errors",
    "ground_stroke_unforced_errors", "ground_stroke_others",
    "overhead_winners", "overhead_forced_errors",
    "overhead_unforced_errors", "overhead_others",
    "passing_winners", "passing_forced_errors",
    "passing_unforced_errors", "passing_others",
    "volley_winners", "volley_forced_errors",
    "volley_unforced_errors", "volley_others",
    "approach_winners", "approach_forced_errors",
    "approach_unforced_errors", "approach_others",
    "drop_shot_winners", "drop_shot_forced_errors",
    "drop_shot_unforced_errors", "drop_shot_others",
    "lob_winners", "lob_forced_errors",
    "lob_unforced_errors", "lob_others",
]

# Rally Analysis fields (after pivot: player_/opp_ prefix)
_RALLY_ANALYSIS_FIELDS = [
    "short_won", "short_err",
    "medium_won", "medium_err",
    "long_won", "long_err",
    "unclassified_won", "unclassified_err",
]

MATCHES_SCHEMA: dict[str, pl.DataType] = {
    # Join keys
    "match_uid": pl.String,
    "player_id": pl.String,
    "opp_id": pl.String,
    "draw_p1_id": pl.String,
    "tournament_id": pl.String,
    "year": pl.Int64,
    "circuit": pl.String,
    "draw_type": pl.String,
    "round": pl.String,
    # Waterfall fields
    "player_seed": pl.Int64,
    "player_entry": pl.String,
    "opp_seed": pl.Int64,
    "opp_entry": pl.String,
    "duration_seconds": pl.Int64,
    "surface": pl.String,
    "court_name": pl.String,
    "won": pl.Boolean,
    "match_id": pl.String,
    "city": pl.String,
    "singles_draw_size": pl.Int64,
    "doubles_draw_size": pl.Int64,
    # Results-only
    "result_type": pl.String,
    **{field: pl.Int64 for field in _SET_SCORE_FIELDS},
    # Match Stats-only
    "reason": pl.String,
    **{field: pl.Int64 for field in _STAT_FIELDS},
    **{f"opp_{field}": pl.Int64 for field in _STAT_FIELDS},
    "number_of_sets": pl.Int64,
    "sets_played": pl.Int64,
    "scoring_system": pl.String,
    "umpire_first_name": pl.String,
    "umpire_last_name": pl.String,
    "round_id": pl.Int64,
    "is_qualifier": pl.Boolean,
    # Tournament dates: Results > MatchStats waterfall
    "tournament_start_date": pl.Date,
    "tournament_end_date": pl.Date,
    "prize_money": pl.Int64,
    "currency": pl.String,
    # Schedule-only
    "match_date": pl.Date,
    "scheduled_datetime": pl.Datetime,
    "time_suffix": pl.String,
    "display_time": pl.String,
    "status": pl.String,
    "score": pl.String,
    "is_time_estimated": pl.Boolean,
    "court_match_num": pl.Int64,
    # Overview-only
    "tournament_name": pl.String,
    "country": pl.String,
    "sponsor_title": pl.String,
    "event_type": pl.String,
    "event_type_detail": pl.Int64,
    "indoor": pl.Boolean,
    "surface_detail": pl.String,
    "prize": pl.String,
    "total_financial_commitment": pl.String,
    # Doubles
    "player_partner_id": pl.String,
    "opp_partner_id": pl.String,
    # Match Beats - shared fields
    "total_points": pl.UInt32,
    "rally_points_with_data": pl.UInt32,
    "rally_short_count": pl.UInt32,
    "rally_medium_count": pl.UInt32,
    "rally_long_count": pl.UInt32,
    "rally_total_shots": pl.Int64,
    "mb_match_duration": pl.Int64,
    "mb_sets_played": pl.Int64,
    # Match Beats - unique player/opp fields (not waterfalled)
    **{f"mb_player_{f}": pl.Float64 if "speed" in f
       else pl.Int64 if "shots" in f
       else pl.UInt32
       for f in _MB_UNIQUE_PLAYER_FIELDS},
    **{f"mb_opp_{f}": pl.Float64 if "speed" in f
       else pl.Int64 if "shots" in f
       else pl.UInt32
       for f in _MB_UNIQUE_PLAYER_FIELDS},
    # Stroke Analysis - player/opp fields
    **{f"player_{f}": pl.Int64 for f in _STROKE_ANALYSIS_FIELDS},
    **{f"opp_{f}": pl.Int64 for f in _STROKE_ANALYSIS_FIELDS},
    # Rally Analysis - player/opp fields
    **{f"player_{f}": pl.Int64 for f in _RALLY_ANALYSIS_FIELDS},
    **{f"opp_{f}": pl.Int64 for f in _RALLY_ANALYSIS_FIELDS},
    "ra_points_missing": pl.Boolean,
}


class TournamentMatchesAggregator(BaseJob):
    """Aggregate staged tournament data into unified player-match rows.

    Loads Results, Match Stats, Schedule, and Overview parquets for a
    single tournament, explodes to player-match grain, joins, and applies
    a priority waterfall to resolve overlapping fields.
    """

    def __init__(
        self,
        circuit: Circuit,
        tid: str,
        year: int,
        data_root: Path | None = None,
    ):
        super().__init__(domain="atptour", data_root=data_root)
        self.circuit = circuit
        self.tid = tid
        self.year = year

    @property
    def _tournament_rel_path(self) -> str:
        return f"tournaments/{self.circuit.value}/{self.tid}/{self.year}"

    def aggregate(self) -> pl.DataFrame:
        """Load staged parquets, explode, join, waterfall, return output."""
        # Step 1: Load staged parquets
        results_df = self._load_parquet("results.parquet")
        match_stats_df = self._load_parquet("match_stats.parquet")
        schedule_df = self._load_parquet("schedule.parquet")
        overview_df = self._load_parquet("overview.parquet")

        # All empty -> return empty
        if (
            results_df.is_empty()
            and match_stats_df.is_empty()
            and schedule_df.is_empty()
            and overview_df.is_empty()
        ):
            return pl.DataFrame()

        # Step 2: P1/P2 authority alignment
        authority = self._build_authority(schedule_df, match_stats_df, results_df)
        results_df = self._align_p1p2(results_df, authority)
        match_stats_df = self._align_p1p2(match_stats_df, authority)
        # Schedule is the top authority, never needs swapping

        # Step 3: Explode to player-match
        ex_results = (
            explode_results(results_df)
            if results_df.width > 0
            else pl.DataFrame()
        )
        ex_stats = (
            explode_match_stats(match_stats_df)
            if match_stats_df.width > 0
            else pl.DataFrame()
        )
        ex_schedule = (
            explode_schedule(schedule_df)
            if schedule_df.width > 0
            else pl.DataFrame()
        )

        # Step 4: FULL OUTER JOIN Results + Schedule
        joined = self._join_results_schedule(ex_results, ex_schedule)

        # Step 4b: Drop stale schedule-only rows replaced by actual results
        joined = self._drop_replaced_schedule_rows(joined)

        # Step 5: LEFT JOIN Match Stats
        joined = self._join_match_stats(joined, ex_stats)

        # Step 6: Coalesce join key fields before Overview join so
        # schedule-only rows (null Results keys) can match Overview.
        for key in ["tournament_id", "year", "circuit"]:
            variants = [
                c for c in joined.columns
                if c == key or c.startswith(f"{key}_")
            ]
            if len(variants) > 1:
                joined = joined.with_columns(
                    pl.coalesce([pl.col(v) for v in variants]).alias(key)
                )

        # Step 7: LEFT JOIN Overview
        joined = self._join_overview(joined, overview_df)

        # Step 8: Coalesce match_id before Match Beats join and normalize case
        match_id_variants = self._find_variants(joined, "match_id")
        if len(match_id_variants) > 1:
            joined = joined.with_columns(
                pl.coalesce([pl.col(v) for v in match_id_variants]).alias(
                    "match_id"
                )
            )
        if "match_id" in joined.columns:
            joined = joined.with_columns(
                pl.col("match_id").str.to_uppercase().alias("match_id")
            )

        # Step 9: LEFT JOIN Match Beats
        match_beats_df = self._load_and_prepare_match_beats()
        joined = self._join_match_beats(joined, match_beats_df)

        # Step 10: LEFT JOIN Stroke Analysis
        stroke_df = self._load_and_prepare_stroke_analysis()
        joined = self._join_stroke_analysis(joined, stroke_df)

        # Step 11: LEFT JOIN Rally Analysis
        rally_df = self._load_and_prepare_rally_analysis()
        joined = self._join_rally_analysis(joined, rally_df)

        # Step 12: Add draw_p1_id from authority
        if authority:
            authority_df = pl.DataFrame({
                "match_uid": list(authority.keys()),
                "draw_p1_id": list(authority.values()),
            })
            joined = joined.join(authority_df, on="match_uid", how="left")

        # Step 13: Waterfall resolution
        result = self._apply_waterfall(joined)

        return result

    def _load_parquet(self, filename: str) -> pl.DataFrame:
        path = self.build_path("stage", self._tournament_rel_path, filename)
        if not path.exists():
            return pl.DataFrame()
        logger.info("Loading %s", self._display_path(path))
        return pl.read_parquet(path)

    def _build_authority(
        self,
        schedule_df: pl.DataFrame,
        match_stats_df: pl.DataFrame,
        results_df: pl.DataFrame,
    ) -> dict[str, str]:
        """Build authority lookup: match_uid -> authoritative p1_id.

        Priority: Schedule > MatchStats > Results.
        """
        authority: dict[str, str] = {}

        # Results (lowest priority, fills gaps)
        if not results_df.is_empty() and "match_uid" in results_df.columns:
            for row in results_df.select("match_uid", "p1_id").iter_rows():
                uid, p1 = row
                if uid is not None:
                    authority[uid] = p1

        # MatchStats (overrides results)
        if not match_stats_df.is_empty() and "match_uid" in match_stats_df.columns:
            for row in match_stats_df.select("match_uid", "p1_id").iter_rows():
                uid, p1 = row
                if uid is not None:
                    authority[uid] = p1

        # Schedule (highest priority, overrides everything)
        if not schedule_df.is_empty() and "match_uid" in schedule_df.columns:
            for row in schedule_df.select("match_uid", "p1_id").iter_rows():
                uid, p1 = row
                if uid is not None:
                    authority[uid] = p1

        return authority

    def _align_p1p2(
        self, df: pl.DataFrame, authority: dict[str, str]
    ) -> pl.DataFrame:
        """Swap p1/p2 columns where this source disagrees with authority."""
        if df.is_empty() or "match_uid" not in df.columns:
            return df

        # Find which rows need swapping
        needs_swap: list[bool] = []
        for row in df.select("match_uid", "p1_id").iter_rows():
            uid, p1 = row
            if uid is not None and uid in authority and authority[uid] != p1:
                needs_swap.append(True)
            else:
                needs_swap.append(False)

        if not any(needs_swap):
            return df

        swap_mask = pl.Series("_swap", needs_swap)
        df = df.with_columns(swap_mask)

        # Build swap expressions for each p1_X / p2_X pair
        swap_exprs: list[pl.Expr] = []
        for col in df.columns:
            if not col.startswith("p1_"):
                continue
            suffix = col[3:]
            p2_col = f"p2_{suffix}"
            if p2_col in df.columns:
                swap_exprs.append(
                    pl.when(pl.col("_swap"))
                    .then(pl.col(p2_col))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                swap_exprs.append(
                    pl.when(pl.col("_swap"))
                    .then(pl.col(col))
                    .otherwise(pl.col(p2_col))
                    .alias(p2_col)
                )

        if swap_exprs:
            df = df.with_columns(swap_exprs)

        df = df.drop("_swap")

        return df

    def _join_results_schedule(
        self, ex_results: pl.DataFrame, ex_schedule: pl.DataFrame
    ) -> pl.DataFrame:
        """FULL OUTER JOIN exploded Results + Schedule on (match_uid, player_id)."""
        if ex_results.is_empty() and ex_schedule.is_empty():
            return pl.DataFrame()
        if ex_results.is_empty():
            return ex_schedule
        if ex_schedule.is_empty():
            return ex_results

        return ex_results.join(
            ex_schedule,
            on=["match_uid", "player_id"],
            how="full",
            coalesce=True,
            suffix="_schedule",
        )

    def _drop_replaced_schedule_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """Drop schedule-only rows where the player has a result in the same round.

        When a draw changes (e.g. player withdrawal), the schedule may show
        the original matchup while results contain the replacement matchup.
        If the same player appears in the same round with a different opponent
        — one from schedule (won is null) and one from results — the
        schedule-only row is stale and should be dropped.

        Round Robin (RR) is excluded since players legitimately have multiple
        matches in the same round.
        """
        if df.is_empty():
            return df

        # Need won column (from results) and round info
        if "won" not in df.columns:
            return df

        # Build effective round/opp from both sources
        round_col = "round"
        round_sched = "round_schedule"
        opp_col = "opp_id"
        opp_sched = "opp_id_schedule"

        has_round = round_col in df.columns
        has_round_sched = round_sched in df.columns
        has_opp = opp_col in df.columns
        has_opp_sched = opp_sched in df.columns

        if not (has_round or has_round_sched):
            return df

        # Add working columns
        work = df.with_columns(
            pl.coalesce(
                [pl.col(c) for c in [round_col, round_sched] if c in df.columns]
            ).alias("_eff_round"),
            pl.coalesce(
                [pl.col(c) for c in [opp_col, opp_sched] if c in df.columns]
            ).alias("_eff_opp"),
        )

        # Schedule-only: won is null; results-backed: won is not null
        schedule_only = work.filter(
            pl.col("won").is_null() & pl.col("_eff_round").is_not_null()
            & (pl.col("_eff_round") != "RR")
        )
        has_results = work.filter(
            pl.col("won").is_not_null() & pl.col("_eff_round").is_not_null()
        )

        if schedule_only.is_empty() or has_results.is_empty():
            return df

        # For each results-backed row, record (player_id, round, opp)
        results_keys = has_results.select(
            pl.col("player_id"),
            pl.col("_eff_round").alias("_r_round"),
            pl.col("_eff_opp").alias("_r_opp"),
        ).unique()

        # Join schedule-only rows against results keys on (player_id, round)
        tagged = schedule_only.join(
            results_keys,
            left_on=["player_id", "_eff_round"],
            right_on=["player_id", "_r_round"],
            how="inner",
        )

        # Keep only where the opponent differs — that's a replaced matchup
        replaced = tagged.filter(
            pl.col("_eff_opp") != pl.col("_r_opp")
        )

        if replaced.is_empty():
            return df

        stale_uids = set(replaced["match_uid"].to_list())

        logger.info(
            "Dropping %d stale schedule-only match(es) replaced by results for %s/%s: %s",
            len(stale_uids),
            self.tid,
            self.year,
            stale_uids,
        )

        return df.filter(~pl.col("match_uid").is_in(list(stale_uids)))

    def _join_match_stats(
        self, joined: pl.DataFrame, ex_stats: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Match Stats on (match_uid, player_id)."""
        if joined.is_empty() or ex_stats.is_empty():
            return joined

        return joined.join(
            ex_stats,
            on=["match_uid", "player_id"],
            how="left",
            suffix="_stats",
        )

    def _join_overview(
        self, joined: pl.DataFrame, overview_df: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Overview on (tournament_id, year, circuit)."""
        if joined.is_empty() or overview_df.is_empty():
            return joined

        # Drop traceability columns from overview before joining
        ov_trace = ("source_file", "parsed_at")
        drop_cols = [c for c in ov_trace if c in overview_df.columns]
        ov = overview_df.drop(drop_cols)

        return joined.join(
            ov,
            on=["tournament_id", "year", "circuit"],
            how="left",
            suffix="_overview",
        )

    def _load_and_prepare_match_beats(self) -> pl.DataFrame:
        """Load staged match_beats, aggregate to player-match level, rename for join."""
        raw = self._load_parquet("match_beats.parquet")
        if raw.is_empty():
            return pl.DataFrame()

        # Filter doubles
        raw = raw.filter(~pl.col("is_doubles"))
        if raw.is_empty():
            return pl.DataFrame()

        # Normalize match_id case
        raw = raw.with_columns(pl.col("match_id").str.to_uppercase())

        # Reuse MatchBeatsAggregator logic to aggregate and pivot
        mb_agg = MatchBeatsAggregator(data_root=self.data_root)
        match_level = mb_agg._aggregate_match_level(raw)
        player_match = mb_agg._pivot_to_player_match(match_level)

        # Drop opp_id (already in main table)
        if "opp_id" in player_match.columns:
            player_match = player_match.drop("opp_id")

        # Rename columns that conflict with existing ones and add mb_ prefix
        # to player/opp fields for clarity
        rename_map = {}
        rename_map["match_duration"] = "mb_match_duration"
        rename_map["sets_played"] = "mb_sets_played"
        for col in player_match.columns:
            if col.startswith("player_") and col != "player_id":
                rename_map[col] = "mb_" + col
            elif col.startswith("opp_"):
                rename_map[col] = "mb_" + col
        player_match = player_match.rename(rename_map)

        return player_match

    def _join_match_beats(
        self, joined: pl.DataFrame, match_beats_df: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Match Beats on (tournament_id, year, match_id, player_id)."""
        if joined.is_empty() or match_beats_df.is_empty():
            return joined

        return joined.join(
            match_beats_df,
            on=["tournament_id", "year", "match_id", "player_id"],
            how="left",
            suffix="_match_beats",
        )

    def _load_and_prepare_stroke_analysis(self) -> pl.DataFrame:
        """Load staged stroke_analysis, pivot to player-match level."""
        raw = self._load_parquet("stroke_analysis.parquet")
        if raw.is_empty():
            return pl.DataFrame()

        raw = raw.filter(~pl.col("is_doubles"))
        if raw.is_empty():
            return pl.DataFrame()

        raw = raw.with_columns(pl.col("match_id").str.to_uppercase())
        sa_agg = StrokeAnalysisAggregator(data_root=self.data_root)
        player_match = sa_agg._pivot_to_player_match(raw)

        if "opp_id" in player_match.columns:
            player_match = player_match.drop("opp_id")

        drop_cols = [c for c in ("is_doubles", "source_file", "parsed_at", "schema_hash")
                     if c in player_match.columns]
        if drop_cols:
            player_match = player_match.drop(drop_cols)

        return player_match

    def _join_stroke_analysis(
        self, joined: pl.DataFrame, stroke_df: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Stroke Analysis on (tournament_id, year, match_id, player_id)."""
        if joined.is_empty() or stroke_df.is_empty():
            return joined

        return joined.join(
            stroke_df,
            on=["tournament_id", "year", "match_id", "player_id"],
            how="left",
            suffix="_stroke",
        )

    def _load_and_prepare_rally_analysis(self) -> pl.DataFrame:
        """Load staged rally_analysis, pivot to player-match level."""
        raw = self._load_parquet("rally_analysis.parquet")
        if raw.is_empty():
            return pl.DataFrame()

        raw = raw.filter(~pl.col("is_doubles"))
        if raw.is_empty():
            return pl.DataFrame()

        raw = raw.with_columns(pl.col("match_id").str.to_uppercase())
        ra_agg = RallyAnalysisAggregator(data_root=self.data_root)
        player_match = ra_agg._pivot_to_player_match(raw)

        if "opp_id" in player_match.columns:
            player_match = player_match.drop("opp_id")

        # Rename points_missing to avoid collision
        if "points_missing" in player_match.columns:
            player_match = player_match.rename({"points_missing": "ra_points_missing"})

        drop_cols = [c for c in ("is_doubles", "source_file", "parsed_at", "schema_hash")
                     if c in player_match.columns]
        if drop_cols:
            player_match = player_match.drop(drop_cols)

        return player_match

    def _join_rally_analysis(
        self, joined: pl.DataFrame, rally_df: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Rally Analysis on (tournament_id, year, match_id, player_id)."""
        if joined.is_empty() or rally_df.is_empty():
            return joined

        return joined.join(
            rally_df,
            on=["tournament_id", "year", "match_id", "player_id"],
            how="left",
            suffix="_rally",
        )

    def _apply_waterfall(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply coalesce waterfall and select final output columns."""
        if df.is_empty():
            return pl.DataFrame()

        coalesce_exprs: list[pl.Expr] = []

        # Identity fields: coalesce across all suffixed variants
        for field in ["tournament_id", "year", "circuit", "draw_type", "round"]:
            candidates = self._find_variants(df, field)
            if len(candidates) > 1:
                coalesce_exprs.append(
                    pl.coalesce([pl.col(c) for c in candidates]).alias(field)
                )

        # opp_id: coalesce across suffixed variants
        for field in ["opp_id"]:
            candidates = self._find_variants(df, field)
            if len(candidates) > 1:
                coalesce_exprs.append(
                    pl.coalesce([pl.col(c) for c in candidates]).alias(field)
                )

        # Waterfall fields: Results > MatchStats > Schedule > Overview
        # player_seed: coalesce(results, stats, schedule)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "player_seed", ["", "_stats", "_schedule"])
        )
        coalesce_exprs.extend(
            self._waterfall_expr(df, "player_entry", ["", "_stats", "_schedule"])
        )
        coalesce_exprs.extend(
            self._waterfall_expr(df, "opp_seed", ["", "_stats", "_schedule"])
        )
        coalesce_exprs.extend(
            self._waterfall_expr(df, "opp_entry", ["", "_stats", "_schedule"])
        )

        # duration_seconds: coalesce(results, stats, mb_match_duration)
        duration_candidates = []
        for c in ["duration_seconds", "duration_seconds_stats"]:
            if c in df.columns:
                duration_candidates.append(pl.col(c))
        if "mb_match_duration" in df.columns:
            duration_candidates.append(pl.col("mb_match_duration"))
        if duration_candidates:
            coalesce_exprs.append(
                pl.coalesce(duration_candidates).alias("duration_seconds")
            )

        # surface: coalesce(stats, overview)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "surface", ["", "_stats", "_overview"])
        )

        # court_name: coalesce(stats, schedule)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "court_name", ["", "_stats", "_schedule"])
        )

        # won: coalesce(results, stats)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "won", ["", "_stats"])
        )

        # match_id: coalesce(results, stats)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "match_id", ["", "_stats"])
        )

        # city: coalesce(overview.city, stats.tournament_city)
        city_candidates = []
        if "city" in df.columns:
            city_candidates.append(pl.col("city"))
        elif "city_overview" in df.columns:
            city_candidates.append(pl.col("city_overview"))
        if "tournament_city" in df.columns:
            city_candidates.append(pl.col("tournament_city"))
        elif "tournament_city_stats" in df.columns:
            city_candidates.append(pl.col("tournament_city_stats"))
        if city_candidates:
            coalesce_exprs.append(
                pl.coalesce(city_candidates).alias("city")
            )

        # singles_draw_size: coalesce(overview, stats)
        # Design says Overview preferred
        sds_candidates = []
        if "singles_draw_size" in df.columns:
            sds_candidates.append(pl.col("singles_draw_size"))
        if "singles_draw_size_overview" in df.columns:
            sds_candidates.append(pl.col("singles_draw_size_overview"))
        # stats version: draw_size_singles
        if "draw_size_singles" in df.columns:
            sds_candidates.append(pl.col("draw_size_singles"))
        elif "draw_size_singles_stats" in df.columns:
            sds_candidates.append(pl.col("draw_size_singles_stats"))
        if sds_candidates:
            coalesce_exprs.append(
                pl.coalesce(sds_candidates).alias("singles_draw_size")
            )

        # doubles_draw_size: coalesce(overview, stats)
        dds_candidates = []
        if "doubles_draw_size" in df.columns:
            dds_candidates.append(pl.col("doubles_draw_size"))
        if "doubles_draw_size_overview" in df.columns:
            dds_candidates.append(pl.col("doubles_draw_size_overview"))
        if "draw_size_doubles" in df.columns:
            dds_candidates.append(pl.col("draw_size_doubles"))
        elif "draw_size_doubles_stats" in df.columns:
            dds_candidates.append(pl.col("draw_size_doubles_stats"))
        if dds_candidates:
            coalesce_exprs.append(
                pl.coalesce(dds_candidates).alias("doubles_draw_size")
            )

        # player_partner_id, opp_partner_id: coalesce(results, stats)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "player_partner_id", ["", "_stats"])
        )
        coalesce_exprs.extend(
            self._waterfall_expr(df, "opp_partner_id", ["", "_stats"])
        )

        # tournament dates: coalesce(results, stats)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "tournament_start_date", ["", "_stats"])
        )
        coalesce_exprs.extend(
            self._waterfall_expr(df, "tournament_end_date", ["", "_stats"])
        )

        # Match Beats waterfall: match_stats primary, match_beats fallback
        # For player stats and their opp_ equivalents
        for ms_col, mb_suffix in _MB_WATERFALL_MAP.items():
            # Player side: match_stats col -> mb_player_ col
            mb_col = f"mb_player_{mb_suffix}"
            candidates = []
            if ms_col in df.columns:
                candidates.append(pl.col(ms_col))
            if mb_col in df.columns:
                candidates.append(pl.col(mb_col))
            if candidates:
                coalesce_exprs.append(pl.coalesce(candidates).alias(ms_col))

            # Opp side: opp_match_stats col -> mb_opp_ col
            opp_ms_col = f"opp_{ms_col}"
            opp_mb_col = f"mb_opp_{mb_suffix}"
            opp_candidates = []
            if opp_ms_col in df.columns:
                opp_candidates.append(pl.col(opp_ms_col))
            if opp_mb_col in df.columns:
                opp_candidates.append(pl.col(opp_mb_col))
            if opp_candidates:
                coalesce_exprs.append(pl.coalesce(opp_candidates).alias(opp_ms_col))

        # sets_played: coalesce(match_stats, mb_sets_played)
        sets_candidates = []
        if "sets_played" in df.columns:
            sets_candidates.append(pl.col("sets_played"))
        if "mb_sets_played" in df.columns:
            sets_candidates.append(pl.col("mb_sets_played"))
        if len(sets_candidates) > 1:
            coalesce_exprs.append(
                pl.coalesce(sets_candidates).alias("sets_played")
            )

        # Apply all coalesce expressions
        if coalesce_exprs:
            df = df.with_columns(coalesce_exprs)

        # Select final output columns (drop all suffixed intermediates)
        final_cols = self._get_final_columns(df)

        # Add missing expected columns as null
        missing_exprs = []
        for col in final_cols:
            if col not in df.columns:
                missing_exprs.append(pl.lit(None).alias(col))
        if missing_exprs:
            df = df.with_columns(missing_exprs)

        df = df.select(final_cols)

        return df

    def _find_variants(self, df: pl.DataFrame, field: str) -> list[str]:
        """Find all column variants for a field (base + suffixed)."""
        variants = []
        if field in df.columns:
            variants.append(field)
        for suffix in ["_schedule", "_stats", "_overview"]:
            suffixed = f"{field}{suffix}"
            if suffixed in df.columns:
                variants.append(suffixed)
        return variants

    def _waterfall_expr(
        self,
        df: pl.DataFrame,
        field: str,
        priority_suffixes: list[str],
    ) -> list[pl.Expr]:
        """Build coalesce expression for a waterfall field.

        Returns a list with 0 or 1 expression.
        """
        candidates = []
        for suffix in priority_suffixes:
            col_name = field if suffix == "" else f"{field}{suffix}"
            if col_name in df.columns:
                candidates.append(pl.col(col_name))

        if not candidates:
            return []

        return [pl.coalesce(candidates).alias(field)]

    def _get_final_columns(self, df: pl.DataFrame) -> list[str]:
        """Determine the final output columns, dropping all suffixed intermediates."""
        # These are the output columns we want
        output_cols = [
            # Join keys
            "match_uid",
            "player_id",
            "opp_id",
            "draw_p1_id",
            "tournament_id",
            "year",
            "circuit",
            "draw_type",
            "round",
            # Waterfall fields
            "player_seed",
            "player_entry",
            "opp_seed",
            "opp_entry",
            "duration_seconds",
            "surface",
            "court_name",
            "won",
            "match_id",
            "city",
            "singles_draw_size",
            "doubles_draw_size",
            # Results-only
            "result_type",
            "player_set1_games",
            "player_set1_tiebreak",
            "player_set2_games",
            "player_set2_tiebreak",
            "player_set3_games",
            "player_set3_tiebreak",
            "player_set4_games",
            "player_set4_tiebreak",
            "player_set5_games",
            "player_set5_tiebreak",
            "opp_set1_games",
            "opp_set1_tiebreak",
            "opp_set2_games",
            "opp_set2_tiebreak",
            "opp_set3_games",
            "opp_set3_tiebreak",
            "opp_set4_games",
            "opp_set4_tiebreak",
            "opp_set5_games",
            "opp_set5_tiebreak",
            # Match Stats-only
            "reason",
            "svc_aces",
            "svc_double_faults",
            "svc_first_serve_in",
            "svc_first_serve_att",
            "svc_first_serve_pts_won",
            "svc_first_serve_pts_played",
            "svc_second_serve_pts_won",
            "svc_second_serve_pts_played",
            "svc_bp_saved",
            "svc_bp_faced",
            "svc_games_played",
            "svc_serve_rating",
            "ret_first_serve_pts_won",
            "ret_first_serve_pts_played",
            "ret_second_serve_pts_won",
            "ret_second_serve_pts_played",
            "ret_bp_converted",
            "ret_bp_opportunities",
            "ret_games_played",
            "ret_return_rating",
            "pts_service_pts_won",
            "pts_service_pts_played",
            "pts_return_pts_won",
            "pts_return_pts_played",
            "pts_total_pts_won",
            "pts_total_pts_played",
            # Opponent stats (from explode_match_stats)
            "opp_svc_aces",
            "opp_svc_double_faults",
            "opp_svc_first_serve_in",
            "opp_svc_first_serve_att",
            "opp_svc_first_serve_pts_won",
            "opp_svc_first_serve_pts_played",
            "opp_svc_second_serve_pts_won",
            "opp_svc_second_serve_pts_played",
            "opp_svc_bp_saved",
            "opp_svc_bp_faced",
            "opp_svc_games_played",
            "opp_svc_serve_rating",
            "opp_ret_first_serve_pts_won",
            "opp_ret_first_serve_pts_played",
            "opp_ret_second_serve_pts_won",
            "opp_ret_second_serve_pts_played",
            "opp_ret_bp_converted",
            "opp_ret_bp_opportunities",
            "opp_ret_games_played",
            "opp_ret_return_rating",
            "opp_pts_service_pts_won",
            "opp_pts_service_pts_played",
            "opp_pts_return_pts_won",
            "opp_pts_return_pts_played",
            "opp_pts_total_pts_won",
            "opp_pts_total_pts_played",
            "number_of_sets",
            "sets_played",
            "scoring_system",
            "umpire_first_name",
            "umpire_last_name",
            "round_id",
            "is_qualifier",
            # Tournament dates: Results > MatchStats waterfall
            "tournament_start_date",
            "tournament_end_date",
            "prize_money",
            "currency",
            # Schedule-only
            "match_date",
            "scheduled_datetime",
            "time_suffix",
            "display_time",
            "status",
            "score",
            "is_time_estimated",
            "court_match_num",
            # Overview-only
            "tournament_name",
            "country",
            "sponsor_title",
            "event_type",
            "event_type_detail",
            "indoor",
            "surface_detail",
            "prize",
            "total_financial_commitment",
            # Doubles
            "player_partner_id",
            "opp_partner_id",
            # Match Beats - shared fields
            *_MB_SHARED_FIELDS,
            # Match Beats - unique player/opp fields
            *[f"mb_player_{f}" for f in _MB_UNIQUE_PLAYER_FIELDS],
            *[f"mb_opp_{f}" for f in _MB_UNIQUE_PLAYER_FIELDS],
            # Stroke Analysis - player/opp fields
            *[f"player_{f}" for f in _STROKE_ANALYSIS_FIELDS],
            *[f"opp_{f}" for f in _STROKE_ANALYSIS_FIELDS],
            # Rally Analysis - player/opp fields
            *[f"player_{f}" for f in _RALLY_ANALYSIS_FIELDS],
            *[f"opp_{f}" for f in _RALLY_ANALYSIS_FIELDS],
            "ra_points_missing",
        ]

        return output_cols

    def _validate_schema(self, df: pl.DataFrame) -> None:
        """Validate output DataFrame matches MATCHES_SCHEMA."""
        expected = set(MATCHES_SCHEMA.keys())
        actual = set(df.columns)
        missing = expected - actual
        extra = actual - expected
        if missing or extra:
            raise ValueError(
                f"Schema mismatch. Missing: {missing}, Extra: {extra}"
            )
        for col, expected_type in MATCHES_SCHEMA.items():
            actual_type = df[col].dtype
            if actual_type != expected_type and actual_type != pl.Null:
                logger.warning(
                    "Column %s: expected %s, got %s",
                    col,
                    expected_type,
                    actual_type,
                )

    def run(self) -> Path | None:
        """Aggregate and write to parquet."""
        df = self.aggregate()
        if df.is_empty():
            logger.info(
                "No matches for %s/%s/%d",
                self.circuit.value,
                self.tid,
                self.year,
            )
            return None
        self._validate_schema(df)
        out_path = self.build_path(
            "aggregate", self._tournament_rel_path, "matches.parquet"
        )
        return self.save_parquet(df, out_path)
