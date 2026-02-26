"""Per-tournament cross-dataset aggregation into player-match rows."""

import logging
from pathlib import Path

import polars as pl

from mvp.atptour.aggregators.helpers import (
    explode_match_stats,
    explode_results,
    explode_schedule,
)
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

MATCHES_SCHEMA: dict[str, pl.DataType] = {
    # Join keys
    "match_uid": pl.String,
    "player_id": pl.String,
    "opp_id": pl.String,
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
    "number_of_sets": pl.Int64,
    "sets_played": pl.Int64,
    "scoring_system": pl.String,
    "umpire_first_name": pl.String,
    "umpire_last_name": pl.String,
    "round_id": pl.Int64,
    "is_qualifier": pl.Boolean,
    # Tournament dates from Match Stats
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

        # Step 5: LEFT JOIN Match Stats
        joined = self._join_match_stats(joined, ex_stats)

        # Step 6: Coalesce join key fields before Overview join so
        # schedule-only rows (null Results keys) can match Overview.
        for key in ["tournament_id", "year", "circuit"]:
            variants = [c for c in joined.columns if c == key or c.startswith(f"{key}_")]
            if len(variants) > 1:
                joined = joined.with_columns(
                    pl.coalesce([pl.col(v) for v in variants]).alias(key)
                )

        # Step 7: LEFT JOIN Overview
        joined = self._join_overview(joined, overview_df)

        # Step 7: Waterfall resolution
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

        # duration_seconds: coalesce(results, stats)
        coalesce_exprs.extend(
            self._waterfall_expr(df, "duration_seconds", ["", "_stats"])
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
            "number_of_sets",
            "sets_played",
            "scoring_system",
            "umpire_first_name",
            "umpire_last_name",
            "round_id",
            "is_qualifier",
            # Tournament dates from Match Stats
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
