"""Cross-tournament aggregation into a single enriched matches dataset."""

import glob
import logging
from pathlib import Path

import polars as pl

from mvp.atptour.elo import compute_elo_ratings
from mvp.common.base_job import BaseJob

logger = logging.getLogger(__name__)

ROUND_ORDER: dict[str, int] = {
    "Q1": 1,
    "Q2": 2,
    "Q3": 3,
    "RR": 4,
    "R128": 5,
    "R64": 6,
    "R32": 7,
    "R16": 8,
    "QF": 9,
    "SF": 10,
    "THIRDPLACE": 11,
    "HCF": 11,
    "BRONZE": 11,
    "F": 12,
}


def filter_dc_tournaments(df: pl.DataFrame) -> pl.DataFrame:
    """Exclude Davis Cup and team events from tournament matches."""
    return df.filter(
        ~(
            pl.col("event_type").str.starts_with("DC").fill_null(False)
            | (pl.col("circuit") == "team")
        )
    )


def filter_dc_activity(df: pl.DataFrame) -> pl.DataFrame:
    """Exclude Davis Cup and team event rows from Activity data."""
    return df.filter(
        (pl.col("event_type") != "DC") & (pl.col("circuit") != "team")
    )


def map_activity_to_matches_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Map Activity columns to matches schema for gap-fill rows.

    Renames rank fields, derives won from win_loss, adds draw_type.
    Does NOT add null columns for stats/overview -- that happens during concat.
    """
    return df.select(
        "match_uid",
        "player_id",
        "opp_id",
        "tournament_id",
        "tournament_name",
        "year",
        "circuit",
        pl.lit("singles").alias("draw_type"),
        "round",
        "surface",
        "indoor",
        "event_type",
        "tournament_start_date",
        "tournament_end_date",
        (pl.col("win_loss") == "W").alias("won"),
        "reason",
        pl.col("player_rank").alias("activity_rank"),
        pl.col("opp_rank").alias("activity_opp_rank"),
        pl.col("points").alias("activity_points"),
        *[f"player_set{n}_{k}" for n in range(1, 6) for k in ("games", "tiebreak")],
        *[f"opp_set{n}_{k}" for n in range(1, 6) for k in ("games", "tiebreak")],
    )


def join_rankings(matches: pl.DataFrame, rankings: pl.DataFrame) -> pl.DataFrame:
    """Join rankings data for both player and opponent using as-of join.

    For each match, finds the most recent rankings snapshot on or before
    tournament_start_date for both player_id and opp_id.
    """
    rnk = rankings.select([
        "player_id", "ranking_date", "rank", "points", "tournaments_played",
    ]).sort("ranking_date")

    # Player rankings
    player_rnk = rnk.rename({
        "player_id": "_rnk_pid",
        "rank": "player_rankings_rank",
        "points": "player_rankings_points",
        "tournaments_played": "player_rankings_tournaments_played",
    })
    result = matches.sort("tournament_start_date").join_asof(
        player_rnk,
        left_on="tournament_start_date",
        right_on="ranking_date",
        by_left="player_id",
        by_right="_rnk_pid",
        strategy="backward",
    )

    # Opponent rankings
    opp_rnk = rnk.rename({
        "player_id": "_rnk_pid",
        "rank": "opp_rankings_rank",
        "points": "opp_rankings_points",
        "tournaments_played": "opp_rankings_tournaments_played",
    })
    result = result.sort("tournament_start_date").join_asof(
        opp_rnk,
        left_on="tournament_start_date",
        right_on="ranking_date",
        by_left="opp_id",
        by_right="_rnk_pid",
        strategy="backward",
    )

    # Drop temporary ranking_date columns from both join_asof calls
    drop_cols = [c for c in result.columns if c.startswith("ranking_date")]
    return result.drop(drop_cols)


_BIO_FIELDS = [
    "first_name", "last_name", "height_cm", "weight_kg",
    "right_handed", "twohand_backhand", "birth_date", "pro_year",
    "nationality", "natl_id",
]


def join_player_bio(matches: pl.DataFrame, bio: pl.DataFrame) -> pl.DataFrame:
    """Join PlayerBio data for both player and opponent.

    Adds player_first_name, player_last_name, ..., opp_first_name, etc.
    """
    bio_subset = bio.select(["player_id"] + _BIO_FIELDS)

    # Player bio
    player_bio = bio_subset.rename(
        {f: f"player_{f}" for f in _BIO_FIELDS} | {"player_id": "_bio_pid"}
    )
    result = matches.join(
        player_bio, left_on="player_id", right_on="_bio_pid", how="left"
    )

    # Opponent bio
    opp_bio = bio_subset.rename(
        {f: f"opp_{f}" for f in _BIO_FIELDS} | {"player_id": "_bio_pid"}
    )
    result = result.join(opp_bio, left_on="opp_id", right_on="_bio_pid", how="left")

    return result


def fill_tournament_dates(df: pl.DataFrame) -> pl.DataFrame:
    """Fill tournament_start_date and tournament_end_date within each tournament.

    If any row in a tournament has a date, all rows get that date.
    Only tournaments with ALL nulls remain null.
    """
    group_keys = ["tournament_id", "year"]
    return df.with_columns([
        pl.col("tournament_start_date")
        .fill_null(pl.col("tournament_start_date").max().over(group_keys))
        .alias("tournament_start_date"),
        pl.col("tournament_end_date")
        .fill_null(pl.col("tournament_end_date").max().over(group_keys))
        .alias("tournament_end_date"),
    ])


def fill_tournament_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Fill tournament-level fields within each tournament group.

    For surface, indoor, event_type: if any row in a tournament has a value,
    propagate it to all rows. Only tournaments with ALL nulls remain null.
    """
    group_keys = ["tournament_id", "year"]
    fill_cols = ["surface", "indoor", "event_type", "tournament_name", "country"]
    return df.with_columns([
        pl.col(c)
        .fill_null(pl.col(c).drop_nulls().first().over(group_keys))
        .alias(c)
        for c in fill_cols
        if c in df.columns
    ])


def add_round_order(df: pl.DataFrame) -> pl.DataFrame:
    """Add round_order column from the round column using ROUND_ORDER mapping."""
    return df.with_columns(
        pl.col("round")
        .replace_strict(ROUND_ORDER, default=None)
        .cast(pl.Int64)
        .alias("round_order")
    )


def add_effective_match_date(df: pl.DataFrame) -> pl.DataFrame:
    """Add effective_match_date column using schedule data or round-offset estimation.

    For each (tournament_id, year) group:
    - If ALL scheduled_datetime values are non-null, use scheduled_datetime directly.
    - Otherwise, estimate by scaling round position across the tournament duration.
      Early rounds (Q1, R128) are placed near tournament_start_date, late rounds (F)
      near tournament_end_date. This handles both short Challengers and 2-week Slams.
    """
    group_keys = ["tournament_id", "year"]

    # Per-group flag: True if every row in the group has a non-null scheduled_datetime
    df = df.with_columns(
        pl.col("scheduled_datetime")
        .is_not_null()
        .all()
        .over(group_keys)
        .alias("_all_scheduled"),
    )

    # Compute round offset: rank round_order ascending within group.
    # Lowest round_order (Q1) gets offset 0, highest (F) gets max.
    df = df.with_columns(
        pl.col("round_order")
        .rank(method="dense", descending=False)
        .over(group_keys)
        .cast(pl.Int64)
        .sub(1)
        .alias("_round_offset"),
    )

    # Get max offset per group (for scaling)
    df = df.with_columns(
        pl.col("_round_offset").max().over(group_keys).alias("_max_offset"),
    )

    # Compute tournament duration in days.
    # Cap ITF/Challenger to 7 days - historical Activity data sometimes merges
    # consecutive weeks (e.g., "Mexico F3" + "Mexico F4") into 13-day spans.
    has_circuit = "circuit" in df.columns
    raw_duration = (
        pl.col("tournament_end_date") - pl.col("tournament_start_date")
    ).dt.total_days()

    if has_circuit:
        is_itf_chal = pl.col("circuit").is_in(["itf", "chal"])
        capped_duration = pl.when(is_itf_chal & (raw_duration > 6)).then(6).otherwise(raw_duration)
        df = df.with_columns(capped_duration.alias("_duration_days"))
    else:
        df = df.with_columns(raw_duration.alias("_duration_days"))

    # Compute scaled day offset: (round_offset / max_offset) * duration
    # When max_offset is 0 (single round), use 0. Replace 0 with 1 before division
    # to avoid NaN, since the result will be masked anyway.
    df = df.with_columns(
        pl.when(pl.col("_max_offset") > 0)
        .then(
            (
                pl.col("_round_offset")
                * pl.col("_duration_days")
                / pl.col("_max_offset").replace(0, 1)
            )
            .round()
            .cast(pl.Int64)
        )
        .otherwise(0)
        .alias("_scaled_offset"),
    )

    # For Grand Slams, qualifying rounds happen before the listed start date.
    # Q1 (round_order=1) -> start - 3 days, Q2 -> start - 2, Q3 -> start - 1
    has_event_type = "event_type" in df.columns
    if has_event_type:
        is_gs_qualifying = (pl.col("event_type") == "GS") & (pl.col("round_order") <= 3)
        gs_qual_offset = pl.col("round_order") - 4  # Q1=-3, Q2=-2, Q3=-1

        df = df.with_columns(
            pl.when(pl.col("_all_scheduled"))
            .then(pl.col("scheduled_datetime"))
            .when(is_gs_qualifying)
            .then(
                pl.col("tournament_start_date").cast(pl.Datetime)
                + pl.duration(days=gs_qual_offset)
            )
            .otherwise(
                pl.col("tournament_start_date").cast(pl.Datetime)
                + pl.duration(days=pl.col("_scaled_offset"))
            )
            .alias("effective_match_date"),
        )
    else:
        df = df.with_columns(
            pl.when(pl.col("_all_scheduled"))
            .then(pl.col("scheduled_datetime"))
            .otherwise(
                pl.col("tournament_start_date").cast(pl.Datetime)
                + pl.duration(days=pl.col("_scaled_offset"))
            )
            .alias("effective_match_date"),
        )

    # Validate no nulls
    bad_rows = df.filter(pl.col("effective_match_date").is_null())
    null_count = bad_rows.height
    if null_count > 0:
        # Summarize by tournament/year for actionable diagnosis
        summary = (
            bad_rows
            .group_by(["tournament_id", "year", "circuit"])
            .agg([
                pl.len().alias("count"),
                pl.col("tournament_start_date").is_null().sum().alias("null_start_date"),
                pl.col("round_order").is_null().sum().alias("null_round_order"),
            ])
            .sort(["year", "tournament_id"])
        )
        logger.error(
            "Missing effective_match_date: %d rows across %d tournaments",
            null_count,
            summary.height,
        )
        logger.error("Affected tournaments (tournament_id, year, circuit, count, "
                     "null_start_date, null_round_order):")
        for row in summary.iter_rows(named=True):
            logger.error(
                "  %s/%s (%s): %d matches, %d missing start_date, %d missing round_order",
                row["tournament_id"],
                row["year"],
                row["circuit"],
                row["count"],
                row["null_start_date"],
                row["null_round_order"],
            )

        # Log sample of actual bad rows (limited to 20)
        detail_cols = group_keys + [
            "match_uid", "round", "round_order",
            "tournament_start_date", "scheduled_datetime",
        ]
        sample_rows = bad_rows.select(detail_cols).head(20)
        logger.debug("Sample rows with null effective_match_date:\n%s", sample_rows)

        msg = (
            f"null effective_match_date: {null_count} rows "
            f"across {summary.height} tournaments"
        )
        raise ValueError(msg)

    # Drop temporary columns
    temp_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(temp_cols)


def add_tournament_level(df: pl.DataFrame) -> pl.DataFrame:
    """Derive tournament_level from event_type + event_type_detail."""
    has_detail = "event_type_detail" in df.columns
    if has_detail:
        return df.with_columns(
            pl.when(pl.col("event_type") == "CH")
            .then(
                pl.when(pl.col("event_type_detail") == 175).then(pl.lit("CH175"))
                .when(pl.col("event_type_detail") == 125).then(pl.lit("CH125"))
                .when(pl.col("event_type_detail") == 110).then(pl.lit("CH100"))
                .when(pl.col("event_type_detail") == 100).then(pl.lit("CH100"))
                .when(pl.col("event_type_detail") == 90).then(pl.lit("CH75"))
                .when(pl.col("event_type_detail") == 80).then(pl.lit("CH75"))
                .when(pl.col("event_type_detail") == 75).then(pl.lit("CH75"))
                .when(pl.col("event_type_detail") == 50).then(pl.lit("CH50"))
                .otherwise(pl.lit("CH75"))
            )
            .when(pl.col("event_type") == "FU").then(pl.lit("FU"))
            .when(pl.col("event_type") == "GS").then(pl.lit("GS"))
            .when(pl.col("event_type") == "1000").then(pl.lit("1000"))
            .when(pl.col("event_type").is_in(["CS", "WC"])).then(pl.lit("1000"))
            .when(pl.col("event_type") == "500").then(pl.lit("500"))
            .when(pl.col("event_type") == "GP").then(pl.lit("500"))
            .when(pl.col("event_type") == "OL").then(pl.lit("500"))
            .when(pl.col("event_type") == "250").then(pl.lit("250"))
            .otherwise(pl.lit("250"))
            .alias("tournament_level")
        )
    # No detail column — map from event_type alone
    return df.with_columns(
        pl.when(pl.col("event_type") == "CH").then(pl.lit("CH75"))
        .when(pl.col("event_type") == "FU").then(pl.lit("FU"))
        .when(pl.col("event_type") == "GS").then(pl.lit("GS"))
        .when(pl.col("event_type") == "1000").then(pl.lit("1000"))
        .when(pl.col("event_type").is_in(["CS", "WC"])).then(pl.lit("1000"))
        .when(pl.col("event_type") == "500").then(pl.lit("500"))
        .when(pl.col("event_type") == "GP").then(pl.lit("500"))
        .when(pl.col("event_type") == "OL").then(pl.lit("500"))
        .when(pl.col("event_type") == "250").then(pl.lit("250"))
        .otherwise(pl.lit("250"))
        .alias("tournament_level")
    )


def add_best_of(df: pl.DataFrame) -> pl.DataFrame:
    """Derive best_of (3 or 5) from tournament metadata.

    Rules:
      - Grand Slam main draw (non-qualifying) → 5
      - Wimbledon Q3 (final qualifying round) → 5
      - Jeddah / Six Kings (tournament_id 7696) → 5
      - Everything else → 3
    """
    is_gs_main = (pl.col("tournament_level") == "GS") & pl.col("round").is_in(
        ["R128", "R64", "R32", "R16", "QF", "SF", "F", "RR"]
    )
    is_wimbledon_q3 = (
        (pl.col("tournament_name") == "Wimbledon") & (pl.col("round") == "Q3")
    )
    is_jeddah = pl.col("tournament_id") == "7696"

    return df.with_columns(
        pl.when(is_gs_main | is_wimbledon_q3 | is_jeddah)
        .then(pl.lit(5))
        .otherwise(pl.lit(3))
        .alias("best_of")
    )


def add_partner_workload_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Add rows for doubles partners so workload features count their appearances.

    For each doubles match row where player_partner_id is not null, creates
    a corresponding row with player_id = player_partner_id. These rows have
    null stats (since individual stats aren't available for partners) but
    preserve the match metadata for workload counting.

    This ensures matches_played(days=30) counts doubles appearances for
    players who appear as partners, not just as the primary player_id.
    """
    # Get doubles rows with partners
    doubles_with_partner = df.filter(
        (pl.col("draw_type") == "doubles")
        & pl.col("player_partner_id").is_not_null()
    )

    if doubles_with_partner.is_empty():
        return df

    # Columns to keep from original (match metadata, no stats)
    metadata_cols = [
        "match_uid", "tournament_id", "year", "circuit", "draw_type",
        "round", "round_order", "surface", "indoor", "event_type",
        "tournament_name", "country",
        "tournament_start_date", "tournament_end_date",
        "effective_match_date", "won",
    ]

    # Keep only columns that exist
    keep_cols = [c for c in metadata_cols if c in df.columns]

    # Create partner rows: swap player_id with player_partner_id
    partner_rows = doubles_with_partner.select(
        keep_cols + ["player_partner_id", "player_id", "opp_partner_id", "opp_id"]
    ).rename({
        "player_partner_id": "player_id",
        "player_id": "player_partner_id",
        "opp_partner_id": "opp_id",
        "opp_id": "opp_partner_id",
    })

    # Concat with diagonal_relaxed to handle missing columns (they become null)
    return pl.concat([df, partner_rows], how="diagonal_relaxed")


def validate_tournament_scheduling(df: pl.DataFrame) -> list[dict]:
    """Flag players with impossible scheduling patterns.

    Detects:
    1. Same effective_match_date, different tournaments
    2. Interleaved tournaments (A, B, A pattern within a short window)

    Returns a list of warning dicts describing the conflicts.
    """
    warnings: list[dict] = []

    # Get player-match-date-tournament combinations
    matches = (
        df.filter(pl.col("effective_match_date").is_not_null())
        .select(["player_id", "tournament_id", "effective_match_date"])
        .unique()
        .sort(["player_id", "effective_match_date"])
    )

    # Check for invalid dates (year < 1 causes Python conversion errors)
    bad_dates = df.filter(pl.col("effective_match_date").dt.year() < 1)
    if bad_dates.height > 0:
        logger.error(
            "Found %d rows with invalid effective_match_date (year < 1):",
            bad_dates.height,
        )
        # Use Polars to extract without Python date conversion
        sample = bad_dates.head(20).with_columns(
            pl.col("effective_match_date").cast(pl.Utf8).alias("date_str")
        )
        for pid, tid, yr, date_str in zip(
            sample["player_id"].to_list(),
            sample["tournament_id"].to_list(),
            sample["year"].to_list(),
            sample["date_str"].to_list(),
        ):
            logger.error(
                "  player=%s, tournament=%s, year=%s, date=%s", pid, tid, yr, date_str
            )
        return warnings  # Skip validation, can't process bad dates

    # Check 1: Same day, different tournaments
    same_day = (
        matches.group_by(["player_id", "effective_match_date"])
        .agg(pl.col("tournament_id").n_unique().alias("n_tournaments"))
        .filter(pl.col("n_tournaments") > 1)
    )
    for row in same_day.iter_rows(named=True):
        tids = (
            matches.filter(
                (pl.col("player_id") == row["player_id"])
                & (pl.col("effective_match_date") == row["effective_match_date"])
            )["tournament_id"]
            .unique()
            .to_list()
        )
        warnings.append({
            "type": "same_day",
            "player_id": row["player_id"],
            "date": row["effective_match_date"],
            "tournament_ids": tids,
        })

    # Check 2: Interleaved tournaments (A on day N, B on day N+1, A on day N+2)
    for pid in matches["player_id"].unique().to_list():
        player_matches = (
            matches.filter(pl.col("player_id") == pid)
            .sort("effective_match_date")
            .select(["tournament_id", "effective_match_date"])
            .rows()
        )
        if len(player_matches) < 3:
            continue

        for i in range(len(player_matches) - 2):
            tid_a, date_a = player_matches[i]
            tid_b, date_b = player_matches[i + 1]
            tid_c, date_c = player_matches[i + 2]

            # A, B, A pattern within 7 days
            if tid_a == tid_c and tid_a != tid_b and (date_c - date_a).days <= 7:
                warnings.append({
                    "type": "interleaved",
                    "player_id": pid,
                    "pattern": [(tid_a, date_a), (tid_b, date_b), (tid_c, date_c)],
                })
                break  # One warning per player

    return warnings


class MatchesAggregator(BaseJob):
    """Cross-tournament aggregation into a single enriched matches dataset."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def aggregate(self) -> pl.DataFrame:
        """Run the full aggregation pipeline."""
        tournament_matches = self._stack_tournament_matches()
        logger.info("Tournament matches stacked: %d rows", len(tournament_matches))

        activity = self._load_activity()
        logger.info("Activity loaded: %d rows", len(activity))

        tournament_matches = self._enrich_from_activity(tournament_matches, activity)

        gap_fill = self._activity_gap_fill(tournament_matches, activity)
        logger.info("Activity gap-fill: %d rows", len(gap_fill))

        combined = pl.concat([tournament_matches, gap_fill], how="diagonal_relaxed")
        logger.info("Combined: %d rows", len(combined))

        # Step 6: Rankings enrichment
        rankings = self._load_rankings()
        if rankings is not None:
            combined = join_rankings(combined, rankings)
            logger.info("Rankings joined")

        # Step 7: Bio enrichment
        bio = self._load_bio()
        if bio is not None:
            combined = join_player_bio(combined, bio)
            logger.info("Bio joined")

        # Coalesce rank columns: rankings (weekly snapshot) preferred, activity as fallback
        combined = combined.with_columns([
            pl.coalesce(["player_rankings_rank", "activity_rank"]).alias("player_rank"),
            pl.coalesce(["opp_rankings_rank", "activity_opp_rank"]).alias("opp_rank"),
        ]).drop(["player_rankings_rank", "opp_rankings_rank",
                 "activity_rank", "activity_opp_rank", "activity_points"])
        logger.info("Rank columns coalesced")

        # Step 8: Fill tournament-level fields within each tournament, then compute
        # effective match date. Any row in a tournament can provide the values.
        combined = fill_tournament_dates(combined)
        combined = fill_tournament_fields(combined)
        combined = add_round_order(combined)
        combined = add_effective_match_date(combined)
        combined = add_tournament_level(combined)
        combined = add_best_of(combined)

        # Compute Elo ratings for singles matches only
        singles = combined.filter(pl.col("draw_type") == "singles")
        if not singles.is_empty():
            singles = compute_elo_ratings(singles)
            # Rejoin with non-singles rows
            non_singles = combined.filter(pl.col("draw_type") != "singles")
            combined = pl.concat([singles, non_singles], how="diagonal_relaxed")

        # Step 9: Add partner rows for doubles workload tracking
        combined = add_partner_workload_rows(combined)
        logger.info("After partner expansion: %d rows", len(combined))

        combined = combined.sort(
            ["effective_match_date", "draw_type", "match_uid", "player_id"],
            nulls_last=True,
        )

        # Step 10: Validation
        warnings = validate_tournament_scheduling(combined)
        for w in warnings:
            if w["type"] == "same_day":
                logger.warning(
                    "Impossible scheduling: player %s in %d tournaments on %s: %s",
                    w["player_id"],
                    len(w["tournament_ids"]),
                    w["date"],
                    w["tournament_ids"],
                )
            elif w["type"] == "interleaved":
                logger.warning(
                    "Interleaved tournaments for player %s: %s",
                    w["player_id"],
                    w["pattern"],
                )

        return combined

    def _stack_tournament_matches(self) -> pl.DataFrame:
        """Glob and concat all per-tournament matches parquets, filtering DC."""
        pattern = str(
            self.data_root
            / "aggregate"
            / "atptour"
            / "tournaments"
            / "**"
            / "matches.parquet"
        )
        files = glob.glob(pattern, recursive=True)
        if not files:
            return pl.DataFrame()
        dfs = [pl.read_parquet(f) for f in files]
        stacked = pl.concat(dfs, how="diagonal_relaxed")
        return filter_dc_tournaments(stacked)

    def _load_activity(self) -> pl.DataFrame:
        """Load Activity parquet and filter out DC and byes."""
        path = self.data_root / "stage" / "atptour" / "activity.parquet"
        if not path.exists():
            return pl.DataFrame()
        act = pl.read_parquet(path)
        act = filter_dc_activity(act)
        act = act.filter(
            (pl.col("is_bye") == False) & pl.col("match_uid").is_not_null()  # noqa: E712
        )
        return act

    def _enrich_from_activity(
        self, matches: pl.DataFrame, activity: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Activity fields onto overlapping tournament matches.

        Enriches rank fields AND tournament-level fields (surface, indoor, etc.)
        that may be missing from per-tournament matches but present in activity.
        """
        if activity.is_empty():
            return matches.with_columns([
                pl.lit(None).cast(pl.Int64).alias("activity_rank"),
                pl.lit(None).cast(pl.Int64).alias("activity_opp_rank"),
                pl.lit(None).cast(pl.Int64).alias("activity_points"),
            ])
        act_enrichment = activity.select([
            "match_uid",
            "player_id",
            pl.col("player_rank").alias("activity_rank"),
            pl.col("opp_rank").alias("activity_opp_rank"),
            pl.col("points").alias("activity_points"),
            pl.col("tournament_start_date").alias("_act_start_date"),
            pl.col("tournament_end_date").alias("_act_end_date"),
            pl.col("surface").alias("_act_surface"),
            pl.col("indoor").alias("_act_indoor"),
            pl.col("event_type").alias("_act_event_type"),
        ])

        result = matches.join(
            act_enrichment, on=["match_uid", "player_id"], how="left"
        )

        # Fill fields from Activity where tournament matches are missing them
        if "_act_start_date" in result.columns:
            result = result.with_columns([
                pl.coalesce([
                    pl.col("tournament_start_date"),
                    pl.col("_act_start_date"),
                ]).alias("tournament_start_date"),
                pl.coalesce([
                    pl.col("tournament_end_date"),
                    pl.col("_act_end_date"),
                ]).alias("tournament_end_date"),
                pl.coalesce([
                    pl.col("surface"),
                    pl.col("_act_surface"),
                ]).alias("surface"),
                pl.coalesce([
                    pl.col("indoor"),
                    pl.col("_act_indoor"),
                ]).alias("indoor"),
                pl.coalesce([
                    pl.col("event_type"),
                    pl.col("_act_event_type"),
                ]).alias("event_type"),
            ]).drop([
                "_act_start_date", "_act_end_date",
                "_act_surface", "_act_indoor", "_act_event_type",
            ])

        return result

    def _activity_gap_fill(
        self, matches: pl.DataFrame, activity: pl.DataFrame
    ) -> pl.DataFrame:
        """Get Activity rows not in tournament matches and map to matches schema."""
        if activity.is_empty():
            return pl.DataFrame()
        existing_uids = (
            set(matches["match_uid"].unique().to_list())
            if not matches.is_empty()
            else set()
        )
        gap = activity.filter(~pl.col("match_uid").is_in(list(existing_uids)))
        if gap.is_empty():
            return pl.DataFrame()
        return map_activity_to_matches_schema(gap)

    def _load_rankings(self) -> pl.DataFrame | None:
        """Load consolidated rankings parquet."""
        path = (
            self.data_root
            / "stage"
            / "atptour"
            / "rankings"
            / "rankings_singles.parquet"
        )
        if not path.exists():
            return None
        return pl.read_parquet(path)

    def _load_bio(self) -> pl.DataFrame | None:
        """Load all player bio parquets into one DataFrame."""
        bio_dir = self.data_root / "stage" / "atptour" / "players"
        if not bio_dir.is_dir():
            return None
        files = sorted(bio_dir.glob("*.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files])

    def run(self) -> Path | None:
        """Aggregate and write to parquet."""
        df = self.aggregate()
        if df.is_empty():
            logger.info("No matches to aggregate")
            return None
        out_path = self.build_path("aggregate", "", "matches.parquet")
        return self.save_parquet(df, out_path)
