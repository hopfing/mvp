"""Cross-tournament aggregation into a single enriched matches dataset."""

import glob
import logging
from pathlib import Path

import polars as pl

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
        "rank": "rankings_rank",
        "points": "rankings_points",
        "tournaments_played": "rankings_tournaments_played",
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
        "rank": "rankings_opp_rank",
        "points": "rankings_opp_points",
        "tournaments_played": "rankings_opp_tournaments_played",
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

    For each (tournament_id, year, draw_type) group:
    - If ALL scheduled_datetime values are non-null, use scheduled_datetime directly.
    - Otherwise, estimate as tournament_end_date - round_offset days (at midnight),
      where round_offset is derived by ranking distinct round_order values descending
      within the group (highest round_order = offset 0).
    """
    group_keys = ["tournament_id", "year", "draw_type"]

    # Per-group flag: True if every row in the group has a non-null scheduled_datetime
    df = df.with_columns(
        pl.col("scheduled_datetime")
        .is_not_null()
        .all()
        .over(group_keys)
        .alias("_all_scheduled"),
    )

    # Compute round offset: rank distinct round_order values descending within group.
    # Highest round_order gets offset 0, next highest gets 1, etc.
    df = df.with_columns(
        pl.col("round_order")
        .rank(method="dense", descending=True)
        .over(group_keys)
        .cast(pl.Int64)
        .sub(1)
        .alias("_round_offset"),
    )

    # Compute effective_match_date
    df = df.with_columns(
        pl.when(pl.col("_all_scheduled"))
        .then(pl.col("scheduled_datetime"))
        .otherwise(
            pl.col("tournament_end_date")
            .cast(pl.Datetime)
            .dt.offset_by(
                pl.concat_str(
                    pl.lit("-"),
                    pl.col("_round_offset").cast(pl.Utf8),
                    pl.lit("d"),
                )
            )
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
                pl.col("tournament_end_date").is_null().sum().alias("null_end_date"),
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
                     "null_end_date, null_round_order):")
        for row in summary.iter_rows(named=True):
            logger.error(
                "  %s/%s (%s): %d matches, %d missing end_date, %d missing round_order",
                row["tournament_id"],
                row["year"],
                row["circuit"],
                row["count"],
                row["null_end_date"],
                row["null_round_order"],
            )

        # Log sample of actual bad rows (limited to 20)
        detail_cols = group_keys + [
            "match_uid", "round", "round_order",
            "tournament_end_date", "scheduled_datetime",
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


def validate_tournament_scheduling(
    df: pl.DataFrame, window_days: int = 3, threshold: int = 3
) -> list[dict]:
    """Flag players with suspiciously many tournaments in a short window.

    Returns a list of warning dicts with player_id, tournament_ids, and dates.
    """
    warnings: list[dict] = []

    pts = df.select(["player_id", "tournament_id", "tournament_end_date"]).unique()
    pts = pts.filter(pl.col("tournament_end_date").is_not_null())

    for pid in pts["player_id"].unique().to_list():
        player = pts.filter(pl.col("player_id") == pid).sort("tournament_end_date")
        tourneys = player.select(["tournament_id", "tournament_end_date"]).rows()

        for i, (tid_i, date_i) in enumerate(tourneys):
            cluster_tids = [tid_i]
            cluster_dates = [date_i]
            for j in range(i + 1, len(tourneys)):
                tid_j, date_j = tourneys[j]
                if (date_j - date_i).days <= window_days:
                    cluster_tids.append(tid_j)
                    cluster_dates.append(date_j)
                else:
                    break
            if len(cluster_tids) >= threshold:
                warnings.append({
                    "player_id": pid,
                    "tournament_ids": cluster_tids,
                    "dates": cluster_dates,
                })
                break  # One warning per player is enough

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

        # Step 8: Round order + effective match date + sort
        combined = add_round_order(combined)
        combined = add_effective_match_date(combined)
        combined = combined.sort(
            ["effective_match_date", "draw_type", "match_uid", "player_id"],
            nulls_last=True,
        )

        # Step 9: Validation
        warnings = validate_tournament_scheduling(combined)
        for w in warnings:
            logger.warning(
                "Suspicious tournament scheduling: player=%s, tournaments=%s, end_dates=%s",
                w["player_id"],
                w["tournament_ids"],
                w["dates"],
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
        """LEFT JOIN Activity rank fields onto overlapping tournament matches."""
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
        ])

        result = matches.join(
            act_enrichment, on=["match_uid", "player_id"], how="left"
        )

        # Fill tournament dates from Activity where tournament matches are missing them
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
            ]).drop(["_act_start_date", "_act_end_date"])

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
