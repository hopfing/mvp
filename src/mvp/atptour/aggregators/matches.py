"""Layer 2: Cross-tournament aggregation into a single enriched matches dataset."""

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
    """Exclude Davis Cup tournaments from a Layer 1 stacked DataFrame.

    Filters out rows where event_type starts with 'DC' or circuit is 'team'.
    """
    return df.filter(
        ~(
            pl.col("event_type").str.starts_with("DC").fill_null(False)
            | (pl.col("circuit") == "team")
        )
    )


def filter_dc_activity(df: pl.DataFrame) -> pl.DataFrame:
    """Exclude Davis Cup rows from Activity data."""
    return df.filter(pl.col("event_type") != "DC")


def map_activity_to_layer2(df: pl.DataFrame) -> pl.DataFrame:
    """Map Activity columns to Layer 2 schema for gap-fill rows.

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


def validate_tournament_clusters(
    df: pl.DataFrame, window_days: int = 7, threshold: int = 3
) -> list[dict]:
    """Flag players with suspiciously many tournaments in a short window.

    Returns a list of warning dicts with player_id, tournament_ids, and dates.
    """
    warnings: list[dict] = []

    pts = df.select(["player_id", "tournament_id", "tournament_start_date"]).unique()
    pts = pts.filter(pl.col("tournament_start_date").is_not_null())

    for pid in pts["player_id"].unique().to_list():
        player = pts.filter(pl.col("player_id") == pid).sort("tournament_start_date")
        tourneys = player.select(["tournament_id", "tournament_start_date"]).rows()

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
    """Layer 2: Cross-tournament aggregation."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def aggregate(self) -> pl.DataFrame:
        """Run the full Layer 2 aggregation pipeline."""
        # Step 1: Stack Layer 1
        l1 = self._stack_layer1()
        logger.info("Layer 1 stacked: %d rows", len(l1))

        # Step 2: Load and filter Activity
        activity = self._load_activity()
        logger.info("Activity loaded: %d rows", len(activity))

        # Step 3: Activity enrichment (overlapping matches)
        l1 = self._enrich_from_activity(l1, activity)

        # Step 4: Activity gap-fill (new matches)
        gap_fill = self._activity_gap_fill(l1, activity)
        logger.info("Activity gap-fill: %d rows", len(gap_fill))

        # Step 5: Concat
        combined = pl.concat([l1, gap_fill], how="diagonal_relaxed")
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

        # Step 8: Round order + sort
        combined = add_round_order(combined)
        combined = combined.sort(
            ["tournament_start_date", "round_order", "match_uid", "player_id"],
            nulls_last=True,
        )

        # Step 9: Validation
        warnings = validate_tournament_clusters(combined)
        for w in warnings:
            logger.warning(
                "Suspicious cluster: player=%s, tournaments=%s, dates=%s",
                w["player_id"],
                w["tournament_ids"],
                w["dates"],
            )

        return combined

    def _stack_layer1(self) -> pl.DataFrame:
        """Glob and concat all Layer 1 matches parquets, filtering DC."""
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
        self, l1: pl.DataFrame, activity: pl.DataFrame
    ) -> pl.DataFrame:
        """LEFT JOIN Activity rank fields onto overlapping Layer 1 rows."""
        if activity.is_empty():
            return l1.with_columns([
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

        result = l1.join(
            act_enrichment, on=["match_uid", "player_id"], how="left"
        )

        # Fill tournament dates from Activity where Layer 1 is missing them
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
        self, l1: pl.DataFrame, activity: pl.DataFrame
    ) -> pl.DataFrame:
        """Get Activity rows not in Layer 1 and map to Layer 2 schema."""
        if activity.is_empty():
            return pl.DataFrame()
        l1_uids = (
            set(l1["match_uid"].unique().to_list()) if not l1.is_empty() else set()
        )
        gap = activity.filter(~pl.col("match_uid").is_in(list(l1_uids)))
        if gap.is_empty():
            return pl.DataFrame()
        return map_activity_to_layer2(gap)

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
