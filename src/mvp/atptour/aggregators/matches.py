"""Layer 2: Cross-tournament aggregation into a single enriched matches dataset."""

import polars as pl

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

    return result


def add_round_order(df: pl.DataFrame) -> pl.DataFrame:
    """Add round_order column from the round column using ROUND_ORDER mapping."""
    return df.with_columns(
        pl.col("round")
        .replace_strict(ROUND_ORDER, default=None)
        .cast(pl.Int64)
        .alias("round_order")
    )
