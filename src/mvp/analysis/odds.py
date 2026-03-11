"""Per-match-book odds computation: closing lines, movement metrics."""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def compute_odds_by_book(
    staged_odds: pl.DataFrame,
    event_map: pl.DataFrame,
    book: str,
    event_id_col: str,
) -> pl.DataFrame:
    """Compute per-match-book odds summary from staged snapshots.

    Args:
        staged_odds: Staged odds parquet for one book.
        event_map: Event mapping table with match_uid, event_id, p1/p2_book_name.
        book: Book identifier (dk, br).
        event_id_col: Column name for event ID in staged_odds (dk_event_id, br_event_id).

    Returns:
        DataFrame with one row per match, containing opening/closing odds,
        movement metrics, and pre-match flag.
    """
    book_map = event_map.filter(pl.col("book") == book)
    if len(book_map) == 0:
        return _empty_result()

    joined = staged_odds.join(
        book_map.select("event_id", "match_uid", "p1_book_name", "p2_book_name"),
        left_on=event_id_col,
        right_on="event_id",
        how="inner",
    )

    if len(joined) == 0:
        return _empty_result()

    joined = joined.with_columns(
        pl.when(pl.col("player_name") == pl.col("p1_book_name"))
        .then(pl.lit("p1"))
        .when(pl.col("player_name") == pl.col("p2_book_name"))
        .then(pl.lit("p2"))
        .otherwise(pl.lit(None))
        .alias("side")
    ).filter(pl.col("side").is_not_null())

    results = []
    for match_uid in joined["match_uid"].unique().to_list():
        match_odds = joined.filter(pl.col("match_uid") == match_uid)
        results.append(_compute_match_odds(match_uid, book, match_odds))

    if not results:
        return _empty_result()

    return pl.DataFrame(results)


def _compute_match_odds(
    match_uid: str,
    book: str,
    match_odds: pl.DataFrame,
) -> dict:
    """Compute odds summary for one match from one book."""
    prematch = match_odds.filter(pl.col("event_status") == "NOT_STARTED")
    has_prematch = len(prematch) > 0

    row = {
        "match_uid": match_uid,
        "book": book,
        "has_prematch": has_prematch,
    }

    if not has_prematch:
        for col in [
            "opening_odds_p1", "opening_odds_p2",
            "closing_odds_p1", "closing_odds_p2",
            "closing_implied_p1", "closing_implied_p2",
            "min_odds_p1", "max_odds_p1",
            "min_odds_p2", "max_odds_p2",
            "direction_p1", "direction_p2",
            "movement_pct_p1", "movement_pct_p2",
            "closing_fetched_at",
        ]:
            row[col] = None
        row["n_snapshots"] = 0
        return row

    n_snapshots = prematch["fetched_at"].unique().len()
    row["n_snapshots"] = n_snapshots

    for side in ("p1", "p2"):
        side_odds = prematch.filter(pl.col("side") == side).sort("fetched_at")
        if len(side_odds) == 0:
            for prefix in ["opening_odds_", "closing_odds_", "closing_implied_",
                           "min_odds_", "max_odds_", "direction_", "movement_pct_"]:
                row[f"{prefix}{side}"] = None
            continue

        opening = side_odds["odds"][0]
        closing = side_odds["odds"][-1]
        row[f"opening_odds_{side}"] = opening
        row[f"closing_odds_{side}"] = closing
        row[f"closing_implied_{side}"] = 1.0 / closing if closing > 0 else None
        row[f"min_odds_{side}"] = side_odds["odds"].min()
        row[f"max_odds_{side}"] = side_odds["odds"].max()

        if opening > 0:
            movement = (closing - opening) / opening
            row[f"movement_pct_{side}"] = movement
            if abs(movement) < 0.005:
                row[f"direction_{side}"] = "STABLE"
            elif movement < 0:
                row[f"direction_{side}"] = "SHORTENED"
            else:
                row[f"direction_{side}"] = "DRIFTED"
        else:
            row[f"movement_pct_{side}"] = None
            row[f"direction_{side}"] = None

    row["closing_fetched_at"] = prematch["fetched_at"].max()

    return row


def _empty_result() -> pl.DataFrame:
    """Return empty DataFrame with the expected schema."""
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "book": pl.Utf8,
        "has_prematch": pl.Boolean,
        "opening_odds_p1": pl.Float64,
        "opening_odds_p2": pl.Float64,
        "closing_odds_p1": pl.Float64,
        "closing_odds_p2": pl.Float64,
        "closing_implied_p1": pl.Float64,
        "closing_implied_p2": pl.Float64,
        "min_odds_p1": pl.Float64,
        "max_odds_p1": pl.Float64,
        "min_odds_p2": pl.Float64,
        "max_odds_p2": pl.Float64,
        "direction_p1": pl.Utf8,
        "direction_p2": pl.Utf8,
        "movement_pct_p1": pl.Float64,
        "movement_pct_p2": pl.Float64,
        "closing_fetched_at": pl.Datetime("us", "UTC"),
        "n_snapshots": pl.Int64,
    })
