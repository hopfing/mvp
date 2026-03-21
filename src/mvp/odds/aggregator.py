"""Odds aggregation: per-book summaries and cross-book summary."""

import logging

import polars as pl

from mvp.common.base_job import get_data_root

logger = logging.getLogger(__name__)

BOOKS = ["dk", "br", "mgm"]


def compute_book_odds(snapshots: pl.DataFrame, book: str) -> pl.DataFrame:
    """Compute per-match odds summary for one book from resolved snapshots.

    Args:
        snapshots: Resolved snapshots (match_uid, book, side, odds,
                   fetched_at, event_status).
        book: Book label to filter on.

    Returns:
        One row per match. Columns: match_uid, book, has_prematch,
        opening/closing/min/max odds, direction, movement, n_snapshots.
    """
    book_data = snapshots.filter(pl.col("book") == book)
    if len(book_data) == 0:
        return _empty_book_odds()

    results = []
    for match_uid in book_data["match_uid"].unique().to_list():
        match_odds = book_data.filter(pl.col("match_uid") == match_uid)
        results.append(_compute_match_odds(match_uid, book, match_odds))

    if not results:
        return _empty_book_odds()

    return pl.DataFrame(results)


def _compute_match_odds(match_uid: str, book: str, match_odds: pl.DataFrame) -> dict:
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
            for prefix in [
                "opening_odds_", "closing_odds_", "closing_implied_",
                "min_odds_", "max_odds_", "direction_", "movement_pct_",
            ]:
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


def compute_cross_book_odds(book_odds_list: list[pl.DataFrame]) -> pl.DataFrame:
    """Compute cross-book odds summary from per-book summaries.

    Args:
        book_odds_list: List of per-book odds DataFrames.

    Returns:
        One row per match. Columns: best/worst/avg closing, best opening,
        best/worst intraday, n_books.
    """
    if not book_odds_list:
        return _empty_cross_book()

    all_books = pl.concat(book_odds_list, how="diagonal_relaxed")
    prematch_only = all_books.filter(pl.col("has_prematch"))

    if len(prematch_only) == 0:
        return _empty_cross_book()

    match_uids = prematch_only["match_uid"].unique().to_list()
    results = []

    for uid in match_uids:
        match_data = prematch_only.filter(pl.col("match_uid") == uid)
        row = {"match_uid": uid}

        row["n_books"] = len(match_data)

        for side in ("p1", "p2"):
            closing_col = f"closing_odds_{side}"
            opening_col = f"opening_odds_{side}"
            max_col = f"max_odds_{side}"
            min_col = f"min_odds_{side}"

            closing = match_data[closing_col].drop_nulls()
            opening = match_data[opening_col].drop_nulls()
            max_odds = match_data[max_col].drop_nulls()
            min_odds = match_data[min_col].drop_nulls()

            def _val(s, fn):
                return fn() if len(s) > 0 else None

            row[f"best_closing_odds_{side}"] = _val(closing, closing.max)
            row[f"worst_closing_odds_{side}"] = _val(closing, closing.min)
            row[f"avg_closing_odds_{side}"] = _val(closing, closing.mean)
            row[f"best_opening_odds_{side}"] = _val(opening, opening.max)
            row[f"best_intraday_odds_{side}"] = _val(max_odds, max_odds.max)
            row[f"worst_intraday_odds_{side}"] = _val(min_odds, min_odds.min)

        results.append(row)

    if not results:
        return _empty_cross_book()

    return pl.DataFrame(results)


def save_book_odds(df: pl.DataFrame, book: str) -> None:
    """Save per-book odds summary."""
    path = get_data_root() / "aggregate" / book / "odds.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    logger.info("%s book odds: %d matches -> %s", book.upper(), len(df), path)


def save_cross_book_odds(df: pl.DataFrame) -> None:
    """Save cross-book odds summary."""
    path = get_data_root() / "aggregate" / "odds" / "odds.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    logger.info("Cross-book odds: %d matches -> %s", len(df), path)


def _empty_book_odds() -> pl.DataFrame:
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


def _empty_cross_book() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "best_closing_odds_p1": pl.Float64,
        "best_closing_odds_p2": pl.Float64,
        "worst_closing_odds_p1": pl.Float64,
        "worst_closing_odds_p2": pl.Float64,
        "avg_closing_odds_p1": pl.Float64,
        "avg_closing_odds_p2": pl.Float64,
        "best_opening_odds_p1": pl.Float64,
        "best_opening_odds_p2": pl.Float64,
        "best_intraday_odds_p1": pl.Float64,
        "best_intraday_odds_p2": pl.Float64,
        "worst_intraday_odds_p1": pl.Float64,
        "worst_intraday_odds_p2": pl.Float64,
        "n_books": pl.Int64,
    })
