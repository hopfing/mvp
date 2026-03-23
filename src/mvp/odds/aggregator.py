"""Odds aggregation: per-book summaries and cross-book summary."""

import logging

import polars as pl

from mvp.common.base_job import get_data_root

logger = logging.getLogger(__name__)

BOOKS = ["dk", "br", "mgm"]


def compute_book_odds(snapshots: pl.DataFrame, book: str) -> pl.DataFrame:
    """Compute per-player odds summary for one book from resolved snapshots.

    Args:
        snapshots: Resolved snapshots (match_uid, book, player_id, odds,
                   fetched_at, event_status).
        book: Book label to filter on.

    Returns:
        One row per match per player. Columns: match_uid, book, player_id,
        has_prematch, opening/closing/min/max odds, direction, movement,
        n_snapshots, closing_fetched_at.
    """
    id_col = "player_id" if "player_id" in snapshots.columns else "side"

    book_data = snapshots.filter(pl.col("book") == book)
    if len(book_data) == 0:
        return _empty_book_odds()

    results = []
    for match_uid in book_data["match_uid"].unique().to_list():
        match_odds = book_data.filter(pl.col("match_uid") == match_uid)
        prematch = match_odds.filter(pl.col("event_status") == "NOT_STARTED")

        if len(prematch) == 0:
            continue

        n_snapshots = prematch["fetched_at"].unique().len()
        closing_fetched_at = prematch["fetched_at"].max()

        for player in prematch[id_col].unique().to_list():
            player_odds = prematch.filter(pl.col(id_col) == player).sort("fetched_at")
            if len(player_odds) == 0:
                continue

            opening = player_odds["odds"][0]
            closing = player_odds["odds"][-1]

            direction = None
            movement_pct = None
            if opening > 0:
                movement_pct = (closing - opening) / opening
                if abs(movement_pct) < 0.005:
                    direction = "STABLE"
                elif movement_pct < 0:
                    direction = "SHORTENED"
                else:
                    direction = "DRIFTED"

            results.append({
                "match_uid": match_uid,
                "book": book,
                "player_id": player,
                "has_prematch": True,
                "opening_odds": opening,
                "closing_odds": closing,
                "closing_implied": 1.0 / closing if closing > 0 else None,
                "min_odds": player_odds["odds"].min(),
                "max_odds": player_odds["odds"].max(),
                "direction": direction,
                "movement_pct": movement_pct,
                "n_snapshots": n_snapshots,
                "closing_fetched_at": closing_fetched_at,
            })

    if not results:
        return _empty_book_odds()

    return pl.DataFrame(results)


def compute_cross_book_odds(book_odds_list: list[pl.DataFrame]) -> pl.DataFrame:
    """Compute cross-book odds summary from per-book summaries.

    Args:
        book_odds_list: List of per-book per-player odds DataFrames.

    Returns:
        One row per match per player. Best/worst/avg closing, best opening,
        best/worst intraday, n_books.
    """
    if not book_odds_list:
        return _empty_cross_book()

    all_books = pl.concat(book_odds_list, how="diagonal_relaxed")
    prematch_only = all_books.filter(pl.col("has_prematch"))

    if len(prematch_only) == 0:
        return _empty_cross_book()

    results = []
    for (uid, pid), group in prematch_only.group_by(["match_uid", "player_id"]):
        closing = group["closing_odds"].drop_nulls()
        opening = group["opening_odds"].drop_nulls()
        max_odds = group["max_odds"].drop_nulls()
        min_odds = group["min_odds"].drop_nulls()

        def _val(s, fn):
            return fn() if len(s) > 0 else None

        results.append({
            "match_uid": uid,
            "player_id": pid,
            "n_books": len(group),
            "best_closing_odds": _val(closing, closing.max),
            "worst_closing_odds": _val(closing, closing.min),
            "avg_closing_odds": _val(closing, closing.mean),
            "best_opening_odds": _val(opening, opening.max),
            "best_intraday_odds": _val(max_odds, max_odds.max),
            "worst_intraday_odds": _val(min_odds, min_odds.min),
        })

    if not results:
        return _empty_cross_book()

    return pl.DataFrame(results)


def compute_opening_odds(snapshots: pl.DataFrame) -> pl.DataFrame:
    """Compute first-available and market-formed opening odds from raw snapshots.

    Uses 15-minute floor buckets to align cross-book fetch times.

    - open_odds: earliest bucket with any book, avg if multiple.
    - market_formed_odds: earliest bucket where 2+ books cover the match,
      avg odds for each player at that bucket.

    Args:
        snapshots: Resolved snapshots with match_uid, book, player_id, odds,
                   fetched_at, event_status.

    Returns:
        One row per (match_uid, player_id) with open_odds and
        market_formed_odds columns.
    """
    if len(snapshots) == 0:
        return _empty_openings()

    id_col = "player_id" if "player_id" in snapshots.columns else "side"

    prematch = snapshots.filter(pl.col("event_status") == "NOT_STARTED")
    if len(prematch) == 0:
        return _empty_openings()

    pm = prematch.with_columns(
        pl.col("fetched_at").dt.truncate("15m").alias("fetch_round")
    )

    # --- First available ---
    # Per match+player+round: average odds across books
    per_round = pm.group_by(["match_uid", id_col, "fetch_round"]).agg(
        pl.col("odds").mean().alias("avg_odds"),
    )

    # Per match+player: earliest round via min join
    min_rounds = per_round.group_by(["match_uid", id_col]).agg(
        pl.col("fetch_round").min().alias("min_round"),
    )
    open_line = (
        per_round.join(min_rounds, on=["match_uid", id_col])
        .filter(pl.col("fetch_round") == pl.col("min_round"))
        .select("match_uid",
                pl.col(id_col).alias("player_id"),
                pl.col("avg_odds").alias("open_odds"))
    )

    # --- Market formed ---
    # Per match+round: count distinct books
    books_per_round = pm.group_by(["match_uid", "fetch_round"]).agg(
        pl.col("book").n_unique().alias("n_books"),
    )

    # Per match: earliest round with 2+ books
    market_min = (
        books_per_round.filter(pl.col("n_books") >= 2)
        .group_by("match_uid")
        .agg(pl.col("fetch_round").min().alias("market_round"))
    )

    # Odds at market_round per player (avg across books present)
    market_formed = (
        pm.join(market_min, on="match_uid")
        .filter(pl.col("fetch_round") == pl.col("market_round"))
        .group_by(["match_uid", id_col])
        .agg(pl.col("odds").mean().alias("market_formed_odds"))
        .rename({id_col: "player_id"})
    )

    # Combine
    result = open_line.join(
        market_formed, on=["match_uid", "player_id"], how="left"
    )

    return result


def _empty_openings() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "player_id": pl.Utf8,
        "open_odds": pl.Float64,
        "market_formed_odds": pl.Float64,
    })


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
        "player_id": pl.Utf8,
        "has_prematch": pl.Boolean,
        "opening_odds": pl.Float64,
        "closing_odds": pl.Float64,
        "closing_implied": pl.Float64,
        "min_odds": pl.Float64,
        "max_odds": pl.Float64,
        "direction": pl.Utf8,
        "movement_pct": pl.Float64,
        "n_snapshots": pl.Int64,
        "closing_fetched_at": pl.Datetime("us", "UTC"),
    })


def _empty_cross_book() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "player_id": pl.Utf8,
        "n_books": pl.Int64,
        "best_closing_odds": pl.Float64,
        "worst_closing_odds": pl.Float64,
        "avg_closing_odds": pl.Float64,
        "best_opening_odds": pl.Float64,
        "best_intraday_odds": pl.Float64,
        "worst_intraday_odds": pl.Float64,
    })
