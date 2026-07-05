"""Odds aggregation: per-book summaries and cross-book summary."""

import logging

import polars as pl

from mvp.common.base_job import get_data_root

logger = logging.getLogger(__name__)

BOOKS = ["dk", "br", "mgm", "b365", "fd"]

# Single source of truth for the timing thresholds used across the
# analysis layer. The values are hours before the first-live anchor.
THRESHOLD_HOURS: tuple[int, ...] = (1, 3, 6, 9, 12, 15, 18)
THRESHOLD_AGGS: tuple[str, ...] = ("best", "worst", "med")


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
        max_odds = group["max_odds"].drop_nulls()
        min_odds = group["min_odds"].drop_nulls()

        def _val(s, fn):
            return fn() if len(s) > 0 else None

        results.append({
            "match_uid": uid,
            "player_id": pid,
            "n_books": len(group),
            # best_opening_odds / best_closing_odds are computed time-aligned in
            # compute_open_close_odds (joined in refresh) — the per-book first/last
            # max here was time-skewed. worst/avg closing stay per-book-last for
            # now; only dataset CLV consumes them, re-alignment is a follow-up.
            "worst_closing_odds": _val(closing, closing.min),
            "avg_closing_odds": _val(closing, closing.mean),
            "best_intraday_odds": _val(max_odds, max_odds.max),
            "worst_intraday_odds": _val(min_odds, min_odds.min),
        })

    if not results:
        return _empty_cross_book()

    return pl.DataFrame(results)


def compute_open_close_odds(snapshots: pl.DataFrame) -> pl.DataFrame:
    """Time-aligned best-across-book opening and closing odds per (match, player).

    Fixes the time-skew in the per-book first/last aggregation: books post their
    first (and last) snapshot hours apart, so maxing each book's first — or last —
    blends prices from different moments. Both points here are read at a single
    real instant across books, via 15-min buckets:

      best_opening_odds  max across books at the EARLIEST bucket with any quote
                         (the first price on the board; usually one book).
      best_closing_odds  max across books at the LAST bucket with any quote — the
                         last common moment before the off (a book that stopped
                         earlier simply isn't in that bucket).

    Args:
        snapshots: Resolved snapshots (match_uid, book, player_id, odds,
            fetched_at, event_status).

    Returns:
        One row per (match_uid, player_id) with best_opening_odds/best_closing_odds;
        an empty typed frame if there are no prematch snapshots.
    """
    if len(snapshots) == 0:
        return _empty_open_close()
    id_col = "player_id" if "player_id" in snapshots.columns else "side"
    pm = snapshots.filter(
        (pl.col("event_status") == "NOT_STARTED") & pl.col("odds").is_not_null()
    )
    if len(pm) == 0:
        return _empty_open_close()

    key = ["match_uid", id_col]

    # Bucket fetches to 15-min rounds; one price per (match, player, round, book)
    # = that book's last quote in the round. Then per round take the best (max)
    # price across the books present.
    rounds = (
        pm.with_columns(pl.col("fetched_at").dt.truncate("15m").alias("_rnd"))
        .sort("fetched_at")
        .group_by(key + ["_rnd", "book"], maintain_order=True)
        .agg(pl.col("odds").last().alias("odds"))
        .group_by(key + ["_rnd"])
        .agg(pl.col("odds").max().alias("_best"))
        .sort("_rnd")
    )
    # First bucket = open, last bucket = close — symmetric, each a single instant.
    out = rounds.group_by(key, maintain_order=True).agg(
        pl.col("_best").first().alias("best_opening_odds"),
        pl.col("_best").last().alias("best_closing_odds"),
    )
    if id_col != "player_id":
        out = out.rename({id_col: "player_id"})
    return out


def _empty_open_close() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "player_id": pl.Utf8,
        "best_opening_odds": pl.Float64,
        "best_closing_odds": pl.Float64,
    })


def compute_threshold_odds(
    snapshots: pl.DataFrame,
    match_anchors: pl.DataFrame,
    book: str,
    hours_before_start: int,
) -> pl.DataFrame:
    """Per-(match, player) line at the latest NOT_STARTED snapshot ≤ anchor − Nh.

    For each (match_uid, player_id), take the snapshot whose run_at is the
    maximum across:
      - book matches the requested book
      - event_status == "NOT_STARTED"
      - run_at <= anchor_at - hours_before_start

    Falls back to fetched_at if run_at is not present in snapshots.

    Args:
        snapshots: Resolved snapshots with (match_uid, book, player_id, odds,
            fetched_at, event_status); optionally run_at.
        match_anchors: Per-match reference timestamp with (match_uid,
            anchor_at). Use ``first_live_fetched_at`` as the anchor
            (same-clock as fetched_at) rather than scheduled_datetime,
            which is in tournament-local tz and not directly comparable
            to fetched_at.
        book: Book label.
        hours_before_start: Threshold in hours before the anchor.

    Returns:
        One row per (match_uid, player_id) where the book had a qualifying
        snapshot. Columns: match_uid, book, player_id, threshold_h,
        threshold_odds, threshold_run_at, threshold_fetched_at,
        threshold_implied. Empty DataFrame if no qualifying snapshots.

    Note:
        Stale lines from books that stopped quoting before anchor − Nh will
        appear here — freshness filtering is the job of the cross-book
        aggregator.
    """
    if len(snapshots) == 0 or "anchor_at" not in match_anchors.columns:
        return _empty_threshold_odds()

    book_snaps = snapshots.filter(
        (pl.col("book") == book)
        & (pl.col("event_status") == "NOT_STARTED")
    )
    if len(book_snaps) == 0:
        return _empty_threshold_odds()

    ts_col = "run_at" if "run_at" in book_snaps.columns else "fetched_at"

    anchors = (
        match_anchors.select("match_uid", "anchor_at")
        .filter(pl.col("anchor_at").is_not_null())
        .unique(subset=["match_uid"])
    )
    if len(anchors) == 0:
        return _empty_threshold_odds()

    qualifying = book_snaps.join(anchors, on="match_uid", how="inner")
    cutoff_expr = pl.col("anchor_at") - pl.duration(hours=hours_before_start)
    qualifying = qualifying.filter(pl.col(ts_col) <= cutoff_expr)
    if len(qualifying) == 0:
        return _empty_threshold_odds()

    qualifying = (
        qualifying
        .sort([ts_col, "fetched_at"])
        .group_by(["match_uid", "player_id"], maintain_order=True)
        .last()
    )

    cutoff_lit = pl.col("anchor_at") - pl.duration(hours=hours_before_start)
    return qualifying.select(
        "match_uid",
        pl.lit(book).alias("book"),
        "player_id",
        pl.lit(hours_before_start, dtype=pl.Int64).alias("threshold_h"),
        pl.col("odds").alias("threshold_odds"),
        pl.col(ts_col).alias("threshold_run_at"),
        pl.col("fetched_at").alias("threshold_fetched_at"),
        pl.when(pl.col("odds") > 0)
        .then(1.0 / pl.col("odds"))
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("threshold_implied"),
        ((cutoff_lit - pl.col(ts_col)).dt.total_seconds() / 60)
        .cast(pl.Float64)
        .alias("threshold_lag_min"),
    )


def _empty_threshold_odds() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "book": pl.Utf8,
        "player_id": pl.Utf8,
        "threshold_h": pl.Int64,
        "threshold_odds": pl.Float64,
        "threshold_run_at": pl.Datetime("us"),
        "threshold_fetched_at": pl.Datetime("us"),
        "threshold_implied": pl.Float64,
        "threshold_lag_min": pl.Float64,
    })


def compute_cross_book_threshold_odds(
    threshold_per_book_list: list[pl.DataFrame],
    max_lag_min: int | None = 30,
) -> pl.DataFrame:
    """Cross-book best/worst/median per (match, player, threshold).

    Drops books whose ``threshold_lag_min`` (minutes between
    ``threshold_run_at`` and the threshold cutoff anchor − Nh) exceeds
    ``max_lag_min``. This is an absolute freshness check — a book that
    stopped quoting more than ``max_lag_min`` before the threshold time
    is excluded, regardless of what other books in the same match did.

    Args:
        threshold_per_book_list: List of per-book DataFrames produced by
            :func:`compute_threshold_odds`. Each row carries
            ``threshold_lag_min`` = (anchor − Nh) − ``threshold_run_at``,
            in minutes; non-negative by construction.
        max_lag_min: Maximum allowed lag (minutes). ``None`` disables
            the freshness filter.

    Returns:
        One row per (match_uid, player_id, threshold_h) with cross-book
        best/worst/avg/median threshold odds and a count of qualifying
        books.
    """
    if not threshold_per_book_list:
        return _empty_cross_book_threshold()

    all_books = pl.concat(threshold_per_book_list, how="diagonal_relaxed")
    if len(all_books) == 0:
        return _empty_cross_book_threshold()

    if max_lag_min is not None and "threshold_lag_min" in all_books.columns:
        all_books = all_books.filter(pl.col("threshold_lag_min") <= max_lag_min)

    if len(all_books) == 0:
        return _empty_cross_book_threshold()

    return (
        all_books
        .group_by(["match_uid", "player_id", "threshold_h"])
        .agg(
            pl.len().alias("n_books"),
            pl.col("threshold_odds").max().alias("best_threshold_odds"),
            pl.col("threshold_odds").min().alias("worst_threshold_odds"),
            pl.col("threshold_odds").median().alias("median_threshold_odds"),
        )
    )


def _empty_cross_book_threshold() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "match_uid": pl.Utf8,
        "player_id": pl.Utf8,
        "threshold_h": pl.Int64,
        "n_books": pl.UInt32,
        "best_threshold_odds": pl.Float64,
        "worst_threshold_odds": pl.Float64,
        "median_threshold_odds": pl.Float64,
    })


def compute_first_live_anchor(snapshots: pl.DataFrame) -> pl.DataFrame:
    """Per-match ``first_live_fetched_at`` for use as a threshold anchor.

    Defined identically to :func:`mvp.analysis.dataset._join_first_live_ts`:
    the first non-NOT_STARTED snapshot whose fetched_at is strictly after
    every NOT_STARTED snapshot seen for that match across any book. This
    is in the same clock as ``fetched_at`` (UTC), so it can be subtracted
    from snapshot timestamps without timezone bias (cf. issue #86).

    Args:
        snapshots: Resolved snapshots with (match_uid, fetched_at,
            event_status).

    Returns:
        One row per match with columns (match_uid, anchor_at). Empty
        DataFrame if no matches have a STARTED/IN_PLAY snapshot.
    """
    if len(snapshots) == 0 or "event_status" not in snapshots.columns:
        return pl.DataFrame(schema={
            "match_uid": pl.Utf8,
            "anchor_at": pl.Datetime("us"),
        })

    last_ns = (
        snapshots.filter(pl.col("event_status") == "NOT_STARTED")
        .group_by("match_uid")
        .agg(pl.col("fetched_at").max().alias("_last_ns"))
    )
    live = snapshots.filter(pl.col("event_status") != "NOT_STARTED")
    if len(live) == 0:
        return pl.DataFrame(schema={
            "match_uid": pl.Utf8,
            "anchor_at": snapshots.schema["fetched_at"],
        })

    return (
        live.join(last_ns, on="match_uid", how="left")
        .filter(
            pl.col("_last_ns").is_null()
            | (pl.col("fetched_at") > pl.col("_last_ns"))
        )
        .group_by("match_uid")
        .agg(pl.col("fetched_at").min().alias("anchor_at"))
    )


def compute_threshold_odds_all(
    snapshots: pl.DataFrame,
    match_anchors: pl.DataFrame,
    thresholds_hours: list[int],
    books: list[str],
    max_lag_min: int | None = 30,
) -> pl.DataFrame:
    """Compute long-format cross-book threshold odds across all thresholds.

    For each ``thresh`` in ``thresholds_hours`` and each book in ``books``,
    runs :func:`compute_threshold_odds` and aggregates across books with
    :func:`compute_cross_book_threshold_odds`. Concatenates the per-threshold
    results.

    Args:
        snapshots: Resolved snapshots.
        match_anchors: Per-match anchor table (match_uid, anchor_at).
        thresholds_hours: List of thresholds in hours.
        books: List of book codes to include.
        max_lag_min: Freshness tolerance passed through to
            :func:`compute_cross_book_threshold_odds`.

    Returns:
        Long-format DataFrame: one row per (match_uid, player_id, threshold_h)
        with cross-book best/worst/avg/median threshold odds and ``n_books``.
    """
    pieces: list[pl.DataFrame] = []
    for thresh in thresholds_hours:
        per_book = []
        for book in books:
            df = compute_threshold_odds(snapshots, match_anchors, book, thresh)
            if len(df) > 0:
                per_book.append(df)
        cross = compute_cross_book_threshold_odds(
            per_book, max_lag_min=max_lag_min,
        )
        if len(cross) > 0:
            pieces.append(cross)
    if not pieces:
        return _empty_cross_book_threshold()
    return pl.concat(pieces, how="diagonal_relaxed")


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
        "worst_closing_odds": pl.Float64,
        "avg_closing_odds": pl.Float64,
        "best_intraday_odds": pl.Float64,
        "worst_intraday_odds": pl.Float64,
    })
