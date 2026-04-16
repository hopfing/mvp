"""Unified analysis dataset: joins predictions with results, sheet data, and odds."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

logger = logging.getLogger(__name__)

CIRCUIT_REVERSE = {"CH": "chal", "ATP": "tour"}

SHEET_COLUMNS = [
    "match_uid",
    "p1_odds",
    "p2_odds",
    "p1_pin",
    "p2_pin",
    "fav_edge",
    "dog_edge",
    "bet_side",
    "bet_odds",
    "stake",
    "book",
    "bet_result",
    "net",
    "notes",
    "bet_placed_at",
]


def build_analysis_dataset(
    predictions: pl.DataFrame,
    match_meta: pl.DataFrame | None = None,
    results: pl.DataFrame | None = None,
    sheet_data: pl.DataFrame | None = None,
    odds_by_book: pl.DataFrame | None = None,
    cross_book_odds: pl.DataFrame | None = None,
    all_snapshots: pl.DataFrame | None = None,
    opening_odds: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build a single wide DataFrame for analysis.

    Predictions are the base. Everything else is left-joined.

    Args:
        predictions: Model predictions (required).
        match_meta: Per-match metadata from matches.parquet (source of truth
            for effective_match_date, scheduled_datetime, round, tournament
            fields). Replaces the stale values baked into predictions.parquet
            at initial prediction time.
        results: Match results with match_uid and result columns.
        sheet_data: Google Sheets data (circuit uses CH/ATP format).
        odds_by_book: Long-format per-book odds summaries.
        cross_book_odds: Pre-computed cross-book odds summary from aggregator.
        all_snapshots: Resolved snapshots for market alignment at bet time.
        opening_odds: First-available and market-formed opening odds per player.

    Returns:
        Wide DataFrame with all joined data and derived metrics.
    """
    ds = predictions.clone()

    ds = _join_match_meta(ds, match_meta)
    ds = _join_results(ds, results)
    ds = _join_sheet_data(ds, sheet_data)

    if cross_book_odds is not None and len(cross_book_odds) > 0:
        ds = _join_cross_book_odds(ds, cross_book_odds)
        # Also join per-book wide columns alongside cross-book aggregates
        if odds_by_book is not None and len(odds_by_book) > 0:
            ds = _join_per_book_odds(ds, odds_by_book)
    else:
        ds = _join_odds(ds, odds_by_book, skip_cross_book=False)

    if opening_odds is not None and len(opening_odds) > 0:
        ds = _join_cross_book_odds(ds, opening_odds)

    ds = _compute_pred_side_metrics(ds)
    ds = _compute_clv(ds)

    if all_snapshots is not None:
        ds = _join_first_live_ts(ds, all_snapshots)
        ds = _compute_market_alignment(ds, all_snapshots)

    return ds


def _join_first_live_ts(
    ds: pl.DataFrame, all_snapshots: pl.DataFrame
) -> pl.DataFrame:
    """Attach first_live_fetched_at per match.

    Defined as: first snapshot with event_status != NOT_STARTED whose fetched_at
    is strictly after the last NOT_STARTED snapshot seen across any book. This
    guards against books that flip STARTED at the scheduled time while other
    books still correctly report NOT_STARTED (common on rescheduled matches).
    """
    if (
        len(all_snapshots) == 0
        or "event_status" not in all_snapshots.columns
        or "fetched_at" not in all_snapshots.columns
    ):
        return ds

    live = all_snapshots.filter(pl.col("event_status") != "NOT_STARTED")
    if len(live) == 0:
        return ds

    last_ns = (
        all_snapshots.filter(pl.col("event_status") == "NOT_STARTED")
        .group_by("match_uid")
        .agg(pl.col("fetched_at").max().alias("_last_ns"))
    )

    first_live = (
        live.join(last_ns, on="match_uid", how="left")
        .filter(
            pl.col("_last_ns").is_null() | (pl.col("fetched_at") > pl.col("_last_ns"))
        )
        .group_by("match_uid")
        .agg(pl.col("fetched_at").min().alias("first_live_fetched_at"))
    )

    return ds.join(first_live, on="match_uid", how="left")


def _join_match_meta(
    ds: pl.DataFrame, match_meta: pl.DataFrame | None
) -> pl.DataFrame:
    """Replace stale match metadata from predictions with fresh values from matches.parquet.

    Predictions.parquet bakes in match metadata at initial prediction time and is
    never refreshed (save_predictions only appends new match_uids). matches.parquet
    is the single source of truth for per-match metadata — drop the stale columns
    from the predictions base and left-join the fresh ones on match_uid.
    """
    if match_meta is None:
        return ds

    replace_cols = [c for c in match_meta.columns if c != "match_uid"]
    drop_cols = [c for c in replace_cols if c in ds.columns]
    if drop_cols:
        ds = ds.drop(drop_cols)

    return ds.join(match_meta, on="match_uid", how="left")


def _join_results(ds: pl.DataFrame, results: pl.DataFrame | None) -> pl.DataFrame:
    """Join results and compute status + model_correct."""
    if results is None:
        return ds.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("result"),
            pl.lit("pending").alias("status"),
            pl.lit(None).cast(pl.Boolean).alias("model_correct"),
        )

    ds = ds.join(
        results.select("match_uid", "result"),
        on="match_uid",
        how="left",
    )

    predicted_side = (
        pl.when(pl.col("p1_win_prob") > 0.5)
        .then(pl.lit("P1"))
        .otherwise(pl.lit("P2"))
    )

    ds = ds.with_columns(
        pl.when(pl.col("result").is_not_null())
        .then(pl.lit("resolved"))
        .otherwise(pl.lit("pending"))
        .alias("status"),
        pl.when(pl.col("result").is_not_null())
        .then(predicted_side == pl.col("result"))
        .otherwise(pl.lit(None).cast(pl.Boolean))
        .alias("model_correct"),
    )

    return ds


def _join_sheet_data(ds: pl.DataFrame, sheet_data: pl.DataFrame | None) -> pl.DataFrame:
    """Join sheet data on match_uid, dropping sheet's circuit column."""
    if sheet_data is None:
        return ds

    available = [c for c in SHEET_COLUMNS if c in sheet_data.columns]
    sheet_subset = sheet_data.select(available)

    # fav_edge/dog_edge come from sheet formulas as Utf8 strings — cast to Float64
    edge_casts = [
        pl.col(c).cast(pl.Float64, strict=False).alias(c)
        for c in ("fav_edge", "dog_edge")
        if c in sheet_subset.columns
    ]
    if edge_casts:
        sheet_subset = sheet_subset.with_columns(edge_casts)

    ds = ds.join(sheet_subset, on="match_uid", how="left")

    return ds


def _join_cross_book_odds(ds: pl.DataFrame, cross_book: pl.DataFrame) -> pl.DataFrame:
    """Join per-player cross-book odds to predictions.

    Cross-book odds are long format: one row per (match_uid, player_id).
    Join twice: once for p1's odds, once for p2's odds.
    """
    odds_cols = [c for c in cross_book.columns if c not in ("match_uid", "player_id")]

    # Join p1's odds
    p1_odds = cross_book.rename({c: f"{c}_p1" for c in odds_cols})
    ds = ds.join(
        p1_odds,
        left_on=["match_uid", "p1_id"],
        right_on=["match_uid", "player_id"],
        how="left",
    )

    # Join p2's odds
    p2_odds = cross_book.rename({c: f"{c}_p2" for c in odds_cols})
    ds = ds.join(
        p2_odds,
        left_on=["match_uid", "p2_id"],
        right_on=["match_uid", "player_id"],
        how="left",
    )

    return ds


def _join_per_book_odds(ds: pl.DataFrame, odds_by_book: pl.DataFrame) -> pl.DataFrame:
    """Join per-book odds as wide columns alongside existing cross-book aggregates.

    odds_by_book is long format: one row per (match_uid, player_id, book).
    Pivots to {book}_{col}_p1 / {book}_{col}_p2 wide columns.
    """
    if "book" not in odds_by_book.columns:
        return ds

    value_cols = [
        c for c in odds_by_book.columns
        if c not in ("match_uid", "player_id", "book")
    ]

    for book in odds_by_book["book"].unique().sort().to_list():
        book_df = odds_by_book.filter(pl.col("book") == book)

        # Join p1's per-book odds
        p1 = book_df.select(
            "match_uid", "player_id",
            *[pl.col(c).alias(f"{book}_{c}_p1") for c in value_cols],
        )
        ds = ds.join(
            p1, left_on=["match_uid", "p1_id"],
            right_on=["match_uid", "player_id"], how="left",
        )

        # Join p2's per-book odds
        p2 = book_df.select(
            "match_uid", "player_id",
            *[pl.col(c).alias(f"{book}_{c}_p2") for c in value_cols],
        )
        ds = ds.join(
            p2, left_on=["match_uid", "p2_id"],
            right_on=["match_uid", "player_id"], how="left",
        )

    return ds


def _join_odds(
    ds: pl.DataFrame,
    odds_by_book: pl.DataFrame | None,
    skip_cross_book: bool = False,
) -> pl.DataFrame:
    """Pivot odds from long to wide, join, and optionally compute cross-book metrics."""
    if odds_by_book is None:
        return ds

    books = odds_by_book["book"].unique().sort().to_list()
    non_key_cols = [c for c in odds_by_book.columns if c not in ("match_uid", "book")]

    for book in books:
        book_df = odds_by_book.filter(pl.col("book") == book).select(
            "match_uid",
            *[pl.col(c).alias(f"{book}_{c}") for c in non_key_cols],
        )
        ds = ds.join(book_df, on="match_uid", how="left")

    if not skip_cross_book:
        ds = _compute_cross_book_metrics(ds, books)

    return ds


def _compute_cross_book_metrics(ds: pl.DataFrame, books: list[str]) -> pl.DataFrame:
    """Compute best odds, model edge, and books_showing_edge (legacy path)."""
    for side in ("p1", "p2"):
        odds_col = f"closing_odds_{side}"
        prematch_col = "has_prematch"

        book_odds_exprs = []
        for book in books:
            col_name = f"{book}_{odds_col}"
            pm_name = f"{book}_{prematch_col}"
            if col_name in ds.columns:
                book_odds_exprs.append(
                    pl.when(pl.col(pm_name).fill_null(False))
                    .then(pl.col(col_name))
                    .otherwise(pl.lit(None))
                )

        if book_odds_exprs:
            ds = ds.with_columns(
                pl.max_horizontal(*book_odds_exprs).alias(f"best_closing_odds_{side}")
            )
            best_odds = pl.col(f"best_closing_odds_{side}")
            ds = ds.with_columns(
                (1.0 / best_odds).alias(f"best_closing_implied_{side}")
            )
        else:
            ds = ds.with_columns(
                pl.lit(None).cast(pl.Float64).alias(f"best_closing_odds_{side}"),
                pl.lit(None).cast(pl.Float64).alias(f"best_closing_implied_{side}"),
            )

    if "best_closing_implied_p1" in ds.columns:
        ds = ds.with_columns(
            (pl.col("p1_win_prob") - pl.col("best_closing_implied_p1"))
            .alias("model_edge_vs_best_p1"),
            (pl.col("p2_win_prob") - pl.col("best_closing_implied_p2"))
            .alias("model_edge_vs_best_p2"),
        )

    predicted_p1 = pl.col("p1_win_prob") > 0.5

    edge_exprs = []
    for book in books:
        pm_name = f"{book}_has_prematch"
        impl_p1 = f"{book}_closing_implied_p1"
        impl_p2 = f"{book}_closing_implied_p2"

        if impl_p1 in ds.columns and impl_p2 in ds.columns:
            has_edge = (
                pl.when(pl.col(pm_name).fill_null(False).not_())
                .then(pl.lit(0))
                .when(predicted_p1)
                .then(
                    pl.when(pl.col("p1_win_prob") > pl.col(impl_p1))
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                )
                .otherwise(
                    pl.when(pl.col("p2_win_prob") > pl.col(impl_p2))
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                )
            )
            edge_exprs.append(has_edge)

    if edge_exprs:
        ds = ds.with_columns(
            pl.sum_horizontal(*edge_exprs).alias("books_showing_edge")
        )

        prematch_count_exprs = [
            pl.col(f"{book}_has_prematch").fill_null(False).cast(pl.Int32)
            for book in books
            if f"{book}_has_prematch" in ds.columns
        ]
        if prematch_count_exprs:
            ds = ds.with_columns(
                pl.sum_horizontal(*prematch_count_exprs).alias("_total_prematch_books")
            )
            edge = pl.col("books_showing_edge").cast(pl.Float64)
            total = pl.col("_total_prematch_books").cast(pl.Float64)
            ds = ds.with_columns(
                pl.when(pl.col("_total_prematch_books") > 0)
                .then(edge / total)
                .otherwise(pl.lit(None).cast(pl.Float64))
                .alias("market_alignment")
            )
            ds = ds.drop("_total_prematch_books")

    return ds


def _compute_pred_side_metrics(ds: pl.DataFrame) -> pl.DataFrame:
    """Compute predicted-side odds and model edge metrics."""
    if "p1_win_prob" not in ds.columns:
        return ds

    ds = ds.with_columns(
        pl.when(pl.col("p1_win_prob") > 0.5)
        .then(pl.lit("P1"))
        .otherwise(pl.lit("P2"))
        .alias("pred_side"),
        pl.max_horizontal("p1_win_prob", "p2_win_prob").alias("pred_prob"),
    )

    # Odds _p1/_p2 columns are joined by player_id, so they always
    # correspond to prediction p1/p2. No alignment needed.
    pred_p1 = pl.col("p1_win_prob") > 0.5

    odds_mappings = [
        ("best_closing_odds", "pred_odds_best_close"),
        ("worst_closing_odds", "pred_odds_worst_close"),
        ("avg_closing_odds", "pred_odds_avg_close"),
        ("best_opening_odds", "pred_odds_best_open"),
        ("best_intraday_odds", "pred_odds_best_intraday"),
        ("worst_intraday_odds", "pred_odds_worst_intraday"),
        ("open_odds", "pred_odds_open"),
        ("market_formed_odds", "pred_odds_market_formed"),
    ]

    for src_prefix, dst_col in odds_mappings:
        p1_col = f"{src_prefix}_p1"
        p2_col = f"{src_prefix}_p2"
        if p1_col in ds.columns and p2_col in ds.columns:
            ds = ds.with_columns(
                pl.when(pred_p1)
                .then(pl.col(p1_col))
                .otherwise(pl.col(p2_col))
                .alias(dst_col)
            )

    for odds_col, edge_col in [
        ("pred_odds_best_close", "model_edge_best_close"),
        ("pred_odds_avg_close", "model_edge_avg_close"),
        ("pred_odds_open", "model_edge_open"),
        ("pred_odds_market_formed", "model_edge_market_formed"),
    ]:
        if odds_col in ds.columns:
            ds = ds.with_columns(
                (pl.col("pred_prob") - 1.0 / pl.col(odds_col))
                .alias(edge_col)
            )

    # Per-book pred-side odds and edge (open, close, best intraday, worst intraday)
    book_cuts = [
        ("opening_odds", "open"),
        ("max_odds", "best_intra"),
        ("min_odds", "worst_intra"),
        ("closing_odds", "close"),
    ]
    seen_books: set[str] = set()
    for col in ds.columns:
        if col.endswith("_closing_odds_p1") and not col.startswith(("best_", "worst_", "avg_")):
            seen_books.add(col.removesuffix("_closing_odds_p1"))

    for book in sorted(seen_books):
        pm_col = f"{book}_has_prematch_p1"
        has_pm = pm_col in ds.columns
        for src_suffix, dst_suffix in book_cuts:
            p1_col = f"{book}_{src_suffix}_p1"
            p2_col = f"{book}_{src_suffix}_p2"
            if p1_col not in ds.columns or p2_col not in ds.columns:
                continue
            pred_odds_col = f"pred_odds_{book}_{dst_suffix}"
            edge_col = f"model_edge_{book}_{dst_suffix}"
            ds = ds.with_columns(
                pl.when(pred_p1)
                .then(pl.col(p1_col))
                .otherwise(pl.col(p2_col))
                .alias(pred_odds_col)
            )
            if has_pm:
                ds = ds.with_columns(
                    pl.when(pl.col(pm_col).fill_null(False))
                    .then(pl.col("pred_prob") - 1.0 / pl.col(pred_odds_col))
                    .otherwise(pl.lit(None).cast(pl.Float64))
                    .alias(edge_col)
                )
            else:
                ds = ds.with_columns(
                    (pl.col("pred_prob") - 1.0 / pl.col(pred_odds_col))
                    .alias(edge_col)
                )

    return ds


def _compute_clv(ds: pl.DataFrame) -> pl.DataFrame:
    """Compute closing line value for rows with bets."""
    if "bet_side" not in ds.columns:
        return ds

    bet_is_p1 = pl.col("bet_side") == "P1"

    clv_sources = [
        ("best_closing_odds", "bet_closing_best", "clv_vs_best"),
        ("worst_closing_odds", "bet_closing_worst", "clv_vs_worst"),
        ("avg_closing_odds", "bet_closing_avg", "clv_vs_avg"),
    ]

    for src_prefix, close_col, clv_col in clv_sources:
        p1_col = f"{src_prefix}_p1"
        p2_col = f"{src_prefix}_p2"
        if p1_col not in ds.columns or p2_col not in ds.columns:
            continue

        ds = ds.with_columns(
            pl.when(bet_is_p1)
            .then(pl.col(p1_col))
            .otherwise(
                pl.when(pl.col("bet_side") == "P2")
                .then(pl.col(p2_col))
                .otherwise(pl.lit(None).cast(pl.Float64))
            )
            .alias(close_col)
        )

    if "bet_odds" not in ds.columns:
        return ds

    bet_odds = pl.col("bet_odds").cast(pl.Float64, strict=False)

    for _, close_col, clv_col in clv_sources:
        if close_col in ds.columns:
            ds = ds.with_columns(
                pl.when(pl.col(close_col).is_not_null() & pl.col(close_col).gt(0))
                .then((bet_odds - pl.col(close_col)) / pl.col(close_col))
                .otherwise(pl.lit(None).cast(pl.Float64))
                .alias(clv_col)
            )

    return ds


_BET_PLACED_AT_RELIABLE_AFTER = "2026-03-21 09:15"


def _compute_market_alignment(
    ds: pl.DataFrame,
    all_snapshots: pl.DataFrame,
) -> pl.DataFrame:
    """Compute market odds at bet time from resolved snapshots."""
    if "bet_placed_at" not in ds.columns or "bet_side" not in ds.columns:
        return ds
    if len(all_snapshots) == 0:
        return ds

    books = sorted(all_snapshots["book"].unique().to_list())

    bet_mask = (
        pl.col("bet_side").is_in(["P1", "P2"])
        & pl.col("bet_placed_at").is_not_null()
        & (pl.col("bet_placed_at").cast(pl.Utf8) != "")
        & (pl.col("bet_placed_at").cast(pl.Utf8) > _BET_PLACED_AT_RELIABLE_AFTER)
    )
    bet_uids = ds.filter(bet_mask)["match_uid"].to_list()

    if not bet_uids:
        return ds

    snap_index: dict[str, pl.DataFrame] = {}
    # Only use pre-match snapshots — live odds are unreliable for alignment
    prematch_filter = pl.col("event_status") == "NOT_STARTED"
    if "event_status" not in all_snapshots.columns:
        prematch_filter = pl.lit(True)
    relevant = all_snapshots.filter(
        pl.col("match_uid").is_in(bet_uids) & prematch_filter
    )
    for uid in set(bet_uids):
        snap_index[uid] = relevant.filter(pl.col("match_uid") == uid)

    snap_id_col = "player_id" if "player_id" in all_snapshots.columns else "side"

    rows: list[dict] = []
    for row in ds.filter(bet_mask).iter_rows(named=True):
        uid = row["match_uid"]
        bet_side = str(row["bet_side"])
        placed_str = str(row.get("bet_placed_at") or "").strip()
        bet_odds_val = _safe_float(row.get("bet_odds"))

        # Resolve bet_side to player_id or side label
        if snap_id_col == "player_id":
            bet_player = row.get("p1_id") if bet_side == "P1" else row.get("p2_id")
        else:
            bet_player = bet_side.lower()

        bet_time = _parse_bet_time(placed_str)
        if bet_time is None:
            rows.append({"match_uid": uid})
            continue

        snaps = snap_index.get(uid)
        if snaps is None or len(snaps) == 0:
            rows.append({"match_uid": uid})
            continue

        entry: dict = {"match_uid": uid}
        book_odds: list[float] = []

        for book in books:
            book_snaps = snaps.filter(
                (pl.col("book") == book) & (pl.col(snap_id_col) == bet_player)
            )
            if len(book_snaps) == 0:
                continue

            bet_us = int(bet_time.timestamp() * 1_000_000)
            max_distance_us = 24 * 3_600_000_000  # 24 hours
            diffs = book_snaps.with_columns(
                (pl.col("fetched_at").cast(pl.Int64) - bet_us)
                .abs()
                .alias("_diff")
            ).filter(pl.col("_diff") <= max_distance_us)
            if len(diffs) == 0:
                continue
            nearest = diffs.sort("_diff").head(1)
            odds_val = nearest["odds"][0]
            entry[f"market_odds_at_bet_{book}"] = odds_val
            book_odds.append(odds_val)

        if book_odds:
            avg = sum(book_odds) / len(book_odds)
            entry["market_avg_at_bet"] = avg
            entry["market_range_at_bet"] = max(book_odds) - min(book_odds)
            if bet_odds_val is not None and avg > 0:
                entry["bet_vs_market_at_bet"] = (bet_odds_val - avg) / avg

        rows.append(entry)

    if not rows:
        return ds

    alignment_df = pl.DataFrame(rows)
    ds = ds.join(alignment_df, on="match_uid", how="left")
    return ds


def _safe_float(val) -> float | None:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_bet_time(s: str) -> datetime | None:
    """Parse bet_placed_at string to UTC datetime."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None
