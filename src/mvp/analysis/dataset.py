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
    results: pl.DataFrame | None = None,
    sheet_data: pl.DataFrame | None = None,
    odds_by_book: pl.DataFrame | None = None,
    cross_book_odds: pl.DataFrame | None = None,
    all_snapshots: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build a single wide DataFrame for analysis.

    Predictions are the base. Everything else is left-joined.

    Args:
        predictions: Model predictions (required).
        results: Match results with match_uid and result columns.
        sheet_data: Google Sheets data (circuit uses CH/ATP format).
        odds_by_book: Long-format per-book odds summaries.
        cross_book_odds: Pre-computed cross-book odds summary from aggregator.
        all_snapshots: Resolved snapshots for market alignment at bet time.

    Returns:
        Wide DataFrame with all joined data and derived metrics.
    """
    ds = predictions.clone()

    ds = _join_results(ds, results)
    ds = _join_sheet_data(ds, sheet_data)
    ds = _join_odds(ds, odds_by_book, skip_cross_book=(cross_book_odds is not None))

    if cross_book_odds is not None:
        ds = ds.join(cross_book_odds, on="match_uid", how="left")

    ds = _align_odds_to_predictions(ds)
    ds = _compute_pred_side_metrics(ds)
    ds = _compute_clv(ds)

    if all_snapshots is not None:
        ds = _compute_market_alignment(ds, all_snapshots)

    return ds


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

    ds = ds.join(sheet_subset, on="match_uid", how="left")

    return ds


def _align_odds_to_predictions(ds: pl.DataFrame) -> pl.DataFrame:
    """Swap _p1/_p2 odds columns where odds p1 differs from prediction p1.

    The odds pipeline assigns p1/p2 based on draw_p1_id, but predictions
    may use a different p1 assignment (e.g., from older runs before
    draw_p1_id was set). This function detects the mismatch via
    odds_p1_id/odds_p2_id and swaps all _p1/_p2 suffixed columns.
    """
    if "odds_p1_id" not in ds.columns or "p1_id" not in ds.columns:
        return ds

    needs_swap = (
        pl.col("odds_p1_id").is_not_null()
        & (pl.col("odds_p1_id") != pl.col("p1_id"))
    )

    # Find all column pairs with _p1/_p2 suffixes (from odds, not predictions)
    p1_cols = [c for c in ds.columns if c.endswith("_p1") and c != "p1_id"
               and c.replace("_p1", "_p2") in ds.columns]

    swap_exprs = []
    for p1_col in p1_cols:
        p2_col = p1_col.replace("_p1", "_p2")
        # When needs_swap: p1 gets p2's value and vice versa
        swap_exprs.append(
            pl.when(needs_swap)
            .then(pl.col(p2_col))
            .otherwise(pl.col(p1_col))
            .alias(p1_col)
        )
        swap_exprs.append(
            pl.when(needs_swap)
            .then(pl.col(p1_col))
            .otherwise(pl.col(p2_col))
            .alias(p2_col)
        )

    if swap_exprs:
        n_swapped = ds.filter(needs_swap).shape[0]
        if n_swapped > 0:
            logger.info("Odds alignment: swapped p1/p2 for %d/%d matches", n_swapped, len(ds))
        ds = ds.with_columns(swap_exprs)

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

    pred_p1 = pl.col("p1_win_prob") > 0.5

    odds_mappings = [
        ("best_closing_odds", "pred_odds_best_close"),
        ("worst_closing_odds", "pred_odds_worst_close"),
        ("avg_closing_odds", "pred_odds_avg_close"),
        ("best_opening_odds", "pred_odds_best_open"),
        ("best_intraday_odds", "pred_odds_best_intraday"),
        ("worst_intraday_odds", "pred_odds_worst_intraday"),
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

    if "pred_odds_best_close" in ds.columns:
        ds = ds.with_columns(
            (pl.col("pred_prob") - 1.0 / pl.col("pred_odds_best_close"))
            .alias("model_edge_best_close")
        )
    if "pred_odds_avg_close" in ds.columns:
        ds = ds.with_columns(
            (pl.col("pred_prob") - 1.0 / pl.col("pred_odds_avg_close"))
            .alias("model_edge_avg_close")
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
    )
    bet_uids = ds.filter(bet_mask)["match_uid"].to_list()

    if not bet_uids:
        return ds

    snap_index: dict[str, pl.DataFrame] = {}
    relevant = all_snapshots.filter(pl.col("match_uid").is_in(bet_uids))
    for uid in set(bet_uids):
        snap_index[uid] = relevant.filter(pl.col("match_uid") == uid)

    rows: list[dict] = []
    for row in ds.filter(bet_mask).iter_rows(named=True):
        uid = row["match_uid"]
        bet_side = str(row["bet_side"]).lower()
        placed_str = str(row.get("bet_placed_at") or "").strip()
        bet_odds_val = _safe_float(row.get("bet_odds"))

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
                (pl.col("book") == book) & (pl.col("side") == bet_side)
            )
            if len(book_snaps) == 0:
                continue

            bet_us = int(bet_time.timestamp() * 1_000_000)
            diffs = book_snaps.with_columns(
                (pl.col("fetched_at").cast(pl.Int64) - bet_us)
                .abs()
                .alias("_diff")
            )
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
