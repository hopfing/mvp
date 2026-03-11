"""Unified analysis dataset: joins predictions with results, sheet data, and odds."""

from __future__ import annotations

import logging

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
    "stake",
    "book",
    "bet_result",
    "net",
    "notes",
]


def build_analysis_dataset(
    predictions: pl.DataFrame,
    results: pl.DataFrame | None = None,
    sheet_data: pl.DataFrame | None = None,
    odds_by_book: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build a single wide DataFrame for analysis.

    Predictions are the base. Everything else is left-joined.

    Args:
        predictions: Model predictions (required).
        results: Match results with match_uid and result columns.
        sheet_data: Google Sheets data (circuit uses CH/ATP format).
        odds_by_book: Long-format odds (one row per match-book).

    Returns:
        Wide DataFrame with all joined data and derived metrics.
    """
    ds = predictions.clone()

    ds = _join_results(ds, results)
    ds = _join_sheet_data(ds, sheet_data)
    ds = _join_odds(ds, odds_by_book)

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


def _join_odds(ds: pl.DataFrame, odds_by_book: pl.DataFrame | None) -> pl.DataFrame:
    """Pivot odds from long to wide, join, and compute derived metrics."""
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

    ds = _compute_cross_book_metrics(ds, books)

    return ds


def _compute_cross_book_metrics(ds: pl.DataFrame, books: list[str]) -> pl.DataFrame:
    """Compute best odds, model edge, and books_showing_edge."""
    # Best closing odds = max across books (only where has_prematch)
    for side in ("p1", "p2"):
        odds_col = f"closing_odds_{side}"
        prematch_col = "has_prematch"

        # Collect odds from each book where has_prematch is true
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
                (1.0 / best_odds).alias(
                    f"best_closing_implied_{side}"
                )
            )
        else:
            ds = ds.with_columns(
                pl.lit(None).cast(pl.Float64).alias(f"best_closing_odds_{side}"),
                pl.lit(None).cast(pl.Float64).alias(f"best_closing_implied_{side}"),
            )

    # Model edge vs best
    if "best_closing_implied_p1" in ds.columns:
        ds = ds.with_columns(
            (pl.col("p1_win_prob") - pl.col("best_closing_implied_p1"))
            .alias("model_edge_vs_best_p1"),
            (pl.col("p2_win_prob") - pl.col("best_closing_implied_p2"))
            .alias("model_edge_vs_best_p2"),
        )

    # Books showing edge: count where model prob for
    # predicted side > closing implied
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

        # Total books with prematch
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
            total = pl.col("_total_prematch_books").cast(
                pl.Float64
            )
            ds = ds.with_columns(
                pl.when(pl.col("_total_prematch_books") > 0)
                .then(edge / total)
                .otherwise(pl.lit(None).cast(pl.Float64))
                .alias("market_alignment")
            )
            ds = ds.drop("_total_prematch_books")

    return ds
