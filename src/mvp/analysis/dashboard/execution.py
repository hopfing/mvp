# src/mvp/analysis/dashboard/execution.py
"""Execution page — CLV analysis, timing, and actual P&L."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def _get_bets(ds: pl.DataFrame) -> pl.DataFrame:
    """Filter to rows that are actual bets."""
    if "bet_side" not in ds.columns:
        return ds.head(0)
    return ds.filter(pl.col("bet_side").is_in(["P1", "P2"]))


_CLV_EMPTY_SCHEMA = {
    "group": pl.Utf8, "n": pl.UInt32,
    "positive": pl.UInt32, "negative": pl.UInt32, "even": pl.UInt32,
    "pos_pct": pl.Float64,
}


_CLV_COL = "clv_vs_best"
_CLOSE_COL = "bet_closing_best"


def _with_wld(df: pl.DataFrame, close_col: str) -> pl.DataFrame:
    """Tag each row as positive / negative / even."""
    bet_f = pl.col("bet_odds").cast(pl.Float64, strict=False)
    close_f = pl.col(close_col).cast(pl.Float64, strict=False)
    return df.with_columns(
        bet_f.alias("_bet_r"),
        close_f.alias("_close_r"),
    ).filter(pl.col("_bet_r").is_not_null() & pl.col("_close_r").is_not_null())


def clv_by_group(
    ds: pl.DataFrame,
    group_col: str,
) -> pl.DataFrame:
    """Count CLV positive/negative/even (2dp-rounded) per group."""
    bets = _get_bets(ds)
    if (
        len(bets) == 0
        or group_col not in bets.columns
        or _CLOSE_COL not in bets.columns
        or "bet_odds" not in bets.columns
    ):
        return pl.DataFrame(schema=_CLV_EMPTY_SCHEMA)

    tagged = _with_wld(
        bets.filter(pl.col(group_col).is_not_null()),
        _CLOSE_COL,
    )
    if len(tagged) == 0:
        return pl.DataFrame(schema=_CLV_EMPTY_SCHEMA)

    return (
        tagged.group_by(group_col)
        .agg(
            pl.len().alias("n"),
            (pl.col("_bet_r") > pl.col("_close_r"))
            .sum().cast(pl.UInt32).alias("positive"),
            (pl.col("_bet_r") < pl.col("_close_r"))
            .sum().cast(pl.UInt32).alias("negative"),
            (pl.col("_bet_r") == pl.col("_close_r"))
            .sum().cast(pl.UInt32).alias("even"),
        )
        .with_columns(
            pl.when(pl.col("n") > 0)
            .then(pl.col("positive") / pl.col("n"))
            .otherwise(None)
            .alias("pos_pct"),
            pl.when((pl.col("positive") + pl.col("negative")) > 0)
            .then(pl.col("positive") / (pl.col("positive") + pl.col("negative")))
            .otherwise(None)
            .alias("pos_neg_pct"),
        )
        .rename({group_col: "group"})
        .with_columns(pl.col("group").cast(pl.Utf8))
        .sort("group")
    )


def execution_summary(ds: pl.DataFrame) -> dict:
    """Compute execution quality headline metrics.

    CLV W/L/D is determined by comparing bet_odds vs closing odds.
    Settled = rows with both odds present = Pos + Neg + Even.
    """
    bets = _get_bets(ds)
    n_bets = len(bets)

    empty = {
        "n_bets": 0,
        "n_settled": 0,
        "n_positive": 0,
        "n_negative": 0,
        "n_even": 0,
        "pos_pct": None,
        "pos_neg_pct": None,
        "avg_bet_odds": None,
        "avg_closing_odds": None,
    }

    if n_bets == 0:
        return empty

    avg_bet_odds = None
    if "bet_odds" in bets.columns:
        odds_f = bets["bet_odds"].cast(pl.Float64, strict=False).drop_nulls()
        if len(odds_f) > 0:
            avg_bet_odds = odds_f.mean()

    avg_closing_odds = None
    if _CLOSE_COL in bets.columns:
        close_vals = bets[_CLOSE_COL].drop_nulls()
        if len(close_vals) > 0:
            avg_closing_odds = close_vals.mean()

    n_settled = n_positive = n_negative = n_even = 0
    pos_pct = pos_neg_pct = None
    if _CLOSE_COL in bets.columns and "bet_odds" in bets.columns:
        pair = bets.select(
            pl.col("bet_odds").cast(pl.Float64, strict=False).alias("_bet"),
            pl.col(_CLOSE_COL).cast(pl.Float64, strict=False).alias("_close"),
        ).filter(pl.col("_bet").is_not_null() & pl.col("_close").is_not_null())
        n_settled = len(pair)
        if n_settled > 0:
            n_positive = int(pair.filter(pl.col("_bet") > pl.col("_close")).height)
            n_negative = int(pair.filter(pl.col("_bet") < pl.col("_close")).height)
            n_even = int(pair.filter(pl.col("_bet") == pl.col("_close")).height)
            pos_pct = n_positive / n_settled
            if n_positive + n_negative > 0:
                pos_neg_pct = n_positive / (n_positive + n_negative)

    return {
        "n_bets": n_bets,
        "n_settled": n_settled,
        "n_positive": n_positive,
        "n_negative": n_negative,
        "n_even": n_even,
        "pos_pct": pos_pct,
        "pos_neg_pct": pos_neg_pct,
        "avg_bet_odds": avg_bet_odds,
        "avg_closing_odds": avg_closing_odds,
    }


_BET_PLACED_AT_RELIABLE_AFTER = "2026-03-21 09:15"

_TIMING_BUCKETS = [
    ("12h+", 12, None),
    ("6-12h", 6, 12),
    ("3-6h", 3, 6),
    ("1-3h", 1, 3),
    ("<1h", 0, 1),
]


def _parse_bet_placed_at(val: str | None) -> datetime | None:
    """Parse bet_placed_at string to datetime."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue
    return None


_TIMING_EMPTY_SCHEMA = {
    "bucket": pl.Utf8, "n": pl.UInt32,
    "positive": pl.UInt32, "negative": pl.UInt32, "even": pl.UInt32,
    "pos_pct": pl.Float64,
    "pos_neg_pct": pl.Float64,
}


def _prep_timing_df(ds: pl.DataFrame) -> pl.DataFrame | None:
    """Shared prep for timing-based CLV tables: filter, parse, tag WLD, compute hours."""
    if (
        "bet_placed_at" not in ds.columns
        or "first_live_fetched_at" not in ds.columns
    ):
        return None

    bets = _get_bets(ds)
    if len(bets) == 0 or _CLOSE_COL not in bets.columns or "bet_odds" not in bets.columns:
        return None

    bets = bets.filter(
        pl.col("bet_placed_at").cast(pl.Utf8) > _BET_PLACED_AT_RELIABLE_AFTER
    )
    if len(bets) == 0:
        return None

    df = bets.with_columns(
        pl.col("bet_placed_at")
        .map_elements(_parse_bet_placed_at, return_dtype=pl.Datetime("us", "UTC"))
        .alias("_bet_ts")
    )

    df = df.filter(
        pl.col("first_live_fetched_at").is_not_null() & pl.col("_bet_ts").is_not_null()
    )
    if len(df) == 0:
        return None

    df = _with_wld(df, _CLOSE_COL)
    if len(df) == 0:
        return None

    return df.with_columns(
        (
            (pl.col("first_live_fetched_at").cast(pl.Int64) - pl.col("_bet_ts").cast(pl.Int64))
            / 3_600_000_000
        ).alias("_hours_before")
    )


def _assign_bucket(df: pl.DataFrame) -> pl.DataFrame:
    """Add a _bucket column based on _hours_before."""
    bucket_expr = pl.when(pl.col("_hours_before") < 0).then(pl.lit("Live"))
    for label, lo, hi in _TIMING_BUCKETS:
        if hi is not None:
            bucket_expr = bucket_expr.when(
                (pl.col("_hours_before") >= lo) & (pl.col("_hours_before") < hi)
            ).then(pl.lit(label))
        else:
            bucket_expr = bucket_expr.when(
                pl.col("_hours_before") >= lo
            ).then(pl.lit(label))
    return df.with_columns(bucket_expr.otherwise(pl.lit(None)).alias("_bucket")).filter(
        pl.col("_bucket").is_not_null()
    )


_BUCKET_ORDER = [label for label, _, _ in _TIMING_BUCKETS] + ["Live"]


def clv_by_timing(ds: pl.DataFrame) -> pl.DataFrame:
    """Bucket bets by hours before match start and report CLV W/L/D counts."""
    empty = pl.DataFrame(schema=_TIMING_EMPTY_SCHEMA)

    df = _prep_timing_df(ds)
    if df is None or len(df) == 0:
        return empty

    df = _assign_bucket(df)
    if len(df) == 0:
        return empty

    def _counts(subset: pl.DataFrame, label: str) -> dict | None:
        if len(subset) == 0:
            return None
        pos = int(subset.filter(pl.col("_bet_r") > pl.col("_close_r")).height)
        neg = int(subset.filter(pl.col("_bet_r") < pl.col("_close_r")).height)
        even = int(subset.filter(pl.col("_bet_r") == pl.col("_close_r")).height)
        n = len(subset)
        return {
            "bucket": label,
            "n": n,
            "positive": pos,
            "negative": neg,
            "even": even,
            "pos_pct": pos / n if n else None,
            "pos_neg_pct": pos / (pos + neg) if (pos + neg) > 0 else None,
        }

    rows = []
    for label in _BUCKET_ORDER:
        r = _counts(df.filter(pl.col("_bucket") == label), label)
        if r is not None:
            rows.append(r)

    if not rows:
        return empty

    return pl.DataFrame(rows, schema=_TIMING_EMPTY_SCHEMA)


def clv_by_book_timing(ds: pl.DataFrame) -> pl.DataFrame | None:
    """Cross-slice CLV W/L/D by book x timing bucket."""
    df = _prep_timing_df(ds)
    if df is None or len(df) == 0 or "book" not in df.columns:
        return None

    df = _assign_bucket(df)
    if len(df) == 0:
        return None

    result = (
        df.group_by("book", "_bucket")
        .agg(
            pl.len().alias("n"),
            (pl.col("_bet_r") > pl.col("_close_r")).sum().cast(pl.UInt32).alias("positive"),
            (pl.col("_bet_r") < pl.col("_close_r")).sum().cast(pl.UInt32).alias("negative"),
            (pl.col("_bet_r") == pl.col("_close_r")).sum().cast(pl.UInt32).alias("even"),
        )
        .with_columns(
            pl.when(pl.col("n") > 0)
            .then(pl.col("positive") / pl.col("n"))
            .otherwise(None)
            .alias("pos_pct"),
            pl.when((pl.col("positive") + pl.col("negative")) > 0)
            .then(pl.col("positive") / (pl.col("positive") + pl.col("negative")))
            .otherwise(None)
            .alias("pos_neg_pct"),
        )
    )

    bucket_order = {label: i for i, label in enumerate(_BUCKET_ORDER)}
    result = result.with_columns(
        pl.col("_bucket").replace_strict(bucket_order, default=999).alias("_sort")
    ).sort("book", "_sort").drop("_sort").rename({"_bucket": "bucket"})

    return result if len(result) > 0 else None


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the execution page."""
    import streamlit as st

    from mvp.analysis.dashboard.components import (
        consensus_selector,
        metric_card_data,
        model_selector,
        render_metric_cards,
    )

    # --- Model filter ---
    model_version = model_selector(ds, key="execution", default_to_active=True)
    if model_version is not None:
        ds = ds.filter(pl.col("model_version") == model_version)

    # --- Consensus filter ---
    consensus = consensus_selector(ds, key="execution")
    if consensus is not None:
        ds = ds.filter(pl.col("consensus") == consensus)

    ex = execution_summary(ds)
    cards = [
        metric_card_data("Settled", ex["n_settled"], fmt="d"),
        metric_card_data("Positive", ex["n_positive"], fmt="d"),
        metric_card_data("Negative", ex["n_negative"], fmt="d"),
        metric_card_data("Even", ex["n_even"], fmt="d"),
        metric_card_data("Pos %", ex["pos_pct"], fmt=".1%"),
        metric_card_data("Pos/Neg %", ex["pos_neg_pct"], fmt=".1%"),
        metric_card_data("Avg Bet Odds", ex["avg_bet_odds"], fmt=".3f"),
        metric_card_data("Avg Close Odds", ex["avg_closing_odds"], fmt=".3f"),
    ]
    render_metric_cards(cards)

    if _CLV_COL not in ds.columns:
        st.info("No CLV data available.")
        return

    def _wld_display(df: pl.DataFrame, label_col: str, label: str) -> pl.DataFrame:
        return df.select(
            pl.col("group").alias(label) if label_col == "group" else pl.col(label_col),
            pl.col("n").alias("Settled"),
            pl.col("positive").alias("Positive"),
            pl.col("negative").alias("Negative"),
            pl.col("even").alias("Even"),
            (pl.col("pos_pct") * 100).round(1).alias("Pos %"),
            (pl.col("pos_neg_pct") * 100).round(1).alias("Pos/Neg %"),
        )

    if "book" in ds.columns:
        st.subheader("CLV vs Best Close by Book")
        clv_book = clv_by_group(ds, group_col="book")
        if len(clv_book) > 0:
            display = _wld_display(clv_book, "group", "Book")
            st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)

    if "bet_placed_at" in ds.columns:
        st.subheader("CLV vs Best Close by Timing")
        clv_timing = clv_by_timing(ds)
        if len(clv_timing) > 0:
            display = clv_timing.select(
                pl.col("bucket").alias("Hours Before Match Start"),
                pl.col("n").alias("Settled"),
                pl.col("positive").alias("Positive"),
                pl.col("negative").alias("Negative"),
                pl.col("even").alias("Even"),
                (pl.col("pos_pct") * 100).round(1).alias("Pos %"),
                (pl.col("pos_neg_pct") * 100).round(1).alias("Pos/Neg %"),
            )
            st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)
        else:
            st.info("No timing data available (bet tracking started 2026-03-21).")

    if "book" in ds.columns and "bet_placed_at" in ds.columns:
        st.subheader("CLV vs Best Close by Book x Timing")
        clv_bt = clv_by_book_timing(ds)
        if clv_bt is not None:
            display = clv_bt.select(
                pl.col("book").alias("Book"),
                pl.col("bucket").alias("Timing"),
                pl.col("n").alias("Settled"),
                pl.col("positive").alias("Positive"),
                pl.col("negative").alias("Negative"),
                pl.col("even").alias("Even"),
                (pl.col("pos_pct") * 100).round(1).alias("Pos %"),
                (pl.col("pos_neg_pct") * 100).round(1).alias("Pos/Neg %"),
            )
            st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)

