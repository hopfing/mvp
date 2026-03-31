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


def clv_by_group(
    ds: pl.DataFrame,
    group_col: str,
    clv_col: str = "clv_vs_avg",
) -> pl.DataFrame:
    """Compute CLV summary grouped by a dimension."""
    bets = _get_bets(ds)
    if len(bets) == 0 or clv_col not in bets.columns or group_col not in bets.columns:
        return pl.DataFrame(schema={
            "group": pl.Utf8, "n": pl.UInt32,
            "mean_clv": pl.Float64, "median_clv": pl.Float64,
        })

    bets = bets.filter(
        pl.col(clv_col).is_not_null() & pl.col(group_col).is_not_null()
    )

    return (
        bets.group_by(group_col)
        .agg(
            pl.len().alias("n"),
            pl.col(clv_col).mean().alias("mean_clv"),
            pl.col(clv_col).median().alias("median_clv"),
        )
        .rename({group_col: "group"})
        .with_columns(pl.col("group").cast(pl.Utf8))
        .sort("group")
    )


def execution_summary(ds: pl.DataFrame) -> dict:
    """Compute execution quality headline metrics."""
    bets = _get_bets(ds)
    n_bets = len(bets)

    if n_bets == 0:
        return {
            "n_bets": 0,
            "avg_bet_odds": None,
            "avg_closing_odds": None,
            "pnl": None,
            "settled": 0,
        }

    avg_bet_odds = None
    if "bet_odds" in bets.columns:
        odds_f = bets["bet_odds"].cast(pl.Float64, strict=False).drop_nulls()
        if len(odds_f) > 0:
            avg_bet_odds = odds_f.mean()

    avg_closing_odds = None
    close_col = next(
        (c for c in ["bet_closing_avg", "bet_closing_best"] if c in bets.columns),
        None,
    )
    if close_col is not None:
        close_vals = bets[close_col].drop_nulls()
        if len(close_vals) > 0:
            avg_closing_odds = close_vals.mean()

    pnl = None
    settled = 0
    if "net" in bets.columns:
        net_vals = bets["net"].cast(pl.Float64, strict=False).drop_nulls()
        settled = len(net_vals)
        if settled > 0:
            pnl = net_vals.sum()

    return {
        "n_bets": n_bets,
        "avg_bet_odds": avg_bet_odds,
        "avg_closing_odds": avg_closing_odds,
        "pnl": pnl,
        "settled": settled,
    }


_BET_PLACED_AT_RELIABLE_AFTER = "2026-03-21 09:15"

_TIMING_BUCKETS = [
    ("<1h", 0, 1),
    ("1-3h", 1, 3),
    ("3-6h", 3, 6),
    ("6-12h", 6, 12),
    ("12h+", 12, None),
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


def clv_by_timing(
    ds: pl.DataFrame,
    clv_col: str = "clv_vs_avg",
) -> pl.DataFrame:
    """Compute CLV bucketed by hours before close."""
    empty = pl.DataFrame(schema={
        "bucket": pl.Utf8, "n": pl.UInt32,
        "mean_clv": pl.Float64, "median_clv": pl.Float64,
    })

    if "bet_placed_at" not in ds.columns or clv_col not in ds.columns:
        return empty

    bets = _get_bets(ds)
    if len(bets) == 0:
        return empty

    bets = bets.filter(
        pl.col("bet_placed_at").cast(pl.Utf8) > _BET_PLACED_AT_RELIABLE_AFTER
    )
    if len(bets) == 0:
        return empty

    close_ts_cols = [c for c in ds.columns if c.endswith("_closing_fetched_at")]
    if not close_ts_cols:
        return empty

    df = bets.with_columns(
        pl.max_horizontal(*[pl.col(c) for c in close_ts_cols])
        .alias("_last_snapshot")
    )

    df = df.with_columns(
        pl.col("bet_placed_at")
        .map_elements(_parse_bet_placed_at, return_dtype=pl.Datetime("us", "UTC"))
        .alias("_bet_ts")
    )

    df = df.filter(
        pl.col("_last_snapshot").is_not_null()
        & pl.col("_bet_ts").is_not_null()
        & pl.col(clv_col).is_not_null()
    )
    if len(df) == 0:
        return empty

    df = df.with_columns(
        (
            (pl.col("_last_snapshot").cast(pl.Int64) - pl.col("_bet_ts").cast(pl.Int64))
            / 3_600_000_000
        ).alias("_hours_before")
    )

    rows = []
    for label, lo, hi in _TIMING_BUCKETS:
        if hi is not None:
            subset = df.filter(
                (pl.col("_hours_before") >= lo) & (pl.col("_hours_before") < hi)
            )
        else:
            subset = df.filter(pl.col("_hours_before") >= lo)
        if len(subset) > 0:
            rows.append({
                "bucket": label,
                "n": len(subset),
                "mean_clv": subset[clv_col].mean(),
                "median_clv": subset[clv_col].median(),
            })

    after = df.filter(pl.col("_hours_before") < 0)
    if len(after) > 0:
        rows.append({
            "bucket": "(after close)",
            "n": len(after),
            "mean_clv": after[clv_col].mean(),
            "median_clv": after[clv_col].median(),
        })

    if not rows:
        return empty

    return pl.DataFrame(rows)


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the execution page."""
    import streamlit as st

    from mvp.analysis.dashboard.components import (
        metric_card_data,
        render_metric_cards,
    )

    ex = execution_summary(ds)
    cards = [
        metric_card_data("Bets", ex["n_bets"], fmt="d"),
        metric_card_data("Settled", ex["settled"], fmt="d"),
        metric_card_data("Avg Bet Odds", ex["avg_bet_odds"], fmt=".3f"),
        metric_card_data("Avg Close Odds", ex["avg_closing_odds"], fmt=".3f"),
        metric_card_data("P&L", ex["pnl"], fmt="$.2f"),
    ]
    render_metric_cards(cards)

    clv_col = next((c for c in ["clv_vs_avg", "clv_vs_best"] if c in ds.columns), None)
    if clv_col is None:
        st.info("No CLV data available.")
        return

    clv_label = "CLV vs Avg Close" if clv_col == "clv_vs_avg" else "CLV vs Best Close"

    if "consensus" in ds.columns:
        st.subheader(f"{clv_label} by Consensus")
        clv_cons = clv_by_group(ds, group_col="consensus", clv_col=clv_col)
        if len(clv_cons) > 0:
            display = clv_cons.select(
                pl.col("group").alias("Consensus"),
                pl.col("n").alias("N"),
                (pl.col("mean_clv") * 100).round(2).alias("Mean CLV %"),
                (pl.col("median_clv") * 100).round(2).alias("Median CLV %"),
            )
            st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)

    if "book" in ds.columns:
        st.subheader(f"{clv_label} by Book")
        clv_book = clv_by_group(ds, group_col="book", clv_col=clv_col)
        if len(clv_book) > 0:
            display = clv_book.select(
                pl.col("group").alias("Book"),
                pl.col("n").alias("N"),
                (pl.col("mean_clv") * 100).round(2).alias("Mean CLV %"),
                (pl.col("median_clv") * 100).round(2).alias("Median CLV %"),
            )
            st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)

    # --- CLV by Timing ---
    if "bet_placed_at" in ds.columns:
        st.subheader(f"{clv_label} by Timing")
        clv_timing = clv_by_timing(ds, clv_col=clv_col)
        if len(clv_timing) > 0:
            display = clv_timing.select(
                pl.col("bucket").alias("Hours Before Close"),
                pl.col("n").alias("N"),
                (pl.col("mean_clv") * 100).round(2).alias("Mean CLV %"),
                (pl.col("median_clv") * 100).round(2).alias("Median CLV %"),
            )
            st.dataframe(display.to_pandas(), use_container_width=True, hide_index=True)
        else:
            st.info("No timing data available (bet tracking started 2026-03-21).")

    if "net" in ds.columns and "stake" in ds.columns:
        st.subheader("Actual P&L")
        bets = _get_bets(ds)
        if len(bets) > 0 and "net" in bets.columns:
            net_vals = bets["net"].cast(pl.Float64, strict=False).drop_nulls()
            if len(net_vals) > 0:
                cumulative = net_vals.cum_sum()
                chart_data = pl.DataFrame({
                    "Bet #": range(1, len(cumulative) + 1),
                    "Cumulative P&L": cumulative,
                })
                st.line_chart(chart_data.to_pandas(), x="Bet #", y="Cumulative P&L")
