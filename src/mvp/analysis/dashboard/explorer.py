"""Model Performance page — flat-bet performance across all resolved predictions."""

from __future__ import annotations

import polars as pl

_BEST_CLOSE = "pred_odds_best_close"
_EDGE_COL = "model_edge_best_close"

_CROSS_BOOK_CLOSE = {
    "pred_odds_best_close",
    "pred_odds_worst_close",
    "pred_odds_avg_close",
    "pred_odds_open",
    "pred_odds_market_formed",
    "pred_odds_best_intraday",
    "pred_odds_worst_intraday",
    "pred_odds_best_open",
}

_EDGE_SLICES = [("All", None), ("Edge", True), ("No Edge", False)]


def _filter_resolved(ds: pl.DataFrame) -> pl.DataFrame:
    """Filter to resolved predictions with available close odds."""
    if "status" not in ds.columns:
        return ds.head(0)
    return ds.filter(
        (pl.col("status") == "resolved") & pl.col(_BEST_CLOSE).is_not_null()
    )


def _add_edge_flag(df: pl.DataFrame) -> pl.DataFrame:
    if _EDGE_COL not in df.columns:
        return df.with_columns(pl.lit(False).alias("has_edge"))
    return df.with_columns(
        (pl.col(_EDGE_COL) > 0).fill_null(False).alias("has_edge")
    )


def _detect_books(columns: list[str]) -> list[str]:
    books = []
    for col in sorted(columns):
        if (
            col.startswith("pred_odds_")
            and col.endswith("_close")
            and col not in _CROSS_BOOK_CLOSE
        ):
            books.append(col.removeprefix("pred_odds_").removesuffix("_close"))
    return books


def _edge_subset(df: pl.DataFrame, edge_flag: bool | None) -> pl.DataFrame:
    if edge_flag is True:
        return df.filter(pl.col("has_edge"))
    if edge_flag is False:
        return df.filter(~pl.col("has_edge"))
    return df


def _flat_bet_stats(df: pl.DataFrame, odds_col: str) -> dict:
    """Compute flat-$1-bet stats: N, W, L, Acc %, ROI %, P&L."""
    subset = df.filter(pl.col(odds_col).is_not_null())
    n = len(subset)
    if n == 0:
        return {
            "N": 0, "W": 0, "L": 0,
            "Acc %": None, "ROI %": None, "P&L": None,
        }
    wins = subset.filter(pl.col("model_correct"))
    w = len(wins)
    returned = float(wins[odds_col].sum()) if w > 0 else 0.0
    pnl = returned - n
    return {
        "N": n,
        "W": w,
        "L": n - w,
        "Acc %": round(w / n * 100, 1),
        "ROI %": round(pnl / n * 100, 1),
        "P&L": round(pnl, 2),
    }


def _aggregate_by(
    df: pl.DataFrame,
    col: str,
    odds_col: str = _BEST_CLOSE,
    sort_order: list[str] | None = None,
) -> pl.DataFrame:
    """Group by dimension x edge (no All summary — used for circuit-sliced tables)."""
    vals = df[col].drop_nulls().unique().sort().to_list()
    if sort_order:
        order_map = {v: i for i, v in enumerate(sort_order)}
        vals = sorted(vals, key=lambda v: order_map.get(str(v), 999))

    rows: list[dict] = []
    for val in vals:
        dim_df = df.filter(pl.col(col) == val)
        for label, flag in _EDGE_SLICES:
            stats = _flat_bet_stats(_edge_subset(dim_df, flag), odds_col)
            stats[col] = str(val)
            stats["Edge"] = label
            rows.append(stats)
    return pl.DataFrame(rows)


def _aggregate_books(df: pl.DataFrame, books: list[str]) -> pl.DataFrame:
    """Aggregate by book x edge using per-book closing odds (no All summary)."""
    rows: list[dict] = []
    for book in books:
        odds_col = f"pred_odds_{book}_close"
        if odds_col not in df.columns:
            continue
        book_df = df.filter(pl.col(odds_col).is_not_null())
        if len(book_df) == 0:
            continue
        for label, flag in _EDGE_SLICES:
            stats = _flat_bet_stats(_edge_subset(book_df, flag), odds_col)
            stats["Book"] = book.upper()
            stats["Edge"] = label
            rows.append(stats)
    return pl.DataFrame(rows)


def _style_breakdown(df: pl.DataFrame, label_col: str, st) -> None:
    """Render a styled breakdown table."""
    display = df.select(pl.col(label_col), "Edge", "N", "W", "L", "Acc %", "ROI %", "P&L")
    pdf = display.to_pandas()

    def _color_negative(val):
        if isinstance(val, (int, float)) and val < 0:
            return "color: #e74c3c"
        return ""

    def _row_style(row):
        edge = row["Edge"]
        if edge == "All":
            return ["font-weight: bold; background-color: rgba(255,255,255,0.08)"] * len(row)
        if edge == "Edge":
            return ["background-color: rgba(46,204,113,0.10)"] * len(row)
        if edge == "No Edge":
            return ["background-color: rgba(231,76,60,0.10)"] * len(row)
        return [""] * len(row)

    styled = (
        pdf.style.apply(_row_style, axis=1)
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format(
            {"Acc %": "{:.1f}%", "ROI %": "{:+.1f}%", "P&L": "${:+,.2f}"},
            na_rep="\u2014",
        )
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_edge_bands(df: pl.DataFrame, st) -> None:
    """Edge band performance table using scanner's 2.5pp buckets.

    Includes All / Edge / No Edge summary rows, then per-band rows
    ordered positive-first (10%+ at top, below -10% at bottom).
    """
    from mvp.analysis.scanner import EDGE_BREAKS, EDGE_LABELS

    if _EDGE_COL not in df.columns:
        st.info("No edge data available.")
        return

    # Bucket edge values — EDGE_BREAKS has 9 breakpoints, EDGE_LABELS has 10 bins.
    # First bin is < EDGE_BREAKS[0], last bin is >= EDGE_BREAKS[-1].
    o = pl.col(_EDGE_COL)
    expr = pl.when(o < EDGE_BREAKS[0]).then(pl.lit(EDGE_LABELS[0]))
    for i in range(len(EDGE_BREAKS) - 1):
        expr = expr.when(o < EDGE_BREAKS[i + 1]).then(pl.lit(EDGE_LABELS[i + 1]))
    expr = expr.otherwise(pl.lit(EDGE_LABELS[-1]))

    bucketed = df.with_columns(expr.alias("edge_band"))

    # Summary rows first
    rows: list[dict] = []
    for label, flag in _EDGE_SLICES:
        stats = _flat_bet_stats(_edge_subset(df, flag), _BEST_CLOSE)
        stats["Edge Band"] = label
        rows.append(stats)

    # Per-band rows, positive first (reverse the label order)
    for label in reversed(EDGE_LABELS):
        stats = _flat_bet_stats(
            bucketed.filter(pl.col("edge_band") == label), _BEST_CLOSE
        )
        stats["Edge Band"] = label
        rows.append(stats)

    table = pl.DataFrame(rows).select("Edge Band", "N", "W", "L", "Acc %", "ROI %", "P&L")
    pdf = table.to_pandas()

    def _color_negative(val):
        if isinstance(val, (int, float)) and val < 0:
            return "color: #e74c3c"
        return ""

    def _bold_summary(row):
        if row["Edge Band"] in ("All", "Edge", "No Edge"):
            return [
                "font-weight: bold; background-color: rgba(255,255,255,0.08)"
            ] * len(row)
        return [""] * len(row)

    styled = (
        pdf.style.apply(_bold_summary, axis=1)
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format(
            {"Acc %": "{:.1f}%", "ROI %": "{:+.1f}%", "P&L": "${:+,.2f}"},
            na_rep="\u2014",
        )
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_charts(df: pl.DataFrame, granularity: str, st) -> None:
    """Cumulative P&L and ROI line charts, edge vs no-edge."""
    import altair as alt

    if granularity == "Days":
        trunc = "1d"
    elif granularity == "Weeks":
        trunc = "1w"
    else:
        trunc = "1mo"

    period_expr = (
        pl.col("effective_match_date").dt.truncate(trunc).cast(pl.Date).alias("period")
    )
    edge_label = (
        pl.when(pl.col("has_edge")).then(pl.lit("Edge")).otherwise(pl.lit("No Edge"))
    )

    grouped = (
        df.with_columns(period_expr, edge_label.alias("edge_group"))
        .group_by("period", "edge_group")
        .agg(
            pl.len().alias("n"),
            (pl.col(_BEST_CLOSE) * pl.col("model_correct").cast(pl.Float64))
            .sum()
            .alias("returned"),
        )
        .with_columns((pl.col("returned") - pl.col("n").cast(pl.Float64)).alias("pnl"))
        .sort("edge_group", "period")
    )

    cum = grouped.with_columns(
        pl.col("pnl").cum_sum().over("edge_group").alias("cum_pnl"),
        pl.col("n").cum_sum().over("edge_group").cast(pl.Float64).alias("cum_n"),
        pl.col("returned").cum_sum().over("edge_group").alias("cum_ret"),
    ).with_columns(
        ((pl.col("cum_ret") - pl.col("cum_n")) / pl.col("cum_n") * 100)
        .round(1)
        .alias("cum_roi_pct"),
        pl.col("period").cast(pl.Utf8).alias("period_str"),
    )

    pdf = cum.to_pandas()

    color_scale = alt.Scale(
        domain=["Edge", "No Edge"], range=["#2ecc71", "#e74c3c"]
    )
    x_enc = alt.X(
        "period_str:N",
        title=None,
        sort=None,
        axis=alt.Axis(labelAngle=-45, labelFontSize=14),
    )
    zero_rule = (
        alt.Chart(pdf)
        .mark_rule(strokeDash=[4, 4], color="gray")
        .encode(y=alt.datum(0))
    )

    cols = st.columns(2)

    with cols[0]:
        st.subheader("Cumulative P&L")
        line = (
            alt.Chart(pdf)
            .mark_line(point=True)
            .encode(
                x=x_enc,
                y=alt.Y("cum_pnl:Q", title="Cumulative P&L", axis=alt.Axis(labelFontSize=14)),
                color=alt.Color("edge_group:N", title="Edge", scale=color_scale),
            )
        )
        st.altair_chart(alt.layer(line, zero_rule), use_container_width=True)

    with cols[1]:
        st.subheader("Cumulative ROI %")
        line = (
            alt.Chart(pdf)
            .mark_line(point=True)
            .encode(
                x=x_enc,
                y=alt.Y("cum_roi_pct:Q", title="Cumulative ROI %", axis=alt.Axis(labelFontSize=14)),
                color=alt.Color("edge_group:N", title="Edge", scale=color_scale),
            )
        )
        st.altair_chart(alt.layer(line, zero_rule), use_container_width=True)


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the Model Performance page."""
    import streamlit as st

    from mvp.analysis.dashboard.components import (
        consensus_selector,
        metric_card_data,
        model_selector,
        render_metric_cards,
    )

    # --- Sidebar ---
    model_version = model_selector(ds, key="perf", default_to_active=True)
    if model_version is not None:
        ds = ds.filter(pl.col("model_version") == model_version)

    consensus = consensus_selector(ds, key="perf")
    if consensus is not None and "consensus" in ds.columns:
        ds = ds.filter(pl.col("consensus") == consensus)

    granularity = st.sidebar.radio(
        "Chart Granularity",
        ["Days", "Weeks", "Months"],
        index=1,
        key="perf_granularity",
    )

    # --- Resolved predictions ---
    resolved = _filter_resolved(ds)
    if len(resolved) == 0:
        st.info("No resolved predictions with closing odds available.")
        return

    resolved = _add_edge_flag(resolved)

    # --- Headline stats ---
    all_stats = _flat_bet_stats(resolved, _BEST_CLOSE)
    edge_stats = _flat_bet_stats(resolved.filter(pl.col("has_edge")), _BEST_CLOSE)
    no_edge_stats = _flat_bet_stats(
        resolved.filter(~pl.col("has_edge")), _BEST_CLOSE
    )

    render_metric_cards([
        metric_card_data("Resolved", all_stats["N"], fmt="d"),
        metric_card_data(
            "Accuracy",
            all_stats["Acc %"] / 100 if all_stats["Acc %"] is not None else None,
            fmt=".1%",
        ),
        metric_card_data("P&L", all_stats["P&L"], fmt="$.2f"),
        metric_card_data(
            "ROI",
            all_stats["ROI %"] / 100 if all_stats["ROI %"] is not None else None,
            fmt=".1%",
        ),
    ])

    sub = st.columns(2)
    with sub[0]:
        st.markdown("**Edge**")
        render_metric_cards([
            metric_card_data("N", edge_stats["N"], fmt="d"),
            metric_card_data(
                "Acc",
                edge_stats["Acc %"] / 100 if edge_stats["Acc %"] is not None else None,
                fmt=".1%",
            ),
            metric_card_data(
                "ROI",
                edge_stats["ROI %"] / 100 if edge_stats["ROI %"] is not None else None,
                fmt=".1%",
            ),
            metric_card_data("P&L", edge_stats["P&L"], fmt="$.2f"),
        ])
    with sub[1]:
        st.markdown("**No Edge**")
        render_metric_cards([
            metric_card_data("N", no_edge_stats["N"], fmt="d"),
            metric_card_data(
                "Acc",
                no_edge_stats["Acc %"] / 100
                if no_edge_stats["Acc %"] is not None
                else None,
                fmt=".1%",
            ),
            metric_card_data(
                "ROI",
                no_edge_stats["ROI %"] / 100
                if no_edge_stats["ROI %"] is not None
                else None,
                fmt=".1%",
            ),
            metric_card_data("P&L", no_edge_stats["P&L"], fmt="$.2f"),
        ])

    # --- Charts ---
    _render_charts(resolved, granularity, st)

    # --- Edge band table ---
    st.subheader("By Edge Band")
    _render_edge_bands(resolved, st)

    # --- Per-circuit breakdowns ---
    from mvp.analysis.scanner import ODDS_BREAKS, ODDS_LABELS

    books = _detect_books(resolved.columns)

    # Pre-compute odds band column
    o = pl.col(_BEST_CLOSE)
    odds_expr = pl.when(o < ODDS_BREAKS[1]).then(pl.lit(ODDS_LABELS[0]))
    for i in range(1, len(ODDS_BREAKS) - 1):
        odds_expr = odds_expr.when(o < ODDS_BREAKS[i + 1]).then(
            pl.lit(ODDS_LABELS[i])
        )
    odds_expr = odds_expr.otherwise(pl.lit(ODDS_LABELS[-1]))
    resolved = resolved.with_columns(odds_expr.alias("odds_band"))

    circuits = [
        (val, label)
        for val, label in [("tour", "Tour"), ("chal", "Challenger")]
        if "circuit" in resolved.columns
        and len(resolved.filter(pl.col("circuit") == val)) > 0
    ]
    circuit_dfs = {
        val: resolved.filter(pl.col("circuit") == val) for val, _ in circuits
    }

    # By Surface
    if "surface" in resolved.columns:
        st.subheader("By Surface")
        for val, label in circuits:
            st.markdown(f"**{label}**")
            table = _aggregate_by(circuit_dfs[val], "surface").rename(
                {"surface": "Surface"}
            )
            _style_breakdown(table, "Surface", st)

    # By Round
    if "round" in resolved.columns:
        round_order = ["Q1", "Q2", "R128", "R64", "R32", "R16", "QF", "SF", "F"]
        st.subheader("By Round")
        for val, label in circuits:
            st.markdown(f"**{label}**")
            table = _aggregate_by(
                circuit_dfs[val], "round", sort_order=round_order
            ).rename({"round": "Round"})
            _style_breakdown(table, "Round", st)

    # By Book (per-book closing odds)
    if books:
        st.subheader("By Book")
        for val, label in circuits:
            st.markdown(f"**{label}**")
            table = _aggregate_books(circuit_dfs[val], books)
            _style_breakdown(table, "Book", st)

    # By Odds Band
    st.subheader("By Odds Band")
    for val, label in circuits:
        st.markdown(f"**{label}**")
        table = _aggregate_by(
            circuit_dfs[val], "odds_band", sort_order=ODDS_LABELS
        ).rename({"odds_band": "Odds Band"})
        _style_breakdown(table, "Odds Band", st)
