"""Bet Performance page — time-bound bet performance with weekly/monthly views."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from mvp.analysis.dashboard.components import expand_by_book


def _filter_bets(ds: pl.DataFrame) -> pl.DataFrame:
    """Filter to actual bets (bet_side is not null) with resolved results."""
    if "bet_side" not in ds.columns:
        return ds.head(0)
    bets = ds.filter(
        pl.col("bet_side").is_in(["P1", "P2"])
        & (pl.col("status") == "resolved")
    )
    return bets


def _headline_stats(bets: pl.DataFrame) -> dict:
    """Compute headline stats from filtered bets."""
    n = len(bets)
    if n == 0:
        return {
            "n": 0, "wins": 0, "losses": 0, "void": 0,
            "accuracy": None, "stake": None, "pnl": None, "roi": None,
        }

    wins = int(bets.filter(pl.col("bet_result") == "W").height)
    losses = int(bets.filter(pl.col("bet_result") == "L").height)
    void = int(bets.filter(pl.col("bet_result") == "V").height)
    decided = wins + losses
    accuracy = wins / decided if decided > 0 else None

    stake = None
    if "stake" in bets.columns:
        stake_vals = bets["stake"].cast(pl.Float64, strict=False).drop_nulls()
        if len(stake_vals) > 0:
            stake = stake_vals.sum()

    pnl = None
    if "net" in bets.columns:
        net_vals = bets["net"].cast(pl.Float64, strict=False).drop_nulls()
        if len(net_vals) > 0:
            pnl = net_vals.sum()

    roi = None
    if pnl is not None and stake is not None and stake > 0:
        roi = pnl / stake

    return {
        "n": n, "wins": wins, "losses": losses, "void": void,
        "accuracy": accuracy, "stake": stake, "pnl": pnl, "roi": roi,
    }


def _aggregate_periods(bets: pl.DataFrame, granularity: str) -> pl.DataFrame:
    """Group bets by week (Mon-Sun) or calendar month.

    Returns a DataFrame sorted chronologically with columns:
    period, period_label, bets, W, L, V, win_pct, stake, pnl, roi
    """
    if len(bets) == 0:
        return pl.DataFrame(schema={
            "period": pl.Date,
            "bets": pl.Int64, "W": pl.Int64, "L": pl.Int64, "V": pl.Int64,
            "win_pct": pl.Float64, "stake": pl.Float64, "pnl": pl.Float64,
            "roi": pl.Float64,
        })

    if granularity == "Days":
        period_expr = (
            pl.col("effective_match_date")
            .dt.truncate("1d")
            .cast(pl.Date)
            .alias("period")
        )
    elif granularity == "Weeks":
        # truncate to Monday of each week
        period_expr = (
            pl.col("effective_match_date")
            .dt.truncate("1w")
            .cast(pl.Date)
            .alias("period")
        )
    else:
        period_expr = (
            pl.col("effective_match_date")
            .dt.truncate("1mo")
            .cast(pl.Date)
            .alias("period")
        )

    grouped = (
        bets.with_columns(period_expr)
        .group_by("period")
        .agg(
            pl.len().alias("bets"),
            (pl.col("bet_result") == "W").sum().cast(pl.Int64).alias("W"),
            (pl.col("bet_result") == "L").sum().cast(pl.Int64).alias("L"),
            (pl.col("bet_result") == "V").sum().cast(pl.Int64).alias("V"),
            pl.col("stake").cast(pl.Float64, strict=False).sum().alias("stake"),
            pl.col("net").cast(pl.Float64, strict=False).sum().alias("pnl"),
        )
        .sort("period")
    )

    grouped = grouped.with_columns(
        pl.when(pl.col("W") + pl.col("L") > 0)
        .then(pl.col("W") / (pl.col("W") + pl.col("L")))
        .otherwise(None)
        .alias("win_pct"),
        pl.when(pl.col("stake") > 0)
        .then(pl.col("pnl") / pl.col("stake"))
        .otherwise(None)
        .alias("roi"),
    )

    return grouped


def _aggregate_by(bets: pl.DataFrame, col: str) -> pl.DataFrame:
    """Group bets by a categorical column and compute stats."""
    return (
        bets.group_by(col)
        .agg(
            pl.len().alias("Bets"),
            (pl.col("bet_result") == "W").sum().cast(pl.Int64).alias("W"),
            (pl.col("bet_result") == "L").sum().cast(pl.Int64).alias("L"),
            (pl.col("bet_result") == "V").sum().cast(pl.Int64).alias("V"),
            pl.col("stake").cast(pl.Float64, strict=False).sum().alias("Stake"),
            pl.col("net").cast(pl.Float64, strict=False).sum().alias("P&L"),
        )
        .with_columns(
            pl.when(pl.col("W") + pl.col("L") > 0)
            .then((pl.col("W") / (pl.col("W") + pl.col("L")) * 100).round(1))
            .otherwise(None)
            .alias("Win %"),
            pl.when(pl.col("Stake") > 0)
            .then((pl.col("P&L") / pl.col("Stake") * 100).round(1))
            .otherwise(None)
            .alias("ROI %"),
        )
        .with_columns(pl.col("Stake").round(2), pl.col("P&L").round(2))
    )


def _style_breakdown(df: pl.DataFrame, label_col: str, st) -> None:
    """Apply standard styling and render a breakdown table."""
    display = df.select(
        pl.col(label_col),
        "Bets", "W", "L", "V", "Win %", "Stake", "P&L", "ROI %",
    )

    def _color_negative(val):
        if isinstance(val, (int, float)) and val < 0:
            return "color: #e74c3c"
        return ""

    pdf = display.to_pandas()

    def _bold_all_row(row):
        if row[label_col] == "All":
            return ["font-weight: bold; background-color: rgba(255,255,255,0.08)"] * len(row)
        return [""] * len(row)

    styled = (
        pdf.style
        .apply(_bold_all_row, axis=1)
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format({
            "Stake": "${:,.2f}", "P&L": "${:+,.2f}",
            "Win %": "{:.1f}%", "ROI %": "{:+.1f}%",
        })
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_breakdown(
    bets: pl.DataFrame, col: str, label: str, st,
    sort_order: list[str] | None = None,
    all_row: bool = False,
) -> None:
    """Aggregate by col, optionally sort by a fixed order, and render."""
    agg = _aggregate_by(bets, col).rename({col: label})
    if sort_order:
        order_map = {v: i for i, v in enumerate(sort_order)}
        agg = agg.with_columns(
            pl.col(label).replace_strict(order_map, default=999).alias("_sort")
        ).sort("_sort").drop("_sort")
    else:
        agg = agg.sort(label)
    if all_row:
        totals = _aggregate_by(
            bets.with_columns(pl.lit("All").alias(col)), col,
        ).rename({col: label})
        agg = pl.concat([totals, agg])
    _style_breakdown(agg, label, st)


def _render_odds_breakdown(bets: pl.DataFrame, st) -> None:
    """Bucket bet_odds into bands and render breakdown."""
    from mvp.analysis.scanner import ODDS_BREAKS, ODDS_LABELS

    odds = bets.with_columns(
        pl.col("bet_odds").cast(pl.Float64, strict=False).alias("_odds_f")
    ).filter(pl.col("_odds_f").is_not_null())

    if len(odds) == 0:
        st.info("No odds data available.")
        return

    # Build chained when/then from scanner's ODDS_BREAKS and ODDS_LABELS
    expr = pl.when(pl.col("_odds_f") < ODDS_BREAKS[1]).then(pl.lit(ODDS_LABELS[0]))
    for i in range(1, len(ODDS_BREAKS) - 1):
        expr = expr.when(pl.col("_odds_f") < ODDS_BREAKS[i + 1]).then(
            pl.lit(ODDS_LABELS[i])
        )
    expr = expr.otherwise(pl.lit(ODDS_LABELS[-1]))

    odds = odds.with_columns(expr.alias("odds_band"))

    agg = _aggregate_by(odds, "odds_band").rename({"odds_band": "Odds Band"})
    order_map = {v: i for i, v in enumerate(ODDS_LABELS)}
    agg = agg.with_columns(
        pl.col("Odds Band").replace_strict(order_map, default=999).alias("_sort")
    ).sort("_sort").drop("_sort")
    _style_breakdown(agg, "Odds Band", st)


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the Bet Performance page."""
    import streamlit as st

    from mvp.analysis.dashboard.components import (
        metric_card_data,
        model_selector,
        render_metric_cards,
    )

    # --- Controls ---
    model_version = model_selector(ds, key="bets")
    if model_version is not None:
        ds = ds.filter(pl.col("model_version") == model_version)

    def _on_gran_change():
        gran = st.session_state["bets_granularity"]
        days = 7 if gran == "Days" else 90
        st.session_state["bets_since"] = date.today() - timedelta(days=days)

    granularity = st.sidebar.radio(
        "Granularity", ["Days", "Weeks", "Months"], index=1,
        key="bets_granularity", on_change=_on_gran_change,
    )
    scope = st.sidebar.radio(
        "Scope", ["Cumulative", "Per Period"], index=0,
        key="bets_scope",
    )

    default_since = date.today() - timedelta(days=90)
    since = st.sidebar.date_input("Since", value=default_since, key="bets_since")

    # Apply date filter
    ds = ds.filter(pl.col("effective_match_date") >= since)

    # Filter to actual resolved bets
    bets = _filter_bets(ds)

    if len(bets) == 0:
        st.info("No bets found for the selected filters.")
        return

    # --- Bet edge filter ---
    # bet_edge = fav_edge when betting the model's predicted side, else dog_edge.
    if "fav_edge" in bets.columns and "dog_edge" in bets.columns and "pred_side" in bets.columns:
        bets = bets.with_columns(
            pl.when(pl.col("bet_side") == pl.col("pred_side"))
            .then(pl.col("fav_edge"))
            .otherwise(pl.col("dog_edge"))
            .alias("bet_edge")
        )
        edge_vals = bets["bet_edge"].drop_nulls()
        if len(edge_vals) > 0:
            edge_min_pct = round(float(edge_vals.min()) * 100 - 0.5, 1)
            edge_max_pct = round(float(edge_vals.max()) * 100 + 0.5, 1)
            if edge_max_pct - edge_min_pct < 0.2:
                edge_min_pct -= 0.1
                edge_max_pct += 0.1

            # Initialize (or clamp, if bounds shifted since last rerun) session state
            def _clamp(v: float) -> float:
                return max(edge_min_pct, min(v, edge_max_pct))

            if "bets_edge_slider" not in st.session_state:
                st.session_state.bets_edge_slider = (edge_min_pct, edge_max_pct)
                st.session_state.bets_edge_lo = edge_min_pct
                st.session_state.bets_edge_hi = edge_max_pct
            else:
                cur_lo, cur_hi = st.session_state.bets_edge_slider
                cur_lo, cur_hi = _clamp(cur_lo), _clamp(cur_hi)
                st.session_state.bets_edge_slider = (cur_lo, cur_hi)
                st.session_state.bets_edge_lo = cur_lo
                st.session_state.bets_edge_hi = cur_hi

            def _sync_from_slider():
                lo, hi = st.session_state.bets_edge_slider
                st.session_state.bets_edge_lo = lo
                st.session_state.bets_edge_hi = hi

            def _sync_from_inputs():
                lo = st.session_state.bets_edge_lo
                hi = st.session_state.bets_edge_hi
                if lo > hi:
                    lo, hi = hi, lo
                st.session_state.bets_edge_slider = (lo, hi)

            st.sidebar.slider(
                "Bet edge",
                min_value=edge_min_pct,
                max_value=edge_max_pct,
                step=0.1,
                format="%.1f%%",
                key="bets_edge_slider",
                on_change=_sync_from_slider,
            )
            col_lo, col_hi = st.sidebar.columns(2)
            with col_lo:
                st.number_input(
                    "Min %",
                    min_value=edge_min_pct,
                    max_value=edge_max_pct,
                    step=0.1,
                    format="%.1f",
                    key="bets_edge_lo",
                    on_change=_sync_from_inputs,
                )
            with col_hi:
                st.number_input(
                    "Max %",
                    min_value=edge_min_pct,
                    max_value=edge_max_pct,
                    step=0.1,
                    format="%.1f",
                    key="bets_edge_hi",
                    on_change=_sync_from_inputs,
                )

            edge_range = st.session_state.bets_edge_slider
            bets = bets.filter(
                pl.col("bet_edge").is_between(
                    edge_range[0] / 100, edge_range[1] / 100
                )
            )

    if len(bets) == 0:
        st.info("No bets found for the selected filters.")
        return

    # --- Headline Stats ---
    stats = _headline_stats(bets)
    record = f"{stats['wins']}-{stats['losses']}-{stats['void']}"
    render_metric_cards([
        metric_card_data("Bets", stats["n"], fmt="d"),
        {"label": "Record", "value": record},
        metric_card_data("Win %", stats["accuracy"], fmt=".1%"),
        {
            "label": "Stake",
            "value": f"${stats['stake']:,.2f}" if stats["stake"] else "\u2014",
        },
        metric_card_data("P&L", stats["pnl"], fmt="$.2f"),
        metric_card_data("ROI", stats["roi"], fmt=".1%"),
    ])

    # --- Period Aggregation ---
    periods = _aggregate_periods(bets, granularity)
    if len(periods) == 0:
        return

    # --- Charts ---
    chart_cols = st.columns(2)

    import altair as alt

    # Convert period to string so Altair treats as ordinal (no interpolated ticks)
    periods = periods.with_columns(
        pl.col("period").cast(pl.Utf8).alias("period_str")
    )

    x_enc = alt.X(
        "period_str:N", title=None, sort=None,
        axis=alt.Axis(labelAngle=-45, labelFontSize=16),
    )

    cumulative = scope == "Cumulative"

    with chart_cols[0]:
        if cumulative:
            st.subheader("Cumulative P&L")
            pnl_data = periods.select(
                pl.col("period_str"),
                pl.col("pnl").cum_sum().alias("pnl_val"),
            ).to_pandas()
            y_title = "Cumulative P&L"
        else:
            st.subheader("P&L")
            pnl_data = periods.select(
                pl.col("period_str"),
                pl.col("pnl").alias("pnl_val"),
            ).to_pandas()
            y_title = "P&L"
        y_max = pnl_data["pnl_val"].max()
        y_min = pnl_data["pnl_val"].min()
        margin = (y_max - y_min) * 0.15 if y_max != y_min else 10
        mark = alt.Chart(pnl_data).mark_line(point=True) if cumulative else alt.Chart(pnl_data).mark_bar()
        enc = dict(
            x=x_enc,
            y=alt.Y("pnl_val:Q", title=y_title,
                     axis=alt.Axis(labelFontSize=16),
                     scale=alt.Scale(domain=[y_min - margin, y_max + margin])),
        )
        if not cumulative:
            enc["color"] = alt.condition(
                alt.datum.pnl_val >= 0,
                alt.value("#2ecc71"),
                alt.value("#e74c3c"),
            )
        chart = mark.encode(**enc)
        labels = (
            alt.Chart(pnl_data)
            .mark_text(dy=-18, fontSize=16, color="white")
            .encode(x=x_enc, y="pnl_val:Q", text=alt.Text("pnl_val:Q", format=",.2f"))
        )
        pnl_layers = [chart, labels]
        if not cumulative and (y_min - margin) <= 0 <= (y_max + margin):
            zero = (
                alt.Chart(pnl_data)
                .mark_rule(strokeDash=[4, 4], color="gray")
                .encode(y=alt.datum(0))
            )
            pnl_layers.append(zero)
        st.altair_chart(alt.layer(*pnl_layers), use_container_width=True)

    with chart_cols[1]:
        if cumulative:
            st.subheader("Cumulative ROI %")
            roi_data = periods.select(
                pl.col("period_str"),
                (pl.col("pnl").cum_sum() / pl.col("stake").cum_sum() * 100)
                .round(1)
                .alias("roi_pct"),
            ).to_pandas()
            roi_title = "Cumulative ROI %"
        else:
            st.subheader("ROI %")
            roi_data = periods.select(
                pl.col("period_str"),
                (pl.col("roi") * 100).round(1).alias("roi_pct"),
            ).to_pandas()
            roi_title = "ROI %"
        y_max = roi_data["roi_pct"].max()
        y_min = roi_data["roi_pct"].min()
        margin = (y_max - y_min) * 0.15 if y_max != y_min else 5
        mark = alt.Chart(roi_data).mark_line(point=True) if cumulative else alt.Chart(roi_data).mark_bar()
        enc = dict(
            x=x_enc,
            y=alt.Y("roi_pct:Q", title=roi_title,
                     axis=alt.Axis(labelFontSize=16),
                     scale=alt.Scale(domain=[y_min - margin, y_max + margin])),
        )
        if not cumulative:
            enc["color"] = alt.condition(
                alt.datum.roi_pct >= 0,
                alt.value("#2ecc71"),
                alt.value("#e74c3c"),
            )
        chart = mark.encode(**enc)
        labels = (
            alt.Chart(roi_data)
            .mark_text(dy=-18, fontSize=16, color="white")
            .encode(x=x_enc, y="roi_pct:Q", text=alt.Text("roi_pct:Q", format=".2f"))
        )
        layers = [chart, labels]
        if (y_min - margin) <= 0 <= (y_max + margin):
            zero = (
                alt.Chart(roi_data)
                .mark_rule(strokeDash=[4, 4], color="gray")
                .encode(y=alt.datum(0))
            )
            layers.append(zero)
        st.altair_chart(alt.layer(*layers), use_container_width=True)

    # --- Breakdown Table ---
    breakdown_label = {"Days": "Daily", "Weeks": "Weekly", "Months": "Monthly"}[granularity]
    period_col_label = {"Days": "Day", "Weeks": "Week", "Months": "Month"}[granularity]
    st.subheader(f"{breakdown_label} Breakdown")
    display = (
        periods.sort("period", descending=True)
        .select(
            pl.col("period").cast(pl.Utf8).alias(period_col_label),
            pl.col("bets").alias("Bets"),
            pl.col("W"),
            pl.col("L"),
            pl.col("V"),
            (pl.col("win_pct") * 100).round(1).alias("Win %"),
            pl.col("stake").round(2).alias("Stake"),
            pl.col("pnl").round(2).alias("P&L"),
            (pl.col("roi") * 100).round(1).alias("ROI %"),
        )
    )
    def _color_negative(val):
        if isinstance(val, (int, float)) and val < 0:
            return "color: #e74c3c"
        return ""

    styled = (
        display.to_pandas()
        .style
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format({"Stake": "${:,.2f}", "P&L": "${:+,.2f}", "Win %": "{:.1f}%", "ROI %": "{:+.1f}%"})
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # --- Performance Breakdowns ---
    round_order = ["Q1", "Q2", "R128", "R64", "R32", "R16", "QF", "SF", "F"]
    for circuit_val, circuit_label in [("tour", "Tour"), ("chal", "Challenger")]:
        circuit_bets = bets.filter(pl.col("circuit") == circuit_val)
        if len(circuit_bets) == 0:
            continue
        st.subheader(circuit_label)
        _render_breakdown(
            circuit_bets, "round", "Round", st,
            sort_order=round_order, all_row=True,
        )

    st.subheader("By Book")
    _render_breakdown(expand_by_book(bets), "book", "Book", st)

    st.subheader("By Odds Band")
    _render_odds_breakdown(bets, st)

    if "net" in bets.columns:
        st.subheader("Cumulative P&L by Bet #")
        net_vals = (
            bets.sort("effective_match_date")
            ["net"].cast(pl.Float64, strict=False).drop_nulls()
        )
        if len(net_vals) > 0:
            cumulative = net_vals.cum_sum()
            chart_data = pl.DataFrame({
                "Bet #": range(1, len(cumulative) + 1),
                "Cumulative P&L": cumulative,
            })
            st.line_chart(chart_data.to_pandas(), x="Bet #", y="Cumulative P&L")
