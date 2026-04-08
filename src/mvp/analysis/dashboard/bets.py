"""Bet Performance page — time-bound bet performance with weekly/monthly views."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl


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

    default_since = date.today() - timedelta(days=90)
    since = st.sidebar.date_input("Since", value=default_since, key="bets_since")

    # Apply date filter
    ds = ds.filter(pl.col("effective_match_date") >= since)

    # Filter to actual resolved bets
    bets = _filter_bets(ds)

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

    with chart_cols[0]:
        st.subheader("Cumulative P&L")
        cum_pnl = periods.select(
            pl.col("period_str"),
            pl.col("pnl").cum_sum().alias("cum_pnl"),
        ).to_pandas()
        y_max = cum_pnl["cum_pnl"].max()
        y_min = cum_pnl["cum_pnl"].min()
        margin = (y_max - y_min) * 0.15 if y_max != y_min else 10
        line = (
            alt.Chart(cum_pnl)
            .mark_line(point=True)
            .encode(
                x=x_enc,
                y=alt.Y("cum_pnl:Q", title="Cumulative P&L",
                         axis=alt.Axis(labelFontSize=16),
                         scale=alt.Scale(domain=[y_min - margin, y_max + margin])),
            )
        )
        labels = (
            alt.Chart(cum_pnl)
            .mark_text(dy=-18, fontSize=16, color="white")
            .encode(x=x_enc, y="cum_pnl:Q", text=alt.Text("cum_pnl:Q", format=",.2f"))
        )
        st.altair_chart(line + labels, use_container_width=True)

    with chart_cols[1]:
        st.subheader("ROI %")
        roi_data = periods.select(
            pl.col("period_str"),
            (pl.col("roi") * 100).round(1).alias("roi_pct"),
        ).to_pandas()
        y_max = roi_data["roi_pct"].max()
        y_min = roi_data["roi_pct"].min()
        margin = (y_max - y_min) * 0.15 if y_max != y_min else 5
        line = (
            alt.Chart(roi_data)
            .mark_line(point=True)
            .encode(
                x=x_enc,
                y=alt.Y("roi_pct:Q", title="ROI %",
                         axis=alt.Axis(labelFontSize=16),
                         scale=alt.Scale(domain=[y_min - margin, y_max + margin])),
            )
        )
        labels = (
            alt.Chart(roi_data)
            .mark_text(dy=-18, fontSize=16, color="white")
            .encode(x=x_enc, y="roi_pct:Q", text=alt.Text("roi_pct:Q", format=".2f"))
        )
        layers = [line, labels]
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
