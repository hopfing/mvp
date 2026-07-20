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


_EDGE_SLICES = [("All", None), ("Edge", True), ("No Edge", False)]

# Bets placed before this date are excluded from edge-provenance / line-
# movement tables: the staged-odds capture wasn't comprehensive enough for
# "opening odds" to be reliable on those matches. Same floor used by
# clv_by_timing in the Execution page.
_OPENING_RELIABLE_AFTER = "2026-03-21 09:15"


def _color_negative(val):
    if isinstance(val, (int, float)) and val < 0:
        return "color: #e74c3c"
    return ""


def _per_bet_stats(bets: pl.DataFrame) -> dict:
    """Compute Bets/W/L/V/Win %/Stake/P&L/ROI % for an arbitrary subset."""
    n = len(bets)
    if n == 0:
        return {
            "Bets": 0, "W": 0, "L": 0, "V": 0,
            "Win %": None, "Stake": None, "P&L": None, "ROI %": None,
        }
    w = int(bets.filter(pl.col("bet_result") == "W").height)
    losses = int(bets.filter(pl.col("bet_result") == "L").height)
    void = int(bets.filter(pl.col("bet_result") == "V").height)
    decided = w + losses

    stake = None
    if "stake" in bets.columns:
        stake_vals = bets["stake"].cast(pl.Float64, strict=False).drop_nulls()
        if len(stake_vals) > 0:
            stake = float(stake_vals.sum())

    pnl = None
    if "net" in bets.columns:
        net_vals = bets["net"].cast(pl.Float64, strict=False).drop_nulls()
        if len(net_vals) > 0:
            pnl = float(net_vals.sum())

    win_pct = round(w / decided * 100, 1) if decided > 0 else None
    roi_pct = round(pnl / stake * 100, 1) if (pnl is not None and stake and stake > 0) else None

    return {
        "Bets": n, "W": w, "L": losses, "V": void,
        "Win %": win_pct,
        "Stake": round(stake, 2) if stake is not None else None,
        "P&L": round(pnl, 2) if pnl is not None else None,
        "ROI %": roi_pct,
    }


def _edge_subset(bets: pl.DataFrame, edge_flag: bool | None) -> pl.DataFrame:
    if edge_flag is True:
        return bets.filter(pl.col("bet_edge") > 0)
    if edge_flag is False:
        return bets.filter(pl.col("bet_edge") <= 0)
    return bets


def _render_edge_bands(bets: pl.DataFrame, st) -> None:
    """By-Edge-Band breakdown using scanner's 2.5pp buckets.

    All / Edge / No Edge summary rows, then per-band rows positive-first.
    """
    from mvp.analysis.scanner import EDGE_BREAKS, EDGE_LABELS

    if "bet_edge" not in bets.columns:
        st.info("No bet edge data available.")
        return

    df = bets.filter(pl.col("bet_edge").is_not_null())
    if len(df) == 0:
        st.info("No bet edge data available.")
        return

    e = pl.col("bet_edge")
    expr = pl.when(e < EDGE_BREAKS[0]).then(pl.lit(EDGE_LABELS[0]))
    for i in range(len(EDGE_BREAKS) - 1):
        expr = expr.when(e < EDGE_BREAKS[i + 1]).then(pl.lit(EDGE_LABELS[i + 1]))
    expr = expr.otherwise(pl.lit(EDGE_LABELS[-1]))
    bucketed = df.with_columns(expr.alias("edge_band"))

    rows: list[dict] = []
    for label, flag in _EDGE_SLICES:
        stats = _per_bet_stats(_edge_subset(df, flag))
        stats["Edge Band"] = label
        rows.append(stats)
    for label in reversed(EDGE_LABELS):
        stats = _per_bet_stats(bucketed.filter(pl.col("edge_band") == label))
        stats["Edge Band"] = label
        rows.append(stats)

    table = pl.DataFrame(rows).select(
        "Edge Band", "Bets", "W", "L", "V", "Win %", "Stake", "P&L", "ROI %"
    )
    pdf = table.to_pandas()

    def _row_style(row):
        band = row["Edge Band"]
        if band == "All":
            return ["font-weight: bold; background-color: rgba(255,255,255,0.08)"] * len(row)
        if band == "Edge":
            return ["background-color: rgba(46,204,113,0.10)"] * len(row)
        if band == "No Edge":
            return ["background-color: rgba(231,76,60,0.10)"] * len(row)
        return [""] * len(row)

    styled = (
        pdf.style.apply(_row_style, axis=1)
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format(
            {
                "Stake": "${:,.2f}", "P&L": "${:+,.2f}",
                "Win %": "{:.1f}%", "ROI %": "{:+.1f}%",
            },
            na_rep="—",
        )
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


_OPEN_EDGE_BUCKETS = [
    ("<0%",      -1.0,   0.0),
    ("0-2.5%",    0.0,   0.025),
    ("2.5-5%",    0.025, 0.05),
    ("5-7.5%",    0.05,  0.075),
    ("7.5-10%",   0.075, 0.10),
    ("10%+",      0.10,  1.0),
]

_BET_EDGE_BUCKETS = [
    ("<0%",      -1.0,   0.0),
    ("0-2.5%",    0.0,   0.025),
    ("2.5-5%",    0.025, 0.05),
    ("5-7.5%",    0.05,  0.075),
    ("7.5-10%",   0.075, 0.10),
    ("10%+",      0.10,  1.0),
]


def _provenance_cell_color(roi: float | None) -> str:
    if roi is None:
        return ""
    if roi >= 0:
        intensity = min(float(roi) / 30.0, 1.0)
        return f"background-color: rgba(46, 204, 113, {0.10 + 0.40 * intensity:.2f})"
    intensity = min(-float(roi) / 30.0, 1.0)
    return f"background-color: rgba(231, 76, 60, {0.10 + 0.40 * intensity:.2f})"


def _render_provenance_matrix(bets: pl.DataFrame, st) -> None:
    """Open-edge × bet-edge matrix with N / Win% / P&L / ROI per cell.

    Rows: bet_edge_open buckets (<0% = no edge at open, drift territory).
    Cols: bet_edge buckets (edge at the time the bet was placed).
    Compares bets where edge was already there at open vs. bets where edge
    came from line drift, at the same bet-edge level.
    Background gradient is by ROI %.
    """
    import pandas as pd

    if "bet_edge" not in bets.columns or "bet_edge_open" not in bets.columns:
        st.info("No edge-provenance data available (bet_edge_open missing).")
        return

    df = bets.filter(
        pl.col("bet_edge").is_not_null() & pl.col("bet_edge_open").is_not_null()
    )
    if len(df) == 0:
        st.info("No edge-provenance data available.")
        return

    open_labels = [b[0] for b in _OPEN_EDGE_BUCKETS]
    bet_labels = [b[0] for b in _BET_EDGE_BUCKETS]

    display_rows: list[list[str]] = []
    roi_rows: list[list[float | None]] = []
    for _, o_lo, o_hi in _OPEN_EDGE_BUCKETS:
        display_row: list[str] = []
        roi_row: list[float | None] = []
        for _, f_lo, f_hi in _BET_EDGE_BUCKETS:
            sub = df.filter(
                (pl.col("bet_edge_open") >= o_lo)
                & (pl.col("bet_edge_open") < o_hi)
                & (pl.col("bet_edge") >= f_lo)
                & (pl.col("bet_edge") < f_hi)
            )
            stats = _per_bet_stats(sub)
            n = stats["Bets"]
            if n == 0:
                display_row.append("—")
                roi_row.append(None)
                continue
            win_str = f"{stats['Win %']:.1f}%" if stats["Win %"] is not None else "—"
            pnl_str = f"${stats['P&L']:+,.0f}" if stats["P&L"] is not None else "—"
            roi_str = f"{stats['ROI %']:+.1f}%" if stats["ROI %"] is not None else "—"
            display_row.append(
                f"n={n}<br>W: {win_str}<br>{pnl_str}<br>ROI: {roi_str}"
            )
            roi_row.append(stats["ROI %"])
        display_rows.append(display_row)
        roi_rows.append(roi_row)

    display_df = pd.DataFrame(display_rows, index=open_labels, columns=bet_labels)
    roi_df = pd.DataFrame(roi_rows, index=open_labels, columns=bet_labels)

    def _color_table(_df):
        out = pd.DataFrame("", index=_df.index, columns=_df.columns)
        for r in _df.index:
            for c in _df.columns:
                roi = roi_df.loc[r, c]
                if roi is None or pd.isna(roi):
                    continue
                out.loc[r, c] = _provenance_cell_color(float(roi))
        return out

    styled = (
        display_df.style
        .apply(_color_table, axis=None)
        .set_properties(**{
            "text-align": "center",
            "vertical-align": "middle",
            "padding": "10px",
            "white-space": "nowrap",
        })
        .set_table_styles([
            {"selector": "th", "props": [("text-align", "center"), ("padding", "6px")]},
            {"selector": "th.col_heading", "props": [("border-bottom", "2px solid #888")]},
            {"selector": "th.row_heading", "props": [("border-right", "2px solid #888")]},
        ])
        .set_table_attributes(
            'style="width: 100%; table-layout: fixed; border-collapse: collapse;"'
        )
    )
    html = styled.to_html(escape=False)
    html = html.replace(
        '<th class="blank level0" >&nbsp;</th>',
        '<th class="blank level0" style="text-align: center; color: #aaa; '
        'font-size: 0.8em; font-weight: normal; padding: 6px;">'
        "Bet Edge →<br>Open Edge ↓</th>",
        1,
    )
    st.markdown(html, unsafe_allow_html=True)


_TIER_ORDER = ["UnderC", "Optimal", "Border", "Risky", "Danger"]
_KILL_THRESHOLD_N = 15
_KILL_THRESHOLD_ROI = -10.0

_BET_SIDE_ORDER = ["All", "Model Fav", "Model Dog"]


def _flipped_bets(bets: pl.DataFrame) -> pl.DataFrame:
    """Bets whose model pick changed between placement and now.

    A flip = the sheet's frozen as-bet pick (``bet_pred_side``) differs from
    the current live pick (``pred_side``, recomputed from the overwritable
    predictions.parquet). Returns the matching rows (empty frame if the
    columns are absent or nothing flipped).
    """
    needed = {"bet_pred_side", "pred_side", "bet_side"}
    if not needed.issubset(bets.columns):
        return bets.head(0)
    return bets.filter(
        pl.col("bet_pred_side").is_in(["P1", "P2"])
        & pl.col("pred_side").is_in(["P1", "P2"])
        & (pl.col("bet_pred_side") != pl.col("pred_side"))
    )


def _render_flipped_bets(bets: pl.DataFrame, st) -> None:
    """Table of bets the model flipped away from after they were placed."""
    flipped = _flipped_bets(bets)
    if len(flipped) == 0:
        return

    flipped = flipped.sort("effective_match_date") if (
        "effective_match_date" in flipped.columns
    ) else flipped

    # Aggregate performance of the bets whose pick later flipped — i.e. how the
    # as-bet pick (which you actually took) held up once the model changed its
    # mind. W/L/V here are the outcomes of the side you bet.
    stats = _per_bet_stats(flipped)
    summary_df = pl.DataFrame([{
        "Bets": stats["Bets"],
        "W": stats["W"],
        "L": stats["L"],
        "V": stats["V"],
        "Win %": stats["Win %"],
        "Stake": stats["Stake"],
        "P&L": stats["P&L"],
        "ROI %": stats["ROI %"],
    }])
    summary_styled = (
        summary_df.to_pandas()
        .style
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format(
            {
                "Stake": "${:,.2f}",
                "P&L": "${:+,.2f}",
                "Win %": "{:.1f}%",
                "ROI %": "{:+.1f}%",
            },
            na_rep="—",
        )
    )
    st.dataframe(summary_styled, use_container_width=True, hide_index=True)

    has_names = {"p1_name", "p2_name"}.issubset(flipped.columns)

    def _name(side_col: str) -> pl.Expr:
        if has_names:
            return (
                pl.when(pl.col(side_col) == "P1").then(pl.col("p1_name"))
                .when(pl.col(side_col) == "P2").then(pl.col("p2_name"))
                .otherwise(pl.lit("—"))
            )
        return pl.col(side_col)

    cols: list[pl.Expr] = []
    if "effective_match_date" in flipped.columns:
        cols.append(
            pl.col("effective_match_date").dt.strftime("%Y-%m-%d").alias("Date")
        )
    if "tournament_name" in flipped.columns:
        cols.append(pl.col("tournament_name").alias("Tournament"))
    if "round" in flipped.columns:
        cols.append(pl.col("round").alias("Round"))
    if has_names:
        cols.append(
            (pl.col("p1_name") + pl.lit(" vs ") + pl.col("p2_name")).alias("Matchup")
        )
    cols.append(_name("bet_side").alias("Bet"))
    cols.append(_name("bet_pred_side").alias("Pick at bet"))
    cols.append(_name("pred_side").alias("Model now"))
    if "bet_edge" in flipped.columns:
        cols.append((pl.col("bet_edge") * 100).round(1).alias("Edge % (at bet)"))
    if "bet_result" in flipped.columns:
        cols.append(pl.col("bet_result").alias("Result"))
    if "net" in flipped.columns:
        cols.append(
            pl.col("net").cast(pl.Float64, strict=False).round(2).alias("P&L")
        )

    pdf = flipped.select(cols).to_pandas()

    fmt: dict = {}
    if "Edge % (at bet)" in pdf.columns:
        fmt["Edge % (at bet)"] = "{:+.1f}%"
    if "P&L" in pdf.columns:
        fmt["P&L"] = "${:+,.2f}"

    subset = [c for c in ("P&L",) if c in pdf.columns]
    styled = pdf.style.format(fmt, na_rep="—")
    if subset:
        styled = styled.applymap(_color_negative, subset=subset)
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_bet_side_breakdown(
    bets: pl.DataFrame, st, pick_col: str = "bet_pick_side",
) -> None:
    """All / Model Fav / Model Dog breakdown with CLV columns.

    "Model Fav" = bet on the model's pick (bet_side == pick_col).
    "Model Dog" = bet against the model's pick (bet_side != pick_col).
    ``pick_col`` is the as-bet pick (``bet_pick_side``), not the live
    ``pred_side`` — a bet the model later flipped away from stays classified
    as the Fav/Dog it was when placed. Anchored to the model's call, not
    market odds.
    """
    if pick_col not in bets.columns or "bet_side" not in bets.columns:
        st.info(f"Missing {pick_col}/bet_side — can't classify bets.")
        return
    if len(bets) == 0:
        st.info("No bets in current filter.")
        return

    df = bets.with_columns(
        pl.when(pl.col("bet_side") == pl.col(pick_col))
        .then(pl.lit("Model Fav"))
        .otherwise(pl.lit("Model Dog"))
        .alias("_pick_type")
    )

    agg = _aggregate_with_clv(df, ["_pick_type"]).rename({"_pick_type": "Bet Side"})
    totals = _aggregate_with_clv(
        df.with_columns(pl.lit("All").alias("_pick_type")), ["_pick_type"],
    ).rename({"_pick_type": "Bet Side"})

    full = pl.concat([totals, agg])
    order_map = {v: i for i, v in enumerate(_BET_SIDE_ORDER)}
    full = full.with_columns(
        pl.col("Bet Side").replace_strict(order_map, default=999).alias("_sort")
    ).sort("_sort").drop("_sort")

    has_clv = "CLV+%" in full.columns
    cols = ["Bet Side", "Bets", "W", "L", "V", "Win %", "Stake", "P&L", "ROI %"]
    if has_clv:
        cols.extend(["CLV+%", "Avg CLV"])
    pdf = full.select(cols).to_pandas()

    def _bold_all(row):
        if row["Bet Side"] == "All":
            return ["font-weight: bold; background-color: rgba(255,255,255,0.08)"] * len(row)
        return [""] * len(row)

    fmt = {
        "Stake": "${:,.2f}", "P&L": "${:+,.2f}",
        "Win %": "{:.1f}%", "ROI %": "{:+.1f}%",
    }
    if has_clv:
        fmt["CLV+%"] = "{:.1f}%"
        fmt["Avg CLV"] = "{:+.2f}pp"

    styled = (
        pdf.style.apply(_bold_all, axis=1)
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format(fmt, na_rep="—")
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _aggregate_with_clv(bets: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
    """Aggregate by group_cols with the standard stats plus CLV+% / Avg CLV.

    Mirrors `_aggregate_by` and adds CLV columns when `clv_vs_best` is present
    in the dataset. Used by the tier and per-cell breakdowns.
    """
    has_clv = "clv_vs_best" in bets.columns
    aggs = [
        pl.len().alias("Bets"),
        (pl.col("bet_result") == "W").sum().cast(pl.Int64).alias("W"),
        (pl.col("bet_result") == "L").sum().cast(pl.Int64).alias("L"),
        (pl.col("bet_result") == "V").sum().cast(pl.Int64).alias("V"),
        pl.col("stake").cast(pl.Float64, strict=False).sum().alias("Stake"),
        pl.col("net").cast(pl.Float64, strict=False).sum().alias("P&L"),
    ]
    if has_clv:
        aggs.extend([
            (pl.col("clv_vs_best") > 0).cast(pl.Float64).mean().alias("_clv_pos_rate"),
            pl.col("clv_vs_best").mean().alias("_clv_avg"),
        ])
    out = bets.group_by(group_cols).agg(aggs)
    out = out.with_columns(
        pl.when(pl.col("W") + pl.col("L") > 0)
        .then((pl.col("W") / (pl.col("W") + pl.col("L")) * 100).round(1))
        .otherwise(None)
        .alias("Win %"),
        pl.when(pl.col("Stake") > 0)
        .then((pl.col("P&L") / pl.col("Stake") * 100).round(1))
        .otherwise(None)
        .alias("ROI %"),
    ).with_columns(pl.col("Stake").round(2), pl.col("P&L").round(2))
    if has_clv:
        out = out.with_columns(
            (pl.col("_clv_pos_rate") * 100).round(1).alias("CLV+%"),
            (pl.col("_clv_avg") * 100).round(2).alias("Avg CLV"),
        ).drop("_clv_pos_rate", "_clv_avg")
    return out


def _render_cal_tier_breakdown(bets: pl.DataFrame, st) -> None:
    """Per-tier breakdown with All row at top, tier order matching backtest.

    Blank/null cal_tier rows (pre-feature historical bets) are excluded —
    the round-level table already covers the no-tier picture.
    """
    if "cal_tier" not in bets.columns:
        st.info("No cal_tier data available (analysis.parquet may need a refresh).")
        return

    df = bets.filter(
        pl.col("cal_tier").is_not_null() & (pl.col("cal_tier") != "")
    )
    if len(df) == 0:
        st.info("No bets with cal_tier in current filter.")
        return

    agg = _aggregate_with_clv(df, ["cal_tier"]).rename({"cal_tier": "Tier"})

    order_map = {v: i for i, v in enumerate(_TIER_ORDER)}
    agg = agg.with_columns(
        pl.col("Tier").replace_strict(order_map, default=999).alias("_sort")
    ).sort("_sort").drop("_sort")

    totals = _aggregate_with_clv(
        df.with_columns(pl.lit("All").alias("cal_tier")), ["cal_tier"],
    ).rename({"cal_tier": "Tier"})
    agg = pl.concat([totals, agg])

    has_clv = "CLV+%" in agg.columns
    cols = ["Tier", "Bets", "W", "L", "V", "Win %", "Stake", "P&L", "ROI %"]
    if has_clv:
        cols.extend(["CLV+%", "Avg CLV"])
    pdf = agg.select(cols).to_pandas()

    def _bold_all(row):
        if row["Tier"] == "All":
            return ["font-weight: bold; background-color: rgba(255,255,255,0.08)"] * len(row)
        return [""] * len(row)

    fmt = {
        "Stake": "${:,.2f}", "P&L": "${:+,.2f}",
        "Win %": "{:.1f}%", "ROI %": "{:+.1f}%",
    }
    if has_clv:
        fmt["CLV+%"] = "{:.1f}%"
        fmt["Avg CLV"] = "{:+.2f}pp"

    styled = (
        pdf.style.apply(_bold_all, axis=1)
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format(fmt, na_rep="—")
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_per_cell_table(bets: pl.DataFrame, st) -> None:
    """Per-(round, tier) drill-down with kill-switch row highlighting.

    Called per-circuit; the Circuit column is dropped since it's implicit
    from the section. Blank/null cal_tier rows are excluded — the
    round-level table covers the no-tier picture.

    Cells with `Bets >= _KILL_THRESHOLD_N` AND `ROI % < _KILL_THRESHOLD_ROI`
    are highlighted red — the per-cell kill threshold for evaluating bets
    against the model.
    """
    needed = {"cal_tier", "round"}
    if not needed.issubset(bets.columns):
        st.info("Missing columns for segment view (need cal_tier, round).")
        return

    df = bets.filter(
        pl.col("cal_tier").is_not_null() & (pl.col("cal_tier") != "")
    )
    if len(df) == 0:
        st.info("No bets with cal_tier in current filter.")
        return

    agg = _aggregate_with_clv(df, ["round", "cal_tier"])

    cell_cal_agg = (
        df.group_by(["round", "cal_tier"])
        .agg(pl.col("cell_cal").cast(pl.Float64, strict=False).mean().alias("_cc"))
    )
    agg = agg.join(cell_cal_agg, on=["round", "cal_tier"], how="left")
    agg = agg.with_columns(
        (pl.col("_cc") * 100).round(2).alias("Cell Cal")
    ).drop("_cc")

    tier_order_map = {v: i for i, v in enumerate(_TIER_ORDER)}
    round_order_map = {
        v: i for i, v in enumerate(
            ["Q1", "Q2", "R128", "R64", "R32", "R16", "QF", "SF", "F"]
        )
    }
    agg = agg.with_columns(
        pl.col("round").replace_strict(round_order_map, default=999).alias("_round_sort"),
        pl.col("cal_tier").replace_strict(tier_order_map, default=999).alias("_tier_sort"),
    ).sort(["_round_sort", "_tier_sort"]).drop("_round_sort", "_tier_sort")

    agg = agg.rename({"round": "Round", "cal_tier": "Tier"})

    has_clv = "CLV+%" in agg.columns
    cols = ["Round", "Tier", "Cell Cal",
            "Bets", "W", "L", "V", "Win %", "Stake", "P&L", "ROI %"]
    if has_clv:
        cols.extend(["CLV+%", "Avg CLV"])
    pdf = agg.select(cols).to_pandas()

    def _kill_row(row):
        n = row["Bets"]
        roi = row["ROI %"]
        if (
            n is not None and roi is not None
            and n >= _KILL_THRESHOLD_N and roi < _KILL_THRESHOLD_ROI
        ):
            return [
                "background-color: rgba(231,76,60,0.20); font-weight: bold"
            ] * len(row)
        return [""] * len(row)

    fmt = {
        "Cell Cal": "{:+.2f}pp",
        "Stake": "${:,.2f}", "P&L": "${:+,.2f}",
        "Win %": "{:.1f}%", "ROI %": "{:+.1f}%",
    }
    if has_clv:
        fmt["CLV+%"] = "{:.1f}%"
        fmt["Avg CLV"] = "{:+.2f}pp"

    styled = (
        pdf.style.apply(_kill_row, axis=1)
        .applymap(_color_negative, subset=["P&L", "ROI %"])
        .format(fmt, na_rep="—")
    )
    st.caption(
        f"Cells highlighted red: Bets ≥ {_KILL_THRESHOLD_N} AND ROI < "
        f"{_KILL_THRESHOLD_ROI:.0f}% (per-cell kill threshold)."
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


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
    from mvp.analysis.dataset import derive_bet_edge_cols
    bets = derive_bet_edge_cols(bets)
    # As-bet model pick (falls back to pred_side pre-rebuild). Fav/Dog and the
    # cal-tier side splits anchor to this, not the overwritable pred_side.
    pick_col = "bet_pick_side" if "bet_pick_side" in bets.columns else "pred_side"
    if "bet_edge" in bets.columns:
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

    circuits = [
        (val, label)
        for val, label in [("tour", "Tour"), ("chal", "Challenger")]
        if "circuit" in bets.columns
        and len(bets.filter(pl.col("circuit") == val)) > 0
    ]
    circuit_dfs = {val: bets.filter(pl.col("circuit") == val) for val, _ in circuits}

    if "surface" in bets.columns:
        st.subheader("By Surface")
        for val, label in circuits:
            st.markdown(f"**{label}**")
            _render_breakdown(
                circuit_dfs[val], "surface", "Surface", st, all_row=True,
            )

    st.subheader("By Round")
    for val, label in circuits:
        st.markdown(f"**{label}**")
        _render_breakdown(
            circuit_dfs[val], "round", "Round", st,
            sort_order=round_order, all_row=True,
        )

    st.subheader("By Bet Side")
    for val, label in circuits:
        st.markdown(f"**{label}**")
        _render_bet_side_breakdown(circuit_dfs[val], st, pick_col)

    st.subheader("By Book")
    for val, label in circuits:
        st.markdown(f"**{label}**")
        _render_breakdown(expand_by_book(circuit_dfs[val]), "book", "Book", st)

    st.subheader("By Odds Band")
    for val, label in circuits:
        st.markdown(f"**{label}**")
        _render_odds_breakdown(circuit_dfs[val], st)

    st.subheader("By Edge Band")
    for val, label in circuits:
        st.markdown(f"**{label}**")
        _render_edge_bands(circuit_dfs[val], st)

    # Cal tier / segment perf is side-dependent: a Danger cell means the
    # model overestimates its pick, so Model Dog wins in a Danger cell are
    # *expected* and would mislead if mixed with Model Fav rows. Split by
    # Bet Side within each circuit.
    side_splits: list[tuple[str, pl.Expr]] = [
        ("Model Fav", pl.col("bet_side") == pl.col(pick_col)),
        ("Model Dog", pl.col("bet_side") != pl.col(pick_col)),
    ]

    if pick_col in bets.columns:
        st.subheader("By Calibration Tier")
        for val, circ_label in circuits:
            for side_label, side_expr in side_splits:
                side_df = circuit_dfs[val].filter(side_expr)
                if len(side_df) == 0:
                    continue
                st.markdown(f"**{circ_label} — {side_label}**")
                _render_cal_tier_breakdown(side_df, st)

        st.subheader("By Calibration Segment")
        for val, circ_label in circuits:
            for side_label, side_expr in side_splits:
                side_df = circuit_dfs[val].filter(side_expr)
                if len(side_df) == 0:
                    continue
                st.markdown(f"**{circ_label} — {side_label}**")
                _render_per_cell_table(side_df, st)
    else:
        st.subheader("By Calibration Tier")
        for val, circ_label in circuits:
            st.markdown(f"**{circ_label}**")
            _render_cal_tier_breakdown(circuit_dfs[val], st)

        st.subheader("By Calibration Segment")
        for val, circ_label in circuits:
            st.markdown(f"**{circ_label}**")
            _render_per_cell_table(circuit_dfs[val], st)

    # Edge-provenance matrix filters to bets placed after opening-odds
    # capture became reliable.
    if "bet_placed_at" in bets.columns:
        prov_bets = bets.filter(
            pl.col("bet_placed_at").cast(pl.Utf8) > _OPENING_RELIABLE_AFTER
        )
        n_excluded = len(bets) - len(prov_bets)
    else:
        prov_bets = bets.head(0)
        n_excluded = len(bets)

    st.subheader("By Open Edge × Bet Edge")
    if n_excluded > 0:
        st.caption(
            f"Excludes {n_excluded} bets placed before {_OPENING_RELIABLE_AFTER} "
            "where opening-odds capture is unreliable."
        )
    for val, label in circuits:
        circuit_prov = prov_bets.filter(pl.col("circuit") == val)
        if len(circuit_prov) == 0:
            continue
        st.markdown(f"**{label}**")
        _render_provenance_matrix(circuit_prov, st)

    if "net" in bets.columns:
        st.subheader("Cumulative P&L by Bet #")
        for val, label in circuits:
            st.markdown(f"**{label}**")
            net_vals = (
                circuit_dfs[val].sort("effective_match_date")
                ["net"].cast(pl.Float64, strict=False).drop_nulls()
            )
            if len(net_vals) == 0:
                continue
            running = net_vals.cum_sum()
            chart_data = pl.DataFrame({
                "Bet #": range(1, len(running) + 1),
                "Cumulative P&L": running,
            })
            st.line_chart(chart_data.to_pandas(), x="Bet #", y="Cumulative P&L")

    # --- Bets the model flipped after placement ---
    if len(_flipped_bets(bets)) > 0:
        st.subheader("Model Flipped After Placement")
        _render_flipped_bets(bets, st)
