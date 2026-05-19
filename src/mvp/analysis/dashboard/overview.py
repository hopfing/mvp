"""Overview page — model performance, bet performance, odds coverage."""

from __future__ import annotations

import polars as pl

# Mirrors the Bet Performance breakdowns in bets.py — kept in sync deliberately
# so model-side and bet-side tables share row ordering and kill thresholds.
_TIER_ORDER = ["UnderC", "Optimal", "Border", "Risky", "Danger"]
_ROUND_ORDER = ["Q1", "Q2", "R128", "R64", "R32", "R16", "QF", "SF", "F"]
_KILL_THRESHOLD_N = 15
_KILL_THRESHOLD_ROI = -10.0


def compute_model_performance(ds: pl.DataFrame) -> dict:
    """Compute model performance metrics (all resolved predictions, flat $1 stake)."""
    resolved = (
        ds.filter(pl.col("status") == "resolved")
        if "status" in ds.columns
        else ds
    )

    n = len(resolved)
    if n == 0 or "model_correct" not in resolved.columns:
        return {
            "n": 0, "wins": 0, "losses": 0,
            "accuracy": None, "stake": 0, "pnl": None, "roi": None,
        }

    wins = int(resolved["model_correct"].sum())
    losses = n - wins
    accuracy = wins / n

    # Flat $1 stake ROI — only count rows that have closing odds
    pnl = None
    roi = None
    if "pred_odds_best_close" in resolved.columns:
        has_odds = resolved.filter(pl.col("pred_odds_best_close").is_not_null())
        n_with_odds = len(has_odds)
        if n_with_odds > 0:
            correct = has_odds.filter(pl.col("model_correct"))
            returned = correct["pred_odds_best_close"].sum()
            pnl = returned - n_with_odds
            roi = pnl / n_with_odds

    return {
        "n": n, "wins": wins, "losses": losses,
        "accuracy": accuracy, "stake": n, "pnl": pnl, "roi": roi,
    }


def compute_bet_performance(ds: pl.DataFrame) -> dict:
    """Compute bet performance metrics (actual bets placed)."""
    if "bet_side" not in ds.columns:
        return {
            "n": 0, "wins": 0, "losses": 0, "void": 0,
            "accuracy": None, "stake": None, "pnl": None, "roi": None,
        }

    bets = ds.filter(
        pl.col("bet_side").is_in(["P1", "P2"])
        & (pl.col("status") == "resolved")
    )
    n = len(bets)
    if n == 0:
        return {
            "n": 0, "wins": 0, "losses": 0, "void": 0,
            "accuracy": None, "stake": None, "pnl": None, "roi": None,
        }

    wins = 0
    losses = 0
    void = 0
    if "bet_result" in bets.columns:
        wins = int(
            bets.filter(pl.col("bet_result") == "W").height
        )
        losses = int(
            bets.filter(pl.col("bet_result") == "L").height
        )
        void = int(
            bets.filter(pl.col("bet_result") == "V").height
        )

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


def compute_odds_coverage(ds: pl.DataFrame) -> dict:
    """Compute odds/data coverage metrics."""
    n_predictions = len(ds)

    n_resolved = 0
    n_pending = 0
    if "status" in ds.columns:
        n_resolved = int(
            ds.filter(pl.col("status") == "resolved").height
        )
        n_pending = n_predictions - n_resolved

    # Detect active books from per-book closing odds columns
    book_cols = [
        c.removesuffix("_closing_odds_p1")
        for c in ds.columns
        if c.endswith("_closing_odds_p1")
        and not c.startswith(("best_", "worst_", "avg_"))
    ]
    books_active = len(book_cols)

    return {
        "n_predictions": n_predictions,
        "n_resolved": n_resolved,
        "n_pending": n_pending,
        "books_active": books_active,
    }


def _color_negative(val):
    if isinstance(val, (int, float)) and val < 0:
        return "color: #e74c3c"
    return ""


def _aggregate_model_perf(
    ds: pl.DataFrame, group_cols: list[str]
) -> pl.DataFrame:
    """Aggregate model-side perf by group_cols.

    Caller is responsible for filtering to resolved predictions with closing
    odds so N is a single consistent denominator: Acc %, P&L, and ROI % all
    use the same N. Matches the convention in the Bet Performance tables.
    """
    aggs = [
        pl.len().alias("N"),
        pl.col("model_correct").cast(pl.Int64).sum().alias("W"),
    ]
    has_odds = "pred_odds_best_close" in ds.columns
    if has_odds:
        aggs.append(
            pl.when(pl.col("model_correct"))
            .then(pl.col("pred_odds_best_close").cast(pl.Float64, strict=False))
            .otherwise(0.0)
            .sum()
            .alias("_returns")
        )
    out = ds.group_by(group_cols).agg(aggs)
    out = out.with_columns(
        (pl.col("N") - pl.col("W")).alias("L"),
        pl.when(pl.col("N") > 0)
        .then((pl.col("W") / pl.col("N") * 100).round(1))
        .otherwise(None)
        .alias("Acc %"),
    )
    if has_odds:
        out = out.with_columns(
            (pl.col("_returns") - pl.col("N")).round(2).alias("P&L"),
        )
        out = out.with_columns(
            pl.when(pl.col("N") > 0)
            .then((pl.col("P&L") / pl.col("N") * 100).round(1))
            .otherwise(None)
            .alias("ROI %"),
        ).drop("_returns")
    return out


def _resolved_with_tier(ds: pl.DataFrame) -> pl.DataFrame:
    """Filter to resolved predictions with usable cal_tier AND closing odds.

    The has-closing-odds filter keeps N a single denominator for the row —
    accuracy, P&L, and ROI all share the same N (matches Bet Performance
    convention where Bets is the count and the denominator everywhere).
    """
    df = ds
    if "status" in df.columns:
        df = df.filter(pl.col("status") == "resolved")
    if "cal_tier" in df.columns:
        df = df.filter(
            pl.col("cal_tier").is_not_null() & (pl.col("cal_tier") != "")
        )
    if "pred_odds_best_close" in df.columns:
        df = df.filter(pl.col("pred_odds_best_close").is_not_null())
    return df


def _render_model_cal_tier_breakdown(ds: pl.DataFrame, st) -> None:
    """Per-tier model-side breakdown with All row at top."""
    if "cal_tier" not in ds.columns or "model_correct" not in ds.columns:
        st.info("Missing columns for tier view (need cal_tier, model_correct).")
        return

    df = _resolved_with_tier(ds)
    if len(df) == 0:
        st.info("No resolved predictions with cal_tier in current filter.")
        return

    agg = _aggregate_model_perf(df, ["cal_tier"]).rename({"cal_tier": "Tier"})
    order_map = {v: i for i, v in enumerate(_TIER_ORDER)}
    agg = agg.with_columns(
        pl.col("Tier").replace_strict(order_map, default=999).alias("_sort")
    ).sort("_sort").drop("_sort")

    totals = _aggregate_model_perf(
        df.with_columns(pl.lit("All").alias("cal_tier")), ["cal_tier"]
    ).rename({"cal_tier": "Tier"})
    agg = pl.concat([totals, agg])

    has_odds = "P&L" in agg.columns
    cols = ["Tier", "N", "W", "L", "Acc %"]
    if has_odds:
        cols.extend(["P&L", "ROI %"])
    pdf = agg.select(cols).to_pandas()

    def _bold_all(row):
        if row["Tier"] == "All":
            return [
                "font-weight: bold; background-color: rgba(255,255,255,0.08)"
            ] * len(row)
        return [""] * len(row)

    fmt = {"Acc %": "{:.1f}%"}
    if has_odds:
        fmt.update({"P&L": "${:+,.2f}", "ROI %": "{:+.1f}%"})

    styled = pdf.style.apply(_bold_all, axis=1)
    if has_odds:
        styled = styled.applymap(_color_negative, subset=["P&L", "ROI %"])
    styled = styled.format(fmt, na_rep="—")
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_model_per_cell_table(ds: pl.DataFrame, st) -> None:
    """Per-(round, tier) model-side drill-down with kill-switch highlighting."""
    needed = {"cal_tier", "round", "model_correct"}
    if not needed.issubset(ds.columns):
        st.info(
            "Missing columns for segment view "
            "(need cal_tier, round, model_correct)."
        )
        return

    df = _resolved_with_tier(ds)
    if len(df) == 0:
        st.info("No resolved predictions with cal_tier in current filter.")
        return

    agg = _aggregate_model_perf(df, ["round", "cal_tier"])

    cell_cal_agg = (
        df.group_by(["round", "cal_tier"])
        .agg(
            pl.col("cell_cal")
            .cast(pl.Float64, strict=False)
            .mean()
            .alias("_cc")
        )
    )
    agg = agg.join(cell_cal_agg, on=["round", "cal_tier"], how="left")
    agg = agg.with_columns(
        (pl.col("_cc") * 100).round(2).alias("Cell Cal")
    ).drop("_cc")

    tier_map = {v: i for i, v in enumerate(_TIER_ORDER)}
    round_map = {v: i for i, v in enumerate(_ROUND_ORDER)}
    agg = agg.with_columns(
        pl.col("round").replace_strict(round_map, default=999).alias("_r"),
        pl.col("cal_tier").replace_strict(tier_map, default=999).alias("_t"),
    ).sort(["_r", "_t"]).drop("_r", "_t")

    agg = agg.rename({"round": "Round", "cal_tier": "Tier"})

    has_odds = "P&L" in agg.columns
    cols = ["Round", "Tier", "Cell Cal", "N", "W", "L", "Acc %"]
    if has_odds:
        cols.extend(["P&L", "ROI %"])
    pdf = agg.select(cols).to_pandas()

    def _kill_row(row):
        n = row["N"]
        roi = row.get("ROI %") if has_odds else None
        if (
            n is not None
            and roi is not None
            and n >= _KILL_THRESHOLD_N
            and roi < _KILL_THRESHOLD_ROI
        ):
            return [
                "background-color: rgba(231,76,60,0.20); font-weight: bold"
            ] * len(row)
        return [""] * len(row)

    fmt = {"Cell Cal": "{:+.2f}pp", "Acc %": "{:.1f}%"}
    if has_odds:
        fmt.update({"P&L": "${:+,.2f}", "ROI %": "{:+.1f}%"})

    styled = pdf.style.apply(_kill_row, axis=1)
    if has_odds:
        styled = styled.applymap(_color_negative, subset=["P&L", "ROI %"])
    styled = styled.format(fmt, na_rep="—")
    st.caption(
        f"Cells highlighted red: N ≥ {_KILL_THRESHOLD_N} AND ROI < "
        f"{_KILL_THRESHOLD_ROI:.0f}% (per-cell kill threshold)."
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _fmt(val: float | int | None, fmt: str) -> str:
    """Format a value for display, returning '—' for None."""
    if val is None:
        return "—"
    if fmt == "d":
        return f"{int(val):,}"
    if fmt == "$":
        sign = "+" if val >= 0 else ""
        return f"{sign}${val:,.2f}"
    if fmt == "%":
        sign = "+" if val >= 0 else ""
        return f"{sign}{val:.1%}"
    return str(val)


def render(ds: pl.DataFrame, sims: pl.DataFrame, latest_run: dict | None = None) -> None:
    """Render the overview page."""
    import streamlit as st

    from mvp.analysis.dashboard.components import (
        consensus_selector,
        metric_card_data,
        model_selector,
        render_metric_cards,
    )

    # --- Pipeline Health Strip ---
    if latest_run:
        books_fetched = latest_run.get("books_fetched", {})
        books_with_odds = sum(1 for v in books_fetched.values() if v > 0)
        books_total = len(books_fetched)
        error_count = len(latest_run.get("errors", []))

        h_cols = st.columns(2)
        with h_cols[0]:
            st.metric("Books with Odds", f"{books_with_odds}/{books_total}")
        with h_cols[1]:
            if error_count > 0:
                st.metric("Pipeline Errors", error_count)
            else:
                st.metric("Pipeline Errors", "0")
        st.divider()

    model_version = model_selector(ds, key="overview", default_to_active=False)
    if model_version is not None:
        ds = ds.filter(pl.col("model_version") == model_version)

    consensus = consensus_selector(ds, key="overview")
    if consensus is not None:
        ds = ds.filter(pl.col("consensus") == consensus)

    m = compute_model_performance(ds)
    b = compute_bet_performance(ds)

    # --- Bet Performance ---
    st.subheader("Bet Performance")
    if b["n"] > 0:
        record_bet = f"{b['wins']} - {b['losses']} - {b['void']}"
    else:
        record_bet = "—"
    render_metric_cards([
        metric_card_data("N", b["n"], fmt="d"),
        {"label": "Record", "value": record_bet},
        metric_card_data("Accuracy", b["accuracy"], fmt=".1%"),
        {
            "label": "Stake",
            "value": f"${b['stake']:,.2f}" if b["stake"] else "\u2014",
        },
        metric_card_data("P&L", b["pnl"], fmt="$.2f"),
        metric_card_data("ROI", b["roi"], fmt=".1%"),
    ])

    # --- Model Performance ---
    st.subheader("Model Performance")
    record_model = f"{m['wins']} - {m['losses']}" if m["n"] > 0 else "—"
    render_metric_cards([
        metric_card_data("N", m["n"], fmt="d"),
        {"label": "Record", "value": record_model},
        metric_card_data("Accuracy", m["accuracy"], fmt=".1%"),
        {
            "label": "Stake",
            "value": f"${m['stake']:,.2f}" if m["stake"] else "\u2014",
        },
        metric_card_data("P&L", m["pnl"], fmt="$.2f"),
        metric_card_data("ROI", m["roi"], fmt=".1%"),
    ])

    # Edge sub-rows
    if "model_edge_best_close" in ds.columns:
        for label, subset in [
            ("Positive Edge", ds.filter(pl.col("model_edge_best_close") > 0)),
            ("Negative Edge", ds.filter(pl.col("model_edge_best_close") <= 0)),
        ]:
            sm = compute_model_performance(subset)
            record = f"{sm['wins']} - {sm['losses']}" if sm["n"] > 0 else "—"
            st.markdown(f"#### {label}")
            render_metric_cards([
                metric_card_data("N", sm["n"], fmt="d"),
                {"label": "Record", "value": record},
                metric_card_data("Accuracy", sm["accuracy"], fmt=".1%"),
                {
                    "label": "Stake",
                    "value": f"${sm['stake']:,.2f}" if sm["stake"] else "\u2014",
                },
                metric_card_data("P&L", sm["pnl"], fmt="$.2f"),
                metric_card_data("ROI", sm["roi"], fmt=".1%"),
            ])

        # No closing odds row
        no_odds = ds.filter(pl.col("model_edge_best_close").is_null())
        nm = compute_model_performance(no_odds)
        if nm["n"] > 0:
            record_no = f"{nm['wins']} - {nm['losses']}"
            st.markdown("#### No Odds")
            render_metric_cards([
                metric_card_data("N", nm["n"], fmt="d"),
                {"label": "Record", "value": record_no},
                metric_card_data("Accuracy", nm["accuracy"], fmt=".1%"),
                {"label": "Stake", "value": "\u2014"},
                {"label": "P&L", "value": "\u2014"},
                {"label": "ROI", "value": "\u2014"},
            ])

    # --- By Calibration Tier / Segment (model-side) ---
    if "cal_tier" in ds.columns and "model_correct" in ds.columns:
        circuits = [
            (val, label)
            for val, label in [("tour", "Tour"), ("chal", "Challenger")]
            if "circuit" in ds.columns
            and len(ds.filter(pl.col("circuit") == val)) > 0
        ]
        if circuits:
            circuit_dfs = {
                val: ds.filter(pl.col("circuit") == val) for val, _ in circuits
            }

            st.subheader("By Calibration Tier")
            for val, label in circuits:
                st.markdown(f"**{label}**")
                _render_model_cal_tier_breakdown(circuit_dfs[val], st)

            st.subheader("By Calibration Segment")
            for val, label in circuits:
                st.markdown(f"**{label}**")
                _render_model_per_cell_table(circuit_dfs[val], st)

    # --- Odds Coverage ---
    st.subheader("Odds Coverage")
    cov = compute_odds_coverage(ds)
    render_metric_cards([
        metric_card_data("Predictions", cov["n_predictions"], fmt="d"),
        metric_card_data("Resolved", cov["n_resolved"], fmt="d"),
        metric_card_data("Pending", cov["n_pending"], fmt="d"),
        metric_card_data("Books Active", cov["books_active"], fmt="d"),
    ])
