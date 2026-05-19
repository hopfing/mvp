"""Overview page — model performance, bet performance, odds coverage."""

from __future__ import annotations

import polars as pl


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

    # --- Odds Coverage ---
    st.subheader("Odds Coverage")
    cov = compute_odds_coverage(ds)
    render_metric_cards([
        metric_card_data("Predictions", cov["n_predictions"], fmt="d"),
        metric_card_data("Resolved", cov["n_resolved"], fmt="d"),
        metric_card_data("Pending", cov["n_pending"], fmt="d"),
        metric_card_data("Books Active", cov["books_active"], fmt="d"),
    ])
