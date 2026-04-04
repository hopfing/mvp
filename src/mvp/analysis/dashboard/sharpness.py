"""Book Sharpness page — per-book signal quality analysis."""

from __future__ import annotations

import re

import polars as pl

from mvp.analysis.simulations import EDGE_BANDS, STAKE

_BAND_NAMES = [b["name"] for b in EDGE_BANDS]

_BOOK_LABELS = {
    "b365": "Bet365", "br": "BetRivers",
    "dk": "DraftKings", "mgm": "MGM",
}

_CUT_LABELS = {
    "open": "Open", "close": "Close",
    "best_intra": "Best Intra", "worst_intra": "Worst Intra",
}


def detect_books(sims: pl.DataFrame) -> list[str]:
    """Detect which books have per-book scenarios in simulations."""
    pattern = re.compile(
        r"^flat_([a-z0-9]+)_(?:open|close|best_intra|worst_intra)$"
    )
    books = set()
    for scenario in sims["scenario"].unique().to_list():
        m = pattern.match(scenario)
        if m:
            books.add(m.group(1))
    return sorted(books)


def book_edge_table(
    sims: pl.DataFrame,
    book: str,
    cut: str = "close",
    model_version: str = "all",
) -> pl.DataFrame:
    """Get edge band results for a specific book and odds cut."""
    expected = {f"{band}_{book}_{cut}" for band in _BAND_NAMES}

    result = sims.filter(
        pl.col("scenario").is_in(expected)
        & (pl.col("segment") == "overall")
        & (pl.col("model_version") == model_version)
    )

    # Sort by canonical band order using a join (same pattern as edge.py)
    band_order_df = pl.DataFrame({
        "scenario": [f"{band}_{book}_{cut}" for band in _BAND_NAMES],
        "_sort_key": list(range(len(_BAND_NAMES))),
    })
    return (
        result.join(band_order_df, on="scenario", how="left")
        .sort("_sort_key")
        .drop("_sort_key")
    )


def book_comparison(
    sims: pl.DataFrame,
    edge_band: str,
    cut: str = "close",
    model_version: str = "all",
) -> pl.DataFrame:
    """Compare a single edge band across all books."""
    books = detect_books(sims)
    scenarios = {f"{edge_band}_{book}_{cut}": book for book in books}

    result = sims.filter(
        pl.col("scenario").is_in(list(scenarios.keys()))
        & (pl.col("segment") == "overall")
        & (pl.col("model_version") == model_version)
    )

    return result.with_columns(
        pl.col("scenario")
        .replace_strict(scenarios, default="unknown")
        .alias("book")
    ).sort("book")


def _compute_band_stats(
    df: pl.DataFrame, odds_col: str, scenario_name: str,
) -> dict | None:
    """Compute flat-bet stats for a filtered slice — mirrors _simulate."""
    bettable = df.filter(pl.col(odds_col).is_not_null())
    n_bets = len(bettable)
    if n_bets == 0:
        return None
    wins = bettable.filter(pl.col("model_correct"))
    n_wins = len(wins)
    total_staked = n_bets * STAKE
    total_returned = wins[odds_col].sum() * STAKE if n_wins > 0 else 0
    net_pnl = total_returned - total_staked
    roi = net_pnl / total_staked
    return {
        "scenario": scenario_name,
        "n_bets": n_bets,
        "accuracy": n_wins / n_bets,
        "roi": roi,
        "net_pnl": net_pnl,
    }


def _apply_edge_filter(
    df: pl.DataFrame, edge_col: str, conditions: list[tuple[str, float]],
) -> pl.DataFrame:
    """Apply edge band conditions (op, val) pairs to df."""
    _ops = {">=": "ge", ">": "gt", "<": "lt", "<=": "le"}
    result = df
    for op, val in conditions:
        result = result.filter(getattr(pl.col(edge_col), _ops[op])(val))
    return result


def compute_book_edge_table(
    ds: pl.DataFrame,
    book: str,
    cut: str = "close",
) -> pl.DataFrame:
    """Compute edge band stats for a book from the raw dataset."""
    odds_col = f"pred_odds_{book}_{cut}"
    edge_col = f"model_edge_{book}_{cut}"

    if odds_col not in ds.columns or edge_col not in ds.columns:
        return pl.DataFrame()

    resolved = ds.filter(pl.col("status") == "resolved") if "status" in ds.columns else ds

    rows = []
    for band in EDGE_BANDS:
        scenario_name = f"{band['name']}_{book}_{cut}"
        filtered = _apply_edge_filter(resolved, edge_col, band["conditions"])
        stats = _compute_band_stats(filtered, odds_col, scenario_name)
        if stats:
            rows.append(stats)

    return pl.DataFrame(rows) if rows else pl.DataFrame()


def compute_book_comparison(
    ds: pl.DataFrame,
    edge_band: str,
    cut: str = "close",
    odds_range: tuple[float, float] | None = None,
) -> pl.DataFrame:
    """Compare a single edge band across all books, computed from raw ds."""
    resolved = ds.filter(pl.col("status") == "resolved") if "status" in ds.columns else ds

    band_def = next((b for b in EDGE_BANDS if b["name"] == edge_band), None)
    if band_def is None:
        return pl.DataFrame()

    # Detect books from ds columns
    pattern = re.compile(rf"^pred_odds_([a-z0-9]+)_{re.escape(cut)}$")
    books = sorted(
        m.group(1) for col in ds.columns if (m := pattern.match(col))
    )

    rows = []
    for book in books:
        odds_col = f"pred_odds_{book}_{cut}"
        edge_col = f"model_edge_{book}_{cut}"
        if edge_col not in ds.columns:
            continue
        book_df = resolved
        if odds_range is not None:
            book_df = book_df.filter(
                pl.col(odds_col).is_not_null()
                & (pl.col(odds_col) >= odds_range[0])
                & (pl.col(odds_col) <= odds_range[1])
            )
        scenario_name = f"{edge_band}_{book}_{cut}"
        filtered = _apply_edge_filter(book_df, edge_col, band_def["conditions"])
        stats = _compute_band_stats(filtered, odds_col, scenario_name)
        if stats:
            stats["book"] = book
            rows.append(stats)

    return pl.DataFrame(rows).sort("book") if rows else pl.DataFrame()


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the book sharpness page."""
    import streamlit as st

    from mvp.analysis.dashboard.components import consensus_selector, model_selector

    # --- Model filter ---
    model_version = model_selector(ds, key="sharpness", default_to_active=True)
    if model_version is not None:
        if "model_version" in sims.columns:
            sims = sims.filter(
                (pl.col("model_version") == model_version)
                | (pl.col("model_version") == "all")
            )
        if "model_version" in ds.columns:
            ds = ds.filter(pl.col("model_version") == model_version)

    # --- Consensus filter ---
    consensus = consensus_selector(ds, key="sharpness")
    if consensus is not None:
        ds = ds.filter(pl.col("consensus") == consensus)

    books = detect_books(sims)
    if not books:
        st.info("No per-book simulation data available.")
        return

    col1, col2 = st.columns(2)
    with col1:
        cut = st.radio(
            "Odds cut",
            options=["open", "close", "best_intra", "worst_intra"],
            format_func=lambda x: _CUT_LABELS.get(x, x),
            horizontal=True,
        )
    with col2:
        view_mode = st.radio(
            "View",
            options=["per_book", "comparison"],
            format_func=lambda x: {
                "per_book": "Per Book",
                "comparison": "Compare Books",
            }[x],
            horizontal=True,
        )

    odds_range = st.slider(
        "Odds range", 1.0, 5.0, (1.0, 5.0), step=0.05,
    )
    odds_filtered = odds_range != (1.0, 5.0)

    if view_mode == "per_book":
        book_sel = st.selectbox(
            "Book",
            options=books,
            format_func=lambda x: _BOOK_LABELS.get(x, x.upper()),
        )

        st.subheader(
            f"{_BOOK_LABELS.get(book_sel, book_sel.upper())} "
            f"— Edge Bands ({_CUT_LABELS[cut]})"
        )

        if odds_filtered:
            odds_col = f"pred_odds_{book_sel}_{cut}"
            ds_filtered = ds.filter(
                pl.col(odds_col).is_not_null()
                & (pl.col(odds_col) >= odds_range[0])
                & (pl.col(odds_col) <= odds_range[1])
            ) if odds_col in ds.columns else ds
            edge_df = compute_book_edge_table(ds_filtered, book=book_sel, cut=cut)
        else:
            edge_df = book_edge_table(sims, book=book_sel, cut=cut)

        if edge_df.is_empty():
            st.info("No data for this selection.")
        else:
            suffix = f"_{book_sel}_{cut}"
            display = edge_df.select([
                pl.col("scenario")
                .str.replace_all(re.escape(suffix) + "$", "")
                .alias("Band"),
                pl.col("n_bets").alias("N"),
                (pl.col("accuracy") * 100).round(1).alias("Acc %"),
                (pl.col("roi") * 100).round(2).alias("ROI %"),
                pl.col("net_pnl").round(2).alias("P&L"),
            ])
            st.dataframe(
                display.to_pandas(),
                use_container_width=True,
                hide_index=True,
            )
    else:
        band_sel = st.selectbox("Edge Band", options=_BAND_NAMES)
        st.subheader(
            f"{band_sel} — Across Books ({_CUT_LABELS[cut]})"
        )

        if odds_filtered:
            comp = compute_book_comparison(
                ds, edge_band=band_sel, cut=cut, odds_range=odds_range,
            )
        else:
            comp = book_comparison(sims, edge_band=band_sel, cut=cut)

        if comp.is_empty():
            st.info("No comparison data for this selection.")
        else:
            display = comp.select([
                pl.col("book")
                .replace_strict(_BOOK_LABELS, default=pl.col("book"))
                .alias("Book"),
                pl.col("n_bets").alias("N"),
                (pl.col("accuracy") * 100).round(1).alias("Acc %"),
                (pl.col("roi") * 100).round(2).alias("ROI %"),
                pl.col("net_pnl").round(2).alias("P&L"),
            ])
            st.dataframe(
                display.to_pandas(),
                use_container_width=True,
                hide_index=True,
            )
