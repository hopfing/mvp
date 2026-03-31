"""Book Sharpness page — per-book signal quality analysis."""

from __future__ import annotations

import re

import polars as pl

from mvp.analysis.simulations import EDGE_BANDS

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


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the book sharpness page."""
    import streamlit as st

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
