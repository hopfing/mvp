"""Edge Analysis page — consensus × edge band profitability."""

from __future__ import annotations

import polars as pl

from mvp.analysis.simulations import EDGE_BANDS

# Band names in canonical order
_BAND_NAMES = [b["name"] for b in EDGE_BANDS]

# Suffix map: basis -> scenario suffix for edge band scenarios
_BASIS_SUFFIX: dict[str, str] = {
    "close": "",
    "open": "_open",
    "formed": "_mkt_formed",
}

# Scenario summary rows to show beneath the edge band table
_SUMMARY_SCENARIOS = [
    "consensus_100",
    "consensus_80",
    "consensus_60",
    "flat_best_close",
    "flat_best_intraday",
    "flat_worst_intraday",
]


def filter_edge_scenarios(
    sims: pl.DataFrame,
    basis: str = "close",
    consensus: str | None = None,
    model_version: str = "all",
) -> pl.DataFrame:
    """Return edge band rows from *sims* for the given basis and consensus cut.

    Parameters
    ----------
    sims:
        Simulation results DataFrame (output of simulations module).
    basis:
        One of "close", "open", "formed".
    consensus:
        If None, return overall segment.  If a string like "0.8", filter to
        segment="consensus", segment_value=<consensus>.
    model_version:
        model_version value to filter on; defaults to "all".

    Returns
    -------
    Filtered DataFrame sorted by canonical EDGE_BANDS order.
    """
    suffix = _BASIS_SUFFIX[basis]
    expected_scenarios = {f"{name}{suffix}" for name in _BAND_NAMES}

    # Segment filter
    if consensus is None:
        seg_filter = (pl.col("segment") == "overall")
    else:
        seg_filter = (
            (pl.col("segment") == "consensus")
            & (pl.col("segment_value") == consensus)
        )

    result = sims.filter(
        (pl.col("model_version") == model_version)
        & seg_filter
        & pl.col("scenario").is_in(expected_scenarios)
    )

    # Sort by canonical band order using an explicit join on an order frame
    band_order_df = pl.DataFrame({
        "scenario": [f"{name}{suffix}" for name in _BAND_NAMES],
        "_sort_key": list(range(len(_BAND_NAMES))),
    })
    result = (
        result.join(band_order_df, on="scenario", how="left")
        .sort("_sort_key")
        .drop("_sort_key")
    )

    return result


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the edge analysis page."""
    import streamlit as st

    # --- Controls ---
    col1, col2 = st.columns(2)

    with col1:
        basis = st.radio(
            "Odds basis",
            options=["close", "open", "formed"],
            format_func=lambda x: {
                "close": "Close",
                "open": "Open",
                "formed": "Mkt Formed",
            }[x],
            horizontal=True,
        )

    # Detect consensus values present in sims
    consensus_vals = (
        sims.filter(pl.col("segment") == "consensus")["segment_value"]
        .unique()
        .sort()
        .to_list()
    )
    consensus_options = ["All"] + consensus_vals

    with col2:
        consensus_sel = st.selectbox(
            "Consensus level",
            options=consensus_options,
        )

    consensus = None if consensus_sel == "All" else consensus_sel

    # --- Edge band table ---
    st.subheader("Edge band profitability")

    edge_df = filter_edge_scenarios(sims, basis=basis, consensus=consensus)

    if edge_df.is_empty():
        st.info("No data for the selected filters.")
    else:
        display = edge_df.select([
            pl.col("scenario").alias("Band"),
            pl.col("n_bets").alias("N"),
            (pl.col("accuracy") * 100).round(1).alias("Acc %"),
            (pl.col("roi") * 100).round(2).alias("ROI %"),
            pl.col("net_pnl").round(2).alias("P&L"),
        ])
        st.dataframe(display, use_container_width=True, hide_index=True)

    # --- Scenario summary table ---
    st.subheader("Scenario summary")

    summary_df = sims.filter(
        (pl.col("model_version") == "all")
        & (pl.col("segment") == "overall")
        & pl.col("scenario").is_in(_SUMMARY_SCENARIOS)
    )

    if summary_df.is_empty():
        st.info("No scenario summary data available.")
    else:
        # Preserve canonical scenario order
        summary_order_df = pl.DataFrame({
            "scenario": _SUMMARY_SCENARIOS,
            "_sort_key": list(range(len(_SUMMARY_SCENARIOS))),
        })
        summary_df = (
            summary_df.join(summary_order_df, on="scenario", how="left")
            .sort("_sort_key")
            .drop("_sort_key")
        )

        display_summary = summary_df.select([
            pl.col("scenario").alias("Scenario"),
            pl.col("n_bets").alias("N"),
            (pl.col("accuracy") * 100).round(1).alias("Acc %"),
            (pl.col("roi") * 100).round(2).alias("ROI %"),
            pl.col("net_pnl").round(2).alias("P&L"),
        ])
        st.dataframe(display_summary, use_container_width=True, hide_index=True)
