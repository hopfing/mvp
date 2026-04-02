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

# Flat scenarios always shown in summary
_FLAT_SCENARIOS = [
    "flat_best_open",
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

    from mvp.analysis.dashboard.components import (
        metric_card_data,
        model_selector,
        render_metric_cards,
    )
    from mvp.analysis.dashboard.overview import compute_model_performance

    # --- Model filter ---
    model_version = model_selector(ds, key="edge", default_to_active=True)
    if model_version is not None:
        ds = ds.filter(pl.col("model_version") == model_version)
        if "model_version" in sims.columns:
            sims = sims.filter(
                (pl.col("model_version") == model_version)
                | (pl.col("model_version") == "all")
            )

    # --- Controls ---
    col1, col2 = st.columns(2)

    with col1:
        basis = st.radio(
            "Odds basis",
            options=["all", "open", "formed", "close"],
            format_func=lambda x: {
                "open": "Open",
                "formed": "Formed",
                "close": "Close",
                "all": "All",
            }[x],
            horizontal=True,
        )

    # Detect consensus values for selected model
    mv = model_version or "all"
    model_sims = sims.filter(pl.col("model_version") == mv)
    consensus_vals = (
        model_sims.filter(pl.col("segment") == "consensus")["segment_value"]
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

    # --- Summary metrics ---
    filtered_ds = ds
    if consensus is not None and "consensus" in ds.columns:
        filtered_ds = ds.filter(pl.col("consensus") == float(consensus))

    for label, subset in [
        ("Positive Edge", filtered_ds.filter(pl.col("model_edge_best_close") > 0)
         if "model_edge_best_close" in filtered_ds.columns else filtered_ds.head(0)),
        ("Negative Edge", filtered_ds.filter(pl.col("model_edge_best_close") <= 0)
         if "model_edge_best_close" in filtered_ds.columns else filtered_ds.head(0)),
    ]:
        m = compute_model_performance(subset)
        if m["n"] == 0:
            continue
        record = f"{m['wins']} - {m['losses']}" if m["n"] > 0 else "—"
        st.markdown(f"**{label}**")
        render_metric_cards([
            metric_card_data("N", m["n"], fmt="d"),
            {"label": "Record", "value": record},
            metric_card_data("Accuracy", m["accuracy"], fmt=".1%"),
            metric_card_data("P&L", m["pnl"], fmt="$.2f"),
            metric_card_data("ROI", m["roi"], fmt=".1%"),
        ])

    # --- Edge band table ---
    st.subheader("Edge band profitability")

    if basis == "all":
        cols = st.columns(3)
        for col_widget, b, label in zip(
            cols,
            ["open", "formed", "close"],
            ["Open", "Formed", "Close"],
        ):
            with col_widget:
                st.subheader(label)
                edge_df = filter_edge_scenarios(
                    sims, basis=b, consensus=consensus,
                    model_version=mv,
                )
                if edge_df.is_empty():
                    st.info("No data.")
                else:
                    suffix = _BASIS_SUFFIX[b]
                    display = edge_df.select([
                        pl.col("scenario")
                        .str.replace_all(f"{suffix}$", "")
                        .alias("Band"),
                        pl.col("n_bets").alias("N"),
                        (pl.col("accuracy") * 100).round(1).alias("Acc %"),
                        (pl.col("roi") * 100).round(2).alias("ROI %"),
                        pl.col("net_pnl").round(2).alias("P&L"),
                    ])
                    st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        edge_df = filter_edge_scenarios(
            sims, basis=basis, consensus=consensus, model_version=mv,
        )

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

    # Flat scenarios, filtered by consensus if selected
    if consensus is not None:
        summary_df = model_sims.filter(
            (pl.col("segment") == "consensus")
            & (pl.col("segment_value") == consensus)
            & pl.col("scenario").is_in(_FLAT_SCENARIOS)
        )
    else:
        summary_df = model_sims.filter(
            (pl.col("segment") == "overall")
            & pl.col("scenario").is_in(_FLAT_SCENARIOS)
        )

    if summary_df.is_empty():
        st.info("No scenario summary data available.")
    else:
        summary_order = {s: i for i, s in enumerate(_FLAT_SCENARIOS)}
        summary_order_df = pl.DataFrame({
            "scenario": list(summary_order.keys()),
            "_sort_key": list(summary_order.values()),
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
