"""Shared UI components for the analysis dashboard."""

from __future__ import annotations

import polars as pl


def get_active_model() -> str | None:
    """Read the active model name from production.yaml."""
    from pathlib import Path

    import yaml

    prod_path = Path("production.yaml")
    if not prod_path.exists():
        return None
    try:
        with open(prod_path) as f:
            config = yaml.safe_load(f)
        config_path = (
            config.get("winner", {}).get("active", {}).get("config", "")
        )
        return Path(config_path).stem if config_path else None
    except Exception:
        return None


def model_selector(
    ds: pl.DataFrame,
    key: str,
    default_to_active: bool = False,
) -> str | None:
    """Render a model version selectbox. Returns selected version or None.

    Args:
        ds: Full (unfiltered) analysis dataset.
        key: Unique Streamlit widget key to avoid conflicts across pages.
        default_to_active: If True, default to production model instead of All.
    """
    import streamlit as st

    if "model_version" not in ds.columns:
        return None

    versions = ds["model_version"].drop_nulls().unique().to_list()
    if len(versions) <= 1:
        return None

    active_model = get_active_model()

    # Sort: active first, then by most recent prediction date
    if "effective_match_date" in ds.columns:
        max_dates = (
            ds.filter(pl.col("model_version").is_not_null())
            .group_by("model_version")
            .agg(pl.col("effective_match_date").max().alias("_d"))
        )
        date_order = {
            r["model_version"]: r["_d"]
            for r in max_dates.iter_rows(named=True)
            if r["_d"] is not None
        }
    else:
        date_order = {}

    if active_model and active_model in versions:
        active = [v for v in versions if v == active_model]
        rest = sorted(
            [v for v in versions if v != active_model],
            key=lambda v: date_order.get(v, ""),
            reverse=True,
        )
        versions = active + rest
    else:
        versions.sort(
            key=lambda v: date_order.get(v, ""), reverse=True
        )

    options = ["All Models"] + versions

    # Default
    state_key = f"model_sel_{key}"
    if state_key not in st.session_state:
        if default_to_active and active_model and active_model in versions:
            st.session_state[state_key] = active_model
        else:
            st.session_state[state_key] = "All Models"
    if st.session_state[state_key] not in options:
        st.session_state[state_key] = "All Models"

    if active_model and active_model in versions:
        st.sidebar.caption(f"Production: {active_model}")

    st.sidebar.selectbox("Model", options=options, key=state_key)

    selected = st.session_state[state_key]
    return None if selected == "All Models" else selected


def consensus_selector(
    ds: pl.DataFrame,
    key: str,
) -> float | None:
    """Render a consensus level selectbox in the sidebar.

    Returns the selected consensus value as a float, or None for 'All'.
    """
    import streamlit as st

    if "consensus" not in ds.columns:
        return None

    vals = ds["consensus"].drop_nulls().unique().sort().to_list()
    if not vals:
        return None

    str_vals = [str(v) for v in vals]
    options = ["All"] + str_vals

    selected = st.sidebar.selectbox(
        "Consensus",
        options=options,
        key=f"consensus_sel_{key}",
    )

    return None if selected == "All" else float(selected)


def metric_card_data(
    label: str,
    value: float | int | None,
    fmt: str = ".1f",
    delta: float | None = None,
    delta_fmt: str | None = None,
) -> dict:
    """Prepare metric card data. Returns dict with label, value, delta strings.

    Does not call Streamlit — pure data prep so it's testable.
    """
    if value is None:
        formatted = "\u2014"
    elif "%" in fmt:
        formatted = f"{value:{fmt}}"
    elif "$" in fmt:
        sign = "+" if value >= 0 else ""
        formatted = f"{sign}${value:{fmt.replace('$', '')}}"
    else:
        formatted = f"{value:{fmt}}"

    result: dict = {"label": label, "value": formatted}

    if delta is not None and delta_fmt is not None:
        if "%" in delta_fmt:
            result["delta"] = f"{delta:{delta_fmt}}"
        else:
            result["delta"] = f"{delta:{delta_fmt}}"

    return result


def render_metric_cards(cards: list[dict]) -> None:
    """Render a row of metric cards using st.metric."""
    import streamlit as st

    cols = st.columns(len(cards))
    for col, card in zip(cols, cards):
        with col:
            st.metric(
                label=card["label"],
                value=card["value"],
                delta=card.get("delta"),
            )


def style_roi(roi: float) -> str:
    """Return CSS color string for ROI value."""
    if roi > 0.05:
        return "color: #4CAF50; font-weight: bold"
    elif roi > 0:
        return "color: #81C784"
    elif roi > -0.05:
        return "color: #EF9A9A"
    else:
        return "color: #EF5350; font-weight: bold"


def format_sim_table(
    sims: pl.DataFrame,
    scenarios: list[str],
    segment: str = "overall",
    segment_value: str = "overall",
    model_version: str = "all",
) -> pl.DataFrame:
    """Filter and format simulation results into a display table.

    Returns a Polars DataFrame with columns: Scenario, N, Acc, ROI, P&L.
    """
    filtered = sims.filter(
        pl.col("scenario").is_in(scenarios)
        & (pl.col("segment") == segment)
        & (pl.col("segment_value") == segment_value)
        & (pl.col("model_version") == model_version)
    )

    if len(filtered) == 0:
        return pl.DataFrame(
            schema={
                "Scenario": pl.Utf8,
                "N": pl.Int64,
                "Acc": pl.Utf8,
                "ROI": pl.Utf8,
                "P&L": pl.Utf8,
            }
        )

    # Preserve scenario order from input list
    order = {name: i for i, name in enumerate(scenarios)}
    result = (
        filtered.with_columns(
            pl.col("scenario")
            .replace_strict(order, default=999)
            .alias("_order")
        )
        .sort("_order")
        .select(
            pl.col("scenario").alias("Scenario"),
            pl.col("n_bets").alias("N"),
            pl.format("{}%", (pl.col("accuracy") * 100).round(1)).alias("Acc"),
            pl.format("{}%", (pl.col("roi") * 100).round(1)).alias("ROI"),
            pl.col("net_pnl").round(2).alias("P&L"),
        )
    )

    return result
