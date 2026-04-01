"""Streamlit dashboard entry point and page registry."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import streamlit as st

from mvp.analysis.dashboard import edge, execution, odds, overview, sharpness
from mvp.analysis.dashboard import insights as insights_page

if TYPE_CHECKING:
    import polars as pl

PAGE_REGISTRY: list[dict] = [
    {"name": "Overview", "icon": "house", "render": overview.render},
    {"name": "Insights", "icon": "search", "render": insights_page.render},
    {"name": "Edge Analysis", "icon": "bar-chart", "render": edge.render},
    {"name": "Odds", "icon": "currency-dollar", "render": odds.render},
    {"name": "Execution", "icon": "activity", "render": execution.render},
    {"name": "Book Sharpness", "icon": "book", "render": sharpness.render},
]


def _load_data(
    data_root: str,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame | None]:
    """Load cached analysis, simulation, and insights parquets."""
    from pathlib import Path

    import polars as pl

    root = Path(data_root)
    ds = pl.read_parquet(root / "analysis" / "analysis.parquet")
    sims = pl.read_parquet(root / "analysis" / "simulations.parquet")
    insights_path = root / "analysis" / "insights.parquet"
    insights = pl.read_parquet(insights_path) if insights_path.exists() else None
    return ds, sims, insights


def _get_active_model() -> str | None:
    """Read the active model name from production.yaml."""
    from pathlib import Path

    import yaml

    prod_path = Path("production.yaml")
    if not prod_path.exists():
        return None
    try:
        with open(prod_path) as f:
            config = yaml.safe_load(f)
        # Config name without path/extension
        config_path = config.get("winner", {}).get("active", {}).get("config", "")
        return Path(config_path).stem if config_path else None
    except Exception:
        return None


def _model_filter_sidebar(ds: pl.DataFrame) -> str | None:
    """Render model version filter in sidebar.

    Returns selected version or None for all.
    """
    import polars as pl

    if "model_version" not in ds.columns:
        return None

    versions = ds["model_version"].drop_nulls().unique().to_list()
    if len(versions) <= 1:
        return None

    active_model = _get_active_model()

    # Sort: active model first, then by most recent prediction date
    max_dates = (
        ds.filter(pl.col("model_version").is_not_null())
        .group_by("model_version")
        .agg(pl.col("effective_match_date").max().alias("_max_date"))
    )
    date_order = {
        row["model_version"]: row["_max_date"]
        for row in max_dates.iter_rows(named=True)
        if row["_max_date"] is not None
    }

    def sort_key(v: str) -> tuple:
        is_active = active_model and v == active_model
        max_date = date_order.get(v)
        return (0 if is_active else 1, max_date or "")

    versions.sort(key=lambda v: (sort_key(v)[0], sort_key(v)[1]), reverse=False)
    # Reverse the date sort within non-active (most recent first)
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

    # Label active model above the dropdown
    if active_model and active_model in versions:
        st.sidebar.caption(f"Production: {active_model}")

    options = ["All Models"] + versions

    # Use session_state to persist selection across reruns
    if "model_filter" not in st.session_state:
        st.session_state.model_filter = "All Models"
    # Validate stored value still exists in options
    if st.session_state.model_filter not in options:
        st.session_state.model_filter = "All Models"

    st.sidebar.selectbox(
        "Model",
        options=options,
        key="model_filter",
    )

    selected = st.session_state.model_filter
    return None if selected == "All Models" else selected


def run(data_root: str) -> None:
    """Launch the Streamlit dashboard."""
    import polars as pl

    st.set_page_config(
        page_title="MVP Analysis",
        page_icon="chart_with_upwards_trend",
        layout="wide",
    )
    ds_full, sims, insights = _load_data(data_root)

    page_names = [p["name"] for p in PAGE_REGISTRY]
    selected = st.sidebar.radio("Page", page_names, index=0)

    # Global model filter (uses unfiltered ds for version list)
    model_version = _model_filter_sidebar(ds_full)
    ds = ds_full
    if model_version is not None:
        ds = ds.filter(pl.col("model_version") == model_version)
        if "model_version" in sims.columns:
            sims = sims.filter(
                (pl.col("model_version") == model_version)
                | (pl.col("model_version") == "all")
            )

    st.title(selected)

    for page in PAGE_REGISTRY:
        if page["name"] == selected:
            if page["name"] == "Insights":
                page["render"](ds, sims, insights)
            else:
                page["render"](ds, sims)
            break


def _get_data_root_from_args() -> str:
    """Extract data_root from command line or environment."""
    from mvp.common.base_job import get_data_root

    # Streamlit passes args after "--" in sys.argv
    try:
        idx = sys.argv.index("--")
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    except ValueError:
        pass
    return str(get_data_root())


run(_get_data_root_from_args())
