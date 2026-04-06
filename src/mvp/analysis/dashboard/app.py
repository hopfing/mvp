"""Streamlit dashboard entry point and page registry."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import streamlit as st

from mvp.analysis.dashboard import bets, edge, execution, health, odds, overview, sharpness
from mvp.analysis.dashboard import insights as insights_page

if TYPE_CHECKING:
    import polars as pl

PAGE_REGISTRY: list[dict] = [
    {"name": "Overview", "icon": "house", "render": overview.render},
    {"name": "Bet Performance", "icon": "cash-coin", "render": bets.render},
    {"name": "Insights", "icon": "search", "render": insights_page.render},
    {"name": "Edge Analysis", "icon": "bar-chart", "render": edge.render},
    {"name": "Odds", "icon": "currency-dollar", "render": odds.render},
    {"name": "Execution", "icon": "activity", "render": execution.render},
    {"name": "Book Sharpness", "icon": "book", "render": sharpness.render},
    {"name": "Pipeline Health", "icon": "heart-pulse", "render": health.render},
]


def _load_data(
    data_root: str,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame | None, dict | None]:
    """Load cached analysis, simulation, insights parquets, and health data."""
    from pathlib import Path

    import polars as pl

    from mvp.analysis.dashboard.health_data import load_latest_run

    root = Path(data_root)
    ds = pl.read_parquet(root / "analysis" / "analysis.parquet")
    sims = pl.read_parquet(root / "analysis" / "simulations.parquet")
    insights_path = root / "analysis" / "insights.parquet"
    insights = pl.read_parquet(insights_path) if insights_path.exists() else None
    latest_run = load_latest_run(root)
    return ds, sims, insights, latest_run


def run(data_root: str) -> None:
    """Launch the Streamlit dashboard."""
    from datetime import datetime

    st.set_page_config(
        page_title="MVP Analysis",
        page_icon="chart_with_upwards_trend",
        layout="wide",
    )
    ds, sims, insights, latest_run = _load_data(data_root)

    # Global refresh indicator (top-right on every page)
    if latest_run and latest_run.get("timestamp"):
        ts = datetime.fromisoformat(latest_run["timestamp"])
        age_minutes = (datetime.now() - ts).total_seconds() / 60
        color = "red" if age_minutes > 30 else "green"
        label = ts.strftime("%Y-%m-%d %H:%M")
        st.markdown(
            f'<div style="text-align: right; color: {color};">'
            f"Last Refreshed {label}</div>",
            unsafe_allow_html=True,
        )

    page_names = [p["name"] for p in PAGE_REGISTRY]
    selected = st.sidebar.radio("Page", page_names, index=0)

    st.title(selected)

    for page in PAGE_REGISTRY:
        if page["name"] == selected:
            if page["name"] == "Insights":
                page["render"](ds, sims, insights)
            elif page["name"] == "Pipeline Health":
                page["render"](data_root)
            elif page["name"] == "Overview":
                page["render"](ds, sims, latest_run)
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
