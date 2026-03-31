"""Streamlit dashboard entry point and page registry."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import streamlit as st

if TYPE_CHECKING:
    import polars as pl

from mvp.analysis.dashboard import edge, execution, odds, overview, sharpness

PAGE_REGISTRY: list[dict] = [
    {"name": "Overview", "icon": "house", "render": overview.render},
    {"name": "Edge Analysis", "icon": "bar-chart", "render": edge.render},
    {"name": "Odds", "icon": "currency-dollar", "render": odds.render},
    {"name": "Execution", "icon": "activity", "render": execution.render},
    {"name": "Book Sharpness", "icon": "book", "render": sharpness.render},
]


def _load_data(data_root: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load cached analysis and simulation parquets."""
    from pathlib import Path

    import polars as pl

    root = Path(data_root)
    ds = pl.read_parquet(root / "analysis" / "analysis.parquet")
    sims = pl.read_parquet(root / "analysis" / "simulations.parquet")
    return ds, sims


def run(data_root: str) -> None:
    """Launch the Streamlit dashboard."""
    st.set_page_config(
        page_title="MVP Analysis",
        page_icon="chart_with_upwards_trend",
        layout="wide",
    )
    ds, sims = _load_data(data_root)

    page_names = [p["name"] for p in PAGE_REGISTRY]
    selected = st.sidebar.radio("Page", page_names, index=0)

    st.title(selected)

    for page in PAGE_REGISTRY:
        if page["name"] == selected:
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
