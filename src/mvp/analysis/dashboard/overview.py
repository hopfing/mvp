"""Overview page — headline metrics at a glance."""

from __future__ import annotations

import polars as pl
import streamlit as st


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the overview page."""
    st.header("Overview")
    st.info("Coming soon.")
