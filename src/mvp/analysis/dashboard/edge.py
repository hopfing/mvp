"""Edge Analysis page — consensus × edge band profitability."""

from __future__ import annotations

import polars as pl
import streamlit as st


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the edge analysis page."""
    st.header("Edge Analysis")
    st.info("Coming soon.")
