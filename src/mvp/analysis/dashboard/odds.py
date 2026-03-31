"""Odds page — edge in context of price level."""

from __future__ import annotations

import polars as pl
import streamlit as st


def render(ds: pl.DataFrame, sims: pl.DataFrame) -> None:
    """Render the odds page."""
    st.header("Odds")
    st.info("Coming soon.")
