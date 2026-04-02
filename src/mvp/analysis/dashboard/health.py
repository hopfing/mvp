"""Pipeline Health detail page."""

from __future__ import annotations

from pathlib import Path


def format_tournaments(run: dict) -> tuple[str, list[dict]]:
    """Format tournament processing summary."""
    processed = run.get("tournaments_processed", 0)
    failed = run.get("tournaments_failed", 0)
    summary = f"{processed} processed, {failed} failed"
    failures = run.get("tournament_failures", [])
    return summary, failures


def format_books_fetched(run: dict) -> list[dict]:
    """Format per-book odds fetch counts."""
    books = run.get("books_fetched", {})
    return [{"book": book, "entries": count} for book, count in books.items()]


def format_unresolved_names(run: dict) -> list[dict]:
    """Format unresolved names across books. Only includes books with names."""
    all_names = run.get("unresolved_names", {})
    rows = []
    for book, names in all_names.items():
        for name in names:
            rows.append({"book": book, "name": name})
    return rows


def format_predictions_without_odds(run: dict) -> tuple[str, list[dict]]:
    """Format predictions without odds summary."""
    items = run.get("predictions_without_odds", [])
    total = run.get("predictions_total", 0)
    summary = f"{len(items)}/{total} predictions without odds from any book"
    return summary, items


def render(data_root: str) -> None:
    """Render the Pipeline Health detail page."""
    import polars as pl
    import streamlit as st

    from mvp.analysis.dashboard.health_data import load_all_runs

    root = Path(data_root)
    runs = load_all_runs(root)

    if not runs:
        st.warning("No pipeline run data found.")
        return

    # Run selector
    timestamps = [r["timestamp"] for r in runs]
    selected_ts = st.selectbox("Pipeline Run", timestamps, index=0)
    run = next(r for r in runs if r["timestamp"] == selected_ts)

    # --- Tournaments ---
    st.subheader("Tournaments")
    summary, failures = format_tournaments(run)
    st.write(summary)
    if failures:
        st.dataframe(
            pl.DataFrame(failures).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )

    # --- Per-Book Odds ---
    st.subheader("Per-Book Odds")
    books_rows = format_books_fetched(run)
    if books_rows:
        st.dataframe(
            pl.DataFrame(books_rows).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )

    # --- Unresolved Names ---
    st.subheader("Unresolved Names")
    name_rows = format_unresolved_names(run)
    if name_rows:
        st.dataframe(
            pl.DataFrame(name_rows).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.write("No unresolved names.")

    # --- Predictions Without Odds ---
    st.subheader("Predictions Without Odds")
    pred_summary, pred_items = format_predictions_without_odds(run)
    st.write(pred_summary)
    if pred_items:
        st.dataframe(
            pl.DataFrame(pred_items).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )

    # --- Sheets Sync ---
    st.subheader("Sheets Sync")
    sheets = run.get("sheets_sync", {})
    if sheets.get("success"):
        st.write(f"Success — {sheets.get('count', 0)} new matches synced")
    else:
        error = sheets.get("error", "unknown")
        st.error(f"Failed: {error}")

    # --- Prediction Count ---
    st.subheader("Predictions")
    st.write(f"{run.get('predictions_total', 0)} winner predictions generated")

    # --- Pipeline Errors ---
    st.subheader("Pipeline Errors")
    errors = run.get("errors", [])
    if errors:
        for err in errors:
            st.error(err)
    else:
        st.success("No errors.")
