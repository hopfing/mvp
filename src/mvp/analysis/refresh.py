"""Refresh analysis parquets (analysis, simulations, insights).

Extracted from cmd_analysis so the same logic can be called from both
the standalone ``mvp analysis`` command and as a post-step in ``mvp live``.
"""

from __future__ import annotations

import importlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from mvp.cli import BookConfig

logger = logging.getLogger(__name__)


def refresh_analysis_data(
    data_root: Path,
    book_registry: list[BookConfig],
) -> bool:
    """Build analysis, simulations, and insights parquets.

    This is the data-building core of ``mvp analysis --no-ui``.  It resolves
    odds snapshots, computes per-book and cross-book summaries, builds the
    unified analysis dataset, runs simulations, and runs the insight scanner.

    Args:
        data_root: Root data directory (e.g. ``B:/``).
        book_registry: List of BookConfig entries for sportsbook integrations.

    Returns:
        True if analysis data was built successfully, False if predictions
        are missing (not an error — just nothing to analyze yet).
    """
    from mvp.analysis.dataset import build_analysis_dataset
    from mvp.analysis.event_map import load_event_map_with_overrides
    from mvp.analysis.scanner import run_scanner
    from mvp.analysis.simulations import run_simulations
    from mvp.odds.aggregator import (
        THRESHOLD_HOURS,
        compute_book_odds,
        compute_cross_book_odds,
        compute_first_live_anchor,
        compute_open_close_odds,
        compute_opening_odds,
        compute_threshold_odds_all,
        save_book_odds,
        save_cross_book_odds,
    )

    # Layer 1: Resolve snapshots through event map
    print("Loading event map...")
    event_map = load_event_map_with_overrides()
    print(f"Event map: {len(event_map)} mappings")

    print("Resolving per-book snapshots...")
    snap_list: list[tuple[str, pl.DataFrame]] = []
    for book in book_registry:
        mod = importlib.import_module(f"mvp.{book.domain}.transformer")
        snaps = mod.transform(event_map)
        snap_list.append((book.code, snaps))

    all_snapshots = pl.concat(
        [s for _, s in snap_list if len(s) > 0],
        how="diagonal_relaxed",
    ) if any(len(s) > 0 for _, s in snap_list) else pl.DataFrame()

    if len(all_snapshots) > 0:
        snap_path = data_root / "stage" / "odds" / "snapshots.parquet"
        snap_path.parent.mkdir(parents=True, exist_ok=True)
        all_snapshots.write_parquet(snap_path)

    if len(all_snapshots) > 0:
        n_matches_snap = all_snapshots["match_uid"].n_unique()
    else:
        n_matches_snap = 0
    print(f"Resolved snapshots: {len(all_snapshots)} rows, {n_matches_snap} matches")

    # Layer 2: Book-level summaries
    print("Computing per-book odds summaries...")
    book_odds_list = []
    all_book_odds = []

    for book_code, snaps in snap_list:
        if len(snaps) > 0:
            book_df = compute_book_odds(snaps, book_code)
            if len(book_df) > 0:
                save_book_odds(book_df, book_code)
                book_odds_list.append(book_df)
                all_book_odds.append(book_df)
                print(f"  {book_code.upper()}: {len(book_df)} matches")

    # Layer 2: Cross-book summary
    print("Computing cross-book odds summary...")
    cross_book = compute_cross_book_odds(book_odds_list)
    # best_opening_odds / best_closing_odds are computed time-aligned from raw
    # snapshots (fixes the per-book first/last time skew) and joined in, so every
    # consumer reads accurate open/close under the existing column names.
    if len(cross_book) > 0 and len(all_snapshots) > 0:
        open_close = compute_open_close_odds(all_snapshots)
        cross_book = cross_book.join(
            open_close, on=["match_uid", "player_id"], how="left"
        )
    if len(cross_book) > 0:
        save_cross_book_odds(cross_book)
    print(f"Cross-book odds: {len(cross_book)} matches")

    # Opening odds from raw snapshots
    opening_odds = (
        compute_opening_odds(all_snapshots)
        if len(all_snapshots) > 0 else None
    )

    # Per-threshold odds anchored on first_live_fetched_at (same UTC clock
    # as fetched_at; sidesteps the scheduled_datetime tz mismatch — issue #86).
    threshold_odds = None
    if len(all_snapshots) > 0:
        anchors = compute_first_live_anchor(all_snapshots)
        if len(anchors) > 0:
            threshold_odds = compute_threshold_odds_all(
                snapshots=all_snapshots,
                match_anchors=anchors,
                thresholds_hours=list(THRESHOLD_HOURS),
                books=[b.code for b in book_registry],
            )
            print(
                f"Threshold odds: {len(threshold_odds)} rows across "
                f"{len(THRESHOLD_HOURS)} thresholds"
            )

    # Concat per-book odds for the per-book wide columns
    odds_by_book = (
        pl.concat(all_book_odds, how="diagonal_relaxed") if all_book_odds else None
    )

    # Load predictions
    preds_path = data_root / "predictions" / "predictions.parquet"
    if not preds_path.exists():
        print("No predictions found. Run the live pipeline first.")
        return False
    predictions = pl.read_parquet(preds_path)
    print(f"Predictions: {len(predictions)}")

    # Load match aggregate — source of truth for per-match metadata AND results
    results_df = None
    match_meta = None
    matches_path = data_root / "aggregate" / "atptour" / "matches.parquet"
    if matches_path.exists():
        matches = pl.read_parquet(matches_path)

        # Per-match metadata (matches.parquet is player-level, so dedupe on match_uid)
        META_COLS = [
            "match_uid", "tournament_id", "tournament_name",
            "circuit", "surface", "round",
            "effective_match_date", "scheduled_datetime", "match_date",
        ]
        available_meta = [c for c in META_COLS if c in matches.columns]
        match_meta = matches.select(available_meta).unique(subset=["match_uid"])

        if "won" in matches.columns:
            won = matches.filter(pl.col("won")).select(
                "match_uid",
                pl.col("player_id").alias("winner_id"),
            )
            if len(won) > 0:
                pred_uids = set(predictions["match_uid"].to_list())
                won_relevant = won.filter(pl.col("match_uid").is_in(list(pred_uids)))
                if len(won_relevant) > 0:
                    pred_p1 = predictions.select("match_uid", "p1_id").unique(
                        subset=["match_uid"]
                    )
                    results_df = (
                        won_relevant.join(pred_p1, on="match_uid")
                        .with_columns(
                            pl.when(pl.col("winner_id") == pl.col("p1_id"))
                            .then(pl.lit("P1"))
                            .otherwise(pl.lit("P2"))
                            .alias("result")
                        )
                        .select("match_uid", "result")
                    )

    # Load sheet data
    sheets_path = data_root / "sheets" / "bets.parquet"
    sheet_data = pl.read_parquet(sheets_path) if sheets_path.exists() else None

    # Layer 4: Build analysis dataset
    print("Building analysis dataset...")
    ds = build_analysis_dataset(
        predictions=predictions,
        match_meta=match_meta,
        results=results_df,
        sheet_data=sheet_data,
        odds_by_book=odds_by_book,
        cross_book_odds=cross_book if len(cross_book) > 0 else None,
        all_snapshots=all_snapshots if len(all_snapshots) > 0 else None,
        opening_odds=opening_odds,
        threshold_odds=threshold_odds,
    )

    analysis_path = data_root / "analysis" / "analysis.parquet"
    analysis_path.parent.mkdir(parents=True, exist_ok=True)
    ds.write_parquet(analysis_path)
    print(f"Analysis dataset: {len(ds)} rows, {len(ds.columns)} columns")

    # Layer 5: Simulations
    print("Running simulations...")
    sims = run_simulations(ds)
    sims_path = data_root / "analysis" / "simulations.parquet"
    sims.write_parquet(sims_path)
    print(f"Simulations: {len(sims)} scenario × segment rows")

    # Layer 6: Insight scanner
    print("Running insight scanner...")
    insights = run_scanner(ds)
    insights_path = data_root / "analysis" / "insights.parquet"
    insights.write_parquet(insights_path)
    print(f"Insights: {len(insights)} slices")

    return True
