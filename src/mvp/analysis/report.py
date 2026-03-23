"""CLI analysis summary report."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def format_summary(ds: pl.DataFrame, days: int = 7) -> str:
    """Format a brief analysis summary for CLI output (used by live pipeline).

    Args:
        ds: Unified analysis dataset.
        days: Lookback window for summary (default 7).

    Returns:
        Formatted string for terminal output.
    """
    if len(ds) == 0:
        return "--- ANALYSIS ---\nNo predictions to analyze."

    total = len(ds)
    if "status" in ds.columns:
        resolved = ds.filter(pl.col("status") == "resolved")
    else:
        resolved = ds
    pending = total - len(resolved)

    lines = [
        "--- ANALYSIS ---",
        f"Predictions: {total} ({len(resolved)} resolved, {pending} pending)",
    ]

    if len(resolved) > 0 and "model_correct" in resolved.columns:
        correct = resolved["model_correct"].sum()
        acc = correct / len(resolved) if len(resolved) > 0 else 0
        lines.append(f"Model accuracy: {acc:.1%}")

    if "net" in ds.columns and "stake" in ds.columns:
        bets = ds.filter(
            pl.col("stake").is_not_null() & (pl.col("stake") != "")
        )
        if len(bets) > 0:
            net_vals = bets["net"].cast(pl.Float64, strict=False).drop_nulls()
            if len(net_vals) > 0:
                pnl = net_vals.sum()
                sign = "+" if pnl >= 0 else ""
                settled = len(net_vals)
                lines.append(
                    f"P&L: {sign}${pnl:.2f}"
                    f" ({settled} settled, {len(bets)} total bets)"
                )

    return "\n".join(lines)


def format_analysis_summary(
    ds: pl.DataFrame, sims: pl.DataFrame
) -> str:
    """Format extended analysis summary for the analysis CLI command.

    Args:
        ds: Unified analysis dataset with all computed columns.
        sims: Simulation results DataFrame.

    Returns:
        Formatted string for terminal output.
    """
    lines = [
        "\n" + "=" * 70,
        "MODEL PERFORMANCE × ODDS ANALYSIS".center(70),
        "=" * 70,
    ]

    total = len(ds)
    if "status" in ds.columns:
        resolved = ds.filter(pl.col("status") == "resolved")
    else:
        resolved = ds
    lines.append(
        f"\nDataset: {total} predictions, {len(resolved)} resolved"
    )

    if len(resolved) > 0 and "model_correct" in resolved.columns:
        correct = resolved["model_correct"].sum()
        acc = correct / len(resolved) if len(resolved) > 0 else 0
        lines.append(f"Model accuracy: {acc:.1%}")

    if "pred_odds_best_close" in ds.columns:
        n_odds = ds.filter(
            pl.col("pred_odds_best_close").is_not_null()
        ).height
        lines.append(
            f"Odds coverage: {n_odds}/{total} have closing odds"
        )

    if "clv_vs_avg" in ds.columns:
        clv = ds["clv_vs_avg"].drop_nulls()
        if len(clv) > 0:
            m, md = clv.mean(), clv.median()
            lines.append(
                f"\nCLV vs avg close: mean={m:.2%},"
                f" median={md:.2%} ({len(clv)} bets)"
            )

    _clv_by_consensus(ds, lines)
    _clv_by_book(ds, lines)
    _execution_quality(ds, lines)
    _clv_by_timing(ds, lines)

    if "net" in ds.columns and "stake" in ds.columns:
        bets = ds.filter(
            pl.col("stake").is_not_null() & (pl.col("stake") != "")
        )
        if len(bets) > 0:
            net_vals = bets["net"].cast(
                pl.Float64, strict=False
            ).drop_nulls()
            if len(net_vals) > 0:
                pnl = net_vals.sum()
                sign = "+" if pnl >= 0 else ""
                lines.append(
                    f"Actual P&L: {sign}${pnl:.2f}"
                    f" ({len(net_vals)} settled)"
                )

    if len(sims) > 0:
        from mvp.analysis.simulations import EDGE_BANDS, SCENARIOS

        scenario_order = [s["name"] for s in SCENARIOS]
        edge_band_names = [b["name"] for b in EDGE_BANDS]

        # Show only the current (most recent) model version
        versions = (
            sims["model_version"].unique().sort(descending=True)
            .to_list()
        )
        if "all" in versions:
            versions.remove("all")
        current = versions[0] if versions else "all"

        for version in [current]:
            v_sims = sims.filter(
                pl.col("model_version") == version
            )
            label = version if version != "all" else "ALL VERSIONS"

            overall = v_sims.filter(
                pl.col("segment") == "overall"
            )

            # Non-edge scenarios (consensus + flat)
            edge_suffixes = ("_open", "_mkt_formed")
            non_edge = [
                s for s in scenario_order
                if s not in edge_band_names
                and not any(s.endswith(sx) for sx in edge_suffixes)
            ]
            lines.append(f"\n{'SIMULATIONS — ' + label:^70}")

            # Edge bands: open / formed / close side by side
            _edge_band_table(lines, overall, edge_band_names)

            _sim_header(lines)
            _sim_rows(lines, overall, non_edge)

            # Consensus cross-cut
            consensus = v_sims.filter(
                pl.col("segment") == "consensus"
            )
            if len(consensus) > 0:
                lines.append(
                    f"\n  {'consensus cross-cut':^66}"
                )
                vals = (
                    consensus["segment_value"]
                    .unique().sort(descending=True).to_list()
                )
                for cv in vals:
                    subset = consensus.filter(
                        pl.col("segment_value") == cv
                    )
                    lines.append(f"\n  consensus = {cv}")
                    _edge_band_table(lines, subset, edge_band_names)
                    _sim_header(lines)
                    _sim_rows(lines, subset, non_edge)

    lines.append("=" * 70)
    return "\n".join(lines)


def _sim_header(lines: list[str]) -> None:
    lines.append("-" * 70)
    lines.append(
        f"{'Scenario':<25} {'N':>6} {'Acc':>7}"
        f" {'ROI':>8} {'P&L':>10}"
    )
    lines.append("-" * 70)


def _edge_band_table(
    lines: list[str],
    sims_overall: pl.DataFrame,
    band_names: list[str],
) -> None:
    """Render edge bands with 1st avail / formed / close side by side."""
    from mvp.analysis.simulations import EDGE_BASES

    # Build {band_name: row} for each basis
    basis_maps: list[tuple[str, dict]] = []
    for basis_name, _ in EDGE_BASES:
        suffix = "" if basis_name == "close" else f"_{basis_name}"
        mapping = {}
        for r in sims_overall.iter_rows(named=True):
            scenario = r["scenario"]
            if suffix and scenario.endswith(suffix):
                mapping[scenario.removesuffix(suffix)] = r
            elif not suffix and scenario in band_names:
                mapping[scenario] = r
        basis_maps.append((basis_name, mapping))

    if not any(m for _, m in basis_maps):
        return

    def _grp(row: dict | None) -> str:
        if row:
            pnl = row["net_pnl"]
            return (
                f" {row['n_bets']:>4}"
                f" {row['accuracy']:>6.1%}"
                f" {row['roi']:>+7.1%}"
                f" {pnl:>+7.1f}"
            )
        return f" {'—':>4} {'—':>6} {'—':>7} {'—':>7}"

    labels = {
        "open": "open",
        "mkt_formed": "formed",
        "close": "close",
    }
    sep = " | "
    grp_width = 27  # " NNNN Acc..% +ROI..% +PnL.."

    # Header: basis label on top, column names below
    lines.append("")
    lbl_parts = [f"{labels.get(n, n):^{grp_width}}" for n, _ in basis_maps]
    lines.append(f"{'':12}" + sep.join(lbl_parts))
    col_parts = [
        f" {'N':>4} {'Acc':>6} {'ROI':>7} {'P&L':>7}" for _ in basis_maps
    ]
    lines.append(f"{'Edge Band':<12}" + sep.join(col_parts))
    row_width = 12 + len(sep.join(col_parts))
    lines.append("-" * row_width)

    for name in band_names:
        parts = [_grp(m.get(name)) for _, m in basis_maps]
        lines.append(f"{name:<12}" + sep.join(parts))


def _sim_rows(
    lines: list[str],
    df: pl.DataFrame,
    scenario_order: list[str],
) -> None:
    by_name = {
        r["scenario"]: r for r in df.iter_rows(named=True)
    }
    for name in scenario_order:
        if name not in by_name:
            continue
        row = by_name[name]
        n = row["n_bets"]
        acc = row["accuracy"]
        roi = row["roi"]
        pnl = row["net_pnl"]
        sign = "+" if pnl >= 0 else ""
        lines.append(
            f"{name:<25} {n:>6} {acc:>6.1%}"
            f" {roi:>7.1%} {sign}{pnl:>9.1f}"
        )


def _get_clv_bets(ds: pl.DataFrame) -> pl.DataFrame | None:
    """Filter to bet rows with CLV data."""
    if "clv_vs_avg" not in ds.columns or "bet_side" not in ds.columns:
        return None
    bets = ds.filter(
        pl.col("bet_side").is_in(["P1", "P2"])
        & pl.col("clv_vs_avg").is_not_null()
    )
    return bets if len(bets) > 0 else None


def _clv_table_header(lines: list[str], title: str) -> None:
    lines.append(f"\n  {title}")
    lines.append("  " + "-" * 50)
    lines.append(
        f"  {'':16s} {'N':>5}  {'Mean CLV':>9}  {'Med CLV':>9}"
    )
    lines.append("  " + "-" * 50)


def _clv_table_row(lines: list[str], label: str, clv: pl.Series) -> None:
    n = len(clv)
    if n == 0:
        return
    m, md = clv.mean(), clv.median()
    lines.append(
        f"  {label:16s} {n:>5}  {m:>+8.2%}  {md:>+8.2%}"
    )


def _clv_by_consensus(ds: pl.DataFrame, lines: list[str]) -> None:
    bets = _get_clv_bets(ds)
    if bets is None or "consensus" not in ds.columns:
        return

    _clv_table_header(lines, "CLV BY CONSENSUS")

    has_cons = bets.filter(pl.col("consensus").is_not_null())
    no_cons = bets.filter(pl.col("consensus").is_null())

    for val in sorted(
        has_cons["consensus"].unique().drop_nulls().to_list(), reverse=True
    ):
        subset = has_cons.filter(pl.col("consensus") == val)
        label = f"{val:.0%} consensus"
        _clv_table_row(lines, label, subset["clv_vs_avg"])

    if len(no_cons) > 0:
        _clv_table_row(lines, "(no consensus)", no_cons["clv_vs_avg"])


def _clv_by_book(ds: pl.DataFrame, lines: list[str]) -> None:
    bets = _get_clv_bets(ds)
    if bets is None or "book" not in ds.columns:
        return

    bets_with_book = bets.filter(
        pl.col("book").is_not_null() & (pl.col("book") != "")
    )
    if len(bets_with_book) == 0:
        return

    _clv_table_header(lines, "CLV BY BOOK")

    for book in sorted(bets_with_book["book"].unique().to_list()):
        subset = bets_with_book.filter(pl.col("book") == book)
        _clv_table_row(lines, book, subset["clv_vs_avg"])


def _execution_quality(ds: pl.DataFrame, lines: list[str]) -> None:
    bets = _get_clv_bets(ds)
    if bets is None or "bet_odds" not in ds.columns:
        return

    bet_odds = bets["bet_odds"].cast(pl.Float64, strict=False)

    # Need closing odds on the bet side
    close_col = (
        "bet_closing_avg" if "bet_closing_avg" in bets.columns else None
    )
    if close_col is None:
        return

    df = bets.with_columns(bet_odds.alias("_bet_odds_f")).filter(
        pl.col("_bet_odds_f").is_not_null()
        & pl.col(close_col).is_not_null()
    )
    if len(df) == 0:
        return

    avg_bet = df["_bet_odds_f"].mean()
    avg_close = df[close_col].mean()

    lines.append(f"\n  EXECUTION QUALITY ({len(df)} bets)")
    lines.append("  " + "-" * 50)
    lines.append(f"  Avg bet odds:        {avg_bet:.3f}")
    lines.append(f"  Avg closing odds:    {avg_close:.3f}")

    has_market = (
        "market_avg_at_bet" in df.columns
        and df["market_avg_at_bet"].drop_nulls().len() > 0
    )
    if has_market:
        df_mkt = df.filter(pl.col("market_avg_at_bet").is_not_null())
        avg_market = df_mkt["market_avg_at_bet"].mean()
        avg_bet_mkt = df_mkt["_bet_odds_f"].mean()
        avg_close_mkt = df_mkt[close_col].mean()
        edge_at_bet = (avg_bet_mkt - avg_market) / avg_market
        clv_mkt = (avg_bet_mkt - avg_close_mkt) / avg_close_mkt
        lines.append(f"  Avg market at bet:   {avg_market:.3f}")
        lines.append(f"  Edge at bet time:    {edge_at_bet:+.2%}  (bet vs market when placed)")
        lines.append(f"  Edge vs close (CLV): {clv_mkt:+.2%}  (bet vs closing line)")
        if abs(clv_mkt) > 0.0001:
            captured = edge_at_bet / clv_mkt
            lines.append(f"  Captured:            {captured:.0%}  (edge at bet / CLV)")


_BET_PLACED_AT_RELIABLE_AFTER = "2026-03-21 09:15"


def _clv_by_timing(ds: pl.DataFrame, lines: list[str]) -> None:
    bets = _get_clv_bets(ds)
    if bets is None or "bet_placed_at" not in ds.columns:
        return

    # bet_placed_at only reliable after tracking was added
    bets = bets.filter(
        pl.col("bet_placed_at").cast(pl.Utf8) > _BET_PLACED_AT_RELIABLE_AFTER
    )
    if len(bets) == 0:
        return

    # Find last snapshot timestamp across books for each match
    close_ts_cols = [
        c for c in ds.columns if c.endswith("_closing_fetched_at")
    ]
    if not close_ts_cols:
        return

    # Compute max closing timestamp per row
    df = bets.with_columns(
        pl.max_horizontal(*[pl.col(c) for c in close_ts_cols])
        .alias("_last_snapshot")
    )

    # Parse bet_placed_at to datetime
    df = df.with_columns(
        pl.col("bet_placed_at")
        .map_elements(_parse_bet_placed_at, return_dtype=pl.Datetime("us", "UTC"))
        .alias("_bet_ts")
    )

    df = df.filter(
        pl.col("_last_snapshot").is_not_null()
        & pl.col("_bet_ts").is_not_null()
    )
    if len(df) == 0:
        return

    # Hours before last snapshot (positive = bet placed before close)
    df = df.with_columns(
        (
            (pl.col("_last_snapshot").cast(pl.Int64) - pl.col("_bet_ts").cast(pl.Int64))
            / 3_600_000_000  # microseconds to hours
        ).alias("_hours_before")
    )

    buckets = [
        ("<1h", 0, 1),
        ("1-3h", 1, 3),
        ("3-6h", 3, 6),
        ("6-12h", 6, 12),
        ("12h+", 12, None),
    ]

    _clv_table_header(lines, "CLV BY HOURS BEFORE CLOSE")

    for label, lo, hi in buckets:
        if hi is not None:
            subset = df.filter(
                (pl.col("_hours_before") >= lo) & (pl.col("_hours_before") < hi)
            )
        else:
            subset = df.filter(pl.col("_hours_before") >= lo)
        if len(subset) > 0:
            _clv_table_row(lines, label, subset["clv_vs_avg"])

    # Bets placed after last snapshot
    after = df.filter(pl.col("_hours_before") < 0)
    if len(after) > 0:
        _clv_table_row(lines, "(after last snap)", after["clv_vs_avg"])


def _parse_bet_placed_at(val: str | None) -> datetime | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None
