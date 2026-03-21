"""CLI analysis summary report."""

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
        from mvp.analysis.simulations import SCENARIOS

        scenario_order = [s["name"] for s in SCENARIOS]

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
            lines.append(f"\n{'SIMULATIONS — ' + label:^70}")
            _sim_header(lines)

            overall = v_sims.filter(
                pl.col("segment") == "overall"
            )
            _sim_rows(lines, overall, scenario_order)

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
                    lines.append(f"\n  consensus = {cv}")
                    _sim_header(lines)
                    subset = consensus.filter(
                        pl.col("segment_value") == cv
                    )
                    _sim_rows(lines, subset, scenario_order)

    lines.append("=" * 70)
    return "\n".join(lines)


def _sim_header(lines: list[str]) -> None:
    lines.append("-" * 70)
    lines.append(
        f"{'Scenario':<25} {'N':>6} {'Acc':>7}"
        f" {'ROI':>8} {'P&L':>10}"
    )
    lines.append("-" * 70)


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
