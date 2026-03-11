"""CLI analysis summary report."""

import polars as pl


def format_summary(ds: pl.DataFrame, days: int = 7) -> str:
    """Format a brief analysis summary for CLI output.

    Args:
        ds: Unified analysis dataset.
        days: Lookback window for summary (default 7).

    Returns:
        Formatted string for terminal output.
    """
    if len(ds) == 0:
        return "--- ANALYSIS ---\nNo predictions to analyze."

    total = len(ds)
    resolved = ds.filter(pl.col("status") == "resolved") if "status" in ds.columns else ds
    pending = total - len(resolved)

    lines = [
        "--- ANALYSIS ---",
        f"Predictions: {total} ({len(resolved)} resolved, {pending} pending)",
    ]

    if len(resolved) > 0 and "model_correct" in resolved.columns:
        correct = resolved["model_correct"].sum()
        acc = correct / len(resolved) if len(resolved) > 0 else 0
        lines.append(f"Model accuracy: {acc:.1%}")

    if "net" in ds.columns and "bet_side" in ds.columns:
        bets = ds.filter(pl.col("bet_side").is_not_null() & (pl.col("bet_side") != ""))
        if len(bets) > 0:
            net_values = bets["net"].cast(pl.Float64, strict=False).drop_nulls()
            if len(net_values) > 0:
                total_pnl = net_values.sum()
                sign = "+" if total_pnl >= 0 else ""
                lines.append(f"P&L: {sign}${total_pnl:.2f} ({len(bets)} bets)")

    return "\n".join(lines)
