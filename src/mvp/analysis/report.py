"""CLI analysis summary report."""

from __future__ import annotations

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
