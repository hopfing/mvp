"""Pure aggregation views over a backtest CSV.

Functions here compute structured stats from filtered slices of the per-row
backtest CSV. They contain no rendering logic — both `backtest.py`'s summary
writer and `report.py`'s Section D consume them.

Conventions:
- All functions take a polars DataFrame (the slice to aggregate).
- They return dicts (single slice) or lists of (label, dict) records.
- Per-side stats (open / close) are computed on the subset where that side's
  pnl is non-null — ROIs and unit totals reflect "rows that could actually
  be priced and settled" rather than the full slice. `n` itself is the
  unfiltered slice size; `n_open` / `n_close` are the priced subset sizes.
"""

from __future__ import annotations

from typing import Any

import polars as pl

TIER_ORDER: tuple[str, ...] = ("Optimal", "Border", "Risky", "Danger")

EDGE_BANDS: tuple[tuple[str, float, float | None], ...] = (
    (">=10%", 0.10, None),
    ("5-10%", 0.05, 0.10),
    ("2-5%", 0.02, 0.05),
    ("0-2%", 0.0, 0.02),
)


def filter_model_side(df: pl.DataFrame) -> pl.DataFrame:
    """Restrict to model-favored side (model_prob > 0.5)."""
    if "model_prob" in df.columns:
        return df.filter(pl.col("model_prob") > 0.5)
    return df


def filter_picks(df: pl.DataFrame) -> pl.DataFrame:
    """Model-side rows with strictly positive opening edge — the standard
    'what we would bet' filter used by both summary entry points.
    """
    out = filter_model_side(df)
    if "opening_edge" in out.columns:
        out = out.filter(pl.col("opening_edge") > 0)
    return out


def filter_band(
    df: pl.DataFrame, col: str, lo: float, hi: float | None
) -> pl.DataFrame:
    """Filter `df[col]` into [lo, hi). hi=None means upper-open."""
    if hi is None:
        return df.filter(pl.col(col) >= lo)
    return df.filter((pl.col(col) >= lo) & (pl.col(col) < hi))


def slice_stats(df: pl.DataFrame) -> dict[str, Any]:
    """Compute betting stats for one filtered slice.

    Returns a dict with these keys (None for fields that can't be computed
    from the available columns):
      n, hit,
      n_open, pnl_open, roi_open,
      n_formed, pnl_formed, roi_formed,
      n_close, pnl_close, roi_close,
      clv_pos, avg_clv,
      me_open_pos, avg_me_open,
      me_close_pos, avg_me_close
    """
    n = len(df)
    out: dict[str, Any] = {
        "n": n,
        "hit": None,
        "n_open": 0,
        "pnl_open": None,
        "roi_open": None,
        "n_formed": 0,
        "pnl_formed": None,
        "roi_formed": None,
        "n_close": 0,
        "pnl_close": None,
        "roi_close": None,
        "clv_pos": None,
        "avg_clv": None,
        "me_open_pos": None,
        "avg_me_open": None,
        "me_close_pos": None,
        "avg_me_close": None,
    }
    if n == 0:
        return out

    if "won" in df.columns:
        won = df["won"].drop_nulls()
        out["hit"] = float(won.mean()) if len(won) else None

    for price in ("open", "formed", "close"):
        pnl_col = f"pnl_{price}"
        if pnl_col not in df.columns:
            continue
        priced = df.filter(pl.col(pnl_col).is_not_null())
        n_p = len(priced)
        out[f"n_{price}"] = n_p
        if n_p == 0:
            continue
        pnl = float(priced[pnl_col].sum())
        out[f"pnl_{price}"] = pnl
        out[f"roi_{price}"] = pnl / n_p

    if "clv" in df.columns:
        clv = df["clv"].drop_nulls()
        if len(clv):
            out["clv_pos"] = float((clv > 0).mean())
            out["avg_clv"] = float(clv.mean())

    if "opening_edge" in df.columns:
        oe = df["opening_edge"].drop_nulls()
        if len(oe):
            out["me_open_pos"] = float((oe > 0).mean())
            out["avg_me_open"] = float(oe.mean())

    if "closing_edge" in df.columns:
        ce = df["closing_edge"].drop_nulls()
        if len(ce):
            out["me_close_pos"] = float((ce > 0).mean())
            out["avg_me_close"] = float(ce.mean())

    return out


def by_tier(
    df: pl.DataFrame,
    tier_order: tuple[str, ...] = TIER_ORDER,
) -> list[tuple[str, dict[str, Any]]]:
    """Group by `cal_tier` in canonical order, then any extra tiers present
    (e.g. UnderC for underconfident-cell picks), then an ALL row.
    """
    if "cal_tier" not in df.columns:
        return [("ALL", slice_stats(df))]
    rows: list[tuple[str, dict[str, Any]]] = []
    for tier in tier_order:
        rows.append((tier, slice_stats(df.filter(pl.col("cal_tier") == tier))))
    seen = set(tier_order)
    extras = (
        df.filter(~pl.col("cal_tier").is_in(list(seen)))
        .filter(pl.col("cal_tier").is_not_null())
        .get_column("cal_tier")
        .unique()
        .to_list()
    )
    for tier in extras:
        if tier is None:
            continue
        rows.append(
            (str(tier), slice_stats(df.filter(pl.col("cal_tier") == tier)))
        )
    rows.append(("ALL", slice_stats(df)))
    return rows


def by_edge_band(
    df: pl.DataFrame,
    edge_col: str,
    bands: tuple[tuple[str, float, float | None], ...] = EDGE_BANDS,
) -> list[tuple[str, dict[str, Any]]]:
    """Group by edge band on `edge_col` plus an ALL row."""
    rows: list[tuple[str, dict[str, Any]]] = []
    for label, lo, hi in bands:
        rows.append((label, slice_stats(filter_band(df, edge_col, lo, hi))))
    rows.append(("ALL", slice_stats(df)))
    return rows


def by_consensus(df: pl.DataFrame) -> list[tuple[str, dict[str, Any]]]:
    """Group by ensemble consensus (`n_agree` from per-sub picks).

    Labels are ``"{n_agree}-{n_disagree}"`` to match the diagnostic table
    convention (e.g. "5-0", "4-1", "3-2"). Buckets in descending
    consensus order plus an ALL row. Returns empty list if `n_agree`
    isn't present (non-ensemble backtest).
    """
    if "n_agree" not in df.columns:
        return []
    n_agree_col = df["n_agree"].drop_nulls()
    if len(n_agree_col) == 0:
        return []
    n_subs = int(n_agree_col.max())
    rows: list[tuple[str, dict[str, Any]]] = []
    for n_agree in range(n_subs, 0, -1):
        n_disagree = n_subs - n_agree
        bucket = df.filter(pl.col("n_agree") == n_agree)
        stats = slice_stats(bucket)
        if stats["n"] == 0:
            continue
        rows.append((f"{n_agree}-{n_disagree}", stats))
    rows.append(("ALL", slice_stats(df)))
    return rows


def month_slices(df: pl.DataFrame) -> list[tuple[str, pl.DataFrame]]:
    """Group by YYYY-MM, returning the (label, sub-DataFrame) for each month in
    chronological order. Callers that need per-bet-point gating (each of
    open/formed/close on its own edge) operate on these raw slices; `by_month`
    is the pre-aggregated wrapper for callers that score one slice per row.
    """
    if "effective_match_date" not in df.columns:
        return []
    tagged = df.with_columns(
        pl.col("effective_match_date").cast(pl.Utf8).str.slice(0, 7).alias("_month")
    ).filter(
        pl.col("_month").is_not_null() & (pl.col("_month").str.len_chars() == 7)
    )
    months = sorted(m for m in tagged["_month"].unique().to_list() if m is not None)
    out: list[tuple[str, pl.DataFrame]] = []
    for m in months:
        sub = tagged.filter(pl.col("_month") == m)
        if len(sub) > 0:
            out.append((m, sub))
    return out


def by_month(df: pl.DataFrame) -> list[tuple[str, dict[str, Any]]]:
    """Group by YYYY-MM, attaching `cum_open` / `cum_formed` / `cum_close`
    running totals to each record's stats dict (in addition to per-month stats).
    """
    rows: list[tuple[str, dict[str, Any]]] = []
    cum_open = 0.0
    cum_formed = 0.0
    cum_close = 0.0
    for m, sub in month_slices(df):
        stats = slice_stats(sub)
        cum_open += float(stats.get("pnl_open") or 0.0)
        cum_formed += float(stats.get("pnl_formed") or 0.0)
        cum_close += float(stats.get("pnl_close") or 0.0)
        stats["cum_open"] = cum_open
        stats["cum_formed"] = cum_formed
        stats["cum_close"] = cum_close
        rows.append((m, stats))
    return rows


# Canonical round display order. Mirrors mvp.model.diagnostics.ROUND_ORDER; kept
# local so this pure-aggregation module doesn't import the sklearn-heavy
# diagnostics just for a list of strings (the list is already duplicated across
# report.py / diagnostics.py / confidence by convention).
_ROUND_ORDER = ["Q1", "Q2", "Q3", "RR", "R128", "R64", "R32", "R16", "QF", "SF", "F"]


def round_slices(df: pl.DataFrame) -> list[tuple[str, pl.DataFrame]]:
    """Group by tournament round, returning (label, sub-DataFrame) ordered
    Q1..F via `_ROUND_ORDER` with any unrecognized rounds appended last (name
    order). Returns empty if `round` isn't present. Callers needing per-bet-point
    gating operate on these raw slices; `by_round` is the aggregated wrapper.
    """
    if "round" not in df.columns:
        return []
    order = {r: i for i, r in enumerate(_ROUND_ORDER)}
    present = sorted(
        (r for r in df["round"].unique().to_list() if r is not None),
        key=lambda r: (order.get(r, len(_ROUND_ORDER)), r),
    )
    out: list[tuple[str, pl.DataFrame]] = []
    for r in present:
        sub = df.filter(pl.col("round") == r)
        if len(sub) > 0:
            out.append((r, sub))
    return out


def by_round(df: pl.DataFrame) -> list[tuple[str, dict[str, Any]]]:
    """Group by tournament round, ordered Q1..F via `_ROUND_ORDER` with any
    unrecognized rounds appended last (name order). Returns empty if `round`
    isn't present."""
    return [(r, slice_stats(sub)) for r, sub in round_slices(df)]
