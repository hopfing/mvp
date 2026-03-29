"""Flat-bet simulation engine for model performance × odds analysis."""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)

_BEST_CLOSE = "pred_odds_best_close"

EDGE_BANDS = [
    {"name": "edge_10pct", "conditions": [(">=", 0.10)]},
    {"name": "edge_9pct", "conditions": [(">=", 0.09), ("<", 0.10)]},
    {"name": "edge_8pct", "conditions": [(">=", 0.08), ("<", 0.09)]},
    {"name": "edge_7pct", "conditions": [(">=", 0.07), ("<", 0.08)]},
    {"name": "edge_6pct", "conditions": [(">=", 0.06), ("<", 0.07)]},
    {"name": "edge_5pct", "conditions": [(">=", 0.05), ("<", 0.06)]},
    {"name": "edge_4pct", "conditions": [(">=", 0.04), ("<", 0.05)]},
    {"name": "edge_3pct", "conditions": [(">=", 0.03), ("<", 0.04)]},
    {"name": "edge_2pct", "conditions": [(">=", 0.02), ("<", 0.03)]},
    {"name": "edge_1pct", "conditions": [(">=", 0.01), ("<", 0.02)]},
    {"name": "edge_0pct", "conditions": [(">", 0), ("<", 0.01)]},
    {"name": "neg_0pct", "conditions": [("<=", 0), (">", -0.01)]},
    {"name": "neg_1pct", "conditions": [("<=", -0.01), (">", -0.02)]},
    {"name": "neg_2pct", "conditions": [("<=", -0.02), (">", -0.03)]},
    {"name": "neg_3pct", "conditions": [("<=", -0.03), (">", -0.04)]},
    {"name": "neg_4pct", "conditions": [("<=", -0.04), (">", -0.05)]},
    {"name": "neg_5pct", "conditions": [("<=", -0.05), (">", -0.06)]},
    {"name": "neg_6pct", "conditions": [("<=", -0.06), (">", -0.07)]},
    {"name": "neg_7pct", "conditions": [("<=", -0.07), (">", -0.08)]},
    {"name": "neg_8pct", "conditions": [("<=", -0.08), (">", -0.09)]},
    {"name": "neg_9pct", "conditions": [("<=", -0.09), (">", -0.10)]},
    {"name": "neg_10pct", "conditions": [("<=", -0.10)]},
]

EDGE_BASES = [
    ("open", {
        "odds_col": "pred_odds_open",
        "edge_col": "model_edge_open",
    }),
    ("mkt_formed", {
        "odds_col": "pred_odds_market_formed",
        "edge_col": "model_edge_market_formed",
    }),
    ("close", {
        "odds_col": "pred_odds_best_close",
        "edge_col": "model_edge_best_close",
    }),
]

SCENARIOS: list[dict] = [
    {"name": "consensus_100", "odds_col": _BEST_CLOSE,
     "filter": ("consensus", "==", 1.0)},
    {"name": "consensus_80", "odds_col": _BEST_CLOSE,
     "filter": ("consensus", "==", 0.8)},
    {"name": "consensus_60", "odds_col": _BEST_CLOSE,
     "filter": ("consensus", "==", 0.6)},
]

for _basis_name, _basis in EDGE_BASES:
    _suffix = "" if _basis_name == "close" else f"_{_basis_name}"
    for _band in EDGE_BANDS:
        SCENARIOS.append({
            "name": f"{_band['name']}{_suffix}",
            "odds_col": _basis["odds_col"],
            "filter": [
                (_basis["edge_col"], op, val)
                for op, val in _band["conditions"]
            ],
        })

SCENARIOS.extend([
    {"name": "flat_best_open", "odds_col": "pred_odds_best_open",
     "filter": None},
    {"name": "flat_best_close", "odds_col": _BEST_CLOSE, "filter": None},
    {"name": "flat_best_intraday", "odds_col": "pred_odds_best_intraday",
     "filter": None},
    {"name": "flat_worst_intraday", "odds_col": "pred_odds_worst_intraday",
     "filter": None},
])

SEGMENTS = [
    {"name": "overall", "column": None},
    {"name": "consensus", "column": "consensus"},
    {"name": "circuit", "column": "circuit"},
    {"name": "surface", "column": "surface"},
]

STAKE = 1.0


def _build_book_scenarios(columns: list[str]) -> list[dict]:
    """Build per-book edge band scenarios from available columns.

    Each book gets four cuts: open, close, best_intra, worst_intra.
    """
    _CUTS = ["open", "close", "best_intra", "worst_intra"]
    _CROSS_BOOK = {"model_edge_best_close", "model_edge_avg_close",
                   "model_edge_open", "model_edge_market_formed"}

    book_scenarios: list[dict] = []
    for col in sorted(columns):
        if not col.startswith("model_edge_") or col in _CROSS_BOOK:
            continue
        for cut in _CUTS:
            if col.endswith(f"_{cut}"):
                book = col.removeprefix("model_edge_").removesuffix(f"_{cut}")
                odds_col = f"pred_odds_{book}_{cut}"
                if odds_col not in columns:
                    break
                suffix = f"_{book}_{cut}"
                for band in EDGE_BANDS:
                    book_scenarios.append({
                        "name": f"{band['name']}{suffix}",
                        "odds_col": odds_col,
                        "filter": [
                            (col, op, val)
                            for op, val in band["conditions"]
                        ],
                    })
                book_scenarios.append({
                    "name": f"flat{suffix}",
                    "odds_col": odds_col,
                    "filter": None,
                })
                break
    return book_scenarios


def run_simulations(ds: pl.DataFrame) -> pl.DataFrame:
    """Run all flat-bet simulation scenarios on the analysis dataset.

    Runs simulations per model_version when available, plus an
    "all" group for the full dataset.

    Args:
        ds: Analysis dataset with pred-side odds, model_correct, and
            optional consensus/edge columns.

    Returns:
        DataFrame with one row per model_version × scenario × segment.
    """
    if "status" in ds.columns:
        resolved = ds.filter(pl.col("status") == "resolved")
    else:
        resolved = ds

    if len(resolved) == 0:
        return _empty_simulations()

    groups: list[tuple[str, pl.DataFrame]] = []

    if "model_version" in resolved.columns:
        versions = (
            resolved["model_version"].drop_nulls()
            .unique().sort().to_list()
        )
        for v in versions:
            subset = resolved.filter(
                pl.col("model_version") == v
            )
            groups.append((str(v), subset))

    groups.append(("all", resolved))

    book_scenarios = _build_book_scenarios(resolved.columns)
    all_scenarios = SCENARIOS + book_scenarios

    results = []
    for version, group_df in groups:
        results.extend(_run_scenarios(group_df, version, all_scenarios))

    if not results:
        return _empty_simulations()

    return pl.DataFrame(results)


def _run_scenarios(
    resolved: pl.DataFrame, model_version: str,
    scenarios: list[dict] | None = None,
) -> list[dict]:
    """Run all scenarios × segments for one model version slice."""
    results = []
    for scenario in (scenarios or SCENARIOS):
        odds_col = scenario["odds_col"]
        if odds_col not in resolved.columns:
            continue

        filtered = _apply_filter(resolved, scenario.get("filter"))
        if filtered is None or len(filtered) == 0:
            continue

        bettable = filtered.filter(pl.col(odds_col).is_not_null())
        if len(bettable) == 0:
            continue

        for segment in SEGMENTS:
            segment_results = _run_segment(
                bettable, scenario["name"], odds_col,
                segment, model_version,
            )
            results.extend(segment_results)

    return results


def _apply_filter(
    df: pl.DataFrame,
    filt: tuple[str, str, float] | list[tuple[str, str, float]] | None,
) -> pl.DataFrame | None:
    """Apply scenario filter to DataFrame.

    Accepts a single (col, op, val) tuple or a list of tuples (ANDed).
    """
    if filt is None:
        return df

    conditions = filt if isinstance(filt, list) else [filt]
    _ops = {">": "gt", ">=": "ge", "<": "lt", "<=": "le", "==": "eq"}

    result = df
    for col, op, val in conditions:
        if col not in result.columns:
            return None
        expr = getattr(pl.col(col), _ops[op])(val)
        result = result.filter(expr)
    return result


def _run_segment(
    df: pl.DataFrame,
    scenario_name: str,
    odds_col: str,
    segment: dict,
    model_version: str,
) -> list[dict]:
    """Run simulation for one scenario across one segment dimension."""
    seg_name = segment["name"]
    seg_col = segment["column"]

    if seg_col is None:
        return [_simulate(
            df, scenario_name, odds_col,
            seg_name, "all", model_version,
        )]

    if seg_col not in df.columns:
        return []

    results = []
    for val in df[seg_col].drop_nulls().unique().sort().to_list():
        subset = df.filter(pl.col(seg_col) == val)
        if len(subset) > 0:
            results.append(_simulate(
                subset, scenario_name, odds_col,
                seg_name, str(val), model_version,
            ))
    return results


def _simulate(
    df: pl.DataFrame,
    scenario: str,
    odds_col: str,
    segment: str,
    segment_value: str,
    model_version: str,
) -> dict:
    """Run flat-bet simulation on a filtered DataFrame."""
    n_bets = len(df)
    wins = df.filter(pl.col("model_correct"))
    n_wins = len(wins)
    n_losses = n_bets - n_wins
    accuracy = n_wins / n_bets if n_bets > 0 else 0

    total_staked = n_bets * STAKE
    total_returned = wins[odds_col].sum() * STAKE if n_wins > 0 else 0
    net_pnl = total_returned - total_staked
    roi = net_pnl / total_staked if total_staked > 0 else 0

    filter_desc = scenario
    for s in SCENARIOS:
        if s["name"] == scenario and s.get("filter"):
            filt = s["filter"]
            conditions = filt if isinstance(filt, list) else [filt]
            parts = [f"{col} {op} {val}" for col, op, val in conditions]
            filter_desc = " & ".join(parts)
            break

    return {
        "model_version": model_version,
        "scenario": scenario,
        "segment": segment,
        "segment_value": segment_value,
        "filter_desc": filter_desc,
        "n_bets": n_bets,
        "n_wins": n_wins,
        "n_losses": n_losses,
        "accuracy": accuracy,
        "total_staked": total_staked,
        "total_returned": total_returned,
        "net_pnl": net_pnl,
        "roi": roi,
        "yield_pct": roi * 100,
    }


def _empty_simulations() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "model_version": pl.Utf8,
        "scenario": pl.Utf8,
        "segment": pl.Utf8,
        "segment_value": pl.Utf8,
        "filter_desc": pl.Utf8,
        "n_bets": pl.Int64,
        "n_wins": pl.Int64,
        "n_losses": pl.Int64,
        "accuracy": pl.Float64,
        "total_staked": pl.Float64,
        "total_returned": pl.Float64,
        "net_pnl": pl.Float64,
        "roi": pl.Float64,
        "yield_pct": pl.Float64,
    })
