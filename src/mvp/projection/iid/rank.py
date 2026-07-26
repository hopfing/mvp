"""Compare evaluated IID projection configs across every instrument.

A sibling of `mvp.model.rank`, not an extension of it: that module's tables are
match-win specific (consensus cells, calibration tiers, err80, round/surface
matrices) and none of it transfers to totals pricing.

Output is one table per (instrument, market), never a combined score and never a
pooled market. Totals and spreads are priced off different projections against
different books, so a shared row would invite exactly the comparison that makes
no sense; and cramming three instruments into one row is what caps how much each
can show.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from mvp.model.tuning import _MAXIMIZE_METRICS
from mvp.projection.iid.artifacts import (
    BACKTEST_CSV,
    discover_fp_dirs,
    read_clv_json,
    read_projection_json,
    read_sources,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketSpec:
    """Everything that differs between the markets one chain prices."""

    key: str
    label: str
    crps: str
    cal: str
    bias: str
    sides: tuple[str, ...]      # bet_type values, in display order
    side_labels: tuple[str, ...]


MARKETS: tuple[MarketSpec, ...] = (
    MarketSpec(
        key="total_games", label="TOTAL GAMES",
        crps="iid_crps_total_games", cal="iid_total_cal",
        bias="signed_total_bias",
        sides=("over", "under"), side_labels=("over", "under"),
    ),
    MarketSpec(
        key="game_spread", label="GAME SPREAD",
        crps="iid_crps_spread", cal="iid_spread_cal",
        bias="signed_spread_bias",
        sides=("favorite", "underdog"), side_labels=("fav", "dog"),
    ),
)


@dataclass
class RankRow:
    fp: str
    sources: list[str] = field(default_factory=list)
    run_ids: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    n_folds: int = 0
    n_matches: int = 0
    fold_metrics: list = field(default_factory=list)
    betting: dict[str, dict[str, Any]] | None = None
    clv: dict[str, Any] | None = None

    @property
    def label(self) -> str:
        return self.run_ids[0] if self.run_ids else self.fp


def _fold_spread(fold_metrics: list, metric: str) -> tuple[float, float] | None:
    vals = [
        f[metric] for f in (fold_metrics or [])
        if isinstance(f, dict) and f.get(metric) is not None
    ]
    if len(vals) < 2:
        return None
    return min(vals), max(vals)


def _by_bet_type(bets: pl.DataFrame) -> dict[str, dict[str, Any]]:
    """Per bet_type split (over/under, favorite/underdog).

    H71 found tuning-metric effects surfacing as one side being flooded rather
    than as a change in overall ROI, so the side split is where that shows.
    """
    if "bet_type" not in bets.columns:
        return {}
    agg = bets.group_by("bet_type").agg(
        pl.len().alias("n"),
        pl.col("pnl_open").mean().alias("roi"),
        pl.col("pnl_open").sum().alias("units"),
    )
    return {
        r["bet_type"]: {"n": r["n"], "roi": r["roi"], "units": r["units"]}
        for r in agg.iter_rows(named=True)
    }


def _betting_summary(fp_dir: Path) -> dict[str, dict[str, Any]] | None:
    """Headline bet-level numbers per market: main line, open entry, no-vig gate.

    The CSV is a broad ledger — every offered book x line x side, including
    negative-edge rows — so the restrictions live here:
      * open no-vig edge > 0 (entry happens at open, and the gate matches)
      * main line only, via the `is_main_line` flag stamped at settle time
      * per market, never pooled

    `n_all` keeps the all-lines count so the gap to the main-line set is visible.
    """
    from mvp.projection.iid.backtest import _select_main_line

    csv_path = fp_dir / BACKTEST_CSV
    if not csv_path.exists():
        return None
    df = pl.read_csv(csv_path)
    if len(df) == 0 or "open_edge_novig" not in df.columns:
        return None
    priced = df.filter(pl.col("open_edge_novig") > 0)

    out: dict[str, dict[str, Any]] = {}
    for market in priced["market"].unique().sort().to_list():
        sub = priced.filter(pl.col("market") == market)
        n_all = len(sub)
        bets = _select_main_line(sub)
        if len(bets) == 0:
            out[market] = {"n_bets": 0, "n_all": n_all, "hit_rate": None,
                           "roi": None, "pl_units": 0.0, "avg_edge": None,
                           "avg_clv": None, "clv_pos_rate": None, "by_type": {}}
            continue
        clv = bets["clv_open"].drop_nulls() if "clv_open" in bets.columns else None
        out[market] = {
            "n_bets": len(bets),
            "n_all": n_all,
            "hit_rate": float(bets["won"].mean()),
            "roi": float(bets["pnl_open"].mean()),
            "pl_units": float(bets["pnl_open"].sum()),
            "avg_edge": float(bets["open_edge_novig"].mean()),
            "avg_clv": float(clv.mean()) if clv is not None and clv.len() else None,
            "clv_pos_rate": float((clv > 0).mean())
            if clv is not None and clv.len() else None,
            "by_type": _by_bet_type(bets),
        }
    return out or None


def collect_rows(source: str | None = None) -> list[RankRow]:
    rows: list[RankRow] = []
    for fp_dir in discover_fp_dirs():
        proj = read_projection_json(fp_dir)
        if proj is None:
            continue
        sources = read_sources(fp_dir)
        source_names = sorted({s[0] for s in sources})
        if source is not None and source not in source_names:
            continue
        rows.append(RankRow(
            fp=fp_dir.name,
            sources=source_names,
            run_ids=sorted({s[1] for s in sources if s[1] != "-"}),
            metrics=proj.get("metrics") or {},
            n_folds=proj.get("n_folds") or 0,
            n_matches=proj.get("n_matches") or 0,
            fold_metrics=proj.get("fold_metrics") or [],
            betting=_betting_summary(fp_dir),
            clv=read_clv_json(fp_dir),
        ))
    return rows


def _sorted(rows: list[RankRow], metric: str) -> list[RankRow]:
    maximize = metric in _MAXIMIZE_METRICS
    worst = float("-inf") if maximize else float("inf")

    def key(r: RankRow):
        v = r.metrics.get(metric, worst)
        return -v if maximize else v

    return sorted(rows, key=key)


# --------------------------------------------------------------------------
# formatting
# --------------------------------------------------------------------------

def _fmt(v: Any, spec: str = ".4f") -> str:
    if v is None:
        return "--"
    try:
        return format(float(v), spec)
    except (TypeError, ValueError):
        return str(v)


def _pct(v: Any) -> Any:
    return None if v is None else float(v) * 100.0


def _display_label(run_id: str, fp: str) -> tuple[str, str]:
    """(config name, variant tag). Sweep stems are `<parent>__d01_t8`.

    Mirrors model-rank's `<name> (<run_id>)`: the config name repeats down the
    column and only the variant changes, which reads far better than a column of
    near-identical long stems.
    """
    if "__" in run_id:
        parent, variant = run_id.split("__", 1)
        return parent, variant
    return run_id or fp, ""


def _label_of(r: RankRow) -> str:
    name, variant = _display_label(r.label, r.fp)
    return f"{name} ({variant})" if variant else name


def _name_width(rows: list[RankRow]) -> int:
    return min(40, max([len("Config")] + [len(_label_of(r)) for r in rows]))


def _banner(title: str) -> list[str]:
    return ["", "=" * 80, title, "=" * 80]


def render_distributional(
    rows: list[RankRow], spec: MarketSpec, sort_metric: str, table_no: int,
) -> list[str]:
    """Distributional quality for one market."""
    w = _name_width(rows)
    lines = _banner(f"Table {table_no}: {spec.label} — distributional")
    lines.append(
        "fold+- is max-min across validation folds; where it exceeds the gap "
        "between adjacent runs the ordering is inside fold noise."
    )
    header = (
        f"{'#':>2} {'Config':<{w}} {'CRPS':>8} {'fold+-':>7} "
        f"{'cal%':>6} {'calmax%':>8} {'bias':>7} {'F':>2} {'matches':>8} {'fp':<12}"
    )
    lines += [header, "-" * len(header)]
    ordered = _sorted(rows, spec.crps) if spec.crps != sort_metric else rows
    for i, r in enumerate(ordered, 1):
        spread = _fold_spread(r.fold_metrics, spec.crps)
        lines.append(
            f"{i:>2} {_label_of(r)[:w]:<{w}} "
            f"{_fmt(r.metrics.get(spec.crps)):>8} "
            f"{(f'{spread[1] - spread[0]:.4f}' if spread else '--'):>7} "
            f"{_fmt(_pct(r.metrics.get(spec.cal)), '.2f'):>6} "
            f"{_fmt(_pct(r.metrics.get(spec.cal + '_max')), '.2f'):>8} "
            f"{_fmt(r.metrics.get(spec.bias), '+.3f'):>7} "
            f"{r.n_folds:>2} {r.n_matches:>8} {r.fp:<12}"
        )
    return lines


def render_betting(
    rows: list[RankRow], spec: MarketSpec, table_no: int,
) -> list[str]:
    """Betting for one market, with each side as its own column group."""
    scored = [r for r in rows if (r.betting or {}).get(spec.key)]
    lines = _banner(f"Table {table_no}: {spec.label} — betting")
    if not scored:
        lines.append("No backtest artifacts for this market.")
        return lines
    w = _name_width(scored)
    lines.append(
        "Main line, open entry, no-vig edge>0. N//all = main-line bets / all "
        "lines clearing the gate; the gap is edge claimed on alternate lines."
    )
    side_w = 20
    band = (
        " " * (2 + 1 + w + 1 + 5 + 1 + 6 + 1 + 5 + 1 + 6 + 1 + 7 + 1 + 6) + "  "
        + "  ".join(lbl.center(side_w) for lbl in spec.side_labels)
    )
    header = (
        f"{'#':>2} {'Config':<{w}} {'N':>5} {'/all':>6} {'Hit%':>5} "
        f"{'ROI%':>6} {'Units':>7} {'edge%':>6}  "
        + "  ".join(
            f"{'n':>5} {'ROI%':>6} {'U':>6}" for _ in spec.side_labels
        )
        + f"  {'fp':<12}"
    )
    lines += [band, header, "-" * len(header)]
    ordered = sorted(
        scored,
        key=lambda r: -(r.betting[spec.key]["pl_units"]
                        if r.betting[spec.key]["pl_units"] is not None else -1e18),
    )
    for i, r in enumerate(ordered, 1):
        b = r.betting[spec.key]
        cells = []
        for side in spec.sides:
            d = b.get("by_type", {}).get(side)
            cells.append(
                f"{d['n']:>5} {_pct(d['roi']):>+6.2f} {d['units']:>+6.1f}"
                if d else f"{'--':>5} {'--':>6} {'--':>6}"
            )
        lines.append(
            f"{i:>2} {_label_of(r)[:w]:<{w}} "
            f"{b['n_bets']:>5} {b['n_all']:>6} "
            f"{_fmt(_pct(b['hit_rate']), '.1f'):>5} "
            f"{_fmt(_pct(b['roi']), '+.2f'):>6} "
            f"{_fmt(b['pl_units'], '+.1f'):>7} "
            f"{_fmt(_pct(b.get('avg_edge')), '+.2f'):>6}  "
            + "  ".join(cells)
            + f"  {r.fp:<12}"
        )
    return lines


def render_clv(rows: list[RankRow], table_no: int) -> list[str]:
    """Sharp CLV vs the Pinnacle de-vigged close. Total games only — the
    oddspapi scorer prices that market and there is no spread equivalent yet."""
    scored = [r for r in rows if r.clv]
    lines = _banner(f"Table {table_no}: TOTAL GAMES — sharp CLV (vs Pinnacle close)")
    if not scored:
        lines.append("No CLV artifacts. Score a fingerprint dir with:")
        lines.append(
            "  poetry run python scripts/oddspapi/oddspapi_total_games_poc.py <fp_dir>"
        )
        return lines
    w = _name_width(scored)
    lines.append(
        "CLV asks whether the market moved toward the model, independent of "
        "whether the match landed."
    )
    header = (
        f"{'#':>2} {'Config':<{w}} {'matches':>8} {'bets':>6} {'N_clv':>6} "
        f"{'CLV+%':>6} {'avgCLV':>8} {'Hit%':>5} {'ROI%':>6} {'fp':<12}"
    )
    lines += [header, "-" * len(header)]
    ordered = sorted(
        scored,
        key=lambda r: -(r.clv.get("avg_clvpin")
                        if r.clv.get("avg_clvpin") is not None else -1e18),
    )
    for i, r in enumerate(ordered, 1):
        c = r.clv
        lines.append(
            f"{i:>2} {_label_of(r)[:w]:<{w}} "
            f"{_fmt(c.get('n_matches_scored'), '.0f'):>8} "
            f"{_fmt(c.get('n_bets'), '.0f'):>6} "
            f"{_fmt(c.get('n'), '.0f'):>6} "
            f"{_fmt(_pct(c.get('positive_rate')), '.1f'):>6} "
            f"{_fmt(_pct(c.get('avg_clvpin')), '+.3f'):>8} "
            f"{_fmt(_pct(c.get('hit_rate')), '.1f'):>5} "
            f"{_fmt(_pct(c.get('roi')), '+.2f'):>6} {r.fp:<12}"
        )
    return lines


def format_rank_table(
    sort_metric: str = "iid_crps_total_games",
    source: str | None = None,
    top_n: int | None = None,
) -> list[str]:
    rows = collect_rows(source=source)
    if not rows:
        return [
            "No evaluated IID projection configs found.",
            "",
            "Produce some with:",
            "  poetry run py -m mvp iid-project <config>          (distributional)",
            "  poetry run py -m mvp iid-sweep <config> --n-trials N",
        ]
    rows = _sorted(rows, sort_metric)
    if top_n:
        rows = rows[:top_n]

    lines = [
        f"IID PROJECTION RUNS ({len(rows)})",
        "One table per (instrument, market). Instruments measure different "
        "samples and are never combined into a score; totals and spreads are "
        "priced off different projections and are never pooled.",
    ]
    table_no = 1
    for spec in MARKETS:
        lines += render_distributional(rows, spec, sort_metric, table_no)
        table_no += 1
    for spec in MARKETS:
        lines += render_betting(rows, spec, table_no)
        table_no += 1
    lines += render_clv(rows, table_no)

    missing_bt = sum(1 for r in rows if r.betting is None)
    missing_clv = sum(1 for r in rows if r.clv is None)
    if missing_bt or missing_clv:
        lines += [
            "",
            f"{missing_bt}/{len(rows)} runs have no backtest; "
            f"{missing_clv}/{len(rows)} have no CLV.",
        ]
    return lines
