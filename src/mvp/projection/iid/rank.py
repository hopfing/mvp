"""Compare evaluated IID projection configs across every instrument.

A sibling of `mvp.model.rank`, not an extension of it: that module's tables are
match-win specific (consensus cells, calibration tiers, err80, round/surface
matrices) and none of it transfers to totals pricing.

Three instruments are shown side by side, deliberately without a composite score:

  distributional  CRPS / line calibration / signed bias   (iid-project)
  soft-book       n_bets, hit rate, ROI, P&L              (iid-backtest, 2026+, 4 books)
  sharp CLV       avg clvpin, positive-CLV rate           (oddspapi vs Pinnacle close)

They measure different things over different samples and they can disagree. A
disagreement is information about the model, so it is shown rather than resolved.
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


# Per market: (display label, CRPS metric, calibration metric, bias metric).
# Each market gets the distributional metric that describes IT — a spread row
# showing total-games CRPS would invite exactly the cross-market comparison this
# split exists to prevent.
MARKETS: dict[str, tuple[str, str, str, str]] = {
    "total_games": ("totals", "iid_crps_total_games", "iid_total_cal",
                    "signed_total_bias"),
    "game_spread": ("spread", "iid_crps_spread", "iid_spread_cal",
                    "signed_spread_bias"),
}


@dataclass
class RankRow:
    fp: str
    sources: list[str] = field(default_factory=list)
    run_ids: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    n_folds: int = 0
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


def _betting_summary(fp_dir: Path) -> dict[str, dict[str, Any]] | None:
    """Headline bet-level numbers on the MAIN LINE, no-vig edge, best price.

    The backtest CSV is a broad ledger: `_settle_totals` emits a row for every
    (book, line, side) whose RAW edge clears zero, with no main-line restriction
    and no vig adjustment in the gate. Summarising that set as-is answers a
    question nobody asks — it counts every alternate line where the model simply
    disagrees with the book, which is why its average edge runs ~9% and its ROI
    is meaningless.

    So three restrictions, matching what `print_backtest_summary` already slices:
      * best price per (match, market, line, side) — shopping books is real
      * main line only, via the backtest's own median-line selection
      * no-vig edge > 0 as the gate, not raw edge

    `n_all` is kept so the gap between the ledger and the bettable set is visible
    rather than hidden.
    """
    from mvp.projection.iid.backtest import _select_main_line

    csv_path = fp_dir / BACKTEST_CSV
    if not csv_path.exists():
        return None
    df = pl.read_csv(csv_path)
    if len(df) == 0 or "open_edge_novig" not in df.columns:
        return None
    # Entry is at OPEN, so the gate and the pnl are both the open-price ones.
    priced = df.filter(pl.col("open_edge_novig") > 0)

    # Per market, never pooled: totals and spreads are priced off different
    # projections (total_games_pmf vs spread_pmf) against different books, so a
    # combined ROI or average edge describes no strategy anyone could run.
    out: dict[str, dict[str, Any]] = {}
    for market in priced["market"].unique().sort().to_list():
        sub = priced.filter(pl.col("market") == market)
        # n_all = every line of this market the model would bet; n_bets = those
        # on the main line. The gap is how much edge lives on alternate lines.
        n_all = len(sub)
        bets = _select_main_line(sub)
        if len(bets) == 0:
            out[market] = {"n_bets": 0, "n_all": n_all, "hit_rate": None,
                           "roi": None, "pl_units": 0.0, "avg_edge": None,
                           "avg_clv": None, "clv_pos_rate": None}
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
        metrics = proj.get("metrics") or {}
        rows.append(RankRow(
            fp=fp_dir.name,
            sources=source_names,
            run_ids=sorted({s[1] for s in sources if s[1] != "-"}),
            metrics=metrics,
            n_folds=proj.get("n_folds") or 0,
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


def _fmt(v: Any, spec: str = ".4f") -> str:
    if v is None:
        return "--"
    try:
        return format(float(v), spec)
    except (TypeError, ValueError):
        return str(v)


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

    name_w = min(38, max([len("Run")] + [len(r.label) for r in rows]))

    dist_sub = f"{'CRPS':>8} {'fold±':>6} {'cal%':>6} {'bias':>6} {'F':>2}"
    bt_sub = f"{'N':>5} {'/all':>5} {'Hit%':>5} {'ROI%':>6} {'U':>7} {'edge%':>6}"
    clv_sub = f"{'N':>5} {'CLV+%':>6} {'avgCLV':>7}"

    lines = [
        "=" * 80,
        f"IID projection runs ({len(rows)}, sorted by {sort_metric})",
        "=" * 80,
        "Instruments measure different samples and are never combined into a score.",
        "One row per (run, market) — totals and spreads are priced off different",
        "projections against different books and are never pooled. CRPS/cal/bias",
        "are the market's own. Betting = MAIN LINE, open no-vig edge>0; /all is",
        "the all-lines count before the main-line filter.",
    ]

    band = (
        " " * (name_w + 8) + "  "
        + "distributional".center(len(dist_sub))
        + " | " + "soft-book".center(len(bt_sub))
        + " | " + "sharp CLV".center(len(clv_sub))
    )
    header = (
        f"{'Run':<{name_w}} {'Market':<7}  {dist_sub} | {bt_sub} | {clv_sub}  "
        f"{'fp':<12}"
    )
    lines.append(band)
    lines.append(header)
    lines.append("-" * len(header))

    for r in rows:
        m = r.metrics
        present = [k for k in MARKETS if (r.betting or {}).get(k)] or ["total_games"]
        for market in present:
            label, crps_key, cal_key, bias_key = MARKETS[market]
            spread = _fold_spread(r.fold_metrics, crps_key)
            spread_str = f"{spread[1] - spread[0]:.3f}" if spread else "--"
            dist = (
                f"{_fmt(m.get(crps_key)):>8} "
                f"{spread_str:>6} "
                f"{_fmt(_pct(m.get(cal_key)), '.2f'):>6} "
                f"{_fmt(m.get(bias_key), '+.3f'):>6} "
                f"{r.n_folds:>2}"
            )
            b = (r.betting or {}).get(market)
            bt = (
                f"{b['n_bets']:>5} "
                f"{b['n_all']:>5} "
                f"{_fmt(_pct(b['hit_rate']), '.1f'):>5} "
                f"{_fmt(_pct(b['roi']), '+.2f'):>6} "
                f"{_fmt(b['pl_units'], '+.1f'):>7} "
                f"{_fmt(_pct(b.get('avg_edge')), '+.2f'):>6}"
            ) if b else f"{'--':>5} {'--':>5} {'--':>5} {'--':>6} {'--':>7} {'--':>6}"
            # CLV is scored on total games only — the oddspapi scorer prices that
            # market; there is no spread equivalent yet.
            c = r.clv if market == "total_games" else None
            clv = (
                f"{c.get('n', 0):>5} "
                f"{_fmt(_pct(c.get('positive_rate')), '.1f'):>6} "
                f"{_fmt(_pct(c.get('avg_clvpin')), '+.3f'):>7}"
            ) if c else f"{'--':>5} {'--':>6} {'--':>7}"
            lines.append(
                f"{r.label[:name_w]:<{name_w}} {label:<7}  {dist} | {bt} | {clv}  "
                f"{r.fp:<12}"
            )

    missing_bt = sum(1 for r in rows if r.betting is None)
    missing_clv = sum(1 for r in rows if r.clv is None)
    if missing_bt or missing_clv:
        lines.append("")
        lines.append(
            f"{missing_bt}/{len(rows)} runs have no backtest "
            f"(from a bare `iid-project`; `iid-sweep` always writes one); "
            f"{missing_clv}/{len(rows)} have no CLV "
            f"(score the fingerprint dir with the oddspapi total-games scorer)."
        )
    return lines


def _pct(v: Any) -> Any:
    return None if v is None else float(v) * 100.0
