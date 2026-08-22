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
    backtest_name,
    discover_fp_dirs,
    read_clv_json,
    read_projection_json,
    read_sources,
)

logger = logging.getLogger(__name__)

# The anchor the headline reports. Entry happens at the open — that is where the
# open betting question lives, and where the sharp-CLV work located its signal.
# The other anchors stay in the ledger; they answer the decay question, which is
# a different report rather than a different weighting of this one.
HEADLINE_ANCHOR = "open"


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

# Market order for reading per-market ledgers. Derived from `MARKETS` rather than
# spelled again, so a market added there is read without a second edit.
MARKET_KEYS: tuple[str, ...] = tuple(spec.key for spec in MARKETS)


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
    """Per bet_type split.

    H71 found tuning-metric effects surfacing as one side being flooded rather
    than as a change in overall ROI, so the side split is where that shows.

    `bet_type` is DERIVED from `side` rather than stored. For totals the two are
    the same thing, so a column would be redundant; for spreads it is
    favourite/underdog/pickem from the sign of the line, which is real
    information — but spreads are out of scope for this layer. Deriving it keeps
    this split working without carrying a duplicate field, and the earlier
    failure mode (drop the column, `_by_bet_type` silently returns `{}`, and the
    one split where H71 found a clear positive quietly disappears) cannot recur.
    """
    if "side" not in bets.columns:
        return {}
    typed = bets.with_columns(pl.col("side").alias("bet_type"))
    agg = typed.group_by("bet_type").agg(
        pl.len().alias("n"),
        pl.col("pnl").mean().alias("roi"),
        pl.col("pnl").sum().alias("units"),
        pl.col("won").mean().alias("hit_rate"),
    )
    return {
        r["bet_type"]: {
            "n": r["n"], "roi": r["roi"], "units": r["units"],
            "hit_rate": r["hit_rate"],
        }
        for r in agg.iter_rows(named=True)
    }


# One bet per (match, market). The ledger has several correlated rows per match —
# one per book, and one per side — which win or lose together. Averaging over them
# weights a match by how many books happened to post a price and treats correlated
# rows as independent bets, so the sample looks larger and less variable than it is.
#
# Three selection policies, deliberately reported side by side: their spread IS the
# selection-bias measurement. If `max_edge` underperforms `main`, that is the
# model's own edge ranking being wrong, visible directly rather than inferred.
#
# The policies do NOT share a candidate set — see `_select_one_per_match`. Which
# rungs a strategy may consider is part of what the strategy IS, so their `n`
# differs by construction and the cells are not paired.
SELECTION_POLICIES = ("main", "safest", "max_edge")

# Edge bands, so the threshold stays a read-time choice. Reporting at one gate
# assumes the crossover instead of showing it.
EDGE_BANDS: tuple[tuple[str, float, float], ...] = (
    ("all", -1.0, 1.0),
    # `<0` exists so the bands PARTITION `all`. Without it the negative-edge
    # rows — 78% of the ledger, and the majority of any gate-free selection —
    # sit in no band, and the rows beneath `all` silently fail to reconcile
    # with it.
    ("<0", -1.0, 0.0),
    ("0-2pp", 0.0, 0.02),
    ("2-5pp", 0.02, 0.05),
    ("5-10pp", 0.05, 0.10),
    ("10pp+", 0.10, 1.0),
)


def _select_one_per_match(bets: pl.DataFrame, policy: str) -> pl.DataFrame:
    """Collapse the anchor's rows to one bet per (match, market) under `policy`.

    `bets` is every rung the entry books had up, NOT a pre-filtered main-line
    set. Which rungs a policy may consider is part of the policy, so each takes
    its own candidate set here.

    `safest` and `max_edge` are the two ends of one eligible set: every rung on
    the ladder that still shows edge. One walks to the shallow end, the other to
    the deep end. `main` is the separate baseline.

      main     — main lines only. The consensus line (the one most books have as
                 their main), at the best price among books offering it. The
                 neutral baseline: bet the number the market agrees on, shop the
                 price. Main lines are the whole candidate set here because
                 consensus is not defined anywhere else.
      safest   — the shallow end: of the rungs with edge, the easiest to beat.
                 `model_p` is P(this bet wins), read off the pmf's cumulative, so
                 within a side it is monotone in the line by construction, and
                 ordering by it is ordering by ladder position.
      max_edge — the deep end: of the same rungs, the largest edge. Longer odds,
                 and the policy most exposed to the model being wrong about the
                 price.

    The edge restriction on both is part of what they mean, not a reporting gate.
    "Safest" over the whole board is just the most extreme rung on offer, and
    "largest edge" over a board with no edge on it is the least-bad losing bet.
    Applying it after the collapse instead let a policy name a dead rung and drop
    a match that had a live one.
    """
    if policy not in SELECTION_POLICIES:
        raise ValueError(f"unknown selection policy: {policy!r}")
    if bets.is_empty():
        return bets
    if policy == "main":
        bets = bets.filter(pl.col("is_main_line").fill_null(False))
    else:
        bets = bets.filter(pl.col("edge") > 0)
    if bets.is_empty():
        return bets
    # Ties are broken on `odds` for every policy. `model_p` in particular is a
    # function of (match, line, side) alone — it does not vary by book — so
    # every book quoting the same main line ties, and without the secondary key
    # `safest` would take whichever row happened to sort first rather than the
    # best price available on the line it chose. Picking the safest line and
    # then not shopping it is not a strategy anyone would run.
    if policy == "max_edge":
        keys, desc = ["edge", "odds"], [True, True]
    elif policy == "safest":
        keys, desc = ["model_p", "odds"], [True, True]
    else:  # main
        # Consensus line first (most books quoting it), then best price on it.
        support = (
            bets.group_by(["match_uid", "market", "points"])
            .agg(pl.col("book").n_unique().alias("_support"))
        )
        bets = bets.join(support, on=["match_uid", "market", "points"], how="left")
        return (
            bets.sort(["_support", "odds"], descending=[True, True], nulls_last=True)
            .group_by(["match_uid", "market"], maintain_order=True)
            .first()
            .drop("_support")
        )
    return (
        bets.sort(keys, descending=desc, nulls_last=True)
        .group_by(["match_uid", "market"], maintain_order=True)
        .first()
    )


def _policy_stats(picked: pl.DataFrame, *, gate: float = 0.0) -> dict[str, Any]:
    """One policy's numbers, gated and ungated.

    `units_all` is every selected bet; `units_gated` and `roi` are the subset
    clearing `gate`. Both are carried for the same reason `model/rank.py` puts
    `Uo>=0` beside `Uo_all`: the ungated figure prices the whole board and is
    mostly vig, and the gap between them IS the edge signal. Reporting either
    alone hides which.

    The band curve — how ROI moves as the threshold rises — belongs in the
    single-model view, not here. This table exists to rank configs, and a curve
    per config per policy buries that under its own rows.

    `safest` and `max_edge` carry the edge restriction in their own definitions,
    so for those two `n == n_gated` and the unit figures coincide. That is the
    policy reporting honestly, not the gate failing to bite. Only `main`, the
    unrestricted baseline, shows a gap.
    """
    if picked.is_empty():
        return {"n": 0, "n_gated": 0, "hit_rate": None, "roi": None,
                "units_gated": None, "units_all": None, "avg_edge": None}
    gated = picked.filter(pl.col("edge") >= gate)
    won = gated["won"].drop_nulls() if gated.height else None
    clv = (
        gated["clv"].drop_nulls()
        if gated.height and "clv" in gated.columns else None
    )
    return {
        "n": picked.height,
        "n_gated": gated.height,
        "hit_rate": float(won.mean()) if won is not None and won.len() else None,
        "roi": float(gated["pnl"].mean()) if gated.height else None,
        "units_gated": float(gated["pnl"].sum()) if gated.height else None,
        "units_all": float(picked["pnl"].sum()),
        "avg_edge": float(gated["edge"].mean()) if gated.height else None,
        # CLV is the metric that separates a bias the market has already priced
        # from one it has not, so it rides in the same cell as the money.
        "avg_clv": float(clv.mean()) if clv is not None and clv.len() else None,
        "clv_pos_rate": float((clv > 0).mean())
        if clv is not None and clv.len() else None,
        "n_clv": int(clv.len()) if clv is not None else 0,
    }


def _band_curve(bets: pl.DataFrame, *, on: str = "edge") -> dict[str, Any]:
    """ROI by edge band — the single-model view's job, not the ranking table's."""
    out: dict[str, Any] = {}
    if on not in bets.columns:
        return out
    for name, lo, hi in EDGE_BANDS:
        band = bets.filter((pl.col(on) >= lo) & (pl.col(on) < hi))
        if band.is_empty():
            continue
        won = band["won"].drop_nulls()
        out[name] = {
            "n": band.height,
            "hit_rate": float(won.mean()) if won.len() else None,
            "roi": float(band["pnl"].mean()),
            "pl_units": float(band["pnl"].sum()),
            "avg_edge": float(band["edge"].mean()),
        }
    return out


def _betting_summary(
    fp_dir: Path, *, anchor: str = HEADLINE_ANCHOR
) -> dict[str, Any] | None:
    """Headline bet-level numbers per market, at ONE named anchor.

    The ledger is the offer set — every entry book's every rung, both sides,
    every anchor, negative edge included — so every restriction lives here.

    **The anchor is named, not implied.** Without an anchor filter the row set
    spans open, formed(2) and close together, and since close carries ~3x open's
    board rows the reported ROI becomes mostly a closing-price number. Model
    comparison would silently become comparison at a price nobody enters at.

    **No edge gate here.** The threshold is reported across `EDGE_BANDS` instead,
    so it stays a read-time choice rather than an assumption baked into the
    comparison — and the crossover is visible instead of guessed. `safest` and
    `max_edge` are the exception, and it is not a gate: an edge restriction is
    what makes those two mean anything (see `_select_one_per_match`).

    Policies get the whole anchor frame, not a main-line subset — each applies
    its own candidate filter. `n_all` and `n_main` keep both counts so the size
    of the ladder those two draw on stays visible beside the main-line set
    `main` is confined to.
    """
    out: dict[str, Any] = {"anchor": anchor, "markets": {}}
    required = {"anchor", "is_main_line", "side", "edge", "pnl", "won", "market"}

    # One file per market. A market with no ledger is NORMAL, not an incomplete
    # dir: every fingerprint written before spreads existed has a totals ledger
    # and nothing else. Skipping the missing one keeps the betting table
    # populated for the market that does have data instead of blanking both.
    for market in MARKET_KEYS:
        path = fp_dir / backtest_name(market)
        if not path.exists():
            continue
        df = pl.read_parquet(path)
        if df.is_empty():
            continue
        missing = required - set(df.columns)
        if missing:
            logger.warning(
                "%s: %s ledger missing %s — betting summary skipped",
                fp_dir.name, market, sorted(missing),
            )
            continue

        at_anchor = df.filter(pl.col("anchor") == anchor)
        if at_anchor.is_empty():
            logger.warning(
                "%s: no %s rows at anchor %s", fp_dir.name, market, anchor
            )
            continue
        # Entry books only. Reference prices are on the board deliberately (they
        # are the CLV benchmark) and are not takeable.
        if "role" in at_anchor.columns:
            at_anchor = at_anchor.filter(pl.col("role") == "entry")
        # A side quoted but no longer live is a carried-forward price, not an offer.
        if "live_side" in at_anchor.columns:
            at_anchor = at_anchor.filter(pl.col("live_side").fill_null(True))
        at_anchor = at_anchor.drop_nulls(["odds", "edge"])
        if at_anchor.is_empty():
            continue

        mains = at_anchor.filter(pl.col("is_main_line").fill_null(False))
        entry: dict[str, Any] = {"n_all": at_anchor.height, "n_main": mains.height}
        for policy in SELECTION_POLICIES:
            picked = _select_one_per_match(at_anchor, policy)
            entry[policy] = {
                **_policy_stats(picked),
                "by_type": _by_bet_type(picked),
            }
        out["markets"][market] = entry
    return out if out["markets"] else None


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


def _market_of(row: RankRow, key: str) -> dict[str, Any] | None:
    return ((row.betting or {}).get("markets") or {}).get(key)


def _policy_cell(stats: dict[str, Any] | None) -> str:
    """`ROI U>=0 U_all` for one policy — the gated pair beside the ungated total.

    Mirrors `model/rank.py`'s backtest cells: the ROI sits next to the units it
    describes, and `U_all` is carried so the gate's contribution is legible
    rather than inferred.
    """
    s = stats or {}
    roi = s.get("roi")
    ug, ua, clv = s.get("units_gated"), s.get("units_all"), s.get("avg_clv")
    roi_s = f"{roi*100:+.2f}" if roi is not None else "--"
    ug_s = f"{ug:+.1f}" if ug is not None else "--"
    ua_s = f"{ua:+.1f}" if ua is not None else "--"
    clv_s = f"{clv*100:+.2f}" if clv is not None else "--"
    return f"{roi_s:>6} {ug_s:>8} {ua_s:>8} {clv_s:>7}"


def render_betting(
    rows: list[RankRow], spec: MarketSpec, table_no: int,
) -> list[str]:
    """One row per config; each selection policy is a cell group.

    `U>=0` is the units from bets clearing a zero edge gate; `U_all` is every
    selected bet. The ungated figure prices the whole board and is mostly vig,
    so the pair is what carries the signal — the same reason `model/rank.py`
    prints `Uo>=0` next to `Uo_all`.

    Sorted by the `main` gated units, and the sort is named in the title rather
    than left implicit: ranking on the ungated number would rank configs by how
    much vig they paid.

    `main` and not `max_edge`. `safest` and `max_edge` are the two ENDS of the
    eligible ladder — they exist so the spread around the main line is visible,
    which is something to read, not to rank on. Ranking on the deepest rung
    ordered configs by a bet set that is not the one placed: it put a run at #2
    on +15.3 max_edge units while its main line returned +1.5, above the run
    holding the best main-line result in the table.

    The edge-band curve is deliberately NOT here. It is a property of one
    config, and a curve per config per policy turns a ranking table into
    eighteen rows per config. It lives in the single-model view
    (`iid-backtest`'s summary).
    """
    scored = [r for r in rows if _market_of(r, spec.key)]
    lines = _banner(f"Table {table_no}: {spec.label} — betting")
    if not scored:
        lines.append("No backtest ledger for this market.")
        return lines
    anchor = (scored[0].betting or {}).get("anchor", "?")
    w = max(_name_width(scored), 12)
    lines += [
        f"Anchor={anchor}, entry books, one bet per match.",
        "main is the consensus main line; safest and max_edge read the whole "
        "ladder and carry an edge>0 restriction, so their U>=0 equals U_all.",
        "Policy cells: ROI% / U>=0 / U_all / CLV% (gate is edge>=0). Sorted by "
        "main U>=0 desc.",
        "CLV is vs the reference book's de-vigged close and is negative by the "
        "entry vig — compare it between configs, never read it as a level.",
    ]
    cell_sub = f"{'ROI%':>6} {'U>=0':>8} {'U_all':>8} {'CLV%':>7}"
    group_w = len(cell_sub)
    base_header = f"{'#':>2} {'Config':<{w}} {'N':>6} {'Hit%':>5}"
    band = (
        " " * len(base_header) + " | "
        + " | ".join(p.center(group_w) for p in SELECTION_POLICIES)
    )
    header = (
        base_header + " | "
        + " | ".join(cell_sub for _ in SELECTION_POLICIES)
        + f" | {'fp':<12}"
    )
    lines += [band, header, "-" * len(header)]

    def _rank_key(r: RankRow) -> float:
        s = (_market_of(r, spec.key) or {}).get("main") or {}
        u = s.get("units_gated")
        return u if u is not None else -1e18

    for i, r in enumerate(sorted(scored, key=lambda x: -_rank_key(x)), 1):
        blk = _market_of(r, spec.key) or {}
        head = blk.get(SELECTION_POLICIES[0]) or {}
        cells = " | ".join(_policy_cell(blk.get(p)) for p in SELECTION_POLICIES)
        lines.append(
            f"{i:>2} {_label_of(r)[:w]:<{w}} "
            f"{head.get('n', 0):>6,} "
            f"{_fmt(_pct(head.get('hit_rate')), '.1f'):>5} | "
            + cells
            + f" | {r.fp:<12}"
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
