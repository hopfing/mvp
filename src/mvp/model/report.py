"""`mvp model-report <model>` — single-model deep dive.

Reads three artifacts (latest mlrun diagnostics JSON, confidence
validation_results.json, backtest CSV) and formats four sections per the
spec at `mvp-docs/specs/2026-05-17-model-evaluation-cli.md`.
"""

from __future__ import annotations

import datetime as _dt
import logging
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from mvp.model.evaluation import (
    ModelArtifacts,
    load_artifacts,
    refresh_pipeline,
)

logger = logging.getLogger(__name__)

CIRCUITS = ("chal", "tour")
MATRIX_SURFACE_COLS = ("Clay", "Hard")
MATRIX_ROUND_COLS = ("Q1", "Q2", "Q3", "R128", "R64", "R32", "R16", "QF", "SF", "F")
MATRIX_N_MIN = 100  # same threshold as Section C, matches review-models convention


def _tier_symbol(cal: float | None) -> str:
    """Tier band for a signed cal value (cal expressed as proportion)."""
    if cal is None:
        return " "
    if cal < -0.01:
        return "X"
    if cal < -0.005:
        return ","
    if cal < 0:
        return "."
    if cal < 0.02:
        return "+"
    return "^"


def _fmt_signed_cal_pp(cal: float | None) -> str:
    """Format signed cal as tier-prefixed pp value: 'X -1.2', '+ +0.4', etc."""
    if cal is None:
        return "       --"
    pp = cal * 100
    return f"{_tier_symbol(cal)} {pp:+5.2f}"


def _fmt_pp(x: float | None, width: int = 6) -> str:
    if x is None:
        return f"{'--':>{width}}"
    return f"{x * 100:+{width}.2f}"


def _fmt_pct(x: float | None, width: int = 6, signed: bool = False) -> str:
    if x is None:
        return f"{'--':>{width}}"
    fmt = f"+{width}.1f" if signed else f"{width}.1f"
    return f"{x * 100:{fmt}}"


def _fmt_n(n: int | float | None) -> str:
    if n is None:
        return "--"
    if n >= 1000:
        return f"{n / 1000:.0f}K"
    return f"{int(n)}"


def _load_config(config_path: Path) -> dict[str, Any]:
    with open(config_path) as f:
        return yaml.safe_load(f)


def format_section_a(art: ModelArtifacts, cfg: dict) -> str:
    lines = ["=" * 80, f"A. IDENTITY", "=" * 80]
    lines.append(f"  Name:      {art.name}")
    lines.append(f"  run_id:    {art.run_id}")
    lines.append(f"  run_ts:    {art.run_ts}")
    lines.append(f"  Config:    {art.config_path}")
    data_cfg = cfg.get("data", {}) or {}
    dr = data_cfg.get("date_range", {}) or {}
    lines.append(f"  Training:  {dr.get('start', '?')} -> {dr.get('end', '?')}")
    val_cfg = cfg.get("validation", {}) or {}
    if val_cfg:
        v_type = val_cfg.get("type", "?")
        v_train = val_cfg.get("initial_train_months", "?")
        v_test = val_cfg.get("test_months", "?")
        lines.append(f"  Validation: {v_type}, initial_train={v_train}mo, test={v_test}mo")
    sw_cfg = cfg.get("sample_weight", {}) or {}
    if sw_cfg:
        sw_type = sw_cfg.get("type", "?")
        sw_hl = sw_cfg.get("half_life_days")
        if sw_hl is not None:
            lines.append(f"  Weighting: {sw_type}, half_life={sw_hl}d")
        else:
            lines.append(f"  Weighting: {sw_type}")
    return "\n".join(lines)


def _cell_signed_cal(diag: dict, circuit: str, segment_kind: str, segment: str) -> tuple[float | None, int | None]:
    """Pull (signed_cal, n) for a (circuit, segment_kind, segment) cell from diagnostics."""
    circ_block = diag["segments"]["by_circuit"].get(circuit, {})
    if segment_kind == "overall":
        cell = circ_block.get("overall", {})
    else:
        cell = circ_block.get(segment_kind, {}).get(segment, {})
    if not cell:
        return None, None
    return cell.get("signed_calibration"), cell.get("n_matches")


def _headline_metrics(diag: dict) -> dict[str, Any]:
    """Aggregate chal + tour overalls into combined headline (n-weighted)."""
    chal = diag["segments"]["by_circuit"].get("chal", {}).get("overall", {})
    tour = diag["segments"]["by_circuit"].get("tour", {}).get("overall", {})
    nc = chal.get("n_matches", 0) or 0
    nt = tour.get("n_matches", 0) or 0
    total = nc + nt
    if total == 0:
        return {}
    def wavg(field: str) -> float | None:
        cv = chal.get(field)
        tv = tour.get(field)
        if cv is None and tv is None:
            return None
        return ((cv or 0) * nc + (tv or 0) * nt) / total
    return {
        "n": total,
        "acc": wavg("accuracy"),
        "ll": wavg("log_loss"),
        "auc": wavg("roc_auc"),
        "brier": wavg("brier_score"),
        "err80": wavg("error_rate_80plus"),
        "signed_cal": wavg("signed_calibration"),
    }


def format_section_b(art: ModelArtifacts) -> str:
    diag = art.diagnostics
    head = _headline_metrics(diag)
    lines = ["=" * 80, "B. STATIC CALIBRATION (mlrun diagnostics)", "=" * 80]
    if head:
        lines.append(
            f"  N={head['n']:,}  Acc={head['acc']:.4f}  LL={head['ll']:.4f}  "
            f"AUC={head['auc']:.4f}  Brier={head['brier']:.4f}  Err80={head['err80']*100:.1f}%"
        )
        lines.append(
            f"  Overall signed cal: {_fmt_signed_cal_pp(head['signed_cal'])} pp  "
            f"(Drift: {diag.get('temporal', {}).get('temporal_drift', 0)*100:+.2f} pp)"
        )

    lines.append("")
    lines.append(f"  Calibration matrix (tier-prefixed signed cal, pp; cells with n<{MATRIX_N_MIN} blanked):")
    lines.append("  Legend: X<-1%  ,<-0.5%  .<0%  +Optimal[0,+2%)  ^>=+2%")
    lines.append("")
    header_cells = ["ov"] + list(MATRIX_SURFACE_COLS) + list(MATRIX_ROUND_COLS)
    header = "  " + f"{'circuit':<8}" + "  ".join(f"{c:>9}" for c in header_cells)
    lines.append(header)
    sep_pad = " " * 10
    lines.append(sep_pad + "-" * (len(header) - 10))

    def _filtered_cell(cal: float | None, n: int | None) -> str:
        if cal is None or n is None or n < MATRIX_N_MIN:
            return f"{'':>9}"
        return f"{_fmt_signed_cal_pp(cal):>9}"

    for circuit in CIRCUITS:
        cells: list[str] = []
        overall_cal, overall_n = _cell_signed_cal(diag, circuit, "overall", "")
        cells.append(_filtered_cell(overall_cal, overall_n))
        for surf in MATRIX_SURFACE_COLS:
            cal, n_cell = _cell_signed_cal(diag, circuit, "surface", surf)
            cells.append(_filtered_cell(cal, n_cell))
        for rnd in MATRIX_ROUND_COLS:
            cal, n_cell = _cell_signed_cal(diag, circuit, "round", rnd)
            cells.append(_filtered_cell(cal, n_cell))
        lines.append("  " + f"{circuit:<8}" + "  ".join(cells))

    # n-counts row for context
    n_cells: list[str] = []
    for circuit in CIRCUITS:
        _, n_overall = _cell_signed_cal(diag, circuit, "overall", "")
        n_cells.append((_fmt_n(n_overall),))
    lines.append("")
    lines.append(f"  n_chal={_fmt_n(_cell_signed_cal(diag, 'chal', 'overall', '')[1])}  "
                 f"n_tour={_fmt_n(_cell_signed_cal(diag, 'tour', 'overall', '')[1])}")
    return "\n".join(lines)


_ROUND_ORDER = ("Q1", "Q2", "Q3", "R128", "R64", "R32", "R16", "QF", "SF", "F",
                "BRONZE", "HCF", "RR")


def _round_sort_key(round_name: str) -> tuple[int, str]:
    """Canonical round ordering; unknown rounds sort to the end alphabetically."""
    try:
        return (_ROUND_ORDER.index(round_name), round_name)
    except ValueError:
        return (len(_ROUND_ORDER), round_name)


def _fmt_pp_cell(x: float | None, width: int = 7) -> str:
    """Right-aligned signed pp value; '--' if None."""
    if x is None:
        return f"{'--':>{width}}"
    return f"{x * 100:>+{width}.2f}"


def format_section_c(art: ModelArtifacts) -> str:
    profiles = art.confidence.get("profiles", {})
    lines = ["=" * 80, "C. TEMPORAL STABILITY (confidence, 12mo rolling)", "=" * 80]

    lines.append("  Per-circuit 12mo cal (signed pp):")
    lines.append(
        f"  {'circuit':<8}  {'min':>8}  {'p25':>8}  {'med':>8}  {'p75':>8}  {'max':>8}"
    )
    lines.append("  " + "-" * 56)
    for circuit in CIRCUITS:
        key = f"circuit:{circuit}"
        prof = profiles.get(key, {}).get("overall", {})
        c12 = prof.get("cal_12mo", {})
        lines.append(
            f"  {circuit:<8}  "
            f"{_fmt_pp_cell(c12.get('min'), 8)}  "
            f"{_fmt_pp_cell(c12.get('p25'), 8)}  "
            f"{_fmt_pp_cell(c12.get('median'), 8)}  "
            f"{_fmt_pp_cell(c12.get('p75'), 8)}  "
            f"{_fmt_pp_cell(c12.get('max'), 8)}"
        )

    # Per-(circuit, round) cells, grouped by circuit, sorted by canonical round order
    lines.append("")
    lines.append("  Per-(circuit, round) 12mo cal (grouped by circuit, round-ordered):")
    lines.append(
        f"  {'cell':<14} {'n':>6}  {'min':>8}  {'p25':>8}  {'med':>8}  {'p75':>8}  {'max':>8}"
    )
    lines.append("  " + "-" * 70)

    by_circuit: dict[str, list[tuple[str, int, dict]]] = {c: [] for c in CIRCUITS}
    for key in profiles:
        if not key.startswith("circuit+round:"):
            continue
        label = key.replace("circuit+round:", "")
        # label format: "<circuit>+<round>"
        if "+" not in label:
            continue
        circuit, round_name = label.split("+", 1)
        if circuit not in by_circuit:
            continue
        prof = profiles[key].get("overall", {})
        n = prof.get("n_matches")
        if n is None or n < 100:
            continue
        by_circuit[circuit].append((round_name, n, prof.get("cal_12mo", {})))

    for circuit in CIRCUITS:
        rows = by_circuit[circuit]
        if not rows:
            continue
        rows.sort(key=lambda r: _round_sort_key(r[0]))
        for round_name, n, c12 in rows:
            cell_label = f"{circuit}+{round_name}"
            lines.append(
                f"  {cell_label:<14} {n:>6}  "
                f"{_fmt_pp_cell(c12.get('min'), 8)}  "
                f"{_fmt_pp_cell(c12.get('p25'), 8)}  "
                f"{_fmt_pp_cell(c12.get('median'), 8)}  "
                f"{_fmt_pp_cell(c12.get('p75'), 8)}  "
                f"{_fmt_pp_cell(c12.get('max'), 8)}"
            )
        # Separator between circuits
        if circuit != CIRCUITS[-1]:
            lines.append("  " + "-" * 70)

    return "\n".join(lines)


_TIER_ORDER = ("Optimal", "Border", "Risky", "Danger")


def _slice_metrics(sub: pl.DataFrame) -> dict[str, Any]:
    """Compute the standard betting metrics on a filtered slice."""
    n = len(sub)
    if n == 0:
        return {"n": 0}
    hit = sub["won"].mean() if "won" in sub.columns else None
    pnl_o = sub["pnl_open"].sum() if "pnl_open" in sub.columns else None
    pnl_c = sub["pnl_close"].sum() if "pnl_close" in sub.columns else None
    roi_o = (pnl_o / n) if pnl_o is not None else None
    roi_c = (pnl_c / n) if pnl_c is not None else None
    clv_pos = (sub["clv"] > 0).mean() if "clv" in sub.columns else None
    avg_clv = sub["clv"].mean() if "clv" in sub.columns else None
    me_o_pos = (sub["opening_edge"] > 0).mean() if "opening_edge" in sub.columns else None
    avg_me_o = sub["opening_edge"].mean() if "opening_edge" in sub.columns else None
    me_c_pos = (sub["closing_edge"] > 0).mean() if "closing_edge" in sub.columns else None
    avg_me_c = sub["closing_edge"].mean() if "closing_edge" in sub.columns else None
    return {
        "n": n, "hit": hit, "pnl_o": pnl_o, "pnl_c": pnl_c,
        "roi_o": roi_o, "roi_c": roi_c,
        "clv_pos": clv_pos, "avg_clv": avg_clv,
        "me_o_pos": me_o_pos, "avg_me_o": avg_me_o,
        "me_c_pos": me_c_pos, "avg_me_c": avg_me_c,
    }


def _fmt_metrics_row(label: str, m: dict[str, Any], label_w: int = 10) -> str:
    """Render one metrics row aligned with the table header."""
    if m["n"] == 0:
        return f"  {label:<{label_w}} {0:>6}" + " " * 80 + "(no rows)"

    def pct(x, w=6, signed=False):
        if x is None:
            return f"{'--':>{w}}"
        fmt = f"+{w-1}.2f" if signed else f"{w-1}.2f"
        return f"{x * 100:{fmt}}"

    return (
        f"  {label:<{label_w}} "
        f"{m['n']:>6}  "
        f"{pct(m['hit'], 5):>5}  "
        f"{pct(m['roi_o'], 7, signed=True):>7}  "
        f"{pct(m['roi_c'], 7, signed=True):>7}  "
        f"{m['pnl_o']:>+7.1f}u  "
        f"{m['pnl_c']:>+7.1f}u  "
        f"{pct(m['clv_pos'], 5):>5}  "
        f"{pct(m['avg_clv'], 6, signed=True):>6}  "
        f"{pct(m['me_o_pos'], 5):>5}  "
        f"{pct(m['avg_me_o'], 6, signed=True):>6}  "
        f"{pct(m['me_c_pos'], 5):>5}  "
        f"{pct(m['avg_me_c'], 6, signed=True):>6}"
    )


def _metrics_header(label: str = "slice", label_w: int = 10) -> list[str]:
    header = (
        f"  {label:<{label_w}} "
        f"{'n':>6}  "
        f"{'hit%':>5}  "
        f"{'ROIo%':>7}  "
        f"{'ROIc%':>7}  "
        f"{'unitso':>8}  "
        f"{'unitsc':>8}  "
        f"{'CLV+%':>5}  "
        f"{'avgCLV':>6}  "
        f"{'MEo+%':>5}  "
        f"{'avgMEo':>6}  "
        f"{'MEc+%':>5}  "
        f"{'avgMEc':>6}"
    )
    return [header, "  " + "-" * (len(header) - 2)]


_EDGE_BANDS = (
    (">=10%", 0.10, None),
    ("5-10%", 0.05, 0.10),
    ("2-5%",  0.02, 0.05),
    ("0-2%",  0.0,  0.02),
)


def _render_breakdowns(df: pl.DataFrame, edge_col: str, lines: list[str]) -> None:
    """Append by-tier + by-edge-band breakdowns for `df` filtered on `edge_col > 0`."""
    if "cal_tier" in df.columns:
        lines.append("")
        lines.append("  By cal tier:")
        lines.extend(_metrics_header("tier"))
        for tier in _TIER_ORDER:
            sub = df.filter(pl.col("cal_tier") == tier)
            lines.append(_fmt_metrics_row(tier, _slice_metrics(sub)))
        seen = set(_TIER_ORDER)
        other_tiers = (
            df.filter(~pl.col("cal_tier").is_in(list(seen)))
            .get_column("cal_tier")
            .unique()
            .to_list()
        )
        for tier in other_tiers:
            if tier is None:
                continue
            sub = df.filter(pl.col("cal_tier") == tier)
            lines.append(_fmt_metrics_row(str(tier), _slice_metrics(sub)))
        lines.append(_fmt_metrics_row("ALL", _slice_metrics(df)))

    lines.append("")
    lines.append(f"  By edge band ({edge_col}):")
    lines.extend(_metrics_header("band"))
    for label, lo, hi in _EDGE_BANDS:
        if hi is None:
            sub = df.filter(pl.col(edge_col) >= lo)
        else:
            sub = df.filter((pl.col(edge_col) >= lo) & (pl.col(edge_col) < hi))
        lines.append(_fmt_metrics_row(label, _slice_metrics(sub)))
    lines.append(_fmt_metrics_row("ALL", _slice_metrics(df)))


def format_section_d(art: ModelArtifacts, cfg: dict) -> str:
    df = art.backtest
    lines = ["=" * 80, "D. BETTING OUTCOMES (backtest)", "=" * 80]

    # Scope = day after training end -> today
    train_end = cfg.get("data", {}).get("date_range", {}).get("end")
    scope_start = None
    if train_end is not None:
        if isinstance(train_end, (_dt.date, _dt.datetime)):
            scope_start = (train_end + _dt.timedelta(days=1)).isoformat()[:10]
        else:
            scope_start = (
                _dt.date.fromisoformat(str(train_end)) + _dt.timedelta(days=1)
            ).isoformat()

    if "effective_match_date" in df.columns and scope_start:
        df = df.filter(pl.col("effective_match_date") >= scope_start)

    if len(df) == 0:
        lines.append(f"  No rows in scope >= {scope_start}.")
        return "\n".join(lines)

    period_lo = str(df["effective_match_date"].min())[:10] if "effective_match_date" in df.columns else "?"
    period_hi = str(df["effective_match_date"].max())[:10] if "effective_match_date" in df.columns else "?"
    lines.append(f"  Period: {period_lo} -> {period_hi}    Rows in scope: {len(df):,}")

    # The CSV has two rows per match (one per side). The model only "bets"
    # the side it favors (model_prob > 0.5); the opponent-side row exists
    # in the CSV so we can see the loss it would have taken on the wrong
    # side, but it is not part of the model's evaluation. Restrict to the
    # model's chosen side before any edge filter.
    if "model_prob" in df.columns:
        df = df.filter(pl.col("model_prob") > 0.5)
    lines.append(f"  Model-side rows: {len(df):,}")

    # Add adj_edge = opening_edge + cell_cal column where cell_cal is present
    has_cell_cal = "cell_cal" in df.columns
    if has_cell_cal and "opening_edge" in df.columns:
        df = df.with_columns(
            (pl.col("opening_edge") + pl.col("cell_cal")).alias("adj_edge")
        )

    # Raw edge filter
    raw_df = df.filter(pl.col("opening_edge") > 0) if "opening_edge" in df.columns else df
    lines.append("")
    lines.append("-" * 80)
    lines.append(f"  RAW edge filter (opening_edge > 0)    N: {len(raw_df):,}")
    lines.append("-" * 80)
    _render_breakdowns(raw_df, "opening_edge", lines)

    # Cal-adjusted edge filter
    if has_cell_cal:
        with_cal = df.filter(pl.col("cell_cal").is_not_null())
        n_missing_cal = len(df) - len(with_cal)
        adj_df = with_cal.filter(pl.col("adj_edge") > 0)
        lines.append("")
        lines.append("-" * 80)
        header = f"  CAL-ADJUSTED edge filter (opening_edge + cell_cal > 0)    N: {len(adj_df):,}"
        if n_missing_cal > 0:
            header += f"  ({n_missing_cal:,} rows w/o cell_cal excluded)"
        lines.append(header)
        lines.append("-" * 80)
        _render_breakdowns(adj_df, "adj_edge", lines)

    # Monthly temporal slices of RAW filter — answers "is the edge consistent
    # across the window or front/back-loaded?" Cumulative columns show where
    # the totals come from row-by-row. Second block filters to Optimal tier
    # only so we can see whether temporal swings are concentrated in the
    # sizing-grade cells or driven by the rougher tiers.
    if "effective_match_date" in raw_df.columns and len(raw_df) > 0:
        lines.append("")
        lines.append("-" * 80)
        lines.append(f"  MONTHLY SLICES (RAW edge filter)    N: {len(raw_df):,}")
        lines.append("-" * 80)
        _render_monthly_slices(raw_df, lines)

        if "cal_tier" in raw_df.columns:
            opt_df = raw_df.filter(pl.col("cal_tier") == "Optimal")
            if len(opt_df) > 0:
                lines.append("")
                lines.append("-" * 80)
                lines.append(
                    f"  MONTHLY SLICES (Optimal tier only)    N: {len(opt_df):,}"
                )
                lines.append("-" * 80)
                _render_monthly_slices(opt_df, lines)

    return "\n".join(lines)


def _render_monthly_slices(df: pl.DataFrame, lines: list[str]) -> None:
    """Append a monthly breakdown table (n, hit%, units, ROI, CLV) + cumulative."""
    monthly = (
        df.with_columns(
            pl.col("effective_match_date").cast(pl.Utf8)
            .str.slice(0, 7).alias("_month")
        )
        .filter(pl.col("_month").is_not_null() & (pl.col("_month").str.len_chars() == 7))
    )
    months = sorted(m for m in monthly["_month"].unique().to_list() if m is not None)
    if not months:
        return

    header = (
        f"  {'month':<8}  {'n':>5}  {'hit%':>5}  "
        f"{'unitso':>8}  {'ROIo%':>7}  {'unitsc':>8}  {'ROIc%':>7}  "
        f"{'CLV+%':>5}  {'avgCLV':>7}  {'cum_o':>8}  {'cum_c':>8}"
    )
    lines.append("")
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))

    cum_o = 0.0
    cum_c = 0.0
    for m in months:
        sub = monthly.filter(pl.col("_month") == m)
        s = _slice_metrics(sub)
        if s["n"] == 0:
            continue
        pnl_o = float(s.get("pnl_o") or 0.0)
        pnl_c = float(s.get("pnl_c") or 0.0)
        cum_o += pnl_o
        cum_c += pnl_c
        hit = s.get("hit") or 0.0
        roi_o = s.get("roi_o") or 0.0
        roi_c = s.get("roi_c") or 0.0
        clv_pos = s.get("clv_pos") or 0.0
        avg_clv = s.get("avg_clv") or 0.0
        lines.append(
            f"  {m:<8}  {s['n']:>5}  {hit * 100:>4.1f}  "
            f"{pnl_o:>+7.1f}u  {roi_o * 100:>+6.2f}  "
            f"{pnl_c:>+7.1f}u  {roi_c * 100:>+6.2f}  "
            f"{clv_pos * 100:>4.1f}  {avg_clv * 100:>+6.2f}pp  "
            f"{cum_o:>+7.1f}u  {cum_c:>+7.1f}u"
        )


def run_report(config_path: Path, no_refresh: bool = False) -> str:
    """Refresh artifacts then format the four-section report. Returns the
    formatted report string. Print is the caller's responsibility.
    """
    model_name = config_path.stem
    if not no_refresh:
        refresh_pipeline(config_path)
    art = load_artifacts(model_name, config_path)
    cfg = _load_config(config_path)
    sections = [
        format_section_a(art, cfg),
        format_section_b(art),
        format_section_c(art),
        format_section_d(art, cfg),
    ]
    return "\n\n".join(sections)
