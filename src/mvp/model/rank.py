"""`mvp model-rank` — cross-model survey.

Walks `models/*.yaml` + `models/production/*.yaml`, runs per-model freshness
checks against artifact mtimes (config YAML vs the three downstream
artifacts), refreshes stale models, then prints four artifacts:

  Table 1: Static diagnostics
  Table 2: Confidence summary (per-circuit 12mo)
  Table 3: Backtest summary (positive bet-time edge, model-side only)
  Calibration matrix
  Regressed models (when any model regressed on Sev / Optimal% / LL)

Three sources, no source merging, no composite scores. See spec at
mvp-docs/specs/2026-05-17-model-evaluation-cli.md.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from mvp.common.base_job import get_data_root
from mvp.common.config_hash import (
    compute_fingerprint,
    fingerprint_dir,
)
from mvp.model.config import ExperimentConfig
from mvp.model.evaluation import (
    MLRUNS_DIR,
    backtest_path,
    confidence_path,
    fp_backtest_path,
    fp_confidence_path,
    fp_diagnostics_path,
    fingerprint_for,
    read_backtest_csv,
    refresh_pipeline,
)

logger = logging.getLogger(__name__)

MODEL_DIR = Path("models")
EXCLUDED_SUBDIRS = {"archive", "voters", "pair_combos", "pairwise"}

CIRCUITS = ("chal", "tour")
ROUND_SEGMENTS = ("Q1", "Q2", "Q3", "R128", "R64", "R32", "R16", "QF", "SF", "F")
SURFACE_SEGMENTS = ("Clay", "Hard")  # Grass dropped: low-n
MATRIX_SEGMENTS = (
    ("overall", "ov "),
    ("Clay",    "Cl "),
    ("Hard",    "Hd "),
    ("Q1",      "Q1 "),
    ("Q2",      "Q2 "),
    ("Q3",      "Q3 "),
    ("R128",    "128"),
    ("R64",     "R64"),
    ("R32",     "R32"),
    ("R16",     "R16"),
    ("QF",      "QF "),
    ("SF",      "SF "),
    ("F",       "F  "),
)
MATRIX_N_MIN = 100

SEV_THRESHOLD = 0.0005     # 0.05pp severity regression
OPTIMAL_THRESHOLD = 3.0    # 3pp Optimal% regression
LL_THRESHOLD = 0.001       # 0.001 LL regression


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_configs() -> list[Path]:
    """Return all model configs to evaluate.

    Top-level `models/*.yaml` plus `models/production/*.yaml`. Subdirs in
    EXCLUDED_SUBDIRS are skipped.
    """
    configs: list[Path] = []
    if not MODEL_DIR.exists():
        return configs
    for p in sorted(MODEL_DIR.glob("*.yaml")):
        configs.append(p)
    prod_dir = MODEL_DIR / "production"
    if prod_dir.exists():
        for p in sorted(prod_dir.glob("*.yaml")):
            configs.append(p)
    return configs


# ---------------------------------------------------------------------------
# Freshness check
# ---------------------------------------------------------------------------

def _latest_mlrun_json_mtime(model_name: str) -> float | None:
    """Most recent mlrun diagnostics JSON mtime for a model, or None if missing."""
    if not MLRUNS_DIR.exists():
        return None
    latest = None
    for exp_dir in MLRUNS_DIR.iterdir():
        if not exp_dir.is_dir() or exp_dir.name.startswith("."):
            continue
        for run_dir in exp_dir.iterdir():
            if not run_dir.is_dir():
                continue
            artifacts = run_dir / "artifacts"
            if not (artifacts / f"{model_name}.yaml").exists():
                continue
            for jp in artifacts.glob("*.json"):
                m = jp.stat().st_mtime
                if latest is None or m > latest:
                    latest = m
    return latest


@dataclass
class FreshnessResult:
    config_path: Path
    name: str
    fresh: bool
    reason: str
    missing: list[str] = field(default_factory=list)


def _newest_mtime(*paths: Path) -> float | None:
    out: float | None = None
    for p in paths:
        if p is None or not p.exists():
            continue
        m = p.stat().st_mtime
        if out is None or m > out:
            out = m
    return out


def check_freshness(config_path: Path) -> FreshnessResult:
    """Return whether all three artifacts exist and are newer than the config.

    Each artifact is checked at its fingerprint path first; if missing there,
    fall back to the legacy name-scoped path (transition).
    """
    name = config_path.stem
    cfg_mtime = config_path.stat().st_mtime

    try:
        fp_diag = fp_diagnostics_path(config_path)
        fp_conf = fp_confidence_path(config_path)
        fp_bt = fp_backtest_path(config_path)
    except Exception:
        fp_diag = fp_conf = fp_bt = None

    missing: list[str] = []
    diag_mtime = _newest_mtime(fp_diag) if fp_diag else None
    if diag_mtime is None:
        diag_mtime = _latest_mlrun_json_mtime(name)
    if diag_mtime is None:
        missing.append("diagnostics")

    conf_mtime = _newest_mtime(fp_conf) if fp_conf else None
    if conf_mtime is None:
        conf_mtime = _newest_mtime(confidence_path(name))
    if conf_mtime is None:
        missing.append("confidence")

    bt_mtime = _newest_mtime(fp_bt) if fp_bt else None
    if bt_mtime is None:
        bt_mtime = _newest_mtime(backtest_path(name))
    if bt_mtime is None:
        missing.append("backtest")

    if missing:
        return FreshnessResult(config_path, name, False, "missing artifacts", missing)

    if (
        diag_mtime < cfg_mtime
        or conf_mtime < cfg_mtime
        or bt_mtime < cfg_mtime
    ):
        return FreshnessResult(config_path, name, False, "config newer than artifacts")
    return FreshnessResult(config_path, name, True, "fresh")


# ---------------------------------------------------------------------------
# Per-model extraction
# ---------------------------------------------------------------------------

@dataclass
class ModelSummary:
    name: str
    config_path: Path
    run_id: str = ""
    run_ts: str = ""

    # Static (from diagnostics)
    n_total: int | None = None
    acc: float | None = None
    ll: float | None = None
    auc: float | None = None
    brier: float | None = None
    err80: float | None = None
    drift: float | None = None
    severity: float = 0.0
    signed_cal: float = 0.0
    underc_pct: float = 0.0
    optimal_pct: float = 0.0
    border_pct: float = 0.0
    risky_pct: float = 0.0
    danger_pct: float = 0.0
    cells: dict[tuple[str, str], tuple[float | None, int | None]] = field(default_factory=dict)

    # Confidence per-circuit 12mo
    chal_12mo_med: float | None = None
    chal_12mo_p25: float | None = None
    chal_12mo_p75: float | None = None
    tour_12mo_med: float | None = None
    tour_12mo_p25: float | None = None
    tour_12mo_p75: float | None = None

    # Backtest (positive bet-time edge, model-side only)
    bt_period_lo: str = ""
    bt_period_hi: str = ""
    bt_n: int = 0
    bt_hit: float | None = None
    bt_roi_o: float | None = None
    bt_roi_c: float | None = None
    bt_units_o: float | None = None
    bt_units_c: float | None = None
    bt_units_o_all: float | None = None  # consensus=1.0, no edge filter (vs bt_units_o which is edge>=0)
    bt_clv_pos: float | None = None
    bt_avg_clv: float | None = None
    bt_me_pos: float | None = None
    bt_avg_me: float | None = None


def _extract_cells(diag: dict) -> dict[tuple[str, str], tuple[float | None, int | None]]:
    """Return dict of (circuit, segment) -> (signed_cal, n_matches) from diagnostics.

    Pulls each (circuit, segment) cell from segments.by_circuit, keeping the
    same segment list the matrix uses.
    """
    cells: dict[tuple[str, str], tuple[float | None, int | None]] = {}
    by_circuit = diag.get("segments", {}).get("by_circuit", {})
    for circuit in CIRCUITS:
        circ = by_circuit.get(circuit, {})
        overall = circ.get("overall", {})
        cells[(circuit, "overall")] = (
            overall.get("signed_calibration"),
            overall.get("n_matches"),
        )
        for surf in SURFACE_SEGMENTS + ("Grass",):  # include Grass for matrix completeness
            cell = circ.get("surface", {}).get(surf, {})
            cells[(circuit, surf)] = (
                cell.get("signed_calibration"),
                cell.get("n_matches"),
            )
        for rnd in ROUND_SEGMENTS:
            cell = circ.get("round", {}).get(rnd, {})
            cells[(circuit, rnd)] = (
                cell.get("signed_calibration"),
                cell.get("n_matches"),
            )
    return cells


def _compute_tier_breakdown(cells: dict) -> dict[str, float]:
    """N-weighted tier percentages + severity + signed_cal over (circuit, round) cells.

    Mirrors review_models.py logic exactly. (circuit, round) is the clean
    partition (each match has exactly one round).
    """
    n_u = n_o = n_b = n_rk = n_d = 0
    n_round_total = 0
    signed_cal_weighted = 0.0
    danger_weight_sum = 0.0
    total_sqrt_n = 0.0
    for circuit in CIRCUITS:
        for rnd in ROUND_SEGMENTS:
            cal, n = cells.get((circuit, rnd), (None, None))
            if cal is None or n is None or n < MATRIX_N_MIN:
                continue
            n_round_total += n
            signed_cal_weighted += cal * n
            total_sqrt_n += math.sqrt(n)
            if cal < -0.01:
                n_d += n
                danger_weight_sum += abs(cal) * math.sqrt(n)
            elif cal < -0.005:
                n_rk += n
            elif cal < 0:
                n_b += n
            elif cal < 0.02:
                n_o += n
            else:
                n_u += n

    def pct(x: int) -> float:
        return (x / n_round_total * 100) if n_round_total else 0.0

    return {
        "underc_pct": pct(n_u),
        "optimal_pct": pct(n_o),
        "border_pct": pct(n_b),
        "risky_pct": pct(n_rk),
        "danger_pct": pct(n_d),
        "signed_cal": (signed_cal_weighted / n_round_total) if n_round_total else 0.0,
        "severity": (danger_weight_sum / total_sqrt_n) if total_sqrt_n > 0 else 0.0,
    }


def _headline_from_diagnostics(diag: dict) -> dict:
    """N-weighted chal + tour overall metrics."""
    by_c = diag.get("segments", {}).get("by_circuit", {})
    chal = by_c.get("chal", {}).get("overall", {})
    tour = by_c.get("tour", {}).get("overall", {})
    nc = chal.get("n_matches") or 0
    nt = tour.get("n_matches") or 0
    total = nc + nt

    def wavg(field: str) -> float | None:
        cv = chal.get(field)
        tv = tour.get(field)
        if cv is None and tv is None:
            return None
        if total == 0:
            return None
        return ((cv or 0) * nc + (tv or 0) * nt) / total

    return {
        "n_total": total or None,
        "acc": wavg("accuracy"),
        "ll": wavg("log_loss"),
        "auc": wavg("roc_auc"),
        "brier": wavg("brier_score"),
        "err80": wavg("error_rate_80plus"),
    }


def _all_fingerprints_for_source(source_name: str) -> list[dict]:
    """Walk `B:/model_evaluations/*/source.txt` for fp dirs whose source list
    includes `source_name`. For each, load the diagnostics.json there.

    Returns dicts shaped like `_all_mlruns_for_model` so downstream consumers
    can treat fp-historical entries and mlrun-historical entries uniformly:
    `{run_id, run_ts, diagnostics, mtime, fp, fp_dir}`. `run_id` is the
    12-char fingerprint (used to disambiguate historical entries when the
    same source_name produced multiple distinct fingerprints).
    """
    out: list[dict] = []
    fp_root = get_data_root() / "model_evaluations"
    if not fp_root.exists():
        return out
    for fp_dir_p in fp_root.iterdir():
        if not fp_dir_p.is_dir():
            continue
        source_path = fp_dir_p / "source.txt"
        diag_path = fp_dir_p / "diagnostics.json"
        if not source_path.exists() or not diag_path.exists():
            continue
        names = set()
        try:
            for line in source_path.read_text(encoding="utf-8").splitlines():
                parts = line.split("\t")
                if parts and parts[0]:
                    names.add(parts[0])
        except OSError:
            continue
        if source_name not in names:
            continue
        try:
            with open(diag_path) as f:
                diag = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        if "segments" not in diag:
            continue
        mtime = diag_path.stat().st_mtime
        out.append({
            "run_id": fp_dir_p.name[:8],
            "run_ts": _dt.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M"),
            "diagnostics": diag,
            "mtime": mtime,
            "fp": fp_dir_p.name,
            "fp_dir": fp_dir_p,
        })
    return out


def _all_mlruns_for_model(model_name: str) -> list[dict]:
    """Return list of {run_id, run_ts, diagnostics_path, diagnostics, mtime} for every
    historical run of a model. Used by the regression view and the leader
    table, which need historical runs not just the latest.

    Two record types exist for the same training: an mlflow run under
    `mlruns/<exp>/<hash>/` (diagnostics only — backtests are never logged
    there) and a fingerprint dir under `model_evaluations/<fp>/` (diagnostics
    + confidence + backtest, keyed by config content-hash). A single
    `model-report` produces several mlflow hashes for one config (training
    pass + the backtest stage's own retrain), all mapping to one fingerprint.

    Fingerprint entries are preferred: they carry every dataset, so historical
    rows show under their content-hash with backtest data intact. An mlflow run
    is only included when it has no fingerprint-dir twin (matched by full
    fingerprint) — i.e. a genuine orphan whose fp dir was pruned/never written.
    """
    # Fingerprint-dir entries first — these carry all datasets.
    out: list[dict] = list(_all_fingerprints_for_source(model_name))
    fp_fingerprints = {e["fp"] for e in out if e.get("fp")}

    if MLRUNS_DIR.exists():
        for exp_dir in MLRUNS_DIR.iterdir():
            if not exp_dir.is_dir() or exp_dir.name.startswith("."):
                continue
            for run_dir in exp_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                artifacts = run_dir / "artifacts"
                yaml_artifact = artifacts / f"{model_name}.yaml"
                if not yaml_artifact.exists():
                    continue
                json_files = list(artifacts.glob("*.json"))
                if not json_files:
                    continue
                json_path = max(json_files, key=lambda p: p.stat().st_mtime)
                try:
                    with open(json_path) as f:
                        diag = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                if "segments" not in diag:
                    continue
                # Compute the fingerprint of this mlrun's stored YAML. If a
                # fingerprint dir already covers it, that dir is the fuller
                # record (it has the backtest) — skip the mlflow copy so the
                # run shows under its content-hash, not a stray mlflow hash.
                run_fp: str | None = None
                try:
                    run_cfg = ExperimentConfig.from_file(str(yaml_artifact))
                    run_fp = compute_fingerprint(run_cfg, config_path=yaml_artifact)
                except Exception:
                    pass
                if run_fp is not None and run_fp in fp_fingerprints:
                    continue
                mtime = json_path.stat().st_mtime
                out.append({
                    "run_id": run_dir.name[:8],
                    "run_ts": _dt.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M"),
                    "diagnostics": diag,
                    "mtime": mtime,
                    "fp": run_fp,
                })
    return out


def _summary_from_diagnostics(diag: dict) -> dict:
    """Compute everything table-ready from a single diagnostics dict."""
    cells = _extract_cells(diag)
    head = _headline_from_diagnostics(diag)
    breakdown = _compute_tier_breakdown(cells)
    drift = diag.get("temporal", {}).get("temporal_drift")
    return {
        "cells": cells,
        "n_total": head["n_total"],
        "acc": head["acc"],
        "ll": head["ll"],
        "auc": head["auc"],
        "brier": head["brier"],
        "err80": head["err80"],
        "drift": drift,
        **breakdown,
    }


def _extract_confidence(model_name: str, config_path: Path | None = None) -> dict:
    """Return per-circuit 12mo summary dict.

    Prefers the fp-scoped path when `config_path` is given; falls back to
    legacy name-scoped path.
    """
    out = {
        "chal_12mo_med": None, "chal_12mo_p25": None, "chal_12mo_p75": None,
        "tour_12mo_med": None, "tour_12mo_p25": None, "tour_12mo_p75": None,
    }
    p: Path | None = None
    if config_path is not None:
        try:
            cand = fp_confidence_path(config_path)
            if cand.exists():
                p = cand
        except Exception:
            p = None
    if p is None:
        legacy = confidence_path(model_name)
        if legacy.exists():
            p = legacy
    if p is None:
        return out
    with open(p) as f:
        data = json.load(f)
    profiles = data.get("profiles", {})
    for circuit in CIRCUITS:
        prof = profiles.get(f"circuit:{circuit}", {}).get("overall", {})
        c12 = prof.get("cal_12mo", {})
        out[f"{circuit}_12mo_med"] = c12.get("median")
        out[f"{circuit}_12mo_p25"] = c12.get("p25")
        out[f"{circuit}_12mo_p75"] = c12.get("p75")
    return out


def _extract_backtest(
    model_name: str,
    train_end: str | _dt.date | None,
    config_path: Path | None = None,
) -> dict:
    """Compute model-eval backtest aggregates: positive bet-time edge, model-side only.

    Prefers the fp-scoped backtest.csv when `config_path` is given; falls back
    to legacy `B:/backtests/lead/<name>.csv`.
    """
    out = {
        "bt_period_lo": "", "bt_period_hi": "", "bt_n": 0,
        "bt_hit": None, "bt_roi_o": None, "bt_roi_c": None,
        "bt_units_o": None, "bt_units_c": None,
        "bt_units_o_all": None,
        "bt_clv_pos": None, "bt_avg_clv": None,
        "bt_me_pos": None, "bt_avg_me": None,
    }
    p: Path | None = None
    if config_path is not None:
        try:
            cand = fp_backtest_path(config_path)
            if cand.exists():
                p = cand
        except Exception:
            p = None
    if p is None:
        legacy = backtest_path(model_name)
        if legacy.exists():
            p = legacy
    if p is None:
        return out
    df = read_backtest_csv(p)

    # Scope = day after training end -> today
    if train_end is not None and "effective_match_date" in df.columns:
        if isinstance(train_end, (_dt.date, _dt.datetime)):
            scope_start = (train_end + _dt.timedelta(days=1)).isoformat()[:10]
        else:
            scope_start = (
                _dt.date.fromisoformat(str(train_end)) + _dt.timedelta(days=1)
            ).isoformat()
        df = df.filter(pl.col("effective_match_date") >= scope_start)

    # Apply user's betting filters: consensus=1.0 (lead+voter agree) and model
    # is on this side (prob > 0.5). The edge filter is applied *after* the
    # "_all" snapshot is taken, so bt_units_o_all reflects consensus picks
    # without the edge gate (matches user's "all" lens).
    if "consensus" in df.columns:
        df = df.filter(pl.col("consensus") == 1.0)
    if "model_prob" in df.columns:
        df = df.filter(pl.col("model_prob") > 0.5)
    if "pnl_open" in df.columns:
        out["bt_units_o_all"] = df["pnl_open"].sum()
    if "opening_edge" in df.columns:
        df = df.filter(pl.col("opening_edge") >= 0)

    n = len(df)
    if n == 0:
        return out

    out["bt_n"] = n
    if "effective_match_date" in df.columns:
        out["bt_period_lo"] = str(df["effective_match_date"].min())[:10]
        out["bt_period_hi"] = str(df["effective_match_date"].max())[:10]
    if "won" in df.columns:
        out["bt_hit"] = df["won"].mean()
    if "pnl_open" in df.columns:
        out["bt_units_o"] = df["pnl_open"].sum()
        out["bt_roi_o"] = out["bt_units_o"] / n
    if "pnl_close" in df.columns:
        out["bt_units_c"] = df["pnl_close"].sum()
        out["bt_roi_c"] = out["bt_units_c"] / n
    if "clv" in df.columns:
        out["bt_clv_pos"] = (df["clv"] > 0).mean()
        out["bt_avg_clv"] = df["clv"].mean()
    if "closing_edge" in df.columns:
        out["bt_me_pos"] = (df["closing_edge"] > 0).mean()
        out["bt_avg_me"] = df["closing_edge"].mean()
    return out


def _load_train_end(config_path: Path) -> str | None:
    import yaml

    with open(config_path) as f:
        cfg = yaml.safe_load(f) or {}
    return (cfg.get("data") or {}).get("date_range", {}).get("end")


def extract_summary(config_path: Path) -> ModelSummary | None:
    """Build a full ModelSummary from a model's three artifacts.

    Returns None if the diagnostics artifact is missing (model never trained).
    """
    name = config_path.stem
    runs = _all_mlruns_for_model(name)
    if not runs:
        return None
    latest = max(runs, key=lambda r: r["mtime"])
    diag = latest["diagnostics"]
    diag_summary = _summary_from_diagnostics(diag)
    conf = _extract_confidence(name, config_path=config_path)
    train_end = _load_train_end(config_path)
    bt = _extract_backtest(name, train_end, config_path=config_path)

    summary = ModelSummary(name=name, config_path=config_path)
    summary.run_id = latest["run_id"]
    summary.run_ts = latest["run_ts"]
    for k, v in diag_summary.items():
        setattr(summary, k, v)
    for k, v in conf.items():
        setattr(summary, k, v)
    for k, v in bt.items():
        setattr(summary, k, v)
    return summary


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _fmt_n(n: int | float | None) -> str:
    if n is None or n <= 0:
        return ""
    if n >= 1000:
        return f"{int(round(n / 1000))}K"
    return str(int(n))


def _tier_symbol(cal: float | None, n: int | None) -> str:
    if cal is None or n is None or n < MATRIX_N_MIN:
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


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

LEADER_TOP_PCT = 0.33  # per-axis cut = this fraction of each axis's eligible pool


@dataclass
class LeaderCandidate:
    """One entry in the leader-table candidate pool.

    Latest-run entries inherit confidence + backtest data from their
    ModelSummary. Historical entries from fingerprint dirs have their own
    confidence + backtest (so they compete on all three axes). Historical
    entries from mlruns-only sources have no fp-scoped confidence/backtest
    and so compete on the two calibration axes only.
    """
    name: str  # model name
    run_id: str  # 8-char identifier (mlrun hash or fp prefix)
    is_latest: bool
    signed_cal: float | None
    optimal_pct: float | None
    bt_clv_pos: float | None  # None when no fp-scoped backtest
    bt_avg_clv: float | None
    bt_units_o: float | None  # net units at open (consensus + edge>=0); None when no fp-scoped backtest
    bt_units_o_all: float | None  # net units at open (consensus only, no edge filter)

    @property
    def label(self) -> str:
        if self.is_latest:
            return self.name
        return f"{self.name} ({self.run_id})"


def _confidence_at(fp_dir: Path) -> dict:
    """Read validation_results.json from an fp dir, return per-circuit 12mo summary."""
    out = {
        "chal_12mo_med": None, "chal_12mo_p25": None, "chal_12mo_p75": None,
        "tour_12mo_med": None, "tour_12mo_p25": None, "tour_12mo_p75": None,
    }
    p = fp_dir / "validation_results.json"
    if not p.exists():
        return out
    try:
        with open(p) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return out
    profiles = data.get("profiles", {})
    for circuit in CIRCUITS:
        prof = profiles.get(f"circuit:{circuit}", {}).get("overall", {})
        c12 = prof.get("cal_12mo", {})
        out[f"{circuit}_12mo_med"] = c12.get("median")
        out[f"{circuit}_12mo_p25"] = c12.get("p25")
        out[f"{circuit}_12mo_p75"] = c12.get("p75")
    return out


def _backtest_stats_at(
    fp_dir: Path,
) -> tuple[float | None, float | None, float | None, float | None]:
    """Return (bt_clv_pos, bt_avg_clv, bt_units_o, bt_units_o_all) from an
    fp dir's backtest.csv.

    Filters: consensus==1.0 + model_prob>0.5 for all metrics; an additional
    opening_edge>=0 gate for the standard set. ``bt_units_o_all`` is the
    consensus-side units WITHOUT the edge gate (the "all" lens).

    All-None when the file is missing/unreadable.
    """
    p = fp_dir / "backtest.csv"
    if not p.exists():
        return None, None, None, None
    try:
        df = read_backtest_csv(p)
    except Exception:
        return None, None, None, None
    if "consensus" in df.columns:
        df = df.filter(pl.col("consensus") == 1.0)
    if "model_prob" in df.columns:
        df = df.filter(pl.col("model_prob") > 0.5)
    units_o_all = df["pnl_open"].sum() if "pnl_open" in df.columns else None
    if "opening_edge" in df.columns:
        df = df.filter(pl.col("opening_edge") >= 0)
    if len(df) == 0:
        return None, None, None, units_o_all
    clv_pos = (df["clv"] > 0).mean() if "clv" in df.columns else None
    clv_avg = df["clv"].mean() if "clv" in df.columns else None
    units_o = df["pnl_open"].sum() if "pnl_open" in df.columns else None
    return clv_pos, clv_avg, units_o, units_o_all


def _build_leader_candidates(summaries: list[ModelSummary]) -> list[LeaderCandidate]:
    """Latest-run candidates + historical bests on each axis (where not latest).

    Historical fp-dir entries carry their own confidence/backtest data and so
    compete on all three axes. Historical mlrun-only entries compete on C+O.

    fp-dir entries whose fp matches the current YAML's fp are excluded — that
    fp IS the latest (its diagnostics is the mirror of the latest training),
    so including it would double-count the latest row.
    """
    candidates: list[LeaderCandidate] = []
    for s in summaries:
        candidates.append(LeaderCandidate(
            name=s.name, run_id=s.run_id, is_latest=True,
            signed_cal=s.signed_cal, optimal_pct=s.optimal_pct,
            bt_clv_pos=s.bt_clv_pos, bt_avg_clv=s.bt_avg_clv,
            bt_units_o=s.bt_units_o,
            bt_units_o_all=s.bt_units_o_all,
        ))
        try:
            current_fp = fingerprint_for(s.config_path)
        except Exception:
            current_fp = None
        # Historical runs for this model name. fp-dir entries carry an `fp_dir`
        # key; mlrun-only entries do not.
        runs = _all_mlruns_for_model(s.name)
        if len(runs) <= 1:
            continue
        latest = max(runs, key=lambda r: r["mtime"])
        per_run = []
        for r in runs:
            if r["run_id"] == latest["run_id"]:
                continue
            if current_fp is not None and r.get("fp") == current_fp:
                continue
            ds = _summary_from_diagnostics(r["diagnostics"])
            per_run.append((r, ds))
        if not per_run:
            continue
        # Best historical on |signed_cal|, Optimal%, CLV+%, and Uo>=0 (fp only)
        best_cal = min(per_run, key=lambda x: abs(x[1]["signed_cal"]))
        best_opt = max(per_run, key=lambda x: x[1]["optimal_pct"])
        # CLV and Uo bests are only meaningful for fp-dir entries — precompute
        # backtest stats per fp_run once so neither axis selection nor the
        # construction step re-reads backtest.csv.
        fp_runs = [(r, ds) for r, ds in per_run if "fp_dir" in r]
        fp_stats: dict[str, tuple] = {
            r["run_id"]: _backtest_stats_at(r["fp_dir"]) for r, _ in fp_runs
        }
        bests: list[tuple[dict, dict]] = [best_cal, best_opt]
        if fp_runs:
            best_clv = max(
                fp_runs,
                key=lambda x: (fp_stats[x[0]["run_id"]][0] or -1),
            )
            bests.append(best_clv)
            best_units = max(
                fp_runs,
                key=lambda x: (
                    fp_stats[x[0]["run_id"]][2]
                    if fp_stats[x[0]["run_id"]][2] is not None else -1e18
                ),
            )
            bests.append(best_units)
        seen_ids: set[str] = set()
        for r, ds in bests:
            if r["run_id"] in seen_ids:
                continue
            seen_ids.add(r["run_id"])
            clv_pos = clv_avg = units_o = units_o_all = None
            if r["run_id"] in fp_stats:
                clv_pos, clv_avg, units_o, units_o_all = fp_stats[r["run_id"]]
            candidates.append(LeaderCandidate(
                name=s.name, run_id=r["run_id"], is_latest=False,
                signed_cal=ds["signed_cal"], optimal_pct=ds["optimal_pct"],
                bt_clv_pos=clv_pos, bt_avg_clv=clv_avg,
                bt_units_o=units_o, bt_units_o_all=units_o_all,
            ))
    return candidates


def render_leader_table(summaries: list[ModelSummary]) -> str:
    """Cross-axis leader table.

    Four ranking axes:
      - C: abs(signed_cal) asc   (calibration magnitude)
      - O: Optimal% desc         (calibration volume)
      - B: CLV+% desc            (market beat rate; fp-scoped backtest only)
      - U: Uo>=0 desc            (net units booked under bet rule; fp-scoped backtest only)

    Latest-run candidates compete on all four axes. Historical-best
    candidates from fingerprint dirs (which carry their own
    validation_results + backtest CSVs) compete on all four axes too.
    Historical mlrun-only candidates compete on C and O only — they have no
    fp-scoped confidence/backtest artifacts.

    Models appearing in 2+ axes float to the top. Within the same #-of-axes,
    ties broken by sum of ranks across axes (lower = better overall position).
    Historical candidates are labelled `<name> (<run_id>)`.
    """
    candidates = _build_leader_candidates(summaries)

    def rank_asc(seq, key, eligible_filter=lambda c: True):
        elig = [c for c in seq if eligible_filter(c)]
        return sorted(elig, key=key)

    cal_full = rank_asc(
        candidates,
        key=lambda c: abs(c.signed_cal),
        eligible_filter=lambda c: c.signed_cal is not None,
    )
    opt_full = rank_asc(
        candidates,
        key=lambda c: -c.optimal_pct,
        eligible_filter=lambda c: c.optimal_pct is not None,
    )
    clv_full = rank_asc(
        candidates,
        key=lambda c: -(c.bt_clv_pos or -1),
        eligible_filter=lambda c: c.bt_clv_pos is not None,
    )
    units_full = rank_asc(
        candidates,
        # Sort by Uo>=0 desc; eligibility guard means we never hit None here.
        key=lambda c: -c.bt_units_o,
        eligible_filter=lambda c: c.bt_units_o is not None,
    )
    def _cut(n: int) -> int:
        return max(1, round(LEADER_TOP_PCT * n))

    cal_cut, opt_cut, clv_cut, units_cut = (
        _cut(len(cal_full)), _cut(len(opt_full)),
        _cut(len(clv_full)), _cut(len(units_full)),
    )
    cal_ranked = cal_full[:cal_cut]
    opt_ranked = opt_full[:opt_cut]
    clv_ranked = clv_full[:clv_cut]
    units_ranked = units_full[:units_cut]

    # Per-axis cut is a fraction of each axis's eligible pool (B and U are
    # smaller: only candidates with an fp-scoped backtest carry CLV / units),
    # so the eliteness of the cut is equal across axes even as pools differ.
    pct_label = f"{LEADER_TOP_PCT*100:.0f}%"
    pool_line = (
        f"  Pool: {len(candidates)} candidates — top {pct_label} per axis "
        f"(C: {cal_cut} of {len(cal_full)}, O: {opt_cut} of {len(opt_full)}, "
        f"B: {clv_cut} of {len(clv_full)}, U: {units_cut} of {len(units_full)})"
    )

    # Use (name, run_id) as the candidate key
    axis_ranks: dict[tuple[str, str], dict[str, int]] = {}
    for i, c in enumerate(cal_ranked):
        axis_ranks.setdefault((c.name, c.run_id), {})["C"] = i + 1
    for i, c in enumerate(opt_ranked):
        axis_ranks.setdefault((c.name, c.run_id), {})["O"] = i + 1
    for i, c in enumerate(clv_ranked):
        axis_ranks.setdefault((c.name, c.run_id), {})["B"] = i + 1
    for i, c in enumerate(units_ranked):
        axis_ranks.setdefault((c.name, c.run_id), {})["U"] = i + 1

    multi_axis = {key: ranks for key, ranks in axis_ranks.items() if len(ranks) >= 2}
    if not multi_axis:
        return (
            "=" * 80
            + f"\nCross-axis leaders (top {pct_label} per axis; 2+ axes shown)\n"
            + "=" * 80
            + f"\n{pool_line}"
            + "\n  No models appear in 2+ axis top-lists."
        )

    by_key = {(c.name, c.run_id): c for c in candidates}
    rows = []
    for key, ranks in multi_axis.items():
        n_axes = len(ranks)
        sum_rank = sum(ranks.values())
        axes_label = "+".join(sorted(ranks.keys()))
        rows.append((n_axes, sum_rank, key, ranks, axes_label))
    rows.sort(key=lambda r: (-r[0], r[1], r[2]))

    lines = [
        "=" * 80,
        f"Cross-axis leaders (top {pct_label} per axis; 2+ axes shown)",
        "=" * 80,
        pool_line,
        "  Axes: C=calibration (abs signed_cal asc), O=Optimal% desc, B=CLV+% desc, U=Uo>=0 desc",
        "  Entries marked `<name> (<run_id>)` are historical bests (latest run regressed).",
        "  fp-dir historical entries compete on all four axes; mlrun-only entries on C and O only.",
    ]
    header = (
        f"  {'Model':<66} {'Axes':>8} {'SCal%':>7} {'Opt%':>6} {'CLV+%':>6} {'avgCLV':>7} {'Uo>=0':>7} {'Uo_all':>8}"
    )
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for n_axes, sum_rank, key, ranks, axes_label in rows:
        c = by_key[key]
        scal = f"{c.signed_cal*100:+.2f}" if c.signed_cal is not None else "--"
        opt = f"{c.optimal_pct:.1f}" if c.optimal_pct is not None else "--"
        clv_pos = f"{c.bt_clv_pos*100:.1f}" if c.bt_clv_pos is not None else "--"
        avg_clv = f"{c.bt_avg_clv*100:+.2f}" if c.bt_avg_clv is not None else "--"
        units_o = f"{c.bt_units_o:+.1f}" if c.bt_units_o is not None else "--"
        units_o_all = f"{c.bt_units_o_all:+.1f}" if c.bt_units_o_all is not None else "--"
        lines.append(
            f"  {c.label[:66]:<66} {axes_label:>8} {scal:>7} {opt:>6} {clv_pos:>6} {avg_clv:>7} {units_o:>7} {units_o_all:>8}"
        )
    return "\n".join(lines)


def render_static_table(summaries: list[ModelSummary]) -> str:
    """Table 1 — Static diagnostics. Sorted by Optimal% desc."""
    rows = sorted(summaries, key=lambda s: -s.optimal_pct)
    lines = ["=" * 80, "Table 1: Static diagnostics (sorted by Optimal% desc)", "=" * 80]
    header = (
        f"{'Model':<50} {'run_ts':<16} {'id':<8} "
        f"{'Acc':>6} {'LL':>7} "
        f"{'Sev%':>5} {'SCal%':>6} "
        f"{'UndC%':>6} {'Opt%':>6} {'Brd%':>6} {'Rsk%':>6} {'Dng%':>6} "
        f"{'Drift':>6} {'Err80%':>6}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for s in rows:
        def f(x, w, prec=4, sign=False):
            if x is None:
                return f"{'--':>{w}}"
            fmt = f"+{w}.{prec}f" if sign else f"{w}.{prec}f"
            return f"{x:{fmt}}"
        lines.append(
            f"{s.name[:50]:<50} {s.run_ts:<16} {s.run_id:<8} "
            f"{f(s.acc, 6)} {f(s.ll, 7)} "
            f"{s.severity*100:>5.2f} {s.signed_cal*100:>+6.2f} "
            f"{s.underc_pct:>5.1f}% {s.optimal_pct:>5.1f}% "
            f"{s.border_pct:>5.1f}% {s.risky_pct:>5.1f}% {s.danger_pct:>5.1f}% "
            f"{(s.drift*100 if s.drift is not None else 0):>+6.2f} "
            f"{(s.err80*100 if s.err80 is not None else 0):>5.1f}%"
        )
    return "\n".join(lines)


def render_confidence_table(summaries: list[ModelSummary]) -> str:
    """Table 2 — Confidence summary, per-circuit 12mo med/p25/p75."""
    rows = sorted(summaries, key=lambda s: -s.optimal_pct)
    lines = ["=" * 80, "Table 2: Confidence summary (12mo rolling, per-circuit, signed pp)", "=" * 80]
    header = (
        f"{'Model':<50} "
        f"{'chal med':>9} {'chal p25':>9} {'chal p75':>9}   "
        f"{'tour med':>9} {'tour p25':>9} {'tour p75':>9}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    def f(x):
        if x is None:
            return f"{'--':>9}"
        return f"{x*100:>+9.2f}"

    for s in rows:
        lines.append(
            f"{s.name[:50]:<50} "
            f"{f(s.chal_12mo_med)} {f(s.chal_12mo_p25)} {f(s.chal_12mo_p75)}   "
            f"{f(s.tour_12mo_med)} {f(s.tour_12mo_p25)} {f(s.tour_12mo_p75)}"
        )
    return "\n".join(lines)


def render_backtest_table(summaries: list[ModelSummary]) -> str:
    """Table 3 — Backtest summary (positive bet-time edge, model-side only)."""
    rows = sorted(summaries, key=lambda s: -s.optimal_pct)
    lines = ["=" * 80, "Table 3: Backtest summary (positive bet-time edge, model-side only)", "=" * 80]
    header = (
        f"{'Model':<50} {'Period':<23} "
        f"{'N':>5} {'Hit%':>5} "
        f"{'ROIo%':>6} {'ROIc%':>6} {'Uo':>7} {'Uc':>7} "
        f"{'CLV+%':>5} {'avgCLV':>6} {'ME+%':>5} {'avgME':>6}"
    )
    lines.append(header)
    lines.append("-" * len(header))
    for s in rows:
        period = (
            f"{s.bt_period_lo} -> {s.bt_period_hi}"
            if s.bt_period_lo and s.bt_period_hi
            else "(no data)"
        )
        def f(x, w, prec=2, signed=False):
            if x is None:
                return f"{'--':>{w}}"
            fmt = f"+{w}.{prec}f" if signed else f"{w}.{prec}f"
            return f"{x:{fmt}}"
        lines.append(
            f"{s.name[:50]:<50} {period[:23]:<23} "
            f"{s.bt_n:>5} "
            f"{(s.bt_hit*100 if s.bt_hit is not None else 0):>5.1f} "
            f"{(s.bt_roi_o*100 if s.bt_roi_o is not None else 0):>+6.2f} "
            f"{(s.bt_roi_c*100 if s.bt_roi_c is not None else 0):>+6.2f} "
            f"{(s.bt_units_o if s.bt_units_o is not None else 0):>+7.1f} "
            f"{(s.bt_units_c if s.bt_units_c is not None else 0):>+7.1f} "
            f"{(s.bt_clv_pos*100 if s.bt_clv_pos is not None else 0):>5.1f} "
            f"{(s.bt_avg_clv*100 if s.bt_avg_clv is not None else 0):>+6.2f} "
            f"{(s.bt_me_pos*100 if s.bt_me_pos is not None else 0):>5.1f} "
            f"{(s.bt_avg_me*100 if s.bt_avg_me is not None else 0):>+6.2f}"
        )
    return "\n".join(lines)


def _render_matrix_rows(rows: list[ModelSummary], name_w: int = 50, label_fn=None) -> list[str]:
    """Tier-symbol matrix: one row per model, columns = (circuit, segment)."""
    label_fn = label_fn or (lambda s: s.name)

    # Max n per (circuit, segment) across all rows, for the n-row at the top
    n_by_cell: dict[tuple[str, str], int] = {}
    for s in rows:
        for k, (_, n) in s.cells.items():
            if n is None:
                continue
            n_by_cell[k] = max(n_by_cell.get(k, 0), n)

    col_w = 5
    band_w = len(MATRIX_SEGMENTS) * col_w
    band_sep = " | "

    circ_line = " " * name_w
    seg_line = " " * name_w
    n_line = " " * name_w
    for i, circuit in enumerate(CIRCUITS):
        if i > 0:
            circ_line += band_sep
            seg_line += band_sep
            n_line += band_sep
        circ_line += circuit.center(band_w)
        for seg, label in MATRIX_SEGMENTS:
            seg_line += f"{label:>4} "
            n_line += f"{_fmt_n(n_by_cell.get((circuit, seg), 0)):>4} "

    out = [circ_line, seg_line, n_line, "-" * (name_w + len(CIRCUITS) * band_w + (len(CIRCUITS) - 1) * len(band_sep))]
    for s in rows:
        label = label_fn(s)[:name_w]
        line = f"{label:<{name_w}}"
        for i, circuit in enumerate(CIRCUITS):
            if i > 0:
                line += band_sep
            for seg, _ in MATRIX_SEGMENTS:
                cal, n = s.cells.get((circuit, seg), (None, None))
                line += f"   {_tier_symbol(cal, n)} "
        out.append(line)
    return out


def render_matrix(summaries: list[ModelSummary], top_n: int = 40) -> str:
    """Calibration matrix, top_n by severity (lower = safer first)."""
    ordered = sorted(summaries, key=lambda s: (s.severity, s.ll if s.ll is not None else 999))
    lines = [
        "=" * 80,
        f"Calibration matrix (latest run per model, top {min(top_n, len(ordered))} by severity, n_min={MATRIX_N_MIN})",
        "=" * 80,
        "Legend: + 0..+2%  . -0.5..0%  , -1..-0.5%  X <-1%  ^ >=+2%  blank: n<n_min",
    ]
    lines.extend(_render_matrix_rows(ordered[:top_n]))
    return "\n".join(lines)


def render_regressions(summaries: list[ModelSummary]) -> str | None:
    """Per-axis regression detection. Compares each model's latest mlrun against
    its own historical best on Sev%, Optimal%, and LL. Returns None if no
    regressions.
    """
    regressions: list[tuple[ModelSummary, list[tuple]]] = []
    for s in summaries:
        runs = _all_mlruns_for_model(s.name)
        if len(runs) <= 1:
            continue
        # Build per-run summaries
        per_run: list[tuple[str, str, dict, float]] = []
        for r in runs:
            ds = _summary_from_diagnostics(r["diagnostics"])
            per_run.append((r["run_id"], r["run_ts"], ds, r["mtime"]))
        latest_run = max(per_run, key=lambda t: t[3])
        latest_id, _, latest_ds, _ = latest_run

        best_sev = min(per_run, key=lambda t: t[2]["severity"])
        best_opt = max(per_run, key=lambda t: t[2]["optimal_pct"])
        best_ll = min(per_run, key=lambda t: t[2]["ll"] if t[2]["ll"] is not None else 999)

        axes: list[tuple] = []
        if best_sev[0] != latest_id and (latest_ds["severity"] - best_sev[2]["severity"]) >= SEV_THRESHOLD:
            axes.append(("Sev",
                         f"{latest_ds['severity']*100:.2f}",
                         f"{best_sev[2]['severity']*100:.2f}",
                         best_sev))
        if best_opt[0] != latest_id and (best_opt[2]["optimal_pct"] - latest_ds["optimal_pct"]) >= OPTIMAL_THRESHOLD:
            axes.append(("Optimal%",
                         f"{latest_ds['optimal_pct']:.1f}%",
                         f"{best_opt[2]['optimal_pct']:.1f}%",
                         best_opt))
        if (
            best_ll[0] != latest_id
            and latest_ds["ll"] is not None
            and best_ll[2]["ll"] is not None
            and (latest_ds["ll"] - best_ll[2]["ll"]) >= LL_THRESHOLD
        ):
            axes.append(("LL",
                         f"{latest_ds['ll']:.4f}",
                         f"{best_ll[2]['ll']:.4f}",
                         best_ll))

        if axes:
            regressions.append((s, axes))

    if not regressions:
        return None

    regressions.sort(key=lambda x: (-len(x[1]), x[0].name))
    lines = [
        "=" * 80,
        "Regressed models (latest worse than own historical best on at least one axis)",
        f"Thresholds: Sev >= {SEV_THRESHOLD*100:.2f}pp | Optimal% drop >= {OPTIMAL_THRESHOLD:.1f}pp | LL >= {LL_THRESHOLD:.4f}",
        "=" * 80,
    ]
    for s, axes in regressions:
        lines.append(f"  {s.name} (latest {s.run_id}, {s.run_ts}):")
        for axis_name, latest_v, best_v, best_run in axes:
            lines.append(f"    {axis_name}: {latest_v} now vs {best_v} best ({best_run[0]}, {best_run[1]})")

    # Historical-best matrix rows
    _AXIS_SHORT = {"Sev": "Sev", "Optimal%": "Opt", "LL": "LL"}
    hist_summaries: list[ModelSummary] = []
    for s, axes in regressions:
        by_run: dict[str, tuple[dict, list[str]]] = {}
        for axis_name, _, _, best_run in axes:
            short = _AXIS_SHORT.get(axis_name, axis_name)
            best_id = best_run[0]
            if best_id not in by_run:
                by_run[best_id] = (best_run, [])
            by_run[best_id][1].append(short)
        for best_id, (best_run, axis_list) in by_run.items():
            run_id, run_ts, ds, _ = best_run
            hist = ModelSummary(name=s.name, config_path=s.config_path)
            hist.run_id = run_id
            hist.run_ts = run_ts
            hist.cells = ds["cells"]
            hist.severity = ds["severity"]
            hist.signed_cal = ds["signed_cal"]
            hist.optimal_pct = ds["optimal_pct"]
            hist.ll = ds["ll"]
            # Stash for label
            setattr(hist, "_axis_label", "+".join(axis_list))
            hist_summaries.append(hist)

    lines.append("")
    lines.extend(_render_matrix_rows(
        hist_summaries,
        name_w=66,
        label_fn=lambda s: f"{s.name} ({s.run_id} best-{getattr(s, '_axis_label', '?')})",
    ))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

@dataclass
class RefreshStats:
    refreshed: list[str] = field(default_factory=list)
    skipped_fresh: list[str] = field(default_factory=list)
    missing_artifacts: list[tuple[str, list[str]]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


def orchestrate_refresh(
    configs: list[Path],
    *,
    force_refresh: bool,
    no_refresh: bool,
) -> RefreshStats:
    """Walk discovered configs, check freshness, refresh stale ones.

    `force_refresh` overrides freshness check (refreshes everything).
    `no_refresh` skips refresh entirely (hard-fails later if artifacts missing).
    """
    stats = RefreshStats()
    if no_refresh:
        return stats

    import contextlib
    import io

    def _try_refresh(cfg: Path) -> bool:
        """Run the refresh pipeline; return True on success. Per-config failures
        are recorded in stats.failed so one broken config doesn't kill the whole
        survey (a model-rank command spans dozens of configs, unlike model-report
        which is one-model integrity)."""
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                refresh_pipeline(cfg)
            return True
        except Exception as e:
            stats.failed.append((cfg.stem, f"{type(e).__name__}: {e}"))
            logger.warning("[refresh] %s FAILED: %s", cfg.stem, e)
            return False

    # Pre-pass: decide what to refresh up front so progress can show k/N.
    todo: list[tuple[Path, str]] = []
    for cfg_path in configs:
        name = cfg_path.stem
        if force_refresh:
            todo.append((cfg_path, "force"))
            continue
        fresh = check_freshness(cfg_path)
        if fresh.fresh:
            stats.skipped_fresh.append(name)
            continue
        if fresh.missing:
            stats.missing_artifacts.append((name, fresh.missing))
            todo.append((cfg_path, f"missing: {', '.join(fresh.missing)}"))
        else:
            todo.append((cfg_path, fresh.reason))

    total = len(todo)
    if total:
        print(f"  Refreshing {total} stale model(s) (retrain + diagnostics/confidence/backtest):", flush=True)
    for i, (cfg_path, reason) in enumerate(todo, 1):
        name = cfg_path.stem
        print(f"    [{i}/{total}] {name} ({reason}) ...", end="", flush=True)
        t0 = time.perf_counter()
        ok = _try_refresh(cfg_path)
        dt = time.perf_counter() - t0
        print(f" {'done' if ok else 'FAILED'} in {dt:.1f}s", flush=True)
        if ok:
            stats.refreshed.append(name)
    return stats


def run_rank(*, force_refresh: bool = False, no_refresh: bool = False) -> str:
    configs = discover_configs()
    if not configs:
        return "No model configs found under models/."

    stats = orchestrate_refresh(configs, force_refresh=force_refresh, no_refresh=no_refresh)

    # Extract summaries (only models with diagnostics)
    summaries: list[ModelSummary] = []
    skipped_no_diag: list[str] = []
    missing_partial: list[tuple[str, list[str]]] = []
    for cfg in configs:
        s = extract_summary(cfg)
        if s is None:
            skipped_no_diag.append(cfg.stem)
            continue
        summaries.append(s)
        # In --no-refresh mode, surface which artifacts are missing per model
        if no_refresh:
            fr = check_freshness(cfg)
            if fr.missing:
                missing_partial.append((cfg.stem, fr.missing))

    sections: list[str] = []

    header = ["=" * 80, "mvp model-rank", "=" * 80]
    header.append(f"  Configs discovered: {len(configs)}")
    if force_refresh:
        header.append(f"  Mode: --refresh (force-refresh all)")
        header.append(f"  Refreshed: {len(stats.refreshed)}")
    elif no_refresh:
        header.append(f"  Mode: --no-refresh (reading existing artifacts only)")
        header.append(f"  With diagnostics: {len(summaries)}")
        if skipped_no_diag:
            header.append(f"  Skipped (no diagnostics): {len(skipped_no_diag)}")
        partial_conf = sum(1 for _, m in missing_partial if "confidence" in m)
        partial_bt = sum(1 for _, m in missing_partial if "backtest" in m)
        if partial_conf or partial_bt:
            header.append(
                f"  Partial data (read what's there): "
                f"{partial_conf} missing confidence, {partial_bt} missing backtest"
            )
    else:
        header.append(f"  Mode: smart-refresh (per-model freshness check)")
        header.append(f"  Refreshed: {len(stats.refreshed)}    Skipped (fresh): {len(stats.skipped_fresh)}")
        if stats.missing_artifacts:
            header.append(f"  Of refreshed, had missing artifacts: {len(stats.missing_artifacts)}")

    if stats.failed:
        header.append("")
        header.append(f"  ! FAILED ({len(stats.failed)}) — survey continued; fix and re-run to include:")
        for name, err in stats.failed:
            header.append(f"     {name}: {err}")

    sections.append("\n".join(header))

    if not summaries:
        sections.append("No models with diagnostics — nothing to rank.")
        return "\n\n".join(sections)

    sections.append(render_leader_table(summaries))
    sections.append(render_static_table(summaries))
    sections.append(render_matrix(summaries))

    regressions = render_regressions(summaries)
    if regressions:
        sections.append(regressions)

    return "\n\n".join(sections)
