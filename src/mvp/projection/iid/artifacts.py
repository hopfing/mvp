"""Fingerprint-keyed evaluation artifacts for IID projection configs.

Every artifact a comparison needs lives in one directory keyed by config content:

    B:/projection_evaluations/<fp>/
        config.yaml             canonical snapshot (collision detector)
        source.txt              which config/run produced this, for grouping
        projection.json         iid-project metrics + per-fold + segments
        total_games_pmf.parquet per-match pmf, the input to CLV scoring
        serve_model.joblib      trained artifact (backtest)
        backtest.csv            per-bet rows (backtest)
        clv.json                sharp-CLV summary (written by the oddspapi scorer)

Content-keying is what makes a sweep over hyperparameter variants meaningful:
stem-keying wrote every variant of a config to the same path, so each run
overwrote the last.

The root is deliberately NOT `model_evaluations/` — see the note on
PROJECTION_EVAL_ROOT in mvp.common.config_hash.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import polars as pl

from mvp.common.config_hash import (
    PROJECTION_EVAL_ROOT,
    append_source,
    compute_iid_fingerprint,
    iid_fingerprint_dir,
    write_iid_config_snapshot,
)
from mvp.projection.iid.config import IIDProjectionConfig

logger = logging.getLogger(__name__)

PROJECTION_JSON = "projection.json"
PMF_PARQUET = "total_games_pmf.parquet"
SPREAD_PMF_PARQUET = "game_spread_pmf.parquet"
FOLD_MATCH_WIN_PARQUET = "fold_match_win.parquet"
BACKTEST_PARQUET = "backtest.parquet"
SPREAD_BACKTEST_PARQUET = "backtest_game_spread.parquet"

# Per-market ledger names. ONE FILE PER MARKET, not one file with a `market`
# column doing the separating: `rank.py`'s own contract is "one table per
# (instrument, market), never a pooled market", and the two markets' outcome
# columns differ (`actual_total` / `actual_spread`), which `vertical_relaxed`
# cannot concatenate anyway.
#
# `total_games` keeps the historic bare name so existing fingerprint dirs stay
# readable without a migration.
BACKTEST_PARQUET_BY_MARKET: dict[str, str] = {
    "total_games": BACKTEST_PARQUET,
    "game_spread": SPREAD_BACKTEST_PARQUET,
}


def backtest_name(market: str) -> str:
    """Ledger filename for one market. Raises rather than deriving a name that no
    reader looks for."""
    try:
        return BACKTEST_PARQUET_BY_MARKET[market]
    except KeyError:
        raise ValueError(
            f"no ledger name for market {market!r}; "
            f"known: {sorted(BACKTEST_PARQUET_BY_MARKET)}"
        ) from None

# Per-market pmf artifact names. A SIBLING file rather than extra columns on the
# totals one: that file's contract is read by the CLV POC and appears in every
# fingerprint dir, and widening it would change what those readers see for a
# market they do not price.
PMF_PARQUET_BY_MARKET: dict[str, str] = {
    "total_games": PMF_PARQUET,
    "game_spread": SPREAD_PMF_PARQUET,
}

# Pre-cutover artifacts, kept named so the cutover can DELETE them from every
# fingerprint dir. `_canonicalize_iid_config` hashes data / features / metrics /
# serve_model / validation and knows nothing about the odds source, so a
# post-cutover run writes into the same `<fp>/` as its predecessor — different
# meaning, same path, indistinguishable afterwards. Everything else in a
# fingerprint dir (projection.json, config.yaml, the pmf, serve_model.joblib) is
# odds-independent and survives.
#
# `clv.json` was never written by anything in `src/` — it came from the POC
# scorer that the cross-book layer retires. Only `rank.py` reads it.
LEGACY_BACKTEST_CSV = "backtest.csv"
CLV_JSON = "clv.json"

# Artifacts a cutover must clear from a fingerprint dir before it is reused.
STALE_AT_CUTOVER = (LEGACY_BACKTEST_CSV, CLV_JSON)


def purge_stale_artifacts(*, dry_run: bool = True) -> list[Path]:
    """Remove pre-cutover odds artifacts from every fingerprint dir.

    Run ONCE at the cutover, not per run. The fingerprint hashes data /
    features / metrics / serve_model / validation and knows nothing about the
    odds source, so a post-cutover run lands in the same `<fp>/` as the run it
    supersedes — a `backtest.csv` sitting next to a `backtest.parquet` is two
    contracts at one path with nothing to tell a reader which is current.

    Deliberately not called from `run_backtest`: deleting on every run would
    also delete a file someone had just regenerated for comparison. Defaults to
    a dry run so the caller sees the list before anything goes.
    """
    doomed = [
        p
        for fp_dir in discover_fp_dirs()
        for name in STALE_AT_CUTOVER
        if (p := fp_dir / name).exists()
    ]
    if not dry_run:
        for p in doomed:
            p.unlink()
    return doomed
SERVE_MODEL_JOBLIB = "serve_model.joblib"


def fp_for(config: IIDProjectionConfig, config_path: Path) -> str:
    return compute_iid_fingerprint(config, config_path=config_path)


def fp_dir_for(config: IIDProjectionConfig, config_path: Path) -> Path:
    return iid_fingerprint_dir(fp_for(config, config_path))


def record_run(
    config: IIDProjectionConfig,
    config_path: Path,
    *,
    source: str | None = None,
    run_id: str | None = None,
) -> Path:
    """Ensure the fingerprint dir exists with a config snapshot; return it.

    `source` groups sweep trials under their parent config (a sweep passes the
    parent stem, so `iid-rank` can show N variants of one config together).
    Defaults to the config's own stem for a plain single run.
    """
    fp = fp_for(config, config_path)
    fp_dir = iid_fingerprint_dir(fp)
    fp_dir.mkdir(parents=True, exist_ok=True)
    write_iid_config_snapshot(config, fp, config_path=config_path)
    append_source(
        fp,
        source or config_path.stem,
        run_id or config_path.stem,
        root=PROJECTION_EVAL_ROOT,
    )
    return fp_dir


def write_projection_json(fp_dir: Path, result: dict[str, Any]) -> Path:
    """Persist an IIDProjectionRunner result.

    Drops the un-serializable / bulky members (`_config`, `diagnostics`) — the
    diagnostics' segment metrics are already flattened into `metrics` by the
    runner, and the config is snapshotted separately.
    """
    payload = {
        k: v for k, v in result.items() if k not in ("_config", "diagnostics")
    }
    fp_dir.mkdir(parents=True, exist_ok=True)
    path = fp_dir / PROJECTION_JSON
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def read_projection_json(fp_dir: Path) -> dict[str, Any] | None:
    path = fp_dir / PROJECTION_JSON
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_pmf_parquet(
    fp_dir: Path, pmf: pl.DataFrame, *, market: str = "total_games"
) -> Path:
    """Persist one market's per-match pmf — the input to pricing and CLV scoring.

    Mirrors what scripts/oddspapi/iid_project_dump.py produces, except keyed by
    fingerprint instead of a single fixed path that every run overwrote. That
    overwrite is the reason CLV could not previously be compared across configs.

    `market` defaults to totals so existing callers are unchanged. Each market
    gets its own file (`PMF_PARQUET_BY_MARKET`) rather than shared columns,
    because the outcome column differs per market — `actual_total` against
    `actual_spread` — and a reader that knows one market should not have to know
    the other's schema to open the file.
    """
    try:
        name = PMF_PARQUET_BY_MARKET[market]
    except KeyError:
        raise ValueError(
            f"no pmf artifact name for market {market!r}; "
            f"known: {sorted(PMF_PARQUET_BY_MARKET)}"
        ) from None
    fp_dir.mkdir(parents=True, exist_ok=True)
    path = fp_dir / name
    pmf.write_parquet(path)
    return path


_FOLD_MATCH_WIN_COLUMNS = [
    "match_uid", "player_id", "opp_id", "effective_match_date",
    "fold_idx", "p_match_win_a", "won_a",
]


def write_fold_match_win(fp_dir: Path, frame: pl.DataFrame) -> Path:
    """Persist the walk-forward per-match win probabilities.

    One row per (fold, match): the chain's `p_match_win_a` for the match's
    "A" player (the lower-sorting `player_id`, per `_collapse_to_match_rows`),
    with both players' ids so a consumer can emit the mirrored orientation.
    This is the per-row OOF store the winner-side prior consumes — before it
    existed, the walk-forward's predictions lived only in memory and
    `projection.json` kept aggregates.
    """
    missing = [c for c in _FOLD_MATCH_WIN_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"fold_match_win frame missing columns: {missing}")
    fp_dir.mkdir(parents=True, exist_ok=True)
    path = fp_dir / FOLD_MATCH_WIN_PARQUET
    frame.select(_FOLD_MATCH_WIN_COLUMNS).write_parquet(path)
    return path


def read_clv_json(fp_dir: Path) -> dict[str, Any] | None:
    path = fp_dir / CLV_JSON
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_sources(fp_dir: Path) -> list[tuple[str, str, str]]:
    """Parse source.txt into (source_name, run_id, timestamp) rows."""
    path = fp_dir / "source.txt"
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 3:
            rows.append((parts[0], parts[1], parts[2]))
    return rows


def discover_fp_dirs() -> list[Path]:
    """All projection-evaluation fingerprint dirs, newest first."""
    from mvp.common.base_job import get_data_root

    root = get_data_root() / PROJECTION_EVAL_ROOT
    if not root.exists():
        return []
    dirs = [p for p in root.iterdir() if p.is_dir()]
    dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return dirs
