"""Lead model betting backtest — simulate predictions and bet outcomes.

Mirrors the structure of `mvp.projection.iid.backtest` but for the binary
win/loss classification lead. Run a candidate lead config forward on a
window where odds data exists, join to cross-book odds, and emit a CSV
of every prediction × side row (no filtering) plus a printed summary
with banded views by consensus / opening-edge / calibration tier.
"""


import json
import logging
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from mvp.common.base_job import get_data_root
from mvp.model.cal_tiers import (
    classify_cal_tier,
    extract_circuit_round_lookup,
)
from mvp.model.config import ExperimentConfig
from mvp.model.predictor import ProductionPredictor

logger = logging.getLogger(__name__)

ARTIFACT_ROOT = Path("B:/backtests/lead")
ODDS_PATH = get_data_root() / "aggregate" / "odds" / "odds.parquet"
MATCHES_PATH = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
PRODUCTION_CONFIG_PATH = Path("production.yaml")


def artifact_dir(config_path: Path) -> Path:
    return ARTIFACT_ROOT / config_path.stem


def output_path(config_path: Path) -> Path:
    return ARTIFACT_ROOT / f"{config_path.stem}.csv"


def summary_path(config_path: Path) -> Path:
    return ARTIFACT_ROOT / f"{config_path.stem}_summary.txt"


def _resolve_voters(override_path: Path | str | None) -> list[dict]:
    """Return the voter list to use — production.yaml by default, override if given."""
    voter_source = Path(override_path) if override_path else PRODUCTION_CONFIG_PATH
    if not voter_source.exists():
        raise FileNotFoundError(f"Voter source not found: {voter_source}")
    with open(voter_source) as f:
        raw = yaml.safe_load(f) or {}
    if "winner" in raw:
        section = raw["winner"]
    else:
        section = raw
    voters = section.get("voters", []) or []
    if not voters:
        logger.warning("No voters in %s — consensus will be 1.0 for every match", voter_source)
    return voters


def _build_temp_config(
    config_path: Path, voters: list[dict], artifact_root: Path
) -> dict:
    """Build a Predictor-shaped config dict pointing at the backtest's artifact paths."""
    lead_cfg = ExperimentConfig.from_file(str(config_path))
    train_range = {
        "start": lead_cfg.data.date_range.start.isoformat(),
        "end": lead_cfg.data.date_range.end.isoformat(),
    }
    voters_out = []
    voters_dir = artifact_root / "voters"
    for voter in voters:
        voter_config = Path(voter["config"])
        voter_artifact = voters_dir / f"{voter_config.stem}.joblib"
        voter_cfg = ExperimentConfig.from_file(str(voter_config))
        voter_train_range = {
            "start": voter_cfg.data.date_range.start.isoformat(),
            "end": voter_cfg.data.date_range.end.isoformat(),
        }
        entry = {
            "config": str(voter_config),
            "artifact": str(voter_artifact),
            "name": voter.get("name", voter_config.stem),
            "scoped": voter.get("scoped", False),
            "train_date_range": voter_train_range,
        }
        if voter_cfg.data.filters:
            entry["filters"] = voter_cfg.data.filters
        voters_out.append(entry)

    return {
        "winner": {
            "active": {
                "config": str(config_path),
                "artifact": str(artifact_root / "lead.joblib"),
                "train_date_range": train_range,
                "filters": lead_cfg.data.filters or {},
            },
            "history": [],
            "voters": voters_out,
        }
    }


def _find_diagnostics_json(config_stem: str) -> Path | None:
    """Find the latest diagnostics JSON for a config name by scanning mlruns/."""
    mlruns = Path("mlruns")
    if not mlruns.exists():
        return None
    candidates: list[tuple[float, Path]] = []
    for exp_dir in mlruns.iterdir():
        if not exp_dir.is_dir() or not exp_dir.name.isdigit():
            continue
        for run_dir in exp_dir.iterdir():
            artifacts = run_dir / "artifacts"
            if not (artifacts / f"{config_stem}.yaml").exists():
                continue
            for json_path in artifacts.glob("*.json"):
                candidates.append((json_path.stat().st_mtime, json_path))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _load_tier_lookup(
    config_stem: str,
    *,
    lead_cfg: ExperimentConfig | None = None,
    config_path: Path | None = None,
) -> tuple[dict[tuple[str, str], float], str | None]:
    """Return {(circuit, round): signed_calibration} from the latest cal_tiers source.

    Preference order:
      1. `<fp_dir>/diagnostics.json` (when `lead_cfg` is provided) — fp-scoped,
         guaranteed to match the current config content.
      2. `<artifact_dir>/lead_cal_tiers.json` sidecar emitted alongside the
         backtest's lead artifact (produced by predictor._train_single).
      3. Latest mlruns diagnostics JSON for that config.

    Second value is a short identifier of the source for the summary header
    (or None when nothing was found).
    """
    if lead_cfg is not None:
        from mvp.common.config_hash import compute_fingerprint, fingerprint_dir

        fp = compute_fingerprint(lead_cfg, config_path=config_path)
        fp_diag = fingerprint_dir(fp) / "diagnostics.json"
        if fp_diag.exists():
            with open(fp_diag) as f:
                diag = json.load(f)
            lookup = extract_circuit_round_lookup(diag)
            return lookup, f"fp:{fp}"

    sidecar_path = ARTIFACT_ROOT / config_stem / "lead_cal_tiers.json"
    if sidecar_path.exists():
        with open(sidecar_path) as f:
            diag = json.load(f)
        lookup = extract_circuit_round_lookup(diag)
        return lookup, "sidecar"

    diag_path = _find_diagnostics_json(config_stem)
    if diag_path is None:
        logger.warning(
            "No cal_tiers sidecar or diagnostics JSON for %s — cal_tier will be null in CSV output",
            config_stem,
        )
        return {}, None
    with open(diag_path) as f:
        diag = json.load(f)
    lookup = extract_circuit_round_lookup(diag)
    # Run id is the parent directory of artifacts/
    run_id = diag_path.parent.parent.name[:8]
    return lookup, run_id


def _build_bet_rows(
    predictions: pl.DataFrame, start: date, end: date
) -> pl.DataFrame:
    """Expand predictions to per-side rows and join odds + outcomes."""
    # Predictions are wide: one row per match with p1_id/p2_id, p1_win_prob, etc.
    # Build a long table: 2 rows per match (one per side).
    is_ds = "deciding_set_prob" in predictions.columns and "p1_win_prob" not in predictions.columns
    if is_ds:
        # Not a winner-target model — skip side expansion and just emit one row
        raise NotImplementedError(
            "Backtest currently supports winner-target models only "
            "(p1_win_prob in predictions)"
        )

    keep_cols = [
        c for c in [
            "match_uid",
            "effective_match_date",
            "circuit",
            "surface",
            "round",
            "tournament_id",
            "p1_id",
            "p2_id",
            "best_of",
            "p1_win_prob",
            "consensus",
            "voter_count",
        ]
        if c in predictions.columns
    ]
    base = predictions.select(keep_cols)

    p1_rows = base.with_columns(
        pl.lit("p1").alias("side"),
        pl.col("p1_id").alias("player_id"),
        pl.col("p2_id").alias("opponent_id"),
        pl.col("p1_win_prob").alias("model_prob"),
    )
    p2_rows = base.with_columns(
        pl.lit("p2").alias("side"),
        pl.col("p2_id").alias("player_id"),
        pl.col("p1_id").alias("opponent_id"),
        (1.0 - pl.col("p1_win_prob")).alias("model_prob"),
    )
    bets = pl.concat([p1_rows, p2_rows]).drop(["p1_win_prob", "p1_id", "p2_id"])
    bets = bets.with_columns((pl.col("model_prob") >= 0.5).alias("is_pick"))

    # Join cross-book odds on (match_uid, player_id)
    if ODDS_PATH.exists():
        odds = pl.read_parquet(ODDS_PATH).select(
            "match_uid",
            "player_id",
            "best_opening_odds",
            "best_closing_odds",
        )
        bets = bets.join(odds, on=["match_uid", "player_id"], how="left")
    else:
        logger.warning("No odds parquet at %s — best_*_odds will be null", ODDS_PATH)
        bets = bets.with_columns(
            pl.lit(None).cast(pl.Float64).alias("best_opening_odds"),
            pl.lit(None).cast(pl.Float64).alias("best_closing_odds"),
        )

    bets = bets.with_columns(
        (1.0 / pl.col("best_opening_odds")).alias("opening_implied"),
        (1.0 / pl.col("best_closing_odds")).alias("closing_implied"),
    ).with_columns(
        (pl.col("model_prob") - pl.col("opening_implied")).alias("opening_edge"),
        (pl.col("model_prob") - pl.col("closing_implied")).alias("closing_edge"),
        (pl.col("closing_implied") - pl.col("opening_implied")).alias("clv"),
    )

    # Settle outcomes — `won` is per-(match_uid, player_id) in matches.parquet
    matches = pl.read_parquet(MATCHES_PATH).select(
        "match_uid", "player_id", "won"
    ).unique(subset=["match_uid", "player_id"])
    bets = bets.join(matches, on=["match_uid", "player_id"], how="left")

    # P&L assuming flat 1u stake on this side. Null odds → null pnl on that
    # side; a bet without a recorded price can't be settled to a real outcome.
    bets = bets.with_columns(
        (
            pl.when(pl.col("best_opening_odds").is_null())
            .then(None)
            .when(pl.col("won") == 1)
            .then(pl.col("best_opening_odds") - 1.0)
            .when(pl.col("won") == 0)
            .then(pl.lit(-1.0))
            .otherwise(None)
        ).alias("pnl_open"),
        (
            pl.when(pl.col("best_closing_odds").is_null())
            .then(None)
            .when(pl.col("won") == 1)
            .then(pl.col("best_closing_odds") - 1.0)
            .when(pl.col("won") == 0)
            .then(pl.lit(-1.0))
            .otherwise(None)
        ).alias("pnl_close"),
    )

    return bets.sort(["effective_match_date", "match_uid", "side"])


def _attach_cal_tiers(
    bets: pl.DataFrame,
    config_stem: str,
    *,
    lead_cfg: ExperimentConfig | None = None,
    config_path: Path | None = None,
) -> tuple[pl.DataFrame, str | None]:
    """Add cal_tier and cell_cal columns by lookup against the lead's diagnostics."""
    lookup, run_id = _load_tier_lookup(
        config_stem, lead_cfg=lead_cfg, config_path=config_path
    )
    if not lookup:
        return (
            bets.with_columns(
                pl.lit(None).cast(pl.Float64).alias("cell_cal"),
                pl.lit(None).cast(pl.Utf8).alias("cal_tier"),
            ),
            run_id,
        )
    keys = list(lookup.keys())
    df_lookup = pl.DataFrame({
        "circuit": [k[0] for k in keys],
        "round": [k[1] for k in keys],
        "cell_cal": [lookup[k] for k in keys],
    })
    bets = bets.join(df_lookup, on=["circuit", "round"], how="left")
    tiers = [classify_cal_tier(v) for v in bets["cell_cal"].to_list()]
    bets = bets.with_columns(pl.Series("cal_tier", tiers, dtype=pl.Utf8))
    return bets, run_id


def run_backtest(
    config_path: Path | str,
    *,
    retrain: bool = False,
    start: date | None = None,
    end: date | None = None,
    voters_override_path: Path | str | None = None,
) -> Path:
    """Run the lead backtest end-to-end and return the CSV path."""
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Lead config not found: {config_path}")

    voters = _resolve_voters(voters_override_path)
    artifact_root = artifact_dir(config_path)
    artifact_root.mkdir(parents=True, exist_ok=True)
    (artifact_root / "voters").mkdir(exist_ok=True)

    temp_config = _build_temp_config(config_path, voters, artifact_root)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8",
    ) as f:
        yaml.safe_dump(temp_config, f)
        temp_yaml_path = Path(f.name)

    try:
        predictor = ProductionPredictor(production_config_path=temp_yaml_path)

        # Train (or skip if artifact exists)
        lead_artifact = Path(temp_config["winner"]["active"]["artifact"])
        if retrain or not lead_artifact.exists():
            logger.info("Training lead model %s ...", config_path.stem)
            predictor.train()
        else:
            logger.info("Using cached lead artifact %s", lead_artifact)

        for voter in temp_config["winner"]["voters"]:
            voter_artifact = Path(voter["artifact"])
            if retrain or not voter_artifact.exists():
                logger.info("Training voter %s ...", voter["name"])
                predictor._train_single(voter)
            else:
                logger.info("Using cached voter artifact %s", voter_artifact)

        # Determine test window
        lead_cfg = ExperimentConfig.from_file(str(config_path))
        if start is None:
            start = lead_cfg.data.date_range.end + timedelta(days=1)
        if end is None:
            end = date.today()
        if start > end:
            raise ValueError(f"Backtest start {start} after end {end}")
        logger.info("Backtest window: %s to %s", start, end)

        # Inference
        predictions = predictor.predict(
            include_settled=True, date_window=(start, end)
        )
        if predictions is None or len(predictions) == 0:
            raise RuntimeError(
                f"No matches in backtest window {start} to {end}"
            )
        logger.info("Lead predicted %d match rows", len(predictions))

        predictions = predictor.predict_voters(
            tournament_keys=None,
            predictions=predictions,
            include_settled=True,
        )

        # Build per-side bet rows + odds + outcomes
        bets = _build_bet_rows(predictions, start, end)
        bets, run_id = _attach_cal_tiers(
            bets, config_path.stem, lead_cfg=lead_cfg, config_path=config_path
        )

        from mvp.common.config_hash import (
            compute_fingerprint,
            fingerprint_dir,
            write_config_snapshot,
        )

        fp = compute_fingerprint(lead_cfg, config_path=config_path)
        fp_dir = fingerprint_dir(fp)
        fp_dir.mkdir(parents=True, exist_ok=True)
        write_config_snapshot(lead_cfg, fp, config_path=config_path)

        out_path = fp_dir / "backtest.csv"
        bets.write_csv(out_path)
        logger.info("Wrote %d bet rows to %s", len(bets), out_path)

        # Print + save summary
        summary_text = _format_summary(
            bets,
            config_stem=config_path.stem,
            window=(start, end),
            csv_path=out_path,
            diag_run_id=run_id,
        )
        print(summary_text)
        (fp_dir / "backtest_summary.txt").write_text(summary_text, encoding="utf-8")

        return out_path
    finally:
        temp_yaml_path.unlink(missing_ok=True)


# --- Summary formatting -----------------------------------------------------

_EDGE_BANDS: list[tuple[str, float, float]] = [
    (">= 10%", 0.10, float("inf")),
    ("5-10%", 0.05, 0.10),
    ("2-5%", 0.02, 0.05),
    ("0-2%", 0.0, 0.02),
    ("< 0%", float("-inf"), 0.0),
]
_TIER_ORDER = ["UnderC", "Optimal", "Border", "Risky", "Danger", "unknown"]


def _picks(bets: pl.DataFrame) -> pl.DataFrame:
    return bets.filter(pl.col("is_pick"))


_LABEL_W = 16
_SIDE_W = 71


def _side_stats(sub: pl.DataFrame, price: str) -> str:
    """Format n / hit% / ROI / units / CLV+% / avg CLV / ME+% / avg ME for one price side.

    ME = model edge at close (model_prob - closing_implied).
    ME+% is the fraction of rows where the model still has positive edge vs
    the closing best-of-books price; avg ME is the mean of that gap in pp.
    """
    pnl_col = f"pnl_{price}"
    sub = sub.filter(pl.col(pnl_col).is_not_null())
    n = len(sub)
    if n == 0:
        return (
            f"{0:>6}  {'-':>6}  {'-':>7}  {'-':>8}  "
            f"{'-':>6}  {'-':>9}  {'-':>6}  {'-':>9}"
        )
    hit = sub["won"].drop_nulls().mean() or 0.0
    pnl = sub[pnl_col].sum()
    roi = pnl / n
    clv = sub["clv"].drop_nulls()
    clv_win = (clv > 0).mean() if len(clv) else 0.0
    avg_clv = clv.mean() if len(clv) else 0.0
    ce = sub["closing_edge"].drop_nulls()
    me_win = (ce > 0).mean() if len(ce) else 0.0
    avg_me = ce.mean() if len(ce) else 0.0
    return (
        f"{n:>6}  {hit:>6.1%}  {roi:>+7.2%}  {pnl:>+7.1f}u  "
        f"{clv_win:>6.1%}  {avg_clv * 100:>+7.2f}pp  "
        f"{me_win:>6.1%}  {avg_me * 100:>+7.2f}pp"
    )


def _wide_header_lines() -> tuple[str, str]:
    open_hdr = "---- OPEN ----".center(_SIDE_W)
    close_hdr = "---- CLOSE ----".center(_SIDE_W)
    side_cols = (
        f"{'n':>6}  {'hit%':>6}  {'ROI':>7}  {'units':>8}  "
        f"{'CLV+%':>6}  {'avg CLV':>9}  {'ME+%':>6}  {'avg ME':>9}"
    )
    label_blank = " " * _LABEL_W
    top = f"  {label_blank}  {open_hdr}    {close_hdr}"
    bottom = f"  {'label':<{_LABEL_W}}  {side_cols}    {side_cols}"
    return top, bottom


def _wide_row(label: str, open_sub: pl.DataFrame, close_sub: pl.DataFrame) -> str:
    return (
        f"  {label:<{_LABEL_W}}  "
        f"{_side_stats(open_sub, 'open')}    "
        f"{_side_stats(close_sub, 'close')}"
    )


def _filter_band(sub: pl.DataFrame, col: str, lo: float, hi: float) -> pl.DataFrame:
    if hi == float("inf"):
        return sub.filter(pl.col(col) >= lo)
    if lo == float("-inf"):
        return sub.filter(pl.col(col) < hi)
    return sub.filter((pl.col(col) >= lo) & (pl.col(col) < hi))


def _kind_subsection(
    sub: pl.DataFrame, label_word: str, diag_run_id: str | None
) -> list[str]:
    """Emit HEADLINE / EDGE × TIER / BY EDGE BAND for one kind (picks or non-picks)."""
    top, bottom = _wide_header_lines()
    n = len(sub)
    lines: list[str] = [f"--- {label_word.upper()}  ({n:,}) ---"]

    def tier_sub(s: pl.DataFrame, tier: str) -> pl.DataFrame:
        if tier == "unknown":
            return s.filter(pl.col("cal_tier").is_null())
        return s.filter(pl.col("cal_tier") == tier)

    def edge_sub(s: pl.DataFrame, col: str, yes: bool) -> pl.DataFrame:
        return s.filter(pl.col(col) >= 0) if yes else s.filter(pl.col(col) < 0)

    diag_suffix = f"  (tiers from diagnostics {diag_run_id})" if diag_run_id else ""
    tiers_present = [t for t in _TIER_ORDER if len(tier_sub(sub, t)) > 0]

    lines.append("")
    lines.append("HEADLINE")
    lines.append(top)
    lines.append(bottom)
    lines.append(_wide_row(f"all {label_word}", sub, sub))

    lines.append("")
    lines.append(f"EDGE × TIER{diag_suffix}")
    lines.append(top)
    lines.append(bottom)
    for yes_flag, edge_label in [(True, "yes"), (False, "no")]:
        for tier in tiers_present:
            t_sub = tier_sub(sub, tier)
            lines.append(
                _wide_row(
                    f"{edge_label} / {tier}",
                    edge_sub(t_sub, "opening_edge", yes_flag),
                    edge_sub(t_sub, "closing_edge", yes_flag),
                )
            )

    lines.append("")
    lines.append("BY EDGE BAND  (all tiers)")
    lines.append(top)
    lines.append(bottom)
    for label, lo, hi in _EDGE_BANDS:
        open_sub = _filter_band(sub, "opening_edge", lo, hi)
        close_sub = _filter_band(sub, "closing_edge", lo, hi)
        lines.append(_wide_row(label, open_sub, close_sub))

    return lines


def _consensus_section(
    bets: pl.DataFrame, consensus_value: float, diag_run_id: str | None
) -> str:
    cons_sub = bets.filter(pl.col("consensus") == consensus_value)
    picks_sub = cons_sub.filter(pl.col("is_pick"))
    dogs_sub = cons_sub.filter(~pl.col("is_pick"))
    n_picks = len(picks_sub)
    n_dogs = len(dogs_sub)
    lines: list[str] = [
        f"=== CONSENSUS = {consensus_value:.2f}  "
        f"({n_picks:,} picks / {n_dogs:,} non-picks) ==="
    ]
    lines.append("")
    lines.extend(_kind_subsection(picks_sub, "picks", diag_run_id))
    lines.append("")
    lines.extend(_kind_subsection(dogs_sub, "non-picks", diag_run_id))
    return "\n".join(lines)


def _format_summary(
    bets: pl.DataFrame,
    *,
    config_stem: str,
    window: tuple[date, date],
    csv_path: Path,
    diag_run_id: str | None,
) -> str:
    n_matches = bets["match_uid"].n_unique()
    header = (
        f"\n=== mvp backtest: {config_stem} ===\n"
        f"Window: {window[0]} → {window[1]} "
        f"({n_matches:,} matches, {len(bets):,} prediction rows)"
    )
    consensus_values = sorted(
        {v for v in bets["consensus"].drop_nulls().to_list()},
        reverse=True,
    )
    sections: list[str] = [header]
    for v in consensus_values:
        sections.append(_consensus_section(bets, v, diag_run_id))
    sections.append(f"Full CSV: {csv_path}  ({len(bets):,} rows)")
    return "\n\n".join(sections)


def print_backtest_summary(csv_path: Path) -> None:
    """Re-render the summary from an existing CSV (for ad-hoc inspection)."""
    bets = pl.read_csv(csv_path, infer_schema_length=10000)
    if "effective_match_date" in bets.columns:
        bets = bets.with_columns(
            pl.col("effective_match_date").cast(pl.Date, strict=False)
        )
    window = (
        bets["effective_match_date"].min(),
        bets["effective_match_date"].max(),
    )
    config_stem = csv_path.stem
    text = _format_summary(
        bets,
        config_stem=config_stem,
        window=window,
        csv_path=csv_path,
        diag_run_id=None,
    )
    print(text)
