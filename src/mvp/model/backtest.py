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

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model import backtest_views as views
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

# Dedicated feature cache, isolated from the shared FS/live cache
# (get_local_data_root()/features/cache). The backtest runs ProductionPredictor
# in file-bytes hash mode (no cutoff), so it would otherwise invalidate the
# cutoff-keyed FS cache on every run (matches.parquet is rewritten every 15 min
# by the live pipeline). A separate dir keeps the next FS run's cache intact.
BACKTEST_CACHE_DIR = get_local_data_root() / "features" / "backtest_cache"


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
            "n_agree",
            "per_sub_probs",
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
    p2_extra_cols = [
        pl.lit("p2").alias("side"),
        pl.col("p2_id").alias("player_id"),
        pl.col("p1_id").alias("opponent_id"),
        (1.0 - pl.col("p1_win_prob")).alias("model_prob"),
    ]
    # per_sub_probs is in p1 orientation from predict(); flip element-wise
    # for p2 rows so it aligns with model_prob. n_agree is symmetric under
    # uniform flip and represents "subs agreeing with the ensemble pick",
    # which is the same on both sides.
    if "per_sub_probs" in base.columns:
        p2_extra_cols.append(
            pl.col("per_sub_probs")
              .list.eval(1.0 - pl.element())
              .alias("per_sub_probs")
        )
    p2_rows = base.with_columns(p2_extra_cols)
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
        predictor = ProductionPredictor(
            production_config_path=temp_yaml_path,
            cache_dir=BACKTEST_CACHE_DIR,
        )

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
            # Default 7 days back from today.
            end = date.today() - timedelta(days=7)
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
        # CSV can't serialize nested list columns; stringify per_sub_probs
        # for the on-disk output. In-memory `bets` keeps the list for the
        # summary aggregation that follows.
        bets_csv = bets
        if "per_sub_probs" in bets_csv.columns:
            bets_csv = bets_csv.with_columns(
                pl.col("per_sub_probs")
                .list.eval(pl.element().round(6).cast(pl.Utf8))
                .list.join(",")
                .alias("per_sub_probs")
            )
        bets_csv.write_csv(out_path)
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

# Use a wider edge-band table than report.py's RAW filter — this view shows
# both edge>=0 and edge<0 rows since non-pick analysis is meaningful here.
_SUMMARY_EDGE_BANDS: tuple[tuple[str, float, float | None], ...] = (
    (">= 10%", 0.10, None),
    ("5-10%", 0.05, 0.10),
    ("2-5%", 0.02, 0.05),
    ("0-2%", 0.0, 0.02),
    ("< 0%", float("-inf"), 0.0),
)

# Backtest summary shows additional tiers (UnderC, unknown=nulls) that
# report.py's standard view doesn't surface.
_SUMMARY_TIER_ORDER: tuple[str, ...] = (
    "UnderC", "Optimal", "Border", "Risky", "Danger",
)


_LABEL_W = 16
_OPEN_W = 71
_CLOSE_W = 52


def _render_open_side(stats: dict) -> str:
    """Render the OPEN cell: n / hit% / ROI / units / CLV+% / avg CLV / ME+% / avg ME.

    n_open (priced subset size) is the row count contributing to ROI.
    """
    n_p = stats.get("n_open", 0)
    if n_p == 0:
        return (
            f"{0:>6}  {'-':>6}  {'-':>7}  {'-':>8}  "
            f"{'-':>6}  {'-':>9}  {'-':>6}  {'-':>9}"
        )
    hit = stats.get("hit") or 0.0
    pnl = stats.get("pnl_open") or 0.0
    roi = stats.get("roi_open") or 0.0
    clv_win = stats.get("clv_pos") or 0.0
    avg_clv = stats.get("avg_clv") or 0.0
    me_win = stats.get("me_open_pos") or 0.0
    avg_me = stats.get("avg_me_open") or 0.0
    return (
        f"{n_p:>6}  {hit:>6.1%}  {roi:>+7.2%}  {pnl:>+7.1f}u  "
        f"{clv_win:>6.1%}  {avg_clv * 100:>+7.2f}pp  "
        f"{me_win:>6.1%}  {avg_me * 100:>+7.2f}pp"
    )


def _render_close_side(stats: dict) -> str:
    """Render the CLOSE cell: n / hit% / ROI / units / ME+% / avg ME.

    CLV is omitted — at the close line, CLV-vs-close is zero by definition.
    """
    n_p = stats.get("n_close", 0)
    if n_p == 0:
        return (
            f"{0:>6}  {'-':>6}  {'-':>7}  {'-':>8}  "
            f"{'-':>6}  {'-':>9}"
        )
    hit = stats.get("hit") or 0.0
    pnl = stats.get("pnl_close") or 0.0
    roi = stats.get("roi_close") or 0.0
    me_win = stats.get("me_close_pos") or 0.0
    avg_me = stats.get("avg_me_close") or 0.0
    return (
        f"{n_p:>6}  {hit:>6.1%}  {roi:>+7.2%}  {pnl:>+7.1f}u  "
        f"{me_win:>6.1%}  {avg_me * 100:>+7.2f}pp"
    )


def _wide_header_lines() -> tuple[str, str]:
    open_hdr = "---- OPEN ----".center(_OPEN_W)
    close_hdr = "---- CLOSE ----".center(_CLOSE_W)
    open_cols = (
        f"{'n':>6}  {'hit%':>6}  {'ROI':>7}  {'units':>8}  "
        f"{'CLV+%':>6}  {'avg CLV':>9}  {'ME+%':>6}  {'avg ME':>9}"
    )
    close_cols = (
        f"{'n':>6}  {'hit%':>6}  {'ROI':>7}  {'units':>8}  "
        f"{'ME+%':>6}  {'avg ME':>9}"
    )
    label_blank = " " * _LABEL_W
    top = f"  {label_blank}  {open_hdr}    {close_hdr}"
    bottom = f"  {'label':<{_LABEL_W}}  {open_cols}    {close_cols}"
    return top, bottom


def _wide_row(label: str, open_stats: dict, close_stats: dict) -> str:
    """Render one wide row from pre-computed open/close stats dicts.
    Pass the same dict twice when both sides come from the same slice.
    """
    return (
        f"  {label:<{_LABEL_W}}  "
        f"{_render_open_side(open_stats)}    "
        f"{_render_close_side(close_stats)}"
    )


def _tier_sub(s: pl.DataFrame, tier: str) -> pl.DataFrame:
    """Backtest-summary tier extraction, including the implicit "unknown"
    bucket for rows with null cal_tier (report.py's by_tier doesn't surface
    this since report's filter already drops null tiers).
    """
    if tier == "unknown":
        return s.filter(pl.col("cal_tier").is_null())
    return s.filter(pl.col("cal_tier") == tier)


def _edge_sign_sub(s: pl.DataFrame, col: str, yes: bool) -> pl.DataFrame:
    """Filter to edge>=0 (yes) or edge<0 (no) on the named edge column."""
    return s.filter(pl.col(col) >= 0) if yes else s.filter(pl.col(col) < 0)


def _kind_subsection(
    sub: pl.DataFrame, label_word: str, diag_run_id: str | None
) -> list[str]:
    """Emit HEADLINE / EDGE × TIER / BY EDGE BAND / BY MONTH for one kind
    (picks or non-picks). All numeric aggregation goes through
    `backtest_views.slice_stats`; this function owns only layout.
    """
    top, bottom = _wide_header_lines()
    n = len(sub)
    lines: list[str] = [f"--- {label_word.upper()}  ({n:,}) ---"]

    diag_suffix = f"  (tiers from diagnostics {diag_run_id})" if diag_run_id else ""
    tiers_present = [t for t in _SUMMARY_TIER_ORDER if len(_tier_sub(sub, t)) > 0]
    if len(_tier_sub(sub, "unknown")) > 0:
        tiers_present.append("unknown")

    sub_stats = views.slice_stats(sub)
    lines.append("")
    lines.append("HEADLINE")
    lines.append(top)
    lines.append(bottom)
    lines.append(_wide_row(f"all {label_word}", sub_stats, sub_stats))

    lines.append("")
    lines.append(f"EDGE × TIER{diag_suffix}")
    lines.append(top)
    lines.append(bottom)
    for yes_flag, edge_label in [(True, "yes"), (False, "no")]:
        for tier in tiers_present:
            t_sub = _tier_sub(sub, tier)
            open_stats = views.slice_stats(
                _edge_sign_sub(t_sub, "opening_edge", yes_flag)
            )
            close_stats = views.slice_stats(
                _edge_sign_sub(t_sub, "closing_edge", yes_flag)
            )
            lines.append(_wide_row(f"{edge_label} / {tier}", open_stats, close_stats))

    # Ensemble consensus breakdowns (only when n_agree is present —
    # backtest of single-model configs won't have this column).
    if "n_agree" in sub.columns:
        consensus_rows = views.by_consensus(sub)
        if consensus_rows:
            lines.append("")
            lines.append("EDGE × CONSENSUS")
            lines.append(top)
            lines.append(bottom)
            n_subs_max = int(sub["n_agree"].drop_nulls().max())
            for yes_flag, edge_label in [(True, "yes"), (False, "no")]:
                for n_agree in range(n_subs_max, 0, -1):
                    n_disagree = n_subs_max - n_agree
                    c_sub = sub.filter(pl.col("n_agree") == n_agree)
                    if len(c_sub) == 0:
                        continue
                    open_stats = views.slice_stats(
                        _edge_sign_sub(c_sub, "opening_edge", yes_flag)
                    )
                    close_stats = views.slice_stats(
                        _edge_sign_sub(c_sub, "closing_edge", yes_flag)
                    )
                    lines.append(
                        _wide_row(
                            f"{edge_label} / {n_agree}-{n_disagree}",
                            open_stats,
                            close_stats,
                        )
                    )

            lines.append("")
            lines.append("BY CONSENSUS  (all edges)")
            lines.append(top)
            lines.append(bottom)
            for label, stats in consensus_rows:
                lines.append(_wide_row(label, stats, stats))

    lines.append("")
    lines.append("BY EDGE BAND  (all tiers)")
    lines.append(top)
    lines.append(bottom)
    for label, lo, hi in _SUMMARY_EDGE_BANDS:
        # Negative-edge band needs a custom lower bound; views.filter_band
        # handles `hi is None` (upper-open) but not `lo == -inf`.
        if lo == float("-inf"):
            open_sub = sub.filter(pl.col("opening_edge") < hi)
            close_sub = sub.filter(pl.col("closing_edge") < hi)
        else:
            open_sub = views.filter_band(sub, "opening_edge", lo, hi)
            close_sub = views.filter_band(sub, "closing_edge", lo, hi)
        lines.append(
            _wide_row(label, views.slice_stats(open_sub), views.slice_stats(close_sub))
        )

    # Monthly slices — same slice for both sides since the temporal cut
    # is independent of which price we're scoring. For picks, scope to
    # opening_edge > 0 so the cumulative trailer is a meaningful "what
    # bankroll would have looked like" number on actually-placed bets.
    # Non-picks keep the full scope (opening_edge > 0 doesn't make sense
    # for the opponent-side of a model pick).
    monthly_input = (
        sub.filter(pl.col("opening_edge") > 0)
        if label_word == "picks" and "opening_edge" in sub.columns
        else sub
    )
    monthly = views.by_month(monthly_input)
    if monthly:
        scope_note = (
            "  (opening_edge > 0)" if label_word == "picks" else ""
        )
        lines.append("")
        lines.append(f"BY MONTH{scope_note}")
        lines.append(top)
        lines.append(bottom)
        for month, stats in monthly:
            lines.append(_wide_row(month, stats, stats))
        # Cumulative trailer — open/close running totals at end of window.
        last_stats = monthly[-1][1]
        cum_open = last_stats.get("cum_open", 0.0)
        cum_close = last_stats.get("cum_close", 0.0)
        lines.append(
            f"  {'cumulative':<{_LABEL_W}}  open: {cum_open:>+7.1f}u    "
            f"close: {cum_close:>+7.1f}u"
        )

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
    sections: list[str] = [header]

    if "circuit" in bets.columns:
        circuits = sorted(
            {c for c in bets["circuit"].drop_nulls().to_list()}
        )
    else:
        circuits = []

    if not circuits:
        # Fall back to combined view when circuit column is missing/empty
        consensus_values = sorted(
            {v for v in bets["consensus"].drop_nulls().to_list()},
            reverse=True,
        )
        for v in consensus_values:
            sections.append(_consensus_section(bets, v, diag_run_id))
    else:
        bar = "=" * 80
        for circuit in circuits:
            circ_bets = bets.filter(pl.col("circuit") == circuit)
            circ_matches = circ_bets["match_uid"].n_unique()
            sections.append(
                f"{bar}\nCIRCUIT = {circuit}  "
                f"({circ_matches:,} matches, {len(circ_bets):,} rows)\n{bar}"
            )
            consensus_values = sorted(
                {v for v in circ_bets["consensus"].drop_nulls().to_list()},
                reverse=True,
            )
            for v in consensus_values:
                sections.append(_consensus_section(circ_bets, v, diag_run_id))

    sections.append(f"Full CSV: {csv_path}  ({len(bets):,} rows)")
    return "\n\n".join(sections)


def print_backtest_summary(csv_path: Path) -> None:
    """Re-render the summary from an existing CSV (for ad-hoc inspection).

    Reads via the shared evaluation helper which pins numeric dtypes but
    leaves `effective_match_date` as String — `backtest_views.by_month`
    handles String dates natively via slice(0, 7). A cast(Date, strict=False)
    here would silently produce all-nulls on the CSV's ISO-with-time format.
    """
    from mvp.model.evaluation import read_backtest_csv

    bets = read_backtest_csv(csv_path)
    window = (
        bets["effective_match_date"].min(),
        bets["effective_match_date"].max(),
    )
    text = _format_summary(
        bets,
        config_stem=csv_path.stem,
        window=window,
        csv_path=csv_path,
        diag_run_id=None,
    )
    print(text)
