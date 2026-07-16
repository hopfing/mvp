"""Lead model betting backtest — simulate predictions and bet outcomes.

Mirrors the structure of `mvp.projection.iid.backtest` but for the binary
win/loss classification lead. Run a candidate lead config forward on a
window where odds data exists, join to cross-book odds, and emit a CSV
of every prediction × side row (no filtering) plus a printed summary
with banded views by consensus / opening-edge / calibration tier.
"""


import json
import logging
import shutil
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import yaml
from dateutil.relativedelta import relativedelta

from mvp.common.base_job import get_data_root, get_local_data_root
from mvp.model import backtest_views as views
from mvp.model.cal_tiers import (
    classify_cal_tier,
    extract_circuit_round_lookup,
)
from mvp.model.config import ExperimentConfig, apply_filters
from mvp.model.predictor import ProductionPredictor

logger = logging.getLogger(__name__)

ARTIFACT_ROOT = Path("B:/backtests/lead")
ODDS_PATH = get_data_root() / "aggregate" / "odds" / "odds.parquet"
MATCHES_PATH = get_data_root() / "aggregate" / "atptour" / "matches.parquet"
PRODUCTION_CONFIG_PATH = Path("production.yaml")

# Hard floor on the betting period. The odds parquet holds a few stray,
# unreliable pre-2026 prices (e.g. a lone 2018 ITF Futures match) that are not a
# real historical feed; trustworthy live-scraped odds start in 2026. The
# backtest never scores anything before this date.
BETTING_START_FLOOR = date(2026, 1, 1)

# Dedicated feature cache, isolated from the shared FS/live cache
# (get_local_data_root()/features/cache). The backtest runs ProductionPredictor
# in file-bytes hash mode (no cutoff), so it would otherwise invalidate the
# cutoff-keyed FS cache on every run (matches.parquet is rewritten every 15 min
# by the live pipeline). A separate dir keeps the next FS run's cache intact.
BACKTEST_CACHE_DIR = get_local_data_root() / "features" / "backtest_cache"

# Per-week frozen snapshot of matches.parquet. The live file is rewritten every
# 15 min by the pipeline, so two backtests otherwise refit on different data and
# aren't comparable. We copy the live file once per ISO week to a scratch path
# and read from that copy, so every backtest run in the same week sees
# byte-identical matches — a within-week batch is internally comparable.
FROZEN_MATCHES_PATH = ARTIFACT_ROOT.parent / "frozen" / "matches.parquet"
_frozen_matches_cache: Path | None = None


def _frozen_matches_path() -> Path:
    """Return this week's frozen matches snapshot, refreshing it from the live
    matches.parquet when missing or stale (mtime from before the current week).

    Memoized per process: the staleness check + copy happen at most once per
    backtest run, and every matches read in that run resolves to one file.
    """
    global _frozen_matches_cache
    if _frozen_matches_cache is not None:
        return _frozen_matches_cache
    today = date.today()
    week_start = today - timedelta(days=today.weekday())  # Monday of the ISO week
    fresh = FROZEN_MATCHES_PATH.exists() and (
        datetime.fromtimestamp(FROZEN_MATCHES_PATH.stat().st_mtime).date() >= week_start
    )
    if fresh:
        logger.info(
            "Using frozen matches snapshot %s (frozen %s)",
            FROZEN_MATCHES_PATH,
            datetime.fromtimestamp(FROZEN_MATCHES_PATH.stat().st_mtime),
        )
    else:
        FROZEN_MATCHES_PATH.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(MATCHES_PATH, FROZEN_MATCHES_PATH)
        logger.info(
            "Froze matches snapshot for week of %s: %s -> %s",
            week_start, MATCHES_PATH, FROZEN_MATCHES_PATH,
        )
    _frozen_matches_cache = FROZEN_MATCHES_PATH
    return FROZEN_MATCHES_PATH


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


def _to_date(v: Any) -> date:
    """Coerce a date/datetime to date (config + parquet dates vary)."""
    return v.date() if isinstance(v, datetime) else v


def _build_temp_config(
    config_path: Path,
    voters: list[dict],
    artifact_root: Path,
    *,
    train_cutoff: date,
    fold_tag: str,
) -> dict:
    """Build a Predictor-shaped config dict for one backtest fold.

    ``train_cutoff`` is the last training date for this fold — the day before the
    fold's test window opens. Every model's training end is capped at it so no
    model can see the test window; each model's own validation block then
    determines its deploy window inside ``_train_single`` (trailing train_months
    for date_sliding, full span for date_expanding). Artifacts are tagged per
    fold so re-runs cache and multi-fold runs don't collide.
    """
    lead_cfg = ExperimentConfig.from_file(str(config_path))
    train_range = {
        "start": lead_cfg.data.date_range.start.isoformat(),
        "end": train_cutoff.isoformat(),
    }
    voters_out = []
    voters_dir = artifact_root / "voters"
    for voter in voters:
        voter_config = Path(voter["config"])
        voter_artifact = voters_dir / f"{voter_config.stem}_{fold_tag}.joblib"
        voter_cfg = ExperimentConfig.from_file(str(voter_config))
        # Cap the voter at the fold cutoff too (never train into the test
        # window), but honour an earlier configured end if it has one.
        voter_end = min(_to_date(voter_cfg.data.date_range.end), train_cutoff)
        voter_train_range = {
            "start": voter_cfg.data.date_range.start.isoformat(),
            "end": voter_end.isoformat(),
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
                "artifact": str(artifact_root / f"lead_{fold_tag}.joblib"),
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

    # Join cross-book odds on (match_uid, player_id). best_opening_odds /
    # best_closing_odds are the time-aligned best-across-book prices written by
    # the aggregator (compute_open_close_odds); no per-book first/last skew.
    if ODDS_PATH.exists():
        _odds = pl.read_parquet(ODDS_PATH)
        _want = ["match_uid", "player_id",
                 "best_opening_odds", "formed_odds", "best_closing_odds"]
        odds = _odds.select([c for c in _want if c in _odds.columns])
        bets = bets.join(odds, on=["match_uid", "player_id"], how="left")
    else:
        logger.warning("No odds parquet at %s — odds columns will be null", ODDS_PATH)

    # Backfill missing odds columns (e.g. an odds.parquet written before these
    # existed) so the expressions below are safe.
    for _c in ("best_opening_odds", "formed_odds", "best_closing_odds"):
        if _c not in bets.columns:
            bets = bets.with_columns(pl.lit(None).cast(pl.Float64).alias(_c))

    bets = bets.with_columns(
        (1.0 / pl.col("best_opening_odds")).alias("opening_implied"),
        (1.0 / pl.col("formed_odds")).alias("formed_implied"),
        (1.0 / pl.col("best_closing_odds")).alias("closing_implied"),
    ).with_columns(
        (pl.col("model_prob") - pl.col("opening_implied")).alias("opening_edge"),
        (pl.col("model_prob") - pl.col("formed_implied")).alias("formed_edge"),
        (pl.col("model_prob") - pl.col("closing_implied")).alias("closing_edge"),
        # CLV = how much the close beat this entry point's price.
        (pl.col("closing_implied") - pl.col("opening_implied")).alias("clv"),
        (pl.col("closing_implied") - pl.col("formed_implied")).alias("clv_formed"),
    )

    # Settle outcomes — `won` is per-(match_uid, player_id) in matches.parquet
    matches = pl.read_parquet(_frozen_matches_path()).select(
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
            pl.when(pl.col("formed_odds").is_null())
            .then(None)
            .when(pl.col("won") == 1)
            .then(pl.col("formed_odds") - 1.0)
            .when(pl.col("won") == 0)
            .then(pl.lit(-1.0))
            .otherwise(None)
        ).alias("pnl_formed"),
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


def _apply_present_filters(
    bets: pl.DataFrame, filters: dict[str, Any] | None
) -> pl.DataFrame:
    """Apply ``filters`` to per-side bet rows over the columns present on ``bets``.

    Filter keys whose column isn't carried onto the bet rows (e.g. ``draw_type``,
    a match-level attribute already enforced at predict) are skipped rather than
    raising. Match-level cols that are present (circuit/surface/…) re-filter as a
    no-op since they were already enforced upstream; anti-symmetric diff-feature
    cols (player_age_diff) bite here because each bet row holds its own
    orientation's value, so the excluded side is dropped and not re-added.
    """
    if not filters:
        return bets
    present = {col: spec for col, spec in filters.items() if col in bets.columns}
    if not present:
        return bets
    return apply_filters(bets, present)


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


def _resolve_betting_period(
    start: date | None,
    end: date | None,
) -> tuple[date, date]:
    """Resolve the [start, end] period the backtest scores bets over.

    Starts at BETTING_START_FLOOR (2026-01-01) — the earliest trustworthy odds —
    and runs to today-7 (so the tail isn't dominated by not-yet-settled matches).
    ``--start``/``--end`` override each bound, but an explicit start earlier than
    the floor is clamped: the backtest never uses pre-2026 prices.
    """
    default_end = date.today() - timedelta(days=7)
    bt_start = start or BETTING_START_FLOOR
    if bt_start < BETTING_START_FLOOR:
        logger.warning(
            "Betting start %s precedes the %s floor (pre-2026 odds are "
            "unreliable) — clamping to floor",
            bt_start, BETTING_START_FLOOR,
        )
        bt_start = BETTING_START_FLOOR
    bt_end = end or default_end
    if bt_start > bt_end:
        raise ValueError(f"Betting period start {bt_start} after end {bt_end}")
    return bt_start, bt_end


def _schedule_test_windows(
    lead_cfg: ExperimentConfig, schedule_end: date
) -> list[tuple[date, date, date]]:
    """Return (train_cutoff, test_start, test_end_inclusive) per the config's
    validation schedule, anchored at date_range.start and stepped by test_months,
    up to schedule_end.

    The geometry matches DateSlidingWindowSplitter / DateExpandingWindowSplitter
    (anchor floored to the 1st of the start month; first test window opens
    train_months / initial_train_months after it) so the backtest reads the
    config the same way FS and the model runner do — the one difference is that a
    final partial test window is kept here rather than dropped, since that
    incomplete window is exactly the live period the backtest evaluates.
    ``train_cutoff`` is test_start - 1 day; capping every model there and letting
    each model's own validation block drive its deploy window keeps train/test
    disjoint without duplicating the deploy math.
    """
    val = lead_cfg.validation
    if val is None:
        raise ValueError("Backtest requires a validation block to derive folds")
    if not val.test_months:
        raise ValueError("Backtest requires validation.test_months")

    start = _to_date(lead_cfg.data.date_range.start)
    anchor = date(start.year, start.month, 1)
    if val.type == "date_sliding":
        if not val.train_months:
            raise ValueError("date_sliding requires train_months")
        first_test_start = anchor + relativedelta(months=val.train_months)
    elif val.type == "date_expanding":
        if not val.initial_train_months:
            raise ValueError("date_expanding requires initial_train_months")
        first_test_start = anchor + relativedelta(months=val.initial_train_months)
    else:
        raise NotImplementedError(
            f"Backtest schedule not implemented for validation type "
            f"'{val.type}' (supported: date_sliding, date_expanding)"
        )

    windows: list[tuple[date, date, date]] = []
    i = 0
    while True:
        test_start = first_test_start + relativedelta(months=val.test_months * i)
        if test_start > schedule_end:
            break
        test_end_incl = (
            test_start + relativedelta(months=val.test_months) - timedelta(days=1)
        )
        train_cutoff = test_start - timedelta(days=1)
        windows.append((train_cutoff, test_start, test_end_incl))
        i += 1
    return windows


def run_backtest(
    config_path: Path | str,
    *,
    retrain: bool = False,
    start: date | None = None,
    end: date | None = None,
    voters_override_path: Path | str | None = None,
) -> Path:
    """Run the lead backtest end-to-end and return the CSV path.

    Reads the lead config's ``validation`` block the same way FS and the model
    runner do: it derives the sliding/expanding fold schedule anchored at
    date_range.start, then materializes only the fold(s) whose test window
    overlaps the betting period (where odds exist). Each such fold trains a model
    capped at the fold's test-window start — so it never sees the test window —
    and predicts that fold's slice of the betting period. Predictions across
    folds are concatenated, joined to odds, and settled. Today that's a single
    fold; the loop covers the multi-fold case if the odds span ever grows.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Lead config not found: {config_path}")

    voters = _resolve_voters(voters_override_path)
    artifact_root = artifact_dir(config_path)
    artifact_root.mkdir(parents=True, exist_ok=True)
    (artifact_root / "voters").mkdir(exist_ok=True)

    lead_cfg = ExperimentConfig.from_file(str(config_path))

    bt_start, bt_end = _resolve_betting_period(start, end)
    logger.info("Betting period: %s to %s", bt_start, bt_end)

    # Select schedule folds whose test window overlaps the betting period. The
    # 2026 floor keeps pre-2026 folds out (they also lack the history to train
    # the config's CV) — their test window clips to empty and is skipped.
    folds: list[tuple[date, date, date, date]] = []
    for train_cutoff, test_start, test_end_incl in _schedule_test_windows(
        lead_cfg, bt_end
    ):
        pred_start = max(test_start, bt_start)
        pred_end = min(test_end_incl, bt_end)
        if pred_start <= pred_end:
            folds.append((train_cutoff, test_start, pred_start, pred_end))
    if not folds:
        raise RuntimeError(
            f"No schedule fold overlaps betting period {bt_start}..{bt_end}; "
            f"check the config's validation block and date_range.start"
        )
    logger.info(
        "Materializing %d fold(s): %s",
        len(folds),
        "; ".join(
            f"train<={tc} predict {ps}..{pe}" for tc, _ts, ps, pe in folds
        ),
    )

    fold_predictions: list[pl.DataFrame] = []
    fold_feature_frames: list[pl.DataFrame] = []
    last_lead_stem: str | None = None
    for train_cutoff, test_start, pred_start, pred_end in folds:
        fold_tag = test_start.isoformat()
        temp_config = _build_temp_config(
            config_path, voters, artifact_root,
            train_cutoff=train_cutoff, fold_tag=fold_tag,
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8",
        ) as f:
            yaml.safe_dump(temp_config, f)
            temp_yaml_path = Path(f.name)
        try:
            predictor = ProductionPredictor(
                production_config_path=temp_yaml_path,
                cache_dir=BACKTEST_CACHE_DIR,
                matches_path=_frozen_matches_path(),
            )

            lead_artifact = Path(temp_config["winner"]["active"]["artifact"])
            if retrain or not lead_artifact.exists():
                logger.info(
                    "Training lead %s for fold %s (train <= %s) ...",
                    config_path.stem, fold_tag, train_cutoff,
                )
                predictor.train()
            else:
                logger.info("Using cached lead artifact %s", lead_artifact)

            for voter in temp_config["winner"]["voters"]:
                voter_artifact = Path(voter["artifact"])
                if retrain or not voter_artifact.exists():
                    logger.info(
                        "Training voter %s for fold %s ...", voter["name"], fold_tag
                    )
                    predictor._train_single(voter)
                else:
                    logger.info("Using cached voter artifact %s", voter_artifact)

            preds = predictor.predict(
                include_settled=True, date_window=(pred_start, pred_end),
                include_features=True,
            )
            if preds is None or len(preds) == 0:
                logger.warning(
                    "Fold %s: no matches in %s..%s", fold_tag, pred_start, pred_end
                )
                continue
            # Grab the per-side feature frame stashed by predict() for this fold
            # before predict_voters / the next fold runs.
            fold_feat = getattr(predictor, "_feature_frame", None)
            if fold_feat is not None:
                fold_feature_frames.append(fold_feat)
            preds = predictor.predict_voters(
                tournament_keys=None, predictions=preds, include_settled=True,
            )
            fold_predictions.append(preds)
            logger.info("Fold %s predicted %d match rows", fold_tag, len(preds))
        finally:
            temp_yaml_path.unlink(missing_ok=True)

    if not fold_predictions:
        raise RuntimeError(
            f"No matches predicted across betting period {bt_start}..{bt_end}"
        )
    predictions = (
        fold_predictions[0]
        if len(fold_predictions) == 1
        else pl.concat(fold_predictions, how="diagonal_relaxed")
    )
    feature_frame = (
        pl.concat(fold_feature_frames, how="diagonal_relaxed").unique(
            subset=["match_uid", "player_id"], keep="first"
        )
        if fold_feature_frames
        else None
    )

    # Preserve the cal_tiers sidecar fallback (_load_tier_lookup looks for a
    # fixed `lead_cal_tiers.json`): expose the most-recent fold's fold-tagged
    # sidecar under that name. The fp-scoped diagnostics.json remains preferred.
    if last_lead_stem is not None:
        fold_sidecar = artifact_root / f"{last_lead_stem}_cal_tiers.json"
        if fold_sidecar.exists():
            shutil.copy2(fold_sidecar, artifact_root / "lead_cal_tiers.json")

    # Build per-side bet rows + odds + outcomes
    bets = _build_bet_rows(predictions, bt_start, bt_end)
    bets, run_id = _attach_cal_tiers(
        bets, config_path.stem, lead_cfg=lead_cfg, config_path=config_path
    )

    # Carry the exact feature values the model saw into the CSV. Joined on
    # (match_uid, player_id) so each bet row gets its OWN side's values in that
    # side's orientation — no synthesized/negated numbers, correct for any
    # feature type (diff, raw, interaction).
    if feature_frame is not None:
        feat_cols = [
            c for c in feature_frame.columns if c not in ("match_uid", "player_id")
        ]
        bets = bets.join(feature_frame, on=["match_uid", "player_id"], how="left")
        logger.info("Joined %d feature column(s) onto bet rows", len(feat_cols))

    # data.filters must also hold on the per-side bet output. It is applied at
    # predict on the pre-expansion frame (predictor), but for an anti-symmetric
    # diff feature (e.g. player_age_diff) the two-sided expansion in
    # _build_bet_rows re-adds the filtered-out orientation, so the filter has to
    # bite again here where each bet row carries its own side's value.
    if lead_cfg.data.filters:
        n_before = len(bets)
        bets = _apply_present_filters(bets, lead_cfg.data.filters)
        logger.info(
            "data.filters applied to bet rows: %d/%d kept (%.1f%%)",
            len(bets), n_before,
            100.0 * len(bets) / n_before if n_before else 0.0,
        )
        if len(bets) == 0:
            raise RuntimeError(
                "data.filters matched 0 bet rows — check the filter spec "
                "and that its columns are present on the bet rows."
            )

    # eval_filters restricts the bet set the same way it restricts the diagnostics
    # test fold (runner.py), so section D describes the same population as section
    # B. No-op when unset (prod). Filter columns are present on `bets` as raw meta
    # (circuit/surface/round/best_of) or joined above (model/eval features).
    if lead_cfg.data.eval_filters:
        n_before = len(bets)
        bets = apply_filters(bets, lead_cfg.data.eval_filters)
        logger.info(
            "eval_filters active: backtest restricted to %d/%d bet rows (%.1f%%)",
            len(bets), n_before,
            100.0 * len(bets) / n_before if n_before else 0.0,
        )
        if len(bets) == 0:
            raise RuntimeError(
                "eval_filters matched 0 bet rows — check the filter spec and that "
                "its columns exist (raw meta, or model/eval features)."
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
        window=(bt_start, bt_end),
        csv_path=out_path,
        diag_run_id=run_id,
    )
    print(summary_text)
    (fp_dir / "backtest_summary.txt").write_text(summary_text, encoding="utf-8")

    return out_path


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
_OPEN_W = 52
_FORMED_W = 33
_CLOSE_W = 33


def _render_open_side(stats: dict) -> str:
    """Render the OPEN cell: n / hit% / ROI / units / CLV+% / avg CLV.

    Stats come from the opening_edge>=0 subset — the bets actually placed at
    open. n_open (priced subset size) is the row count contributing to ROI.
    """
    n_p = stats.get("n_open", 0)
    if n_p == 0:
        return f"{0:>6}  {'-':>6}  {'-':>7}  {'-':>8}  {'-':>6}  {'-':>9}"
    hit = stats.get("hit") or 0.0
    pnl = stats.get("pnl_open") or 0.0
    roi = stats.get("roi_open") or 0.0
    clv_win = stats.get("clv_pos") or 0.0
    avg_clv = stats.get("avg_clv") or 0.0
    return (
        f"{n_p:>6}  {hit:>6.1%}  {roi:>+7.2%}  {pnl:>+7.1f}u  "
        f"{clv_win:>6.1%}  {avg_clv * 100:>+7.2f}pp"
    )


def _render_point_side(stats: dict, price: str) -> str:
    """Render a FORMED or CLOSE cell: n / hit% / ROI / units.

    `price` is "formed" or "close". CLV is omitted on both — formed-vs-close CLV
    is a deferred split, and close-vs-close CLV is zero by definition. ME is
    dropped everywhere: it is the same edge the bet set is already gated on.
    """
    n_p = stats.get(f"n_{price}", 0)
    if n_p == 0:
        return f"{0:>6}  {'-':>6}  {'-':>7}  {'-':>8}"
    hit = stats.get("hit") or 0.0
    pnl = stats.get(f"pnl_{price}") or 0.0
    roi = stats.get(f"roi_{price}") or 0.0
    return f"{n_p:>6}  {hit:>6.1%}  {roi:>+7.2%}  {pnl:>+7.1f}u"


def _render_formed_side(stats: dict) -> str:
    return _render_point_side(stats, "formed")


def _render_close_side(stats: dict) -> str:
    return _render_point_side(stats, "close")


def _wide_header_lines() -> tuple[str, str]:
    open_hdr = "---- OPEN ----".center(_OPEN_W)
    formed_hdr = "--- FORMED ---".center(_FORMED_W)
    close_hdr = "--- CLOSE ---".center(_CLOSE_W)
    open_cols = (
        f"{'n':>6}  {'hit%':>6}  {'ROI':>7}  {'units':>8}  "
        f"{'CLV+%':>6}  {'avg CLV':>9}"
    )
    point_cols = f"{'n':>6}  {'hit%':>6}  {'ROI':>7}  {'units':>8}"
    label_blank = " " * _LABEL_W
    top = f"  {label_blank}  {open_hdr}    {formed_hdr}    {close_hdr}"
    bottom = f"  {'label':<{_LABEL_W}}  {open_cols}    {point_cols}    {point_cols}"
    return top, bottom


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


def _wide_row3(
    label: str, open_stats: dict, formed_stats: dict, close_stats: dict
) -> str:
    """Render one wide row from three pre-computed per-point stats dicts."""
    return (
        f"  {label:<{_LABEL_W}}  "
        f"{_render_open_side(open_stats)}    "
        f"{_render_formed_side(formed_stats)}    "
        f"{_render_close_side(close_stats)}"
    )


def _edge_sign_stats(s: pl.DataFrame, col: str, yes: bool = True) -> dict:
    """slice_stats over the edge>=0 (yes) / edge<0 (no) subset of one edge
    column. Empty-slice stats when the edge column is absent (pre-formed CSV).
    """
    if col not in s.columns:
        return views.slice_stats(s.head(0))
    return views.slice_stats(_edge_sign_sub(s, col, yes))


def _edge_band_sub(
    s: pl.DataFrame, col: str, lo: float, hi: float | None
) -> pl.DataFrame:
    """Edge-band slice with the summary's `< 0%` (lo == -inf) special case;
    empty when the edge column is absent."""
    if col not in s.columns:
        return s.head(0)
    if lo == float("-inf"):
        return s.filter(pl.col(col) < hi)
    return views.filter_band(s, col, lo, hi)


def _wide_row_pt(label: str, s: pl.DataFrame) -> str:
    """Per-point row: each cell is its OWN bet set — open gated on
    opening_edge>=0, formed on formed_edge>=0, close on closing_edge>=0 — and
    settled at its own price. n differs per cell.
    """
    return _wide_row3(
        label,
        _edge_sign_stats(s, "opening_edge"),
        _edge_sign_stats(s, "formed_edge"),
        _edge_sign_stats(s, "closing_edge"),
    )


def _wide_row_all(label: str, s: pl.DataFrame) -> str:
    """All-edges row: every cell scores all priced rows at that point, no edge
    gate. Used for the opponent (non-picks) side and `all edges` scopes.
    """
    stats = views.slice_stats(s)
    return _wide_row3(label, stats, stats, stats)


def _kind_subsection(
    sub: pl.DataFrame, label_word: str, diag_run_id: str | None
) -> list[str]:
    """Emit HEADLINE / EDGE × TIER / BY EDGE BAND / BY ROUND / BY MONTH for one
    kind (picks or non-picks). All numeric aggregation goes through
    `backtest_views.slice_stats`; this function owns only layout.

    Each bet point (open / formed / close) is its own strategy: for picks, a
    cell is gated on that point's own edge>=0 and settled at its own price, so
    the three cells are independent bet sets with their own n. Non-picks (the
    opponent side) are shown all-edges — an edge>=0 gate on the wrong side is
    almost always empty.
    """
    top, bottom = _wide_header_lines()
    n = len(sub)
    lines: list[str] = [f"--- {label_word.upper()}  ({n:,}) ---"]

    diag_suffix = f"  (tiers from diagnostics {diag_run_id})" if diag_run_id else ""
    tiers_present = [t for t in _SUMMARY_TIER_ORDER if len(_tier_sub(sub, t)) > 0]
    if len(_tier_sub(sub, "unknown")) > 0:
        tiers_present.append("unknown")

    is_picks = label_word == "picks"

    lines.append("")
    lines.append("HEADLINE")
    lines.append(top)
    lines.append(bottom)
    if is_picks:
        lines.append(_wide_row_all("all picks", sub))
        lines.append(_wide_row_pt("edge>=0", sub))
    else:
        lines.append(_wide_row_all(f"all {label_word}", sub))

    lines.append("")
    lines.append(f"EDGE × TIER{diag_suffix}")
    lines.append(top)
    lines.append(bottom)
    for yes_flag, edge_label in [(True, "yes"), (False, "no")]:
        for tier in tiers_present:
            t_sub = _tier_sub(sub, tier)
            lines.append(_wide_row3(
                f"{edge_label} / {tier}",
                _edge_sign_stats(t_sub, "opening_edge", yes_flag),
                _edge_sign_stats(t_sub, "formed_edge", yes_flag),
                _edge_sign_stats(t_sub, "closing_edge", yes_flag),
            ))

    # Ensemble consensus breakdowns (only when n_agree is present —
    # backtest of single-model configs won't have this column).
    if "n_agree" in sub.columns and len(sub["n_agree"].drop_nulls()) > 0:
        n_subs_max = int(sub["n_agree"].drop_nulls().max())
        lines.append("")
        lines.append("EDGE × CONSENSUS")
        lines.append(top)
        lines.append(bottom)
        for yes_flag, edge_label in [(True, "yes"), (False, "no")]:
            for n_agree in range(n_subs_max, 0, -1):
                n_disagree = n_subs_max - n_agree
                c_sub = sub.filter(pl.col("n_agree") == n_agree)
                if len(c_sub) == 0:
                    continue
                lines.append(_wide_row3(
                    f"{edge_label} / {n_agree}-{n_disagree}",
                    _edge_sign_stats(c_sub, "opening_edge", yes_flag),
                    _edge_sign_stats(c_sub, "formed_edge", yes_flag),
                    _edge_sign_stats(c_sub, "closing_edge", yes_flag),
                ))

        lines.append("")
        lines.append("BY CONSENSUS  (all edges)")
        lines.append(top)
        lines.append(bottom)
        for n_agree in range(n_subs_max, 0, -1):
            n_disagree = n_subs_max - n_agree
            c_sub = sub.filter(pl.col("n_agree") == n_agree)
            if len(c_sub) == 0:
                continue
            lines.append(_wide_row_all(f"{n_agree}-{n_disagree}", c_sub))
        lines.append(_wide_row_all("ALL", sub))

    lines.append("")
    lines.append("BY EDGE BAND  (all tiers)")
    lines.append(top)
    lines.append(bottom)
    for band_label, lo, hi in _SUMMARY_EDGE_BANDS:
        lines.append(_wide_row3(
            band_label,
            views.slice_stats(_edge_band_sub(sub, "opening_edge", lo, hi)),
            views.slice_stats(_edge_band_sub(sub, "formed_edge", lo, hi)),
            views.slice_stats(_edge_band_sub(sub, "closing_edge", lo, hi)),
        ))

    # Round slices. Picks get a per-point placed scope (each cell on its own
    # edge>=0) plus an all-edges scope, so the gap shows how the edge filter
    # performs per round. Non-picks get all-edges only — an edge>=0 gate on the
    # opponent side is almost always empty.
    round_scopes: list[tuple[str, bool]] = []
    if is_picks:
        round_scopes.append(("  (edge>=0)", True))
    round_scopes.append(("  (all edges)", False))
    rslices = views.round_slices(sub)
    for scope_note, per_point in round_scopes:
        if not rslices:
            continue
        lines.append("")
        lines.append(f"BY ROUND{scope_note}")
        lines.append(top)
        lines.append(bottom)
        for rnd, rsub in rslices:
            lines.append(
                _wide_row_pt(rnd, rsub) if per_point else _wide_row_all(rnd, rsub)
            )

    # Monthly slices. Picks: each point placed on its own edge>=0, with a
    # per-point cumulative trailer on actually-placed bets. Non-picks: all-edges
    # (an edge gate on the opponent side doesn't make sense).
    mslices = views.month_slices(sub)
    if mslices:
        lines.append("")
        lines.append("BY MONTH" + ("  (edge>=0)" if is_picks else ""))
        lines.append(top)
        lines.append(bottom)
        cum_open = cum_formed = cum_close = 0.0
        for month, msub in mslices:
            if is_picks:
                o = _edge_sign_stats(msub, "opening_edge")
                f = _edge_sign_stats(msub, "formed_edge")
                c = _edge_sign_stats(msub, "closing_edge")
            else:
                o = f = c = views.slice_stats(msub)
            lines.append(_wide_row3(month, o, f, c))
            cum_open += float(o.get("pnl_open") or 0.0)
            cum_formed += float(f.get("pnl_formed") or 0.0)
            cum_close += float(c.get("pnl_close") or 0.0)
        lines.append(
            f"  {'cumulative':<{_LABEL_W}}  open: {cum_open:>+7.1f}u    "
            f"formed: {cum_formed:>+7.1f}u    close: {cum_close:>+7.1f}u"
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
